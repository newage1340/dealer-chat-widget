import type { Hono } from "hono";
import { beforeEach, describe, expect, it } from "vitest";

import type { Config } from "../../src/config.ts";
import { onboardingRoutes } from "../../src/http/onboarding.ts";
import { Hours } from "../../src/prompts/hours.ts";
import { FakeLlm, LoggingEmail, LoggingMessaging } from "../../src/providers/index.ts";
import { buildApp, type App } from "../../src/server.ts";
import { openStore } from "../../src/storage/index.ts";
import { plumber } from "../support/fixtures.ts";

const CONFIG: Config = {
    port: 0, publicBaseUrl: "", databaseUrl: "", embeddedDbDir: null, adminToken: "admin", uploadToken: "upload",
    llm: { primary: "openai", openaiApiKey: "", openaiModels: { small: "m", fast: "m", capable: "m" }, localBaseUrl: "", localApiKey: "", localModels: { small: "", fast: "", capable: "" }, localMaxConcurrency: 1 },
    pricesFile: new URL("../../prices.json", import.meta.url).pathname,
    twilioAccountSid: "", twilioAuthToken: "", twilioMessagingSid: "", twilioDefaultFrom: "", validateTwilio: false,
    resendApiKey: "", emailFrom: "test@example.com", deepgramApiKey: "", termsUrl: "https://t", privacyUrl: "https://p",
    defaultSlug: "", dashboardUrl: "", sessionIdleMinutes: 30, runWorker: false, onboardingCode: "",
};

let app: App;
let routes: Hono;

function mount(config: Config): Hono {
    return onboardingRoutes({ config, services: app.services, engine: app.engine });
}

async function post(body: Record<string, string | string[]>, on: Hono = routes) {
    const form = new URLSearchParams();
    for (const [k, v] of Object.entries(body)) {
        for (const value of Array.isArray(v) ? v : [v]) {
            form.append(k, value);
        }
    }
    return await on.request("/onboarding", { method: "POST", body: form });
}

function valid(): Record<string, string | string[]> {
    return {
        "type":                  "plumbing",
        "name":                  "Bob's Pipes & Drains",
        "phone":                 "(317) 555-0199",
        "address":               "1 Main St",
        "website":               "https://bobs.example",
        "timezone":              "America/Chicago",
        "email":                 "owner@bobs.example",
        "opens.Monday":          "08:00",  "closes.Monday":    "17:30",
        "opens.Tuesday":         "08:00",  "closes.Tuesday":   "17:30",
        "opens.Wednesday":       "08:00",  "closes.Wednesday": "17:30",
        "opens.Thursday":        "08:00",  "closes.Thursday":  "17:30",
        "opens.Friday":          "08:00",  "closes.Friday":    "12:00",
        "appointment.Saturday":  "on",
        "closed.Sunday":         "on",
        "profile.service_area":  "Indianapolis and suburbs",
        "professionalism":       "friendly",
        "languages":             ["es"],
        "defaultLanguage":       "en",
        "catalogRows":           "code,name,category,price_range\nDRN-1,Drain cleaning,Drain,$150-$250\nWH-1,Water heater install,Water heater,$1200+\n",
        "notes":                 "Cash and cards. No septic work.",
    };
}

beforeEach(async () => {
    const store = await openStore("", null);
    await store.tenants.save(plumber());
    app = await buildApp(CONFIG, { store, llm: new FakeLlm(() => "ok"), messaging: new LoggingMessaging(), email: new LoggingEmail(), transcriber: null, fetchRecording: null });
    routes = mount(CONFIG);
});

describe("onboarding pages", () => {
    it("offers every business type", async () => {
        const res = await routes.request("/onboarding");
        expect(res.status).toBe(200);
        const html = await res.text();
        expect(html).toContain("action=\"/onboarding/form\"");
        expect(html).toContain("<option value=\"plumbing\"");
        expect(html).toContain("<option value=\"hair_salon\"");
    });

    it("asks the business type's own questions", async () => {
        const html = await (await routes.request("/onboarding/form?type=plumbing")).text();
        expect(html).toContain("Service area (required)");
        expect(html).toContain("What towns, zip codes, or radius do you cover?");
        expect(html).toContain("name=\"profile.service_area\"");
        expect(html).toContain("Your services");
        expect(html).not.toContain("Invite code");
        expect((await routes.request("/onboarding/form?type=nope")).status).toBe(400);
    });

    it("renders bare HTML with no styling", async () => {
        for (const path of ["/onboarding", "/onboarding/form?type=plumbing"]) {
            expect(await (await routes.request(path)).text()).not.toMatch(/style=|class=|<style|<script/);
        }
    });

    it("turns a submission into a tenant with hours, languages, staff email and catalog", async () => {
        const res = await post(valid());
        expect(res.status).toBe(303);
        expect(res.headers.get("location")).toBe("/onboarding/done?slug=bob-s-pipes-and-drains");

        const saved = (await app.store.tenants.bySlug("bob-s-pipes-and-drains"))!;
        expect(saved.domain).toBe("plumbing");
        expect(saved.timezone).toBe("America/Chicago");
        expect(saved.endpoints).toEqual({ sms: [], voice: [] });
        expect(saved.hoursText).toBe("Monday: 8am to 5:30pm, Tuesday: 8am to 5:30pm, Wednesday: 8am to 5:30pm, "
            + "Thursday: 8am to 5:30pm, Friday: 8am to 12pm, Saturday: by appointment only, Sunday: closed");
        const hours = Hours.parse(saved.hoursText);
        expect(Object.keys(hours)).toHaveLength(7);
        expect(hours.Monday).toMatchObject({ opens: 8 * 60, closes: 17 * 60 + 30 });
        expect(hours.Saturday?.byAppointment).toBe(true);
        expect(Hours.isClosed(hours.Sunday!)).toBe(true);
        expect(saved.settings.languages).toEqual(["en", "es"]);
        expect(saved.settings.style.professionalism).toBe("friendly");
        expect(saved.staffContacts).toEqual([{ kind: "email", value: "owner@bobs.example" }]);
        expect(saved.profile).toEqual({ service_area: "Indianapolis and suburbs", policies: "Cash and cards. No septic work." });
        expect(saved.settings.catalog.source).toEqual({ kind: "manual" });
        expect(await app.store.tenants.catalog(saved.id)).toHaveLength(2);

        const done = await (await routes.request(res.headers.get("location")!)).text();
        expect(done).toContain("Bob&#39;s Pipes &amp; Drains");
        expect(done).toContain("phone number");
        expect(done).not.toContain("owner@bobs.example");
        expect(done).not.toMatch(/style=|class=|<style|<script/);
    });

    it("gives a taken name the next free web address", async () => {
        await post(valid());
        const res = await post(valid());
        expect(res.headers.get("location")).toBe("/onboarding/done?slug=bob-s-pipes-and-drains-2");
    });

    it("re-renders missing answers with problems, keeping what was entered", async () => {
        const res = await post({ ...valid(), "email": "", "profile.service_area": "", "opens.Tuesday": "", "address": "<b>9 Elm</b>" });
        expect(res.status).toBe(400);
        const html = await res.text();
        expect(html).toContain("Email for summaries and alerts is required.");
        expect(html).toContain("Service area is required.");
        expect(html).toContain("Tuesday: enter when you open and close");
        expect(html).toContain("value=\"Bob&#39;s Pipes &amp; Drains\"");
        expect(html).toContain("value=\"&lt;b&gt;9 Elm&lt;/b&gt;\"");
        expect(html).toContain("Cash and cards. No septic work.");
        expect(html).toMatch(/name="closed.Sunday" value="on" checked/);
        expect(await app.store.tenants.all()).toHaveLength(1);
    });

    it("reports tenant validation problems, such as a bad phone number", async () => {
        const res = await post({ ...valid(), phone: "call me" });
        expect(res.status).toBe(400);
        expect(await res.text()).toContain("Main phone: not a phone number");
    });

    it("never lets the form set numbers, plan or privacy", async () => {
        await post({ ...valid(), "endpoints.voice": "+13175550100", "plan.extraLines": "5", "redact": "card", "sensitive": "on" });
        const saved = (await app.store.tenants.bySlug("bob-s-pipes-and-drains"))!;
        expect(saved.endpoints).toEqual({ sms: [], voice: [] });
        expect(saved.plan.subscription.extraLines).toBe(0);
        expect(saved.settings.privacy.sensitive).toBe(false);
    });

    it("keeps the onboarding when the pasted list can't be read", async () => {
        const res = await post({ ...valid(), catalogRows: "[not json" });
        expect(res.status).toBe(303);
        const done = await (await routes.request(res.headers.get("location")!)).text();
        expect(done).toContain("we couldn't read your list (that isn&#39;t valid JSON)");
    });

    it("requires the invite code when one is configured", async () => {
        const gated = mount({ ...CONFIG, onboardingCode: "letmein" });
        expect(await (await gated.request("/onboarding/form?type=plumbing")).text()).toContain("Invite code (required)");
        const wrong = await post({ ...valid(), inviteCode: "nope" }, gated);
        expect(wrong.status).toBe(400);
        expect(await wrong.text()).toContain("The invite code isn&#39;t right.");
        const right = await post({ ...valid(), inviteCode: "letmein" }, gated);
        expect(right.status).toBe(303);
    });
});
