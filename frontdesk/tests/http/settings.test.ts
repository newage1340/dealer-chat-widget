import { beforeEach, describe, expect, it } from "vitest";

import type { Config } from "../../src/config.ts";
import { TEST_CONFIG } from "../support/config.ts";
import { Address } from "../../src/core/index.ts";
import { FakeLlm, LoggingEmail, LoggingMessaging } from "../../src/providers/index.ts";
import { buildApp, type App } from "../../src/server.ts";
import { openStore } from "../../src/storage/index.ts";
import { CUSTOMER, PLUMBER_NUMBER, plumber, tenant } from "../support/fixtures.ts";

const CONFIG: Config = { ...TEST_CONFIG };

let app: App;
let cookie: string;

async function post(path: string, body: Record<string, string | string[]>) {
    const form = new URLSearchParams();
    for (const [k, v] of Object.entries(body)) {
        for (const value of Array.isArray(v) ? v : [v]) {
            form.append(k, value);
        }
    }
    return await app.http.request(path, { method: "POST", body: form, headers: { Cookie: cookie } });
}

async function get(path: string) {
    return await app.http.request(path, { headers: { Cookie: cookie } });
}

beforeEach(async () => {
    const store = await openStore("", null);
    await store.tenants.save(plumber());
    await store.tenants.save(tenant({ id: "salon", slug: "salon", domain: "hair_salon" }));
    app = await buildApp(CONFIG, { store, llm: new FakeLlm(() => "ok"), messaging: new LoggingMessaging(), email: new LoggingEmail(), transcriber: null, fetchRecording: null });
    const login = await app.http.request("/settings/login", { method: "POST", body: new URLSearchParams({ token: "admin" }) });
    cookie = (login.headers.get("set-cookie") ?? "").split(";")[0]!;
});

describe("settings pages", () => {
    it("needs the admin token", async () => {
        expect((await app.http.request("/settings")).headers.get("location")).toBe("/settings/login");
        const wrong = await app.http.request("/settings/login", { method: "POST", body: new URLSearchParams({ token: "nope" }) });
        expect(wrong.status).toBe(401);
    });

    it("renders bare HTML with no styling", async () => {
        const html = await (await get("/settings/demo-plumber")).text();
        expect(html).toContain("<h2>Languages</h2>");
        expect(html).not.toMatch(/style=|class=|<style|<script/);
    });

    it("limits the offered languages, always keeping the main one", async () => {
        const res = await post("/settings/demo-plumber/languages", { defaultLanguage: "en", staffLanguage: "en", languages: ["es"] });
        expect(res.status).toBe(303);
        const saved = await app.store.tenants.bySlug("demo-plumber");
        expect(saved?.settings.languages).toEqual(["en", "es"]);
    });

    it("resets a customer's stuck language", async () => {
        const t = (await app.store.tenants.bySlug("demo-plumber"))!;
        const { contact } = await app.store.conversations.resolve(t.id, Address.phone(CUSTOMER)!, new Date());
        await app.store.conversations.updateContact(contact.id, { preferredLanguage: "fr" });
        await post("/settings/demo-plumber/languages/reset", { phone: CUSTOMER });
        expect((await app.store.conversations.contact(contact.id))?.preferredLanguage).toBeNull();
    });

    it("saves tone and privacy, and refuses invalid input without saving", async () => {
        await post("/settings/demo-plumber/assistant", { professionalism: "formal", honestAboutAi: "yes", maxSessionMinutes: "20" });
        await post("/settings/demo-plumber/privacy", { redact: ["card", "birth_date"], sensitive: "on" });
        const saved = (await app.store.tenants.bySlug("demo-plumber"))!;
        expect(saved.settings.style).toEqual({ professionalism: "formal", honestAboutAi: true });
        expect(saved.settings.privacy).toEqual({ redact: ["card", "birth_date"], sensitive: true, crisisSupport: false });
        expect(saved.settings.limits.maxSessionSeconds).toBe(20 * 60);

        const tooLong = await post("/settings/demo-plumber/assistant", { maxSessionMinutes: "500", silenceSeconds: "1" });
        expect(tooLong.status).toBe(400);
        const page = await tooLong.text();
        expect(page).toContain("at most 45");
        expect(page).toContain("at least 3");
        await post("/settings/demo-plumber/assistant", { silenceSeconds: "4", stillTherePrompts: "0", hiddenCallerId: "voicemail" });
        expect((await app.store.tenants.bySlug("demo-plumber"))?.settings.calls).toEqual({ silenceSeconds: 4, stillTherePrompts: 0, hiddenCallerId: "voicemail" });

        const bad = await post("/settings/demo-plumber/business", { name: "X", domain: "plumbing", timezone: "Mars/Olympus", hoursText: "" });
        expect(bad.status).toBe(400);
        expect(await bad.text()).toContain("unknown timezone");
        expect((await app.store.tenants.bySlug("demo-plumber"))?.name).not.toBe("X");
    });

    it("replaces the catalog from pasted CSV and from a sample dataset", async () => {
        const res = await post("/settings/demo-plumber/catalog/upload", { rows: "code,name,category,price_range\nA1,Toilet repair,Fixture,$150\nA2,Drain cleaning,Drain,$129" });
        expect(res.status).toBe(303);
        const t = (await app.store.tenants.bySlug("demo-plumber"))!;
        expect((await app.store.tenants.catalog(t.id)).map(i => i.fields["name"])).toEqual(["Toilet repair", "Drain cleaning"]);

        await post("/settings/demo-plumber/catalog", { kind: "dataset", dataset: "plumbing.json", refreshMinutes: "60", fieldMap: "" });
        expect((await app.store.tenants.catalog(t.id)).length).toBe(13);
        expect((await app.store.tenants.syncStatus(t.id))?.error).toBeNull();
    });

    it("moves a phone number from one business to another", async () => {
        await post("/settings/salon/numbers", { voice: PLUMBER_NUMBER, sms: PLUMBER_NUMBER, transferTo: "" });
        const plumberNow = (await app.store.tenants.bySlug("demo-plumber"))!;
        expect(plumberNow.endpoints.voice ?? []).toEqual([]);
        expect((await app.store.tenants.byEndpoint("voice", Address.phone(PLUMBER_NUMBER)!))?.slug).toBe("salon");
    });

    it("creates a new business", async () => {
        const res = await post("/settings/new", { name: "Joe's Pizza", slug: "joes", domain: "chinese_restaurant" });
        expect(res.status).toBe(303);
        expect((await app.store.tenants.bySlug("joes"))?.name).toBe("Joe's Pizza");
    });
});

describe("quality page", () => {
    it("renders bare tables", async () => {
        const html = await (await get("/settings/quality?days=30")).text();
        expect(html).toContain("<h2>Eval pass rates</h2>");
        expect(html).toContain("control");
        expect(html).not.toMatch(/style=|class=|<script/);
    });
});

describe("sign-in cookie", () => {
    it("works on http://localhost even when the public URL is https, and is Secure behind an https tunnel", async () => {
        const store = await openStore("", null);
        await store.tenants.save(plumber());
        const tunnelled = await buildApp({ ...CONFIG, publicBaseUrl: "https://example.trycloudflare.com" }, { store, llm: new FakeLlm(() => "ok"), messaging: new LoggingMessaging(), email: new LoggingEmail(), transcriber: null, fetchRecording: null });
        const local = await tunnelled.http.request("http://localhost/settings/login", { method: "POST", body: new URLSearchParams({ token: "admin" }) });
        const localCookie = local.headers.get("set-cookie") ?? "";
        expect(localCookie).not.toMatch(/Secure/i);
        const page = await tunnelled.http.request("http://localhost/settings", { headers: { Cookie: localCookie.split(";")[0]! } });
        expect(page.status).toBe(200);

        const viaTunnel = await tunnelled.http.request("http://localhost/settings/login", { method: "POST", body: new URLSearchParams({ token: "admin" }), headers: { "X-Forwarded-Proto": "https" } });
        expect(viaTunnel.headers.get("set-cookie")).toMatch(/Secure/i);
    });
});
