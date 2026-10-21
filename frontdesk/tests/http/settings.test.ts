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

describe("business owner logins", () => {
    async function ownerCookie(email: string, password: string) {
        const res = await app.http.request("/settings/login", { method: "POST", body: new URLSearchParams({ email, password }) });
        return { status: res.status, location: res.headers.get("location"), cookie: (res.headers.get("set-cookie") ?? "").split(";")[0]! };
    }
    async function as(owner: string, path: string, body?: Record<string, string>) {
        return await app.http.request(path, body === undefined
            ? { headers: { Cookie: owner } }
            : { method: "POST", body: new URLSearchParams(body), headers: { Cookie: owner } });
    }

    it("lets the admin add a login that only sees its own business", async () => {
        const added = await post("/settings/demo-plumber/accounts", { email: "Owner@Plumber.example", name: "Pat", password: "pipes and drains 1" });
        expect(added.status).toBe(303);
        const login = await ownerCookie("owner@plumber.example", "pipes and drains 1");
        expect(login.status).toBe(303);

        expect((await as(login.cookie, "/settings")).headers.get("location")).toBe("/settings/demo-plumber");
        const page = await (await as(login.cookie, "/settings/demo-plumber")).text();
        expect(page).toContain("Signed in as owner@plumber.example");
        expect(page).not.toContain('name="domain"');
        expect(page).toContain("To add or change a number, contact us.");

        expect((await as(login.cookie, "/settings/salon")).status).toBe(404);
        expect((await as(login.cookie, "/settings/quality")).status).toBe(404);
        expect((await as(login.cookie, "/settings/demo-plumber/numbers", { voice: "+13175550999", sms: "", transferTo: "" })).status).toBe(404);
        expect((await as(login.cookie, "/settings/new", { name: "x", slug: "x", domain: "plumbing" })).status).toBe(404);

        await as(login.cookie, "/settings/demo-plumber/assistant", { professionalism: "casual", honestAboutAi: "", maxSessionMinutes: "20" });
        expect((await app.store.tenants.bySlug("demo-plumber"))?.settings.style.professionalism).toBe("casual");
        await as(login.cookie, "/settings/demo-plumber/business", { name: "Renamed Plumbing", domain: "therapy_practice", timezone: "America/Indiana/Indianapolis", hoursText: "" });
        const after = (await app.store.tenants.bySlug("demo-plumber"))!;
        expect(after.name).toBe("Renamed Plumbing");
        expect(after.domain).toBe("plumbing");
    });

    it("lets an owner change their own password, which signs out old sessions", async () => {
        await post("/settings/demo-plumber/accounts", { email: "pat@plumber.example", password: "first password 1" });
        const first = await ownerCookie("pat@plumber.example", "first password 1");
        const account = (await app.store.accounts.byEmail("pat@plumber.example"))!;
        const wrong = await as(first.cookie, `/settings/demo-plumber/accounts/${account.id}/password`, { current: "nope", password: "second password 2" });
        expect(wrong.status).toBe(400);
        const changed = await as(first.cookie, `/settings/demo-plumber/accounts/${account.id}/password`, { current: "first password 1", password: "second password 2" });
        expect(changed.status).toBe(303);
        expect((await as(first.cookie, "/settings/demo-plumber")).headers.get("location")).toBe("/settings/login");
        expect((await ownerCookie("pat@plumber.example", "second password 2")).status).toBe(303);
    });

    it("refuses wrong passwords and slows down guessing", async () => {
        await post("/settings/demo-plumber/accounts", { email: "sam@plumber.example", password: "the real password" });
        for (let i = 0; i < 8; i++) {
            expect((await ownerCookie("sam@plumber.example", `guess ${i}`)).status).toBe(401);
        }
        expect((await ownerCookie("sam@plumber.example", "the real password")).status).toBe(429);
    });
});
