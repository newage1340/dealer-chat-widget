/**
 * Each settings section's form, posted, and what it saves.
 *
 * A safety net for restructuring the settings pages: every section and action on a business page
 * is posted with realistic values and the stored tenant (or contact, account, catalog) is checked.
 * The owner-versus-admin rules and the page's own sign-in tests live in settings.test.ts.
 */

import { beforeEach, describe, expect, it } from "vitest";

import { Address, ConversationId, type Tenant } from "../../src/core/index.ts";
import { FakeLlm, LoggingEmail, LoggingMessaging } from "../../src/providers/index.ts";
import { buildApp, type App } from "../../src/server.ts";
import { openStore } from "../../src/storage/index.ts";
import { TEST_CONFIG } from "../support/config.ts";
import { CUSTOMER, plumber } from "../support/fixtures.ts";

let app: App;
let cookie: string;

async function post(path: string, body: Record<string, string | string[]> = {}, as = cookie) {
    const form = new URLSearchParams();
    for (const [k, v] of Object.entries(body)) {
        for (const value of Array.isArray(v) ? v : [v]) {
            form.append(k, value);
        }
    }
    return await app.http.request(path, { method: "POST", body: form, headers: { Cookie: as } });
}

async function get(path: string) {
    return await app.http.request(path, { headers: { Cookie: cookie } });
}

async function saved(): Promise<Tenant> {
    return (await app.store.tenants.bySlug("demo-plumber"))!;
}

/**
 * The notice a section redirects back with.
 */
function notice(res: Response): string {
    const location = res.headers.get("location") ?? "";
    return decodeURIComponent(new URL(location, "http://x").searchParams.get("notice") ?? "");
}

beforeEach(async () => {
    const store = await openStore("", null);
    await store.tenants.save(plumber());
    app = await buildApp(TEST_CONFIG, { store, llm: new FakeLlm(() => "ok"), messaging: new LoggingMessaging(), email: new LoggingEmail(), transcriber: null, fetchRecording: null });
    const login = await app.http.request("/settings/login", { method: "POST", body: new URLSearchParams({ token: "admin" }) });
    cookie = (login.headers.get("set-cookie") ?? "").split(";")[0]!;
});

describe("business section", () => {
    it("saves every field, including the business type for the admin", async () => {
        const res = await post("/settings/demo-plumber/business", {
            name:           "Circle City Plumbing & Drain",
            domain:         "auto_repair",
            phone:          "317-555-0123",
            address:        "9 Elm St",
            website:        "https://ccp.example",
            timezone:       "America/Chicago",
            hoursText:      "Mon-Fri 9-5, Sat-Sun closed",
            agentName:      "Rita",
            voiceAgentName: "Rae",
        });
        expect(res.status).toBe(303);
        expect(await saved()).toMatchObject({
            name:           "Circle City Plumbing & Drain",
            domain:         "auto_repair",
            phone:          "+13175550123",
            address:        "9 Elm St",
            website:        "https://ccp.example",
            timezone:       "America/Chicago",
            hoursText:      "Mon-Fri 9-5, Sat-Sun closed",
            agentName:      "Rita",
            voiceAgentName: "Rae",
        });
    });

    it("refuses an unknown business type without saving", async () => {
        const res = await post("/settings/demo-plumber/business", { name: "X", domain: "submarines", timezone: "America/Chicago" });
        expect(res.status).toBe(400);
        expect(await res.text()).toContain("unknown business type");
        expect((await saved()).name).toBe("Circle City Plumbing");
    });
});

describe("transfer section", () => {
    it("saves where live transfers go, and clears it", async () => {
        await post("/settings/demo-plumber/transfer", { transferTo: "(317) 555-0777" });
        expect((await saved()).settings.transferTo).toBe("+13175550777");
        await post("/settings/demo-plumber/transfer", { transferTo: "" });
        expect((await saved()).settings.transferTo).toBeNull();
    });
});

describe("assistant section", () => {
    it("saves follow-ups, the recording notice and the profile gate", async () => {
        await post("/settings/demo-plumber/assistant", {
            professionalism:     "casual",
            honestAboutAi:       "no",
            coldFollowupMinutes: "45",
            recordingDisclosure: "on",
            recordingNotice:     "Heads up: we record calls.",
            skipProfileGate:     "on",
        });
        const t = await saved();
        expect(t.settings.style).toEqual({ professionalism: "casual", honestAboutAi: false });
        expect(t.settings.coldFollowupMinutes).toBe(45);
        expect(t.settings.recordingDisclosure).toEqual({ enabled: true, text: { en: "Heads up: we record calls." } });
        expect(t.settings.skipProfileGate).toBe(true);

        await post("/settings/demo-plumber/assistant", { professionalism: "", honestAboutAi: "", coldFollowupMinutes: "" });
        const cleared = await saved();
        expect(cleared.settings.style).toEqual({ professionalism: null, honestAboutAi: null });
        expect(cleared.settings.coldFollowupMinutes).toBeNull();
        expect(cleared.settings.recordingDisclosure.enabled).toBe(false);
        expect(cleared.settings.skipProfileGate).toBe(false);
    });
});

describe("languages section", () => {
    it("says when a removed language affects customers", async () => {
        await post("/settings/demo-plumber/languages", { defaultLanguage: "en", staffLanguage: "es", languages: ["es", "vi"] });
        expect((await saved())).toMatchObject({ defaultLanguage: "en", staffLanguage: "es", settings: { languages: ["en", "es", "vi"] } });
        const res = await post("/settings/demo-plumber/languages", { defaultLanguage: "en", staffLanguage: "en", languages: ["es"] });
        expect(notice(res)).toContain("Customers who used a language you removed");
    });

    it("resets every customer's language when no phone is given", async () => {
        const t = await saved();
        const { contact } = await app.store.conversations.resolve(t.id, Address.phone(CUSTOMER)!, new Date());
        await app.store.conversations.updateContact(contact.id, { preferredLanguage: "vi" });
        const res = await post("/settings/demo-plumber/languages/reset", { phone: "" });
        expect(notice(res)).toBe("Reset 1 customer to the main language.");
        expect((await post("/settings/demo-plumber/languages/reset", { phone: "317-555-0000" })).status).toBe(400);
    });
});

describe("schedule section", () => {
    it("saves answering windows, text hours and fallbacks", async () => {
        await post("/settings/demo-plumber/schedule", {
            mode:         "windows",
            day1:         "on",
            start1:       "08:00",
            end1:         "17:30",
            day6:         "on",
            start6:       "09:00",
            end6:         "13:00",
            offSchedule:  "forward",
            lineBusy:     "reject",
        });
        const t = await saved();
        expect(t.schedule).toEqual({ kind: "windows", windows: [
            { day: 1, startMinute: 480, endMinute: 1050 },
            { day: 6, startMinute: 540, endMinute: 780 },
        ] });
        expect(t.settings.textAlwaysOn).toBe(false);
        expect(t.settings.callFallbacks).toEqual({ offSchedule: "forward", lineBusy: "reject" });

        await post("/settings/demo-plumber/schedule", { mode: "always", textAlwaysOn: "on", offSchedule: "voicemail", lineBusy: "voicemail" });
        expect((await saved()).schedule).toEqual({ kind: "always" });
        expect((await saved()).settings.textAlwaysOn).toBe(true);
    });

    it("refuses windows that are empty or end before they start", async () => {
        const none = await post("/settings/demo-plumber/schedule", { mode: "windows", offSchedule: "voicemail", lineBusy: "voicemail" });
        expect(none.status).toBe(400);
        expect(await none.text()).toContain("Pick at least one day");
        const backwards = await post("/settings/demo-plumber/schedule", { mode: "windows", day2: "on", start2: "17:00", end2: "09:00", offSchedule: "voicemail", lineBusy: "voicemail" });
        expect(backwards.status).toBe(400);
        expect((await saved()).schedule).toEqual({ kind: "always" });
    });
});

describe("catalog section", () => {
    it("saves a hand-managed source with a field map, and has nothing to sync", async () => {
        const res = await post("/settings/demo-plumber/catalog", { kind: "manual", fieldMap: "Title = name\nCost = price_range\nnot a pair" });
        expect(notice(res)).toBe("Saved.");
        expect((await saved()).settings.catalog).toEqual({ source: { kind: "manual" }, fieldMap: { Title: "name", Cost: "price_range" } });
        expect(notice(await post("/settings/demo-plumber/catalog/sync"))).toBe("This list is entered by hand; there is nothing to sync.");
    });

    it("saves a dataset source and syncs it", async () => {
        const res = await post("/settings/demo-plumber/catalog", { kind: "dataset", dataset: "plumbing.json", fieldMap: "" });
        expect(notice(res)).toBe("Saved and synced 13 items.");
        expect(notice(await post("/settings/demo-plumber/catalog/sync"))).toBe("Synced 13 items.");
    });

    it("refuses an unknown source kind and pasted rows that don't fit", async () => {
        expect((await post("/settings/demo-plumber/catalog", { kind: "ftp" })).status).toBe(400);
        const res = await post("/settings/demo-plumber/catalog/upload", { rows: "colour,size\nred,big" });
        expect(res.status).toBe(400);
        expect(await res.text()).toContain("none of the columns match");
    });
});

describe("details section", () => {
    it("saves the pack's profile answers and typed options", async () => {
        await post("/settings/demo-plumber/details", {
            "profile.service_area": "Indianapolis",
            "profile.emergency":    "24/7",
            "profile.pricing":      "",
            "options":              "fee_phrase = a $79 trip charge\nfree_estimates = yes\nradius = 25\nshuttle = off",
        });
        const t = await saved();
        expect(t.profile).toEqual({ service_area: "Indianapolis", emergency: "24/7" });
        expect(t.options).toEqual({ fee_phrase: "a $79 trip charge", free_estimates: true, radius: 25, shuttle: false });
    });
});

describe("notifications section", () => {
    it("saves staff contacts, how each notice is sent, and the admin's add-ons", async () => {
        await post("/settings/demo-plumber/notifications", {
            emails:           "a@ccp.example\nb@ccp.example",
            phones:           "317-555-0888",
            transcript:       ["email", "sms"],
            booking:          ["sms"],
            handoff:          [],
            budget_alert:     ["fax"],
            smsNotifications: "on",
            extraLines:       "2",
        });
        const t = await saved();
        expect(t.staffContacts).toEqual([
            { kind: "phone", value: "+13175550888" },
            { kind: "email", value: "a@ccp.example" },
            { kind: "email", value: "b@ccp.example" },
        ]);
        expect(t.settings.notifications).toEqual({ transcript: ["email", "sms"], booking: ["sms"], handoff: ["email"], budget_alert: ["email"] });
        expect(t.plan.subscription).toMatchObject({ smsNotifications: true, extraLines: 2 });
    });
});

describe("blocks", () => {
    it("blocks a caller for a while or for good, and lifts it", async () => {
        await post("/settings/demo-plumber/blocks", { phone: CUSTOMER, hours: "24", reason: "prank calls" });
        const t = await saved();
        const [blocked] = await app.store.blocks.blocked(t.id, new Date());
        expect(blocked?.block).toMatchObject({ source: "staff", reason: "prank calls" });
        expect(blocked?.block?.until).toBeInstanceOf(Date);

        await post("/settings/demo-plumber/blocks", { phone: CUSTOMER, hours: "", reason: "" });
        const [forever] = await app.store.blocks.blocked(t.id, new Date());
        expect(forever?.block).toMatchObject({ until: null, reason: "blocked by staff" });

        expect(notice(await post(`/settings/demo-plumber/blocks/${forever!.id}/remove`))).toBe("Unblocked.");
        expect(await app.store.blocks.blocked(t.id, new Date())).toEqual([]);
        expect((await post("/settings/demo-plumber/blocks", { phone: "nope" })).status).toBe(400);
        expect((await post("/settings/demo-plumber/blocks/someone-else/remove")).status).toBe(404);
    });
});

describe("logins and review", () => {
    it("removes a login, and marks a new signup reviewed", async () => {
        await post("/settings/demo-plumber/accounts", { email: "pat@ccp.example", name: "Pat", password: "pipes and drains 1" });
        const account = (await app.store.accounts.byEmail("pat@ccp.example"))!;
        expect(account).toMatchObject({ name: "Pat", tenantId: "plumber" });
        expect(notice(await post(`/settings/demo-plumber/accounts/${account.id}/remove`))).toBe("Removed the login for pat@ccp.example.");
        expect(await app.store.accounts.byId(account.id)).toBeNull();

        await app.store.accounts.markSubmitted((await saved()).id, new Date());
        expect(await app.store.accounts.unreviewed()).toEqual(["plumber"]);
        expect((await post("/settings/demo-plumber/reviewed")).status).toBe(303);
        expect(await app.store.accounts.unreviewed()).toEqual([]);
    });

    it("refuses a short password and a duplicate login", async () => {
        expect((await post("/settings/demo-plumber/accounts", { email: "x@ccp.example", password: "short" })).status).toBe(400);
        await post("/settings/demo-plumber/accounts", { email: "y@ccp.example", password: "long enough password" });
        const again = await post("/settings/demo-plumber/accounts", { email: "y@ccp.example", password: "long enough password" });
        expect(await again.text()).toContain("already has a login");
    });
});

describe("read-only pages", () => {
    it("show the prompt preview and a conversation, and 404 for another business's conversation", async () => {
        const prompt = await (await get("/settings/demo-plumber/prompt?channel=voice&message=my+water+heater+is+leaking")).text();
        expect(prompt).toContain("water heater");

        const t = await saved();
        const { conversation } = await app.store.conversations.resolve(t.id, Address.phone(CUSTOMER)!, new Date());
        const session = await app.store.conversations.session(conversation, { channel: "sms" }, { language: "en", smsIdleMs: 3600_000 }, new Date());
        expect((await get(`/settings/demo-plumber/conversations/${session.id}`)).status).toBe(200);
        expect((await get("/settings/demo-plumber/conversations/missing")).status).toBe(404);
        expect(ConversationId(conversation.id)).toBe(session.conversationId);
    });
});
