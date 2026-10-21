import { beforeEach, describe, expect, it } from "vitest";

import type { Config } from "../../src/config.ts";
import { TEST_CONFIG } from "../support/config.ts";
import { TwilioSignature } from "../../src/providers/twilio.ts";
import { FakeLlm, LoggingEmail, LoggingMessaging, type LlmRequest, type ScriptedReply } from "../../src/providers/index.ts";
import { buildApp, type App } from "../../src/server.ts";
import { openStore } from "../../src/storage/index.ts";
import { CUSTOMER, PLUMBER_NUMBER, plumber } from "../support/fixtures.ts";
import { lastUser } from "../support/harness.ts";

const CONFIG: Config = { ...TEST_CONFIG, publicBaseUrl: "https://frontdesk.example" };

function script(request: LlmRequest): ScriptedReply | string {
    const said = lastUser(request);
    if (said.includes("real person")) {
        return { text: "Okay, connecting you now.", tools: [{ name: "transfer_call", arguments: { reason: "asked for a person" } }] };
    }
    if (said.includes("that's all")) {
        return { text: "Alright, take it easy!", tools: [{ name: "end_call", arguments: {} }] };
    }
    return "Sure thing. What's going on with the plumbing? We can usually get someone out the same day.";
}

let app: App;
let messaging: LoggingMessaging;

async function post(path: string, body: Record<string, string>, headers: Record<string, string> = {}) {
    return await app.http.request(path, { method: "POST", body: new URLSearchParams(body), headers });
}

beforeEach(async () => {
    const store = await openStore("", null);
    await store.tenants.save(plumber({ settings: { transferTo: "317-555-0777" } }));
    messaging = new LoggingMessaging();
    app = await buildApp(CONFIG, { store, llm: new FakeLlm(script), messaging, email: new LoggingEmail(), transcriber: null, fetchRecording: null });
});

describe("SMS", () => {
    it("replies in TwiML, adds the opt-out primer once, and meters the reply", async () => {
        const first = await (await post("/sms", { From: CUSTOMER, To: PLUMBER_NUMBER, Body: "my drain is slow", MessageSid: "SM1" })).text();
        expect(first).toContain("<Message>Sure thing.");
        expect(first).toContain("Reply STOP to opt out.");
        const second = await (await post("/sms", { From: CUSTOMER, To: PLUMBER_NUMBER, Body: "ok", MessageSid: "SM2" })).text();
        expect(second).not.toContain("Reply STOP");
        const usage = await app.store.spend.summary(null, new Date(0));
        expect(usage.find(u => u.provider === "twilio")?.calls).toBe(2);
    });
});

describe("Twilio voice", () => {
    it("greets inside a Gather, transfers on request, and closes the session on the status callback", async () => {
        const greeting = await (await post("/voice", { CallSid: "CA1", From: CUSTOMER, To: PLUMBER_NUMBER })).text();
        expect(greeting).toContain("<Gather");
        expect(greeting).toContain("Circle City Plumbing");
        const turn = await (await post("/voice/turn?call=CA1", { CallSid: "CA1", From: CUSTOMER, To: PLUMBER_NUMBER, SpeechResult: "can I talk to a real person", Confidence: "0.9" })).text();
        expect(turn).toContain("connecting you now");
        expect(turn).toContain("<Dial timeout=\"25\">+13175550777</Dial>");
        expect((await post("/voice/status", { CallSid: "CA1", CallStatus: "completed", CallDuration: "60", From: CUSTOMER, To: PLUMBER_NUMBER })).status).toBe(204);
        expect(await app.store.conversations.activeCalls("plumber" as never, null)).toBe(0);
    });

    it("hangs up when the assistant ends the call", async () => {
        await post("/voice", { CallSid: "CA2", From: CUSTOMER, To: PLUMBER_NUMBER });
        const turn = await (await post("/voice/turn?call=CA2", { CallSid: "CA2", From: CUSTOMER, To: PLUMBER_NUMBER, SpeechResult: "that's all thanks", Confidence: "0.9" })).text();
        expect(turn).toMatch(/take it easy!<\/Say><Hangup\/>/);
    });

    it("sends a second caller to voicemail while the line is busy", async () => {
        await post("/voice", { CallSid: "CA3", From: CUSTOMER, To: PLUMBER_NUMBER });
        const second = await (await post("/voice", { CallSid: "CA4", From: "+13175550144", To: PLUMBER_NUMBER })).text();
        expect(second).toContain("<Record");
    });

    it("rejects unsigned webhooks when signature checks are on", async () => {
        const store = await openStore("", null);
        await store.tenants.save(plumber());
        const signed = await buildApp({ ...CONFIG, twilioAuthToken: "tok", validateTwilio: true }, { store, llm: new FakeLlm(script), messaging, email: new LoggingEmail(), transcriber: null, fetchRecording: null });
        const params = { From: CUSTOMER, To: PLUMBER_NUMBER, Body: "hi", MessageSid: "SM9" };
        const bad = await signed.http.request("/sms", { method: "POST", body: new URLSearchParams(params), headers: { "X-Twilio-Signature": "nope" } });
        expect(bad.status).toBe(403);
        const signature = TwilioSignature.expected("tok", "https://frontdesk.example/sms", params);
        const good = await signed.http.request("/sms", { method: "POST", body: new URLSearchParams(params), headers: { "X-Twilio-Signature": signature } });
        expect(good.status).toBe(200);
    });
});

describe("Vapi", () => {
    const call = { id: "v1", customer: { number: CUSTOMER }, phoneNumber: { number: PLUMBER_NUMBER } };
    const vapi = async (body: unknown) => await (await app.http.request("/vapi/chat/completions", {
        method: "POST", body: JSON.stringify(body), headers: { "Content-Type": "application/json" },
    })).text();

    it("streams the greeting, then the reply sentence by sentence, then a transfer tool call", async () => {
        expect(await vapi({ call, messages: [] })).toContain("Circle City Plumbing");
        const reply = await vapi({ call, messages: [{ role: "user", content: "my drain is slow" }] });
        const contents = reply.split("\n\n").filter(l => l.startsWith("data: {")).map(l => JSON.parse(l.slice(6)).choices[0].delta.content).filter(Boolean);
        expect(contents).toEqual(["Sure thing. ", "What's going on with the plumbing? ", "We can usually get someone out the same day."]);
        const transfer = await vapi({ call, messages: [{ role: "user", content: "my drain is slow" }, { role: "user", content: "a real person please" }], tools: [{ type: "function", function: { name: "transferCall" } }] });
        expect(transfer).toContain("\"name\":\"transferCall\"");
        expect(transfer).toContain("+13175550777");
        expect(transfer.trim().endsWith("data: [DONE]")).toBe(true);
    });
});

describe("admin", () => {
    it("uploads catalogs with the upload token and refuses to wipe them", async () => {
        const rows = [{ name: "Drain cleaning", category: "Drain", price_range: "$129-$249", code: "D1" }];
        expect((await app.http.request("/admin/catalog-upload", { method: "POST", body: JSON.stringify({ slug: "demo-plumber", items: rows }), headers: { "Content-Type": "application/json" } })).status).toBe(401);
        const ok = await app.http.request("/admin/inventory-upload", { method: "POST", body: JSON.stringify({ twilio_number: PLUMBER_NUMBER, vehicles: rows }), headers: { "Content-Type": "application/json", "X-Upload-Token": "upload" } });
        expect(await ok.json()).toEqual({ ok: true, tenant: "plumber", items: 1 });
        const empty = await app.http.request("/admin/catalog-upload", { method: "POST", body: JSON.stringify({ slug: "demo-plumber", items: [] }), headers: { "Content-Type": "application/json", "X-Upload-Token": "upload" } });
        expect(empty.status).toBe(400);
    });

    it("blocks and unblocks callers for staff", async () => {
        const auth = { Authorization: "Bearer admin", "Content-Type": "application/json" };
        const blocked = await (await app.http.request("/admin/blocks", { method: "POST", headers: auth, body: JSON.stringify({ tenant: "plumber", phone: CUSTOMER, hours: 48, reason: "prank calls" }) })).json() as { contactId: string };
        const list = await (await app.http.request("/admin/blocks?tenant=plumber", { headers: auth })).json() as unknown[];
        expect(list).toHaveLength(1);
        const silent = await (await post("/sms", { From: CUSTOMER, To: PLUMBER_NUMBER, Body: "hello", MessageSid: "SM5" })).text();
        expect(silent).not.toContain("<Message>");
        await app.http.request(`/admin/blocks/${blocked.contactId}`, { method: "DELETE", headers: auth });
        expect(await (await app.http.request("/admin/blocks?tenant=plumber", { headers: auth })).json()).toEqual([]);
        expect((await app.http.request("/admin/blocks?tenant=plumber")).status).toBe(401);
    });
});

describe("call settings", () => {
    it("listens as long and re-prompts as often as the tenant allows", async () => {
        await app.store.tenants.save(plumber({ settings: { calls: { silenceSeconds: 4, stillTherePrompts: 0 } } }));
        const greeting = await (await post("/voice", { CallSid: "CA9", From: CUSTOMER, To: PLUMBER_NUMBER })).text();
        expect(greeting.match(/<Gather/g)?.length).toBe(1);
        expect(greeting).toContain('timeout="4"');
    });

    it("sends hidden caller ID to voicemail when the tenant says so", async () => {
        await app.store.tenants.save(plumber({ settings: { calls: { hiddenCallerId: "voicemail" } } }));
        const twiml = await (await post("/voice", { CallSid: "CA10", From: "+266696687", To: PLUMBER_NUMBER })).text();
        expect(twiml).toContain("<Record");
        expect(twiml).not.toContain("<Gather");
    });
});
