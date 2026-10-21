import { beforeEach, describe, expect, it } from "vitest";

import {
    Address,
    AppointmentId,
    ContactId,
    EventId,
    SessionMeter,
    TenantId,
    type Appointment,
    type Job,
} from "../../src/core/index.ts";
import { openStore, type Store } from "../../src/storage/index.ts";
import { CUSTOMER, PLUMBER_NUMBER, plumber } from "../support/fixtures.ts";

const T0 = new Date("2026-10-21T15:00:00Z");
const DEFAULTS = { language: "en" as const, smsIdleMs: 2 * 3600_000 };
const caller = Address.phone(CUSTOMER)!;

let store: Store;

beforeEach(async () => {
    store = await openStore("", null);
    await store.tenants.save(plumber());
});

describe("tenants", () => {
    it("are found by slug and by endpoint", async () => {
        expect((await store.tenants.bySlug("demo-plumber"))?.name).toBe("Circle City Plumbing");
        expect((await store.tenants.byEndpoint("voice", Address.phone(PLUMBER_NUMBER)!))?.id).toBe("plumber");
        expect(await store.tenants.byEndpoint("voice", caller)).toBeNull();
    });

    it("replace their catalog", async () => {
        const item = (id: string) => ({ id: id as never, tenantId: TenantId("plumber"), fields: { name: id }, updatedAt: T0 });
        await store.tenants.replaceCatalog(TenantId("plumber"), [item("a"), item("b")]);
        await store.tenants.replaceCatalog(TenantId("plumber"), [item("c")]);
        expect((await store.tenants.catalog(TenantId("plumber"))).map(i => i.id)).toEqual(["c"]);
    });
});

describe("conversations", () => {
    it("create a contact on first contact and reuse it after", async () => {
        const first = await store.conversations.resolve(TenantId("plumber"), caller, T0);
        const again = await store.conversations.resolve(TenantId("plumber"), caller, T0);
        expect(again.contact.id).toBe(first.contact.id);
        expect(first.contact.phone).toBe(CUSTOMER);
        expect(first.contact.addresses).toEqual([caller]);
    });

    it("keep SMS in one session until it goes idle; calls by CallSid", async () => {
        const { conversation } = await store.conversations.resolve(TenantId("plumber"), caller, T0);
        const a = await store.conversations.session(conversation, { channel: "sms" }, DEFAULTS, T0);
        const b = await store.conversations.session(conversation, { channel: "sms" }, DEFAULTS, new Date(T0.getTime() + 3600_000));
        const c = await store.conversations.session(conversation, { channel: "sms" }, DEFAULTS, new Date(T0.getTime() + 3 * 3600_000));
        expect(b.id).toBe(a.id);
        expect(c.id).not.toBe(a.id);

        const call = { channel: "voice", externalId: "CA1", vendor: "twilio", handling: "assistant" } as const;
        const v1 = await store.conversations.session(conversation, call, DEFAULTS, T0);
        const v2 = await store.conversations.session(conversation, call, DEFAULTS, T0);
        expect(v2.id).toBe(v1.id);
        expect(v1.channel === "voice" && v1.call.status).toBe("listening");
        expect(await store.conversations.activeCalls(TenantId("plumber"), null)).toBe(1);
        expect(await store.conversations.closeSession(v1.id, T0)).not.toBeNull();
        expect(await store.conversations.closeSession(v1.id, T0)).toBeNull();
        expect(await store.conversations.activeCalls(TenantId("plumber"), null)).toBe(0);
    });

    it("update contacts, including blocks and strikes", async () => {
        const { contact } = await store.conversations.resolve(TenantId("plumber"), caller, T0);
        const block = { until: new Date(T0.getTime() + 86400_000), source: "auto" as const, reason: "abuse", createdAt: T0 };
        const updated = await store.conversations.updateContact(contact.id, { firstName: "Alex", abuseStrikes: [T0], block });
        expect(updated).toMatchObject({ firstName: "Alex", abuseStrikes: [T0], block });
        expect((await store.blocks.blocked(TenantId("plumber"), T0)).map(c => c.id)).toEqual([contact.id]);
        await store.blocks.unblock(contact.id);
        expect(await store.blocks.blocked(TenantId("plumber"), T0)).toEqual([]);
    });
});

describe("turns", () => {
    it("commit atomically, once per dedupe key, saving booked appointments", async () => {
        const { contact, conversation } = await store.conversations.resolve(TenantId("plumber"), caller, T0);
        const session = await store.conversations.session(conversation, { channel: "sms" }, DEFAULTS, T0);
        const appointment: Appointment = {
            id: AppointmentId("appt-1"), tenantId: TenantId("plumber"), contactId: contact.id, conversationId: conversation.id,
            timeText: "Saturday at 10", startsAt: null, subject: "kitchen drain", location: "1 Main St", details: { problem: "clog" },
            itemIds: [], status: "confirmed", sentNotices: [], createdAt: T0,
        };
        const job: Job = { kind: "send_sms", to: CUSTOMER as never, text: "booked", purpose: "booking_confirmation", tenantId: TenantId("plumber"), dedupeKey: "j1", runAt: null };
        const commit = {
            dedupeKey:      "SM1",
            reply:          "You're booked!",
            contactId:      contact.id,
            contactChanges: { firstName: "Alex" },
            session:        { ...session, meter: SessionMeter.record(session.meter, { kind: "ok" }, "productive", 0.01) },
            messages:       [
                { conversationId: conversation.id, sessionId: session.id, role: "user" as const, text: "book it", channel: "sms" as const },
                { conversationId: conversation.id, sessionId: session.id, role: "assistant" as const, text: "You're booked!", channel: "sms" as const },
            ],
            events:         [{
                id: EventId("e1"), tenantId: TenantId("plumber"), conversationId: conversation.id, sessionId: session.id,
                occurredAt: T0, causedBy: "SM1", kind: "appointment_booked" as const, appointment, replaces: null,
            }],
            jobs:           [job],
            at:             T0,
        };
        expect(await store.turns.commit(commit)).toBe(true);
        expect(await store.turns.commit(commit)).toBe(false);
        expect(await store.turns.find("SM1")).toEqual({ reply: "You're booked!", sessionId: session.id });
        expect((await store.conversations.history(conversation.id, 10)).map(m => m.text)).toEqual(["book it", "You're booked!"]);
        expect((await store.conversations.contact(contact.id))?.firstName).toBe("Alex");
        expect((await store.conversations.sessionById(session.id))?.meter.turns).toBe(1);
        expect(await store.appointments.latest(conversation.id)).toMatchObject({ subject: "kitchen drain", details: { problem: "clog" } });

        const claimed = await store.jobs.claim(10, 60_000, T0);
        expect(claimed).toHaveLength(1);
        expect(claimed[0]).toMatchObject({ kind: "send_sms", text: "booked", attempts: 1 });
    });
});

describe("jobs", () => {
    const job = (key: string, runAt: Date | null = null): Job => ({
        kind: "classify_session", sessionId: "s" as never, tenantId: TenantId("plumber"), dedupeKey: key, runAt,
    });

    it("dedupe, wait for run_at, retry, and re-claim expired leases", async () => {
        await store.jobs.enqueue([job("a"), job("a"), job("later", new Date(T0.getTime() + 60_000))]);
        const first = await store.jobs.claim(10, 1000, T0);
        expect(first.map(j => j.dedupeKey)).toEqual(["a"]);
        expect(await store.jobs.claim(10, 1000, T0)).toEqual([]);

        const leaseExpired = await store.jobs.claim(10, 1000, new Date(T0.getTime() + 2000));
        expect(leaseExpired.map(j => [j.dedupeKey, j.attempts])).toEqual([["a", 2]]);
        await store.jobs.fail(leaseExpired[0]!.id, "boom", new Date(T0.getTime() + 5000));
        await store.jobs.complete((await store.jobs.claim(10, 1000, new Date(T0.getTime() + 5000)))[0]!.id);

        const later = await store.jobs.claim(10, 1000, new Date(T0.getTime() + 61_000));
        expect(later.map(j => j.dedupeKey)).toEqual(["later"]);
    });
});

describe("transcripts, usage and deletion", () => {
    it("list closed sessions with classifications, total spend, and delete a contact completely", async () => {
        const { contact, conversation } = await store.conversations.resolve(TenantId("plumber"), caller, T0);
        const session = await store.conversations.session(conversation, { channel: "sms" }, DEFAULTS, T0);
        await store.conversations.closeSession(session.id, T0);
        await store.transcripts.saveClassification({
            sessionId: session.id, outcome: "lead", topic: "drain", sentiment: "neutral", urgency: "normal",
            language: "en", followupNeeded: true, gist: "Wants a drain cleared.", model: "fake", classifiedAt: T0,
        });
        const listed = await store.transcripts.list({ tenantId: TenantId("plumber"), since: null, until: null, outcomes: ["lead"], limit: 10 });
        expect(listed.map(t => t.classification?.gist)).toEqual(["Wants a drain cleared."]);
        expect(await store.transcripts.list({ tenantId: TenantId("plumber"), since: null, until: null, outcomes: ["booked"], limit: 10 })).toEqual([]);

        await store.usage.record({
            provider: "openai", model: "m", service: "llm", purpose: "turn", occurredAt: T0, units: {}, costUsd: 0.25,
            latencyMs: 1, tenantId: TenantId("plumber"), channel: "sms",
        });
        const tenant = (await store.tenants.byId(TenantId("plumber")))!;
        expect(await store.spend.monthToDate(tenant, T0)).toBeCloseTo(0.25);
        expect(await store.spend.monthToDate(tenant, new Date("2026-11-02T15:00:00Z"))).toBe(0);

        await store.conversations.deleteContact(contact.id);
        expect(await store.conversations.contact(ContactId(contact.id))).toBeNull();
        expect(await store.transcripts.transcript(session.id)).toBeNull();
    });
});
