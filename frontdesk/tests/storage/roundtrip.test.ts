/**
 * Everything written to the store comes back exactly as written, field for field.
 *
 * A safety net for restructuring storage (splitting postgres.ts, parsing rows with zod):
 * each test fills in every field of a type, with values unlike the defaults, and compares whole objects.
 */

import { beforeEach, describe, expect, it } from "vitest";

import {
    Address,
    AppointmentId,
    CatalogItemId,
    ConversationId,
    DomainId,
    EventId,
    PhoneNumber,
    SessionMeter,
    TenantId,
    type Account,
    type Appointment,
    type CallState,
    type CatalogItem,
    type Classification,
    type Contact,
    type ContactChanges,
    type Conversation,
    type Job,
    type Session,
    type Voicemail,
} from "../../src/core/index.ts";
import { openStore, type SessionKey, type Store } from "../../src/storage/index.ts";
import { CUSTOMER, plumber } from "../support/fixtures.ts";

// activeCalls and spend compare with the database clock, so tests start near the real time.
const T0 = new Date(Math.floor(Date.now() / 60_000) * 60_000 - 60_000);
const T1 = new Date(T0.getTime() + 90_000);
const TENANT = TenantId("plumber");
const caller = Address.phone(CUSTOMER)!;
const DEFAULTS = { language: "en" as const, smsIdleMs: 2 * 3600_000 };

let store: Store;

beforeEach(async () => {
    store = await openStore("", null);
    await store.tenants.save(plumber());
});

async function newConversation(): Promise<{ contact: Contact; conversation: Conversation }> {
    return await store.conversations.resolve(TENANT, caller, T0);
}

describe("tenants", () => {
    it("come back with every setting as saved", async () => {
        const tenant = plumber({
            phone:           "317-555-0299",
            address:         "1 Main St",
            website:         "https://circlecity.example",
            agentName:       "Rita",
            voiceAgentName:  "Rae",
            defaultLanguage: "es",
            staffLanguage:   "vi",
            branding:        { color: "#123456", logoUrl: "https://circlecity.example/logo.png" },
            profile:         { service_area: "Indianapolis", emergency: "yes" },
            options:         { fee_phrase: "a $79 trip charge", free_estimates: true, radius: 25 },
            settings:        {
                skipProfileGate:     true,
                transferTo:          "317-555-0777",
                coldFollowupMinutes: 45,
                textAlwaysOn:        false,
                links:               [{ label: "Coupons", url: "https://circlecity.example/c", trigger: "coupon" }],
                callFallbacks:       { offSchedule: "forward", lineBusy: "reject" },
                notifications:       { transcript: ["email", "sms"], booking: ["sms"], handoff: ["email"], budget_alert: ["email"] },
                maxSessionMinutes:   20,
                recordingDisclosure: true,
                languages:           ["en", "es", "vi"],
                privacy:             { sensitive: true, crisisSupport: true, redact: ["card"] },
                style:               { professionalism: "formal", honestAboutAi: true },
                calls:               { silenceSeconds: 7, stillTherePrompts: 2, hiddenCallerId: "voicemail" },
                catalog:             { source: { kind: "dataset", name: "plumbing.json" }, fieldMap: { Title: "name" } },
            },
        });
        await store.tenants.save(tenant);
        expect(await store.tenants.byId(TENANT)).toEqual(tenant);
        expect(await store.tenants.bySlug(tenant.slug)).toEqual(tenant);
        expect(await store.tenants.all()).toEqual([tenant]);
    });

    it("keep catalog items and sync status as written", async () => {
        const items: CatalogItem[] = [
            { id: CatalogItemId("a"), tenantId: TENANT, fields: { name: "Drain cleaning", price: 149, notes: null }, updatedAt: T0 },
            { id: CatalogItemId("b"), tenantId: TENANT, fields: { name: "Water heater", price: "from $1,200" }, updatedAt: T1 },
        ];
        await store.tenants.replaceCatalog(TENANT, items);
        expect(await store.tenants.catalog(TENANT)).toEqual(items);

        const status = { syncedAt: T1, items: 2, error: "row 3 had no name" };
        await store.tenants.recordSync(TENANT, status);
        expect(await store.tenants.syncStatus(TENANT)).toEqual(status);
        await store.tenants.recordSync(TENANT, { ...status, error: null });
        expect(await store.tenants.syncStatus(TENANT)).toEqual({ ...status, error: null });
    });
});

describe("contacts", () => {
    it("keep every field a turn can change", async () => {
        const { contact } = await newConversation();
        const changes: Required<ContactChanges> = {
            firstName:         "Maria",
            lastName:          "Lopez",
            email:             "maria@example.com",
            phone:             PhoneNumber.parse("+13175550123")!,
            optedOut:          true,
            termsAcceptedAt:   T0,
            preferredLanguage: "es",
            block:             { until: null, source: "staff", reason: "rude to staff", createdAt: T1 },
            abuseStrikes:      [T0, T1],
            attributes:        { vehicle: "2019 Civic", gate_code: "1234" },
        };
        const expected: Contact = { ...contact, ...changes };
        expect(await store.conversations.updateContact(contact.id, changes)).toEqual(expected);
        expect(await store.conversations.contact(contact.id)).toEqual(expected);
        expect(await store.conversations.contactByAddress(TENANT, caller)).toEqual(expected);
    });

    it("clear nullable fields and gain addresses", async () => {
        const { contact } = await newConversation();
        await store.conversations.updateContact(contact.id, { preferredLanguage: "fr", block: { until: T1, source: "auto", reason: "abuse", createdAt: T0 } });
        const cleared = await store.conversations.updateContact(contact.id, { preferredLanguage: null, block: null });
        expect(cleared).toEqual({ ...contact, preferredLanguage: null, block: null });

        const web = Address.webSession("w1");
        await store.conversations.addAddress(contact.id, web);
        expect((await store.conversations.contact(contact.id))?.addresses).toEqual([caller, web]);
        expect((await store.conversations.contactByAddress(TENANT, web))?.id).toBe(contact.id);
    });

    it("forget preferred languages and put open sessions back in the given language", async () => {
        const { contact, conversation } = await newConversation();
        const session = await store.conversations.session(conversation, { channel: "sms" }, { ...DEFAULTS, language: "es" }, T0);
        await store.conversations.updateContact(contact.id, { preferredLanguage: "es" });
        expect(await store.conversations.resetLanguages(TENANT, contact.id, "en")).toBe(1);
        expect((await store.conversations.contact(contact.id))?.preferredLanguage).toBeNull();
        expect((await store.conversations.sessionById(session.id))?.language).toBe("en");
        expect(await store.conversations.resetLanguages(TENANT, null, "en")).toBe(0);
    });
});

describe("sessions", () => {
    const keys: Record<Session["channel"], SessionKey> = {
        web:   { channel: "web", externalId: "w1" },
        sms:   { channel: "sms" },
        voice: { channel: "voice", externalId: "CA1", vendor: "vapi", handling: "voicemail" },
    };

    it("keep language, sent links, meter and call state on every channel", async () => {
        const { conversation } = await newConversation();
        const call: CallState = { status: "transferring", turns: 4, to: PhoneNumber.parse("+13175550777")! };
        for (const key of Object.values(keys)) {
            const opened = await store.conversations.session(conversation, key, DEFAULTS, T0);
            const meter: SessionMeter = {
                ...SessionMeter.record(SessionMeter.start(T0), { kind: "closing", reason: "abusive", turnsLeft: 3 }, "abusive", 0.0125),
                startedAt: T0,
            };
            const changed: Session = opened.channel === "voice"
                ? { ...opened, language: "vi", sentUrls: ["https://a.example", "https://b.example"], meter, call }
                : { ...opened, language: "vi", sentUrls: ["https://a.example", "https://b.example"], meter };
            await store.conversations.saveSession(changed);
            expect(await store.conversations.sessionById(opened.id)).toEqual(changed);
        }
    });

    it("are found again by their key, and come back closed with the end time", async () => {
        const { conversation } = await newConversation();
        const voice = await store.conversations.session(conversation, keys.voice, DEFAULTS, T0);
        expect(voice).toMatchObject({ channel: "voice", vendor: "vapi", handling: "voicemail", externalId: "CA1" });
        expect(await store.conversations.findSession(TENANT, keys.voice)).toEqual(voice);
        expect(await store.conversations.closeSession(voice.id, T1)).toEqual({ ...voice, endedAt: T1 });
    });
});

describe("turns", () => {
    it("store messages, appointments and notices exactly", async () => {
        const { contact, conversation } = await newConversation();
        const session = await store.conversations.session(conversation, { channel: "web", externalId: "w1" }, DEFAULTS, T0);
        const appointment: Appointment = {
            id:             AppointmentId("appt-1"),
            tenantId:       TENANT,
            contactId:      contact.id,
            conversationId: conversation.id,
            timeText:       "Saturday at 10 AM",
            startsAt:       T1,
            subject:        "kitchen drain",
            location:       "1 Main St, Indianapolis",
            details:        { problem: "clog", access: "side door" },
            itemIds:        [CatalogItemId("a"), CatalogItemId("b")],
            status:         "confirmed",
            sentNotices:    [],
            createdAt:      T0,
        };
        await store.turns.commit({
            dedupeKey:      "web:1",
            reply:          "You're booked!",
            contactId:      contact.id,
            contactChanges: {},
            session,
            messages:       [
                { conversationId: conversation.id, sessionId: session.id, role: "user", text: "book it", channel: "web" },
                { conversationId: conversation.id, sessionId: session.id, role: "assistant", text: "You're booked!", channel: "web" },
            ],
            events:         [{
                id:             EventId("e1"),
                tenantId:       TENANT,
                conversationId: conversation.id,
                sessionId:      session.id,
                occurredAt:     T0,
                causedBy:       "web:1",
                kind:           "appointment_booked",
                appointment,
                replaces:       null,
            }],
            jobs:           [],
            at:             T0,
        });

        const history = await store.conversations.history(conversation.id, 10);
        expect(history.map(({ id: _, createdAt: __, ...m }) => m)).toEqual([
            { conversationId: conversation.id, sessionId: session.id, role: "user", text: "book it", channel: "web" },
            { conversationId: conversation.id, sessionId: session.id, role: "assistant", text: "You're booked!", channel: "web" },
        ]);
        expect(await store.conversations.sessionMessages(session.id)).toEqual(history);
        expect(await store.conversations.lastMessage(conversation.id)).toEqual(history[1]);

        expect(await store.appointments.byId(appointment.id)).toEqual(appointment);
        expect(await store.appointments.latest(conversation.id)).toEqual(appointment);
        await store.appointments.markNotice(appointment.id, "confirmation");
        await store.appointments.markNotice(appointment.id, "reminder");
        expect((await store.appointments.byId(appointment.id))?.sentNotices).toEqual(["confirmation", "reminder"]);
    });
});

describe("jobs", () => {
    it("come back with every field of every kind", async () => {
        const meta = (dedupeKey: string) => ({ tenantId: TENANT, dedupeKey, runAt: null });
        const jobs: Job[] = [
            { ...meta("1"), kind: "send_sms", to: PhoneNumber.parse(CUSTOMER)!, text: "hi", purpose: "reminder" },
            { ...meta("2"), kind: "send_email", to: "a@example.com", subject: "s", text: "t", purpose: "transcript" },
            { ...meta("3"), kind: "notify_staff", notice: "handoff", subject: "s", text: "t" },
            { ...meta("4"), kind: "transcribe_voicemail", sessionId: "s1" as never, recordingUrl: "https://r.example/1" },
            { ...meta("5"), kind: "classify_session", sessionId: "s1" as never },
            { ...meta("6"), kind: "deliver_transcript", sessionId: "s1" as never },
            { ...meta("7"), kind: "send_followup", followup: "reminder", appointmentId: AppointmentId("appt-1") },
            { ...meta("8"), kind: "cold_followup", conversationId: ConversationId("c1"), afterMessageAt: T0.toISOString() },
            { ...meta("9"), kind: "record_fixed_costs", month: "2026-10" },
        ];
        await store.jobs.enqueue(jobs);
        const claimed = await store.jobs.claim(20, 60_000, T1);
        const bodies = claimed
            .map(({ id: _, attempts: __, ...job }) => job)
            .sort((a, b) => a.dedupeKey.localeCompare(b.dedupeKey));
        expect(bodies).toEqual(jobs);
        expect(claimed.every(j => j.attempts === 1)).toBe(true);
    });

    it("keep run_at", async () => {
        const later = new Date(T1.getTime() + 60_000);
        await store.outbox.publish([], [{ tenantId: TENANT, dedupeKey: "later", runAt: later, kind: "classify_session", sessionId: "s1" as never }]);
        expect(await store.jobs.claim(10, 60_000, T1)).toEqual([]);
        expect((await store.jobs.claim(10, 60_000, later))[0]).toMatchObject({ dedupeKey: "later", runAt: later });
    });
});

describe("transcripts and voicemails", () => {
    it("keep classifications and voicemails exactly", async () => {
        const { contact, conversation } = await newConversation();
        const session = await store.conversations.session(conversation, { channel: "voice", externalId: "CA9", vendor: "twilio", handling: "voicemail" }, DEFAULTS, T0);
        const voicemail: Voicemail = {
            sessionId:       session.id,
            tenantId:        TENANT,
            conversationId:  conversation.id,
            from:            caller,
            recordingUrl:    "https://api.twilio.com/recordings/RE1",
            durationSeconds: 42,
            receivedAt:      T1,
        };
        await store.voicemails.save(voicemail);
        expect(await store.voicemails.bySession(session.id)).toEqual(voicemail);

        const classification: Classification = {
            sessionId:      session.id,
            outcome:        "lead",
            topic:          "water_heater",
            sentiment:      "negative",
            urgency:        "urgent",
            language:       "es",
            followupNeeded: true,
            gist:           "No hot water since this morning.",
            model:          "openai:gpt-4o-mini",
            classifiedAt:   T1,
        };
        await store.transcripts.saveClassification(classification);
        const transcript = await store.transcripts.transcript(session.id);
        expect(transcript?.classification).toEqual(classification);
        expect(transcript?.session).toEqual(await store.conversations.sessionById(session.id));
        expect(contact.id).toBe(conversation.contactId);
    });
});

describe("usage and quality", () => {
    it("total spend by provider and purpose", async () => {
        const record = (provider: string, purpose: "turn" | "reminder", costUsd: number) => store.usage.record({
            provider, model: "m", service: "llm", purpose, occurredAt: T0, units: { input_tokens: 10 },
            costUsd, latencyMs: 12, tenantId: TENANT, channel: "sms",
        });
        await record("openai", "turn", 0.25);
        await record("openai", "turn", 0.5);
        await record("twilio", "reminder", 0.01);
        const totals = [...await store.spend.summary(TENANT, new Date(T0.getTime() - 1000))]
            .sort((a, b) => a.provider.localeCompare(b.provider));
        expect(totals).toEqual([
            { provider: "openai", purpose: "turn", calls: 2, costUsd: expect.closeTo(0.75, 6) },
            { provider: "twilio", purpose: "reminder", calls: 1, costUsd: expect.closeTo(0.01, 6) },
        ]);
    });

    it("report turn metrics and eval runs", async () => {
        const { conversation } = await newConversation();
        const session = await store.conversations.session(conversation, { channel: "sms" }, DEFAULTS, T0);
        const metric = {
            tenantId:     TENANT,
            domain:       DomainId("plumbing"),
            sessionId:    session.id,
            channel:      "sms" as const,
            variant:      "control",
            model:        "gpt-4o-mini",
            occurredAt:   T0,
            latencyMs:    800,
            firstTextMs:  300,
            rounds:       2,
            tools:        ["book_appointment"],
            toolFailures: 1,
            corrected:    true,
            fallback:     false,
            inputTokens:  1000,
            cachedTokens: 500,
            outputTokens: 50,
            costUsd:      0.002,
            replyChars:   120,
            language:     "en" as const,
            switched:     false,
        };
        await store.quality.recordTurn(metric);
        await store.quality.recordTurn({ ...metric, latencyMs: 400, firstTextMs: null, corrected: false, toolFailures: 0, fallback: true });
        const [row] = await store.quality.report({ since: new Date(T0.getTime() - 1000), tenantId: TENANT, domain: "plumbing" });
        expect(row).toMatchObject({
            variant:        "control",
            model:          "gpt-4o-mini",
            channel:        "sms",
            turns:          2,
            sessions:       1,
            avgLatencyMs:   600,
            avgFirstTextMs: 300,
            correctedRate:  0.5,
            fallbackRate:   0.5,
        });

        await store.quality.recordEval({ runId: "r1", scenario: "books", variant: "control", model: "m", passed: true, failures: [], costUsd: 0.01, ranAt: T0 });
        await store.quality.recordEval({ runId: "r2", scenario: "books", variant: "control", model: "m", passed: false, failures: ["no booking"], costUsd: 0.01, ranAt: T1 });
        expect(await store.quality.evals(new Date(T0.getTime() - 1000))).toEqual([
            { scenario: "books", variant: "control", model: "m", runs: 2, passRate: 0.5, lastRun: T1 },
        ]);
    });
});

describe("accounts and webhooks", () => {
    it("keep accounts as created and track sign-ins, passwords and review", async () => {
        const account: Account = {
            id:           "acct-1",
            tenantId:     TENANT,
            email:        "owner@circlecity.example",
            name:         "Pat",
            passwordHash: "scrypt$abc",
            createdAt:    T0,
            lastLoginAt:  null,
        };
        expect(await store.accounts.create(account)).toBe(true);
        expect(await store.accounts.create({ ...account, id: "acct-2" })).toBe(false);
        expect(await store.accounts.byId(account.id)).toEqual(account);
        expect(await store.accounts.byEmail(" Owner@CircleCity.example ")).toEqual(account);
        expect(await store.accounts.forTenant(TENANT)).toEqual([account]);

        await store.accounts.setPassword(account.id, "scrypt$def");
        await store.accounts.touchLogin(account.id, T1);
        expect(await store.accounts.byId(account.id)).toEqual({ ...account, passwordHash: "scrypt$def", lastLoginAt: T1 });

        await store.accounts.markSubmitted(TENANT, T0);
        expect(await store.accounts.unreviewed()).toEqual([TENANT]);
        await store.accounts.markReviewed(TENANT, T1);
        expect(await store.accounts.unreviewed()).toEqual([]);

        await store.accounts.remove(account.id);
        expect(await store.accounts.byId(account.id)).toBeNull();
    });

    it("claim each webhook event once", async () => {
        expect(await store.webhooks.claim("stripe", "evt_1", T0)).toBe(true);
        expect(await store.webhooks.claim("stripe", "evt_1", T1)).toBe(false);
        expect(await store.webhooks.claim("stripe", "evt_2", T1)).toBe(true);
    });
});
