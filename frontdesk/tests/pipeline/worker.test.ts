import { describe, expect, it } from "vitest";

import { Address, EventId, TenantId } from "../../src/core/index.ts";
import { closeSession } from "../../src/pipeline/sessions.ts";
import type { LlmRequest } from "../../src/providers/index.ts";
import { plumber } from "../support/fixtures.ts";
import { harness } from "../support/harness.ts";

function script(request: LlmRequest): string {
    if (request.purpose === "classification") {
        const urgent = request.messages.some(m => m.role === "user" && m.text.includes("flooding"));
        return JSON.stringify({
            outcome: "lead", topic: urgent ? "emergency" : "drain", sentiment: "neutral", urgency: urgent ? "urgent" : "normal",
            language: "en", followup_needed: true, gist: urgent ? "Sam's basement is flooding; call back now." : "Asked about a slow drain.",
        });
    }
    return "We can take a look - when works for you?";
}

describe("Worker", () => {
    it("classifies a session once it goes idle and emails the transcript to staff", async () => {
        const h = await harness([plumber()], script);
        await h.turn({ text: "My drain is slow" });
        h.clock.now = new Date(h.clock.now.getTime() + 31 * 60_000);
        await h.worker.tick();

        const classification = h.llm.requests.find(r => r.purpose === "classification");
        expect(classification?.tier).toBe("small");
        expect(classification?.format.kind).toBe("json_schema");
        const mail = h.email.sent.at(-1)!;
        expect(mail.subject).toBe("Text - Drains & sewer - (317) 555-0111: Asked about a slow drain.");
        expect(mail.text).toContain("--- Transcript ---\nCustomer: My drain is slow\nDave: We can take a look - when works for you?");
        expect(h.messaging.sent).toEqual([]);
    });

    it("texts staff transcripts only with the SMS add-on", async () => {
        const h = await harness([plumber({
            plan:     { smsNotifications: true },
            settings: { notifications: { transcript: ["email", "sms"] } },
        })], script);
        await h.turn({ text: "My drain is slow" });
        h.clock.now = new Date(h.clock.now.getTime() + 31 * 60_000);
        await h.worker.tick();
        expect(h.messaging.sent.map(m => m.to)).toEqual(["+13175550888"]);
    });

    it("transcribes a voicemail, classifies it, and flags it urgent", async () => {
        const h = await harness([plumber()], script);
        const { contact, conversation } = await h.store.conversations.resolve(TenantId("plumber"), Address.phone("+13175550111")!, h.clock.now);
        const session = await h.store.conversations.session(conversation, { channel: "voice", externalId: "CA9", vendor: "twilio", handling: "voicemail" }, { language: "en", smsIdleMs: 1 }, h.clock.now);
        await h.store.voicemails.save({
            sessionId: session.id, tenantId: TenantId("plumber"), conversationId: conversation.id, from: Address.phone("+13175550111")!,
            recordingUrl: "https://api.twilio.com/rec/RE1", durationSeconds: 14, receivedAt: h.clock.now,
        });
        await h.store.outbox.publish([{
            id: EventId("vm1"), tenantId: TenantId("plumber"), conversationId: conversation.id, sessionId: session.id,
            occurredAt: h.clock.now, causedBy: null, kind: "voicemail_left", recordingUrl: "https://api.twilio.com/rec/RE1", durationSeconds: 14,
        }], [{ kind: "transcribe_voicemail", sessionId: session.id, recordingUrl: "https://api.twilio.com/rec/RE1", tenantId: TenantId("plumber"), dedupeKey: "t1", runAt: null }]);
        await h.worker.tick();

        expect(contact.id).toBeTruthy();
        const mail = h.email.sent.at(-1)!;
        expect(mail.subject).toMatch(/^URGENT - Voicemail - Emergency/);
        expect(mail.text).toContain("Recording: https://api.twilio.com/rec/RE1");
        expect(mail.text).toContain("my basement is flooding");
    });

    it("sends one cold follow-up when the customer goes quiet, and not after they reply", async () => {
        const h = await harness([plumber({ settings: { coldFollowupMinutes: 45 } })], script);
        await h.turn({ text: "how much for a water heater?" });
        h.clock.now = new Date(h.clock.now.getTime() + 20 * 60_000);
        await h.turn({ text: "hmm let me think" });
        h.clock.now = new Date(h.clock.now.getTime() + 46 * 60_000);
        await h.worker.tick();
        const followups = h.messaging.sent.filter(m => m.purpose === "followup");
        expect(followups).toHaveLength(1);
        expect(followups[0]!.text).toContain("Circle City Plumbing");
    });

    it("does not close or classify a session twice", async () => {
        const h = await harness([plumber()], script);
        const result = await h.turn({ text: "hi" });
        expect(await closeSession(h.services, result.session.id, "idle")).toBe(true);
        expect(await closeSession(h.services, result.session.id, "idle")).toBe(false);
    });
});
