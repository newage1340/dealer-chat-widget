import { describe, expect, it } from "vitest";

import { Address, TenantId } from "../../src/core/index.ts";
import type { LlmRequest, ScriptedReply } from "../../src/providers/index.ts";
import { plumber } from "../support/fixtures.ts";
import { harness, hasToolResult, lastUser } from "../support/harness.ts";

const BOOK = {
    name:      "book_appointment",
    arguments: {
        time_text: "Monday at 10 AM",
        time_iso:  "2026-10-26T10:00",
        subject:   "kitchen sink drain clog",
        location:  "12 Elm St, Carmel",
        details:   { problem: "kitchen sink clogged since Sunday" },
    },
};

function script(request: LlmRequest): ScriptedReply | string {
    if (request.purpose === "classification") {
        return JSON.stringify({
            outcome: "booked", topic: "drain", sentiment: "positive", urgency: "normal",
            language: "en", followup_needed: false, gist: "Alex booked a drain clearing for Monday at 10 AM.",
        });
    }
    const said = lastUser(request);
    if (said.includes("book it")) {
        return hasToolResult(request, "booked")
            ? "You're all set, Alex! Your service call is confirmed for Monday at 10 AM."
            : { text: "", tools: [BOOK] };
    }
    if (said.includes("sunday at 10")) {
        return hasToolResult(request)
            ? "We're closed Sundays - would Monday work instead?"
            : { text: "", tools: [{ ...BOOK, arguments: { ...BOOK.arguments, time_text: "Sunday at 10 AM", time_iso: "2026-10-25T10:00" } }] };
    }
    if (said.includes("my name is")) {
        return { text: "Nice to meet you, Alex! What's going on with the plumbing?", tools: [{ name: "save_customer_details", arguments: { first_name: "Alex" } }] };
    }
    if (said.includes("hablas")) {
        return { text: "¡Claro! ¿En qué le puedo ayudar?", tools: [{ name: "switch_language", arguments: { language: "es" } }] };
    }
    if (said.includes("banana")) {
        return { text: "Ha, I'm not sure about that - anything plumbing I can help with?", tools: [{ name: "flag_conversation", arguments: { kind: "off_topic", note: "nonsense" } }] };
    }
    return "Sure, we can help with that. What's going on?";
}

describe("TurnEngine", () => {
    it("answers an SMS, remembers details from tools, and replays retried webhooks", async () => {
        const h = await harness([plumber()], script);
        const first = await h.turn({ text: "Hi, my name is Alex", dedupeKey: "SM1" });
        expect(first.outcome.reply).toBe("Nice to meet you, Alex! What's going on with the plumbing?");
        expect(first.contact.firstName).toBe("Alex");

        const again = await h.turn({ text: "Hi, my name is Alex", dedupeKey: "SM1" });
        expect(again.replayed).toBe(true);
        expect(h.llm.requests).toHaveLength(1);

        const history = await h.store.conversations.history(first.session.conversationId, 10);
        expect(history.map(m => m.role)).toEqual(["user", "assistant"]);
    });

    it("books through the tool, rejects closed days, and queues confirmations, staff alerts and reminders", async () => {
        const h = await harness([plumber()], script);
        const closed = await h.turn({ text: "Can you come Sunday at 10?" });
        expect(closed.outcome.reply).toContain("closed Sundays");
        expect(await h.store.appointments.latest(closed.session.conversationId)).toBeNull();
        const toolMessage = h.llm.requests[1]!.messages.find(m => m.role === "tool");
        expect(toolMessage?.role === "tool" && toolMessage.output).toContain("closed on Sundays");

        const booked = await h.turn({ text: "Monday at 10 works, book it", channel: "web", tenant: { kind: "slug", slug: "demo-plumber" }, sender: Address.webSession("w1"), externalSessionId: "w1" }, {});
        // The web profile gate asks first: no name or phone yet.
        expect(booked.data["needs_profile"]).toBe(true);

        const sms = await h.turn({ text: "book it" });
        expect(sms.outcome.reply).toContain("You're all set");
        const appointment = await h.store.appointments.latest(sms.session.conversationId);
        expect(appointment).toMatchObject({ subject: "kitchen sink drain clog", location: "12 Elm St, Carmel", timeText: "Monday at 10 AM" });
        expect(appointment?.startsAt?.toISOString()).toBe("2026-10-26T14:00:00.000Z");

        await h.worker.tick();
        expect(h.email.sent.map(e => e.subject)).toEqual(["service call booked: Monday at 10 AM - kitchen sink drain clog"]);
        expect(h.email.sent[0]!.to).toEqual(["owner@circlecity.example"]);
        // SMS bookings are confirmed by the reply itself; the reminder waits for its time.
        expect(h.messaging.sent).toEqual([]);
        h.clock.now = new Date("2026-10-26T13:05:00Z");
        await h.worker.tick();
        expect(h.messaging.sent.map(m => m.text)).toEqual([expect.stringContaining("Reminder from Circle City Plumbing")]);
    });

    it("follows the customer into another language", async () => {
        const h = await harness([plumber()], script);
        const result = await h.turn({ text: "¿Hablas español?" });
        expect(result.session.language).toBe("es");
        expect(result.contact.preferredLanguage).toBe("es");
    });

    it("winds down after repeated nonsense, closes the session, and blocks a repeat offender", async () => {
        const h = await harness([plumber()], script);
        const first = [];
        for (let i = 0; i < 6; i++) {
            first.push(await h.turn({ text: "banana banana" }));
        }
        // 3 off-topic turns start a 3-turn wind-down; after the last one the session closes.
        const prompt = (i: number) => h.llm.requests[i]!.messages.filter(m => m.role === "system").map(m => (m as { text: string }).text).join("\n");
        expect(prompt(2)).not.toContain("WRAP UP NOW");
        expect(prompt(3)).toContain("You have 3 replies left");
        expect(prompt(5)).toContain("This is your LAST reply");
        expect(await h.store.conversations.sessionById(first[5]!.session.id)).toMatchObject({ endedAt: expect.any(Date) });
        const contact = await h.store.conversations.contactByAddress(TenantId("plumber"), Address.phone("+13175550111")!);
        expect(contact?.abuseStrikes).toHaveLength(1);

        h.clock.now = new Date(h.clock.now.getTime() + 60 * 60_000);
        for (let i = 0; i < 6; i++) {
            await h.turn({ text: "banana banana" });
        }
        const blocked = await h.store.conversations.contactByAddress(TenantId("plumber"), Address.phone("+13175550111")!);
        expect(blocked?.block).toMatchObject({ source: "auto" });
        expect((await h.turn({ text: "hello?" })).silent).toBe(true);
        expect(h.llm.requests).toHaveLength(12);
    });

    it("honors STOP and stays silent afterwards", async () => {
        const h = await harness([plumber()], script);
        expect((await h.turn({ text: "STOP" })).outcome.reply).toContain("unsubscribed");
        expect((await h.turn({ text: "hello?" })).silent).toBe(true);
        expect((await h.turn({ text: "START" })).outcome.reply).toContain("resubscribed");
    });
});
