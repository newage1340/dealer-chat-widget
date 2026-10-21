import { describe, expect, it } from "vitest";

import type { LlmRequest, ScriptedReply } from "../../src/providers/index.ts";
import { plumber, tenant } from "../support/fixtures.ts";
import { harness, lastUser } from "../support/harness.ts";

function script(request: LlmRequest): ScriptedReply | string {
    const said = lastUser(request);
    if (said.includes("français")) {
        return { text: "Bien sûr, je peux vous aider en français. Quel est le problème?", tools: [{ name: "switch_language", arguments: { language: "fr" } }] };
    }
    if (said.includes("English please")) {
        // The model switches back without calling switch_language: the reply's language must still win.
        return "Of course, we can continue in English. What can I help you with today?";
    }
    if (said.includes("card")) {
        return "Thanks, we don't take payment by text, the team will handle that in person.";
    }
    return "Sure, what's going on?";
}

describe("conversation language", () => {
    it("follows the language the assistant actually replied in, even without the tool", async () => {
        const h = await harness([plumber({ settings: { languages: ["en", "fr"] } })], script);
        const fr = await h.turn({ text: "parlez-vous français?" });
        expect(fr.session.language).toBe("fr");
        const en = await h.turn({ text: "English please" });
        expect(en.session.language).toBe("en");
        expect(en.contact.preferredLanguage).toBe("en");
    });

    it("only offers the tenant's languages", async () => {
        const h = await harness([plumber({ settings: { languages: ["en", "es"] } })], script);
        const result = await h.turn({ text: "parlez-vous français?" });
        expect(result.session.language).toBe("en");
        expect(h.llm.requests.at(-1)?.messages[0]).toMatchObject({ text: expect.stringContaining("Languages we offer: English, Spanish") });
    });

    it("gives every tenant of a type the same tools and the same opening prompt, for the prompt cache", async () => {
        const a = await harness([plumber({ settings: { languages: ["en"] } })], script);
        const b = await harness([plumber({ id: "other", slug: "other", name: "Other Plumbing Co", agentName: "Rita", settings: { languages: ["en", "es", "vi"], links: [{ label: "Coupons", url: "https://x.example/c", trigger: "coupon" }] } })], script);
        await a.turn({ text: "hello" });
        await b.turn({ text: "hello" });
        const [ra, rb] = [a.llm.requests.at(-1)!, b.llm.requests.at(-1)!];
        expect(JSON.stringify(ra.tools)).toBe(JSON.stringify(rb.tools));
        expect(ra.cacheKey).toBe("turn:plumbing:sms");
        const [sa, sb] = [ra.messages[0]!, rb.messages[0]!].map(m => "text" in m ? m.text : "");
        const shared = sa!.slice(0, sa!.indexOf("=== YOUR IDENTITY ==="));
        expect(shared.length).toBeGreaterThan(3000);
        expect(sb!.startsWith(shared)).toBe(true);
        expect(shared).not.toContain("Circle City");
    });
});

describe("privacy", () => {
    it("never sends or stores a card number", async () => {
        const h = await harness([plumber()], script);
        const result = await h.turn({ text: "my card is 4111 1111 1111 1111" });
        expect(lastUser(h.llm.requests.at(-1)!)).toContain("[card number removed]");
        const history = await h.store.conversations.history(result.session.conversationId, 10);
        expect(history.map(m => m.text).join(" ")).not.toContain("4111");
    });

    it("does nothing special with a crisis message unless the business turns crisis support on", async () => {
        const h = await harness([tenant({ id: "t", slug: "therapy", domain: "therapy_practice", endpoints: { sms: ["+13175550200"] } })], script);
        await h.turn({ text: "honestly I don't want to live anymore" });
        const jobs = await h.store.jobs.claim(50, 60_000, h.clock.now);
        expect(jobs.some(j => j.kind === "notify_staff" && j.subject.startsWith("URGENT"))).toBe(false);
        const system = h.llm.requests.at(-1)!.messages.slice(0, 2).map(m => "text" in m ? m.text : "").join("\n");
        expect(system).not.toContain("988");
        expect(system).toContain("PRIVACY & CONFIDENTIALITY");
    });

    it("with crisis support on, alerts staff and gives the model safety instructions", async () => {
        const h = await harness([tenant({
            id: "t", slug: "therapy", domain: "therapy_practice", endpoints: { sms: ["+13175550200"] },
            settings: { privacy: { crisisSupport: true } },
        })], script);
        await h.turn({ text: "honestly I don't want to live anymore" });
        const jobs = await h.store.jobs.claim(50, 60_000, h.clock.now);
        const alert = jobs.find(j => j.kind === "notify_staff" && j.subject.startsWith("URGENT"));
        expect(alert).toBeDefined();
        const system = h.llm.requests.at(-1)!.messages.slice(0, 2).map(m => "text" in m ? m.text : "").join("\n");
        expect(system).toContain("988");
        expect(system).toContain("PRIVACY & CONFIDENTIALITY");
        expect(system).toContain("virtual assistant");
    });
});
