import { describe, expect, it } from "vitest";

import { ConversationId, PromptVariant, type ConversationId as Id } from "../../src/core/index.ts";
import { plumber } from "../support/fixtures.ts";
import { harness } from "../support/harness.ts";

describe("prompt variants", () => {
    const variants = [
        { id: "a", weight: 70, description: "", templates: {}, temperature: null, tier: null },
        { id: "b", weight: 30, description: "", templates: {}, temperature: 0, tier: null },
    ] as const;

    it("keeps a conversation in one variant and splits by weight", () => {
        const pick = (n: number) => PromptVariant.assign(variants, "exp", ConversationId(`c${n}`) as Id).id;
        expect(pick(1)).toBe(pick(1));
        const counts = { a: 0, b: 0 };
        for (let i = 0; i < 2000; i++) {
            counts[pick(i) as "a" | "b"]++;
        }
        expect(counts.a / 2000).toBeGreaterThan(0.65);
        expect(counts.a / 2000).toBeLessThan(0.75);
    });

    it("falls back to control when nothing has weight", () => {
        expect(PromptVariant.assign([{ ...variants[0], weight: 0 }], "exp", ConversationId("x")).id).toBe("control");
    });
});

describe("quality log", () => {
    it("records every model turn and reports per variant, model and channel", async () => {
        const h = await harness([plumber()], () => ({ text: "Sure, I'll pass that along.", tools: [{ name: "request_callback", arguments: { note: "call back" } }] }));
        await h.turn({ text: "please have someone call me" });
        await h.turn({ text: "thanks" });
        const rows = await h.store.quality.report({ since: new Date(0), tenantId: null, domain: null });
        expect(rows).toHaveLength(1);
        expect(rows[0]).toMatchObject({ variant: "control", channel: "sms", turns: 2, sessions: 1, fallbackRate: 0 });

        await h.store.quality.recordEval({ runId: "r", scenario: "s", variant: "control", model: "m", passed: true, failures: [], costUsd: 0.001, ranAt: new Date() });
        await h.store.quality.recordEval({ runId: "r", scenario: "s", variant: "control", model: "m", passed: false, failures: ["x"], costUsd: 0.001, ranAt: new Date() });
        expect((await h.store.quality.evals(new Date(0)))[0]).toMatchObject({ scenario: "s", runs: 2, passRate: 0.5 });
    });
});
