import { describe, expect, it } from "vitest";

import type { UsageRecord } from "../../src/core/index.ts";
import {
    FakeLlm,
    LlmProvider,
    LlmRouter,
    ProviderError,
    type LlmRequest,
    type LlmStreamPart,
} from "../../src/providers/index.ts";
import { plainSms, smsSegments, TwilioSignature } from "../../src/providers/twilio.ts";

const REQUEST: LlmRequest = {
    messages:    [{ role: "user", text: "hi" }],
    tools:       [],
    format:      { kind: "text" },
    tier:        "fast",
    maxTokens:   100,
    temperature: 0,
    timeoutMs:   1000,
    purpose:     "turn",
    tenantId:    null,
    channel:     null,
};

class Failing implements LlmProvider {
    readonly name: string;
    readonly #afterText: boolean;
    calls = 0;

    constructor(name: string, afterText = false) {
        this.name = name;
        this.#afterText = afterText;
    }

    async *stream(): AsyncIterable<LlmStreamPart> {
        this.calls++;
        if (this.#afterText) {
            yield { kind: "text", delta: "partial " };
        }
        throw new ProviderError(this.name, "unavailable", "down");
    }
}

class Slow implements LlmProvider {
    readonly name = "slow";
    release: () => void = () => {};

    async *stream(): AsyncIterable<LlmStreamPart> {
        await new Promise<void>(resolve => { this.release = resolve; });
        yield* new FakeLlm(() => "slow reply").stream(REQUEST);
    }
}

describe("LlmRouter", () => {
    it("falls through to the next route on a retryable failure and meters the result", async () => {
        const recorded: UsageRecord[] = [];
        const router = new LlmRouter(
            { fast: [{ provider: new Failing("local"), maxConcurrency: 2 }, { provider: new FakeLlm(() => "hello there"), maxConcurrency: null }] },
            { record: async u => { recorded.push(u); } },
            { fake: { fake: { input_tokens: 0.01, output_tokens: 0.1 } } },
        );
        const response = await LlmProvider.complete(router, REQUEST);
        expect(response.text).toBe("hello there");
        expect(response.usage.costUsd).toBeCloseTo(100 * 0.01 + 20 * 0.1);
        expect(recorded).toHaveLength(1);
    });

    it("does not retry once text has reached the caller", async () => {
        const backup = new FakeLlm(() => "backup");
        const router = new LlmRouter(
            { fast: [{ provider: new Failing("local", true), maxConcurrency: null }, { provider: backup, maxConcurrency: null }] },
            null,
            {},
        );
        await expect(LlmProvider.complete(router, REQUEST)).rejects.toThrow("down");
        expect(backup.requests).toHaveLength(0);
    });

    it("spills over to the next route when the first is at its concurrency cap", async () => {
        const slow = new Slow();
        const cloud = new FakeLlm(() => "cloud reply");
        const router = new LlmRouter(
            { fast: [{ provider: slow, maxConcurrency: 1 }, { provider: cloud, maxConcurrency: null }] },
            null,
            {},
        );
        const first = LlmProvider.complete(router, REQUEST);
        await new Promise(r => setTimeout(r, 0));
        const second = await LlmProvider.complete(router, REQUEST);
        slow.release();
        expect(second.text).toBe("cloud reply");
        expect((await first).text).toBe("slow reply");
    });
});

describe("Twilio", () => {
    it("validates webhook signatures", () => {
        // Expected value computed with the official twilio Python library (RequestValidator).
        const params = { CallSid: "CA1234567890ABCDE", Caller: "+12349013030", Digits: "1234", From: "+12349013030", To: "+18005551212" };
        const url = "https://example.com/myapp.php?foo=1&bar=2";
        const signature = TwilioSignature.expected("12345", url, params);
        expect(signature).toBe("vNe7KK2kJwCsxc9K3OLkkKB3qqI=");
        expect(TwilioSignature.valid("12345", url, params, signature)).toBe(true);
        expect(TwilioSignature.valid("12345", url, { ...params, Digits: "9" }, signature)).toBe(false);
    });

    it("counts SMS segments", () => {
        expect(smsSegments("hello")).toBe(1);
        expect(smsSegments("a".repeat(161))).toBe(2);
        expect(smsSegments("你好".repeat(40))).toBe(2);
    });

    it("keeps model punctuation from turning a text into UCS-2", () => {
        const reply = "Sure \u2014 we\u2019re open \u201Ctill 6\u201D tomorrow\u2026 see you then! ".repeat(3);
        expect(smsSegments(reply)).toBe(3);
        expect(plainSms(reply)).toBe("Sure - we're open \"till 6\" tomorrow... see you then! ".repeat(3));
        expect(smsSegments(plainSms(reply))).toBe(1);
        expect(plainSms("¿Mañana a las 3?")).toBe("¿Mañana a las 3?");
    });
});
