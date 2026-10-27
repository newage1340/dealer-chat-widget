import { describe, expect, it } from "vitest";

import { Coverage, DEFAULT_CALL_FALLBACKS, PhoneNumber } from "../../src/core/index.ts";

describe("Coverage.fallbackFor", () => {
    const forwardBoth = { offSchedule: "forward", lineBusy: "forward" } as const;
    const business = PhoneNumber.parse("317-555-0100");
    const direct = { forwardTo: business, forwardedFrom: null };
    const forwarded = { forwardTo: business, forwardedFrom: business };

    it("defaults to our own voicemail", () => {
        expect(Coverage.fallbackFor(DEFAULT_CALL_FALLBACKS, "off_schedule", direct)).toBe("voicemail");
        expect(Coverage.fallbackFor(DEFAULT_CALL_FALLBACKS, "line_busy", direct)).toBe("voicemail");
    });

    it("forwards only calls that came to us directly, and only with a number to forward to", () => {
        expect(Coverage.fallbackFor(forwardBoth, "line_busy", direct)).toBe("forward");
        expect(Coverage.fallbackFor(forwardBoth, "line_busy", forwarded)).toBe("voicemail");
        expect(Coverage.fallbackFor(forwardBoth, "off_schedule", { forwardTo: null, forwardedFrom: null })).toBe("voicemail");
    });

    it("rejects for free when nothing is left, but keeps voicemail on the reserve", () => {
        expect(Coverage.fallbackFor(forwardBoth, "tenant_cap", direct)).toBe("reject");
        expect(Coverage.fallbackFor(forwardBoth, "blocked", direct)).toBe("reject");
        expect(Coverage.fallbackFor(forwardBoth, "allowance_used", direct)).toBe("voicemail");
    });
});
