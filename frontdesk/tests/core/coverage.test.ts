import { describe, expect, it } from "vitest";

import { Coverage, DEFAULT_CALL_FALLBACKS } from "../../src/core/index.ts";

describe("Coverage.fallbackFor", () => {
    const forwardBoth = { offSchedule: "forward", lineBusy: "forward" } as const;

    it("defaults to our own voicemail", () => {
        expect(Coverage.fallbackFor("always", DEFAULT_CALL_FALLBACKS, "off_schedule")).toBe("voicemail");
        expect(Coverage.fallbackFor("always", DEFAULT_CALL_FALLBACKS, "line_busy")).toBe("voicemail");
    });

    it("forwards only when calls reach us directly", () => {
        expect(Coverage.fallbackFor("always", forwardBoth, "line_busy")).toBe("forward");
        expect(Coverage.fallbackFor("overflow", forwardBoth, "line_busy")).toBe("voicemail");
        expect(Coverage.fallbackFor("after_hours", forwardBoth, "off_schedule")).toBe("voicemail");
    });

    it("rejects for free when nothing is left, but keeps voicemail on the reserve", () => {
        expect(Coverage.fallbackFor("always", forwardBoth, "tenant_cap")).toBe("reject");
        expect(Coverage.fallbackFor("always", forwardBoth, "blocked")).toBe("reject");
        expect(Coverage.fallbackFor("always", forwardBoth, "allowance_used")).toBe("voicemail");
    });
});
