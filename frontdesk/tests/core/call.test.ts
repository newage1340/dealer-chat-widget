import { describe, expect, it } from "vitest";

import { CallState, PhoneNumber, type CallEvent } from "../../src/core/index.ts";

function run(events: readonly CallEvent[], limits = { maxTurns: 30, maxSilentTurns: 3 }): CallState {
    let state = CallState.start();
    for (const event of events) {
        const next = CallState.next(state, event, limits);
        if (!next.ok) {
            throw new Error(`invalid: ${next.error.event} from ${next.error.from}`);
        }
        state = next.value;
    }
    return state;
}

describe("CallState", () => {
    it("alternates listening and responding", () => {
        const state = run([
            { kind: "heard", text: "hi" },
            { kind: "replied", directive: { kind: "continue" } },
        ]);
        expect(state).toEqual({ status: "listening", turns: 1, silentTurns: 0 });
    });

    it("ends after too much silence", () => {
        const state = run([{ kind: "silence" }, { kind: "silence" }, { kind: "silence" }]);
        expect(state).toMatchObject({ status: "ended", reason: "silence" });
    });

    it("ends at the turn limit", () => {
        const turn: CallEvent[] = [{ kind: "heard", text: "x" }, { kind: "replied", directive: { kind: "continue" } }];
        const state = run([...turn, ...turn, { kind: "heard", text: "x" }], { maxTurns: 2, maxSilentTurns: 3 });
        expect(state).toMatchObject({ status: "ended", reason: "turn_limit", turns: 2 });
    });

    it("records a hang-up during a transfer as transferred", () => {
        const to = PhoneNumber.parse("317-555-0100")!;
        const state = run([
            { kind: "heard", text: "a human please" },
            { kind: "replied", directive: { kind: "transfer", to } },
            { kind: "hung_up" },
        ]);
        expect(state).toMatchObject({ status: "ended", reason: "transferred" });
    });

    it("rejects events that make no sense in the current state", () => {
        const result = CallState.next(CallState.start(), { kind: "replied", directive: { kind: "end" } });
        expect(result).toEqual({ ok: false, error: { from: "listening", event: "replied" } });
    });
});

describe("PhoneNumber.parse", () => {
    it.each([
        ["(317) 555-0100", "+13175550100"],
        ["1-317-555-0100", "+13175550100"],
        ["+44 20 7946 0958", "+442079460958"],
        ["555-0100", null],
        ["", null],
    ])("%s -> %s", (raw, expected) => {
        expect(PhoneNumber.parse(raw)).toBe(expected);
    });
});
