import { describe, expect, it } from "vitest";

import {
    Abuse,
    Budget,
    DEFAULT_ABUSE_POLICY,
    DEFAULT_PRICING,
    DEFAULT_SESSION_LIMITS,
    SessionMeter,
    type BudgetInput,
    type BudgetVerdict,
    type Contact,
    type Plan,
    type TurnAssessment,
} from "../../src/core/index.ts";

// Basic plan: $99, cap = 40% = $39.60.
const PLAN: Plan = {
    pricing:      DEFAULT_PRICING,
    subscription: { extraLines: 0, smsNotifications: false, topUpsUsd: 0 },
};
// Wednesday 2026-10-21, 11:00 in Indianapolis; open 9:00-17:00 that day.
const T0 = new Date("2026-10-21T15:00:00Z");
const minutes = (n: number) => new Date(T0.getTime() + n * 60_000);
const INPUT: BudgetInput = {
    channel:       "voice",
    plan:          PLAN,
    schedule:      { kind: "windows", windows: [{ day: 3, startMinute: 9 * 60, endMinute: 17 * 60 }] },
    timeZone:      "America/Indiana/Indianapolis",
    textAlwaysOn:  true,
    costThisMonth: 0,
    activeCalls:   0,
    now:           T0,
};

/**
 * Run turns through check + record the way the turn engine will, returning each verdict.
 */
function turns(assessments: readonly TurnAssessment[], input: Partial<BudgetInput> = {}): BudgetVerdict[] {
    const at = { ...INPUT, ...input };
    let meter = SessionMeter.start(T0);
    const verdicts: BudgetVerdict[] = [];
    for (const assessment of assessments) {
        const verdict = Budget.check(meter, DEFAULT_SESSION_LIMITS, at);
        verdicts.push(verdict);
        meter = SessionMeter.record(meter, verdict, assessment, 0.001);
    }
    verdicts.push(Budget.check(meter, DEFAULT_SESSION_LIMITS, at));
    return verdicts;
}

describe("Budget.check wind-down", () => {
    it("gives three polite closing turns after three off-topic turns, then ends", () => {
        const kinds = turns(["off_topic", "off_topic", "off_topic", "off_topic", "off_topic", "off_topic"])
            .map(v => v.kind === "closing" ? `closing:${v.turnsLeft}` : v.kind);
        expect(kinds).toEqual(["ok", "ok", "ok", "closing:3", "closing:2", "closing:1", "end"]);
    });

    it("forgives off-topic turns once the conversation gets back on track", () => {
        const verdicts = turns(["off_topic", "off_topic", "productive", "off_topic"]);
        expect(verdicts.every(v => v.kind === "ok")).toBe(true);
    });

    it("starts closing right after an abusive turn", () => {
        expect(turns(["abusive"])[1]).toEqual({ kind: "closing", reason: "abusive", turnsLeft: 3 });
    });

    it("starts closing two minutes before the session limit", () => {
        expect(turns([], { now: minutes(27) })[0]).toEqual({ kind: "ok" });
        expect(turns([], { now: minutes(28) })[0]).toEqual({ kind: "closing", reason: "duration", turnsLeft: 3 });
    });

    it("starts closing when the assistant's share of the cap runs out mid-session", () => {
        expect(turns([], { costThisMonth: 37.62 })[0]).toMatchObject({ kind: "closing", reason: "tenant_cap" });
    });

    it("starts closing two minutes before the schedule ends", () => {
        const late = new Date("2026-10-21T20:59:00Z");  // 16:59 local, call started a minute ago
        const meter = SessionMeter.start(new Date(late.getTime() - 60_000));
        expect(Budget.check(meter, DEFAULT_SESSION_LIMITS, { ...INPUT, now: late })).toMatchObject({ kind: "closing", reason: "schedule_end" });
    });
});

describe("Budget.admit", () => {
    it("refuses blocked contacts and tenants over their cap", () => {
        const block = { until: minutes(60), source: "auto" as const, reason: "", createdAt: T0 };
        expect(Budget.admit(block, INPUT)).toEqual({ kind: "refuse", reason: "blocked" });
        expect(Budget.admit({ ...block, until: null }, INPUT)).toEqual({ kind: "refuse", reason: "blocked" });
        expect(Budget.admit(block, { ...INPUT, now: minutes(61) })).toEqual({ kind: "admit" });
        expect(Budget.admit(null, { ...INPUT, costThisMonth: 37.62 })).toEqual({ kind: "refuse", reason: "allowance_used" });
        expect(Budget.admit(null, { ...INPUT, costThisMonth: 39.6 })).toEqual({ kind: "refuse", reason: "tenant_cap" });
    });

    it("refuses calls outside the schedule but keeps text open when textAlwaysOn", () => {
        const night = { ...INPUT, now: new Date("2026-10-22T03:00:00Z") };
        expect(Budget.admit(null, night)).toEqual({ kind: "refuse", reason: "off_schedule" });
        expect(Budget.admit(null, { ...night, channel: "sms" })).toEqual({ kind: "admit" });
        expect(Budget.admit(null, { ...night, channel: "sms", textAlwaysOn: false })).toEqual({ kind: "refuse", reason: "off_schedule" });
    });

    it("allows one call at a time unless extra lines are bought", () => {
        expect(Budget.admit(null, { ...INPUT, activeCalls: 1 })).toEqual({ kind: "refuse", reason: "line_busy" });
        const twoLines = { ...PLAN, subscription: { ...PLAN.subscription, extraLines: 1 } };
        expect(Budget.admit(null, { ...INPUT, activeCalls: 1, plan: twoLines })).toEqual({ kind: "admit" });
    });

    it("warns at 80% of the assistant's share, then keeps a voicemail reserve", () => {
        // cap $39.60; assistant share 95% = $37.62; warning from 80% of that = $30.10
        expect(Budget.capStatus(30, PLAN)).toBe("ok");
        expect(Budget.capStatus(30.2, PLAN)).toBe("warning");
        expect(Budget.capStatus(37.62, PLAN)).toBe("assistant_used");
        expect(Budget.capStatus(39.6, PLAN)).toBe("exceeded");
    });
});

describe("Abuse.strike", () => {
    const contact = { abuseStrikes: [], block: null } as unknown as Contact;

    it("blocks for 24 hours on the second strike", () => {
        const first = Abuse.strike(contact, DEFAULT_ABUSE_POLICY, T0);
        expect(first.block).toBeUndefined();
        const second = Abuse.strike({ ...contact, ...first }, DEFAULT_ABUSE_POLICY, minutes(5));
        expect(second.block).toMatchObject({ source: "auto", until: minutes(5 + 24 * 60) });
    });

    it("forgets strikes older than the window", () => {
        const old = { ...contact, abuseStrikes: [new Date("2026-08-01T00:00:00Z")] };
        expect(Abuse.strike(old, DEFAULT_ABUSE_POLICY, T0)).toEqual({ abuseStrikes: [T0] });
    });

    it("never replaces a staff block", () => {
        const banned = { ...contact, abuseStrikes: [T0], block: { until: null, source: "staff", reason: "", createdAt: T0 } } as Contact;
        expect(Abuse.strike(banned, DEFAULT_ABUSE_POLICY, minutes(1)).block).toBeUndefined();
    });
});
