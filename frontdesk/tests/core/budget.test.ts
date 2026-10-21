import { describe, expect, it } from "vitest";

import {
    Abuse,
    Budget,
    DEFAULT_ABUSE_POLICY,
    DEFAULT_SESSION_LIMITS,
    SessionMeter,
    type BudgetVerdict,
    type Contact,
    type Plan,
    type TurnAssessment,
} from "../../src/core/index.ts";

const PLAN: Plan = { priceUsd: 99, monthlyCostCapUsd: 40, smsNotifications: false };
const T0 = new Date("2026-10-21T15:00:00Z");
const minutes = (n: number) => new Date(T0.getTime() + n * 60_000);

/**
 * Run turns through check + record the way the turn engine will, returning each verdict.
 */
function turns(assessments: readonly TurnAssessment[], now = T0, costThisMonth = 0): BudgetVerdict[] {
    let meter = SessionMeter.start(T0);
    const verdicts: BudgetVerdict[] = [];
    for (const assessment of assessments) {
        const verdict = Budget.check(meter, DEFAULT_SESSION_LIMITS, costThisMonth, PLAN, now);
        verdicts.push(verdict);
        meter = SessionMeter.record(meter, verdict, assessment, 0.001);
    }
    verdicts.push(Budget.check(meter, DEFAULT_SESSION_LIMITS, costThisMonth, PLAN, now));
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
        expect(turns([], minutes(27))[0]).toEqual({ kind: "ok" });
        expect(turns([], minutes(28))[0]).toEqual({ kind: "closing", reason: "duration", turnsLeft: 3 });
    });

    it("starts closing when the tenant reaches its monthly cap mid-session", () => {
        expect(turns([], T0, 40)[0]).toMatchObject({ kind: "closing", reason: "tenant_cap" });
    });
});

describe("Budget.admit", () => {
    it("refuses blocked contacts and tenants over their cap", () => {
        const block = { until: minutes(60), source: "auto" as const, reason: "", createdAt: T0 };
        expect(Budget.admit(block, 0, PLAN, T0)).toEqual({ kind: "refuse", reason: "blocked" });
        expect(Budget.admit({ ...block, until: null }, 0, PLAN, minutes(10_000))).toEqual({ kind: "refuse", reason: "blocked" });
        expect(Budget.admit(block, 0, PLAN, minutes(61))).toEqual({ kind: "admit" });
        expect(Budget.admit(null, 40, PLAN, T0)).toEqual({ kind: "refuse", reason: "tenant_cap" });
    });

    it("warns at 80% of the cap", () => {
        expect(Budget.capStatus(31, PLAN)).toBe("ok");
        expect(Budget.capStatus(32, PLAN)).toBe("warning");
        expect(Budget.capStatus(40, PLAN)).toBe("exceeded");
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
