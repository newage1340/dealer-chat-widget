import { describe, expect, it } from "vitest";

import { Budget, SessionMeter, type Limits, type Spend } from "../../src/core/index.ts";

const LIMITS: Limits = {
    session: { maxTurns: 30, maxSilentTurns: 3, maxDurationSeconds: 900, maxCostUsd: 0.5, maxUnproductiveTurns: 3 },
    contact: { maxSessionsPerDay: 5, maxCostUsdPerDay: 1 },
    tenant:  { maxCostUsdPerDay: 20, maxCostUsdPerMonth: 300 },
};
const NO_SPEND: Spend = { contactSessionsToday: 0, contactCostToday: 0, tenantCostToday: 0, tenantCostThisMonth: 0 };
const T0 = new Date("2026-10-21T15:00:00Z");

describe("Budget.admit", () => {
    it("refuses blocked callers and tenants over budget", () => {
        expect(Budget.admit(NO_SPEND, LIMITS, new Date("2026-10-22T00:00:00Z"), T0)).toEqual({ kind: "refuse", reason: "blocked" });
        expect(Budget.admit({ ...NO_SPEND, tenantCostThisMonth: 300 }, LIMITS, null, T0)).toEqual({ kind: "refuse", reason: "tenant_monthly" });
        expect(Budget.admit({ ...NO_SPEND, contactSessionsToday: 5 }, LIMITS, null, T0)).toEqual({ kind: "refuse", reason: "contact_daily" });
    });

    it("admits once a block has expired", () => {
        expect(Budget.admit(NO_SPEND, LIMITS, new Date("2026-10-20T00:00:00Z"), T0)).toEqual({ kind: "ok" });
    });
});

describe("Budget.check", () => {
    it("warns on the last allowed unproductive turn, then ends", () => {
        let meter = SessionMeter.start(T0);
        meter = SessionMeter.record(meter, "off_topic", 0.01);
        meter = SessionMeter.record(meter, "off_topic", 0.01);
        expect(Budget.check(meter, NO_SPEND, LIMITS, T0)).toEqual({ kind: "warn", reason: "unproductive" });
        meter = SessionMeter.record(meter, "abusive", 0.01);
        expect(Budget.check(meter, NO_SPEND, LIMITS, T0)).toEqual({ kind: "end", reason: "unproductive" });
    });

    it("forgives off-topic turns once the conversation gets back on track", () => {
        let meter = SessionMeter.start(T0);
        meter = SessionMeter.record(meter, "off_topic", 0.01);
        meter = SessionMeter.record(meter, "off_topic", 0.01);
        meter = SessionMeter.record(meter, "productive", 0.01);
        expect(meter.unproductiveTurns).toBe(0);
        expect(Budget.check(meter, NO_SPEND, LIMITS, T0)).toEqual({ kind: "ok" });
    });

    it("ends on session cost, duration and the contact's daily cost", () => {
        const meter = SessionMeter.start(T0);
        expect(Budget.check({ ...meter, costUsd: 0.5 }, NO_SPEND, LIMITS, T0)).toMatchObject({ reason: "session_cost" });
        expect(Budget.check(meter, NO_SPEND, LIMITS, new Date(T0.getTime() + 900_000))).toMatchObject({ reason: "duration" });
        expect(Budget.check({ ...meter, costUsd: 0.2 }, { ...NO_SPEND, contactCostToday: 0.8 }, LIMITS, T0)).toMatchObject({ reason: "contact_daily" });
    });
});
