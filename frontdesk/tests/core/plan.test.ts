import { describe, expect, it } from "vitest";

import { DEFAULT_PRICING, LocalClock, Plan, ServiceSchedule } from "../../src/core/index.ts";

describe("Plan", () => {
    it("scales the cost cap with everything the tenant buys", () => {
        const basic = { pricing: DEFAULT_PRICING, subscription: { extraWeeklyHours: 0, extraLines: 0, smsNotifications: false, topUpsUsd: 0 } };
        expect(Plan.costCapUsd(basic)).toBeCloseTo(39.6);
        const more = { ...basic, subscription: { extraWeeklyHours: 10, extraLines: 1, smsNotifications: true, topUpsUsd: 20 } };
        expect(Plan.monthlyRevenueUsd(more)).toBe(99 + 50 + 25 + 10 + 20);
        expect(Plan.costCapUsd(more)).toBeCloseTo(204 * 0.4);
        expect(Plan.weeklyHours(more)).toBe(50);
        expect(Plan.maxConcurrentCalls(more)).toBe(2);
    });
});

describe("ServiceSchedule", () => {
    const weekdays = { windows: [1, 2, 3, 4, 5].map(day => ({ day: day as 1 | 2 | 3 | 4 | 5, startMinute: 9 * 60, endMinute: 17 * 60 })) };

    it("counts weekly hours and enforces the plan's allowance", () => {
        expect(ServiceSchedule.weeklyHours(weekdays)).toBe(40);
        expect(ServiceSchedule.validate(weekdays, 40).ok).toBe(true);
        const extra = { windows: [...weekdays.windows, { day: 6 as const, startMinute: 600, endMinute: 660 }] };
        expect(ServiceSchedule.validate(extra, 40)).toMatchObject({ ok: false, error: [{ kind: "too_many_hours", hours: 41 }] });
    });

    it("rejects overlapping and malformed windows", () => {
        const bad = { windows: [{ day: 1 as const, startMinute: 600, endMinute: 700 }, { day: 1 as const, startMinute: 650, endMinute: 800 }, { day: 2 as const, startMinute: 900, endMinute: 800 }] };
        const result = ServiceSchedule.validate(bad, 40);
        expect(result.ok ? [] : result.error.map(p => p.kind).sort()).toEqual(["bad_window", "overlap"]);
    });

    it("reads the tenant's wall clock, including daylight saving", () => {
        expect(LocalClock.of(new Date("2026-10-21T15:00:00Z"), "America/Indiana/Indianapolis")).toEqual({ day: 3, minute: 11 * 60 });
        expect(LocalClock.of(new Date("2026-12-21T15:00:00Z"), "America/Indiana/Indianapolis")).toEqual({ day: 1, minute: 10 * 60 });
    });

    it("joins back-to-back windows across midnight when counting minutes left", () => {
        const overnight = { windows: [{ day: 5 as const, startMinute: 22 * 60, endMinute: 1440 }, { day: 6 as const, startMinute: 0, endMinute: 120 }] };
        expect(ServiceSchedule.minutesLeft(overnight, { day: 5, minute: 23 * 60 })).toBe(180);
        expect(ServiceSchedule.minutesLeft(overnight, { day: 5, minute: 12 * 60 })).toBeNull();
    });
});
