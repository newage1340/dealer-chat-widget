/**
 * When the assistant answers calls that reach it.
 * By default it always does; a tenant can narrow that to chosen weekly hours
 * (outside them, calls get the off-schedule fallback, our voicemail by default).
 * Hours are a preference, not a plan limit: the monthly cost cap is what bounds spend.
 */

import { Result } from "./result.ts";

/**
 * 0 is Sunday, as in `Date.getDay()`.
 */
export type Weekday = 0 | 1 | 2 | 3 | 4 | 5 | 6;

/**
 * One stretch of answering within a single day, in the tenant's local time,
 * as minutes after midnight (`endMinute` 1440 means midnight at the end of the day).
 * Overnight hours are two windows, one on each day.
 */
export type WeeklyWindow = {
    readonly day:         Weekday;
    readonly startMinute: number;
    readonly endMinute:   number;
};

export type ServiceSchedule =
    | { readonly kind: "always" }
    | { readonly kind: "windows";
		readonly windows: readonly WeeklyWindow[]
	};

/**
 * A moment as the tenant's wall clock shows it.
 */
export type LocalClock = {
    readonly day:    Weekday;
    readonly minute: number;
};

export type ScheduleProblem =
    | { readonly kind: "bad_window";
		readonly window: WeeklyWindow
	}
    | { readonly kind: "overlap";
		readonly a: WeeklyWindow;
		readonly b: WeeklyWindow
	}
    | { readonly kind: "empty" };

const MINUTES_PER_DAY = 24 * 60;
const WEEKDAYS: Record<string, Weekday> = {
    Sun: 0,
    Mon: 1,
    Tue: 2,
    Wed: 3,
    Thu: 4,
    Fri: 5,
    Sat: 6
};

export const LocalClock = {
    of(instant: Date, timeZone: string): LocalClock {
        const parts = new Intl.DateTimeFormat("en-US", {
            timeZone,
            weekday:   "short",
            hour:      "2-digit",
            minute:    "2-digit",
            hourCycle: "h23",
        }).formatToParts(instant);
        const part = (type: Intl.DateTimeFormatPartTypes) => parts.find(p => p.type === type)?.value ?? "";
        return {
            day:    WEEKDAYS[part("weekday")] ?? 0,
            minute: Number(part("hour")) * 60 + Number(part("minute")),
        };
    },
} as const;

export const ServiceSchedule = {
    always: { kind: "always" } as ServiceSchedule,

    weeklyHours(schedule: ServiceSchedule): number {
        if (schedule.kind === "always") {
            return 7 * 24;
        }
        return schedule.windows.reduce(
            (sum, w) => sum + (w.endMinute - w.startMinute), 0
        ) / 60;
    },

    isOpen(schedule: ServiceSchedule, clock: LocalClock): boolean {
        if (schedule.kind === "always") {
            return true;
        }
        return schedule.windows.some(
            w => w.day === clock.day
                && clock.minute >= w.startMinute
                && clock.minute < w.endMinute
        );
    },

    /**
     * Minutes until coverage ends (Infinity when always open), or null if it is not open now.
     * Back-to-back windows (Monday until midnight, Tuesday from midnight) count as one.
     */
    minutesLeft(schedule: ServiceSchedule, clock: LocalClock): number | null {
        if (schedule.kind === "always") {
            return Number.POSITIVE_INFINITY;
        }
        let day = clock.day;
        let minute = clock.minute;
        let total = 0;
        for (let i = 0; i < 7; i++) {
            const window = schedule.windows.find(
                w => w.day === day
                    && minute >= w.startMinute
                    && minute < w.endMinute
            );
            if (window === undefined) {
                return i === 0
                    ? null
                    : total;
            }
            total += window.endMinute - minute;
            if (window.endMinute < MINUTES_PER_DAY) {
                return total;
            }
            day = ((day + 1) % 7) as Weekday;
            minute = 0;
        }
        return total;
    },

    validate(schedule: ServiceSchedule): Result<ServiceSchedule, ScheduleProblem[]> {
        if (schedule.kind === "always") {
            return Result.ok(schedule);
        }
        const problems: ScheduleProblem[] = [];
        if (schedule.windows.length === 0) {
            problems.push({ kind: "empty" });
        }
        for (const w of schedule.windows) {
            if (
                !Number.isInteger(w.startMinute)
                || !Number.isInteger(w.endMinute)
                || w.startMinute < 0
                || w.endMinute > MINUTES_PER_DAY
                || w.startMinute >= w.endMinute
            ) {
                problems.push({ kind: "bad_window", window: w });
            }
        }
        const sorted = [...schedule.windows].sort(
            (a, b) => a.day - b.day || a.startMinute - b.startMinute
        );
        for (let i = 1; i < sorted.length; i++) {
            const a = sorted[i - 1]!;
            const b = sorted[i]!;
            if (a.day === b.day && b.startMinute < a.endMinute) {
                problems.push({ kind: "overlap", a, b });
            }
        }
        return problems.length === 0
            ? Result.ok(schedule)
            : Result.err(problems);
    },
} as const;
