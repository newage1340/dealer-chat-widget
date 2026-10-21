/**
 * Wall-clock time in a tenant's timezone.
 * Instants are stored as `Date` everywhere; this is only for showing and reasoning about local time.
 */

import type { Weekday } from "./schedule.ts";

export const WEEKDAY_NAMES = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
] as const;
export type WeekdayName = (typeof WEEKDAY_NAMES)[number];

const MONTH_NAMES = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec"
] as const;

/**
 * `month` is 1-12; `minuteOfDay` is minutes after local midnight.
 */
export type LocalTime = {
    readonly instant:     Date;
    readonly timeZone:    string;
    readonly year:        number;
    readonly month:       number;
    readonly day:         number;
    readonly weekday:     Weekday;
    readonly hour:        number;
    readonly minute:      number;
    readonly minuteOfDay: number;
};

const formatters = new Map<string, Intl.DateTimeFormat>();

function formatter(timeZone: string): Intl.DateTimeFormat {
    let f = formatters.get(timeZone);
    if (f === undefined) {
        f = new Intl.DateTimeFormat("en-US", {
            timeZone,
            year:      "numeric",
            month:     "numeric",
            day:       "numeric",
            weekday:   "short",
            hour:      "numeric",
            minute:    "numeric",
            hourCycle: "h23",
        });
        formatters.set(timeZone, f);
    }
    return f;
}

const SHORT_WEEKDAYS: Record<string, Weekday> = {
    Sun: 0,
    Mon: 1,
    Tue: 2,
    Wed: 3,
    Thu: 4,
    Fri: 5,
    Sat: 6
};

export const LocalTime = {
    of(instant: Date, timeZone: string): LocalTime {
        const parts = formatter(timeZone).formatToParts(instant);
        const get = (type: Intl.DateTimeFormatPartTypes) => parts.find(p => p.type === type)?.value ?? "";
        const hour = Number(get("hour")) % 24;
        const minute = Number(get("minute"));
        return {
            instant,
            timeZone,
            year:        Number(get("year")),
            month:       Number(get("month")),
            day:         Number(get("day")),
            weekday:     SHORT_WEEKDAYS[get("weekday")] ?? 0,
            hour,
            minute,
            minuteOfDay: hour * 60 + minute,
        };
    },

    /**
     * The same wall-clock moment `days` later (close enough across daylight-saving changes for day labels).
     */
    plusDays(time: LocalTime, days: number): LocalTime {
        return LocalTime.of(new Date(time.instant.getTime() + days * 86_400_000), time.timeZone);
    },

    weekdayName(time: LocalTime): WeekdayName {
        return WEEKDAY_NAMES[time.weekday];
    },

    /**
     * "Wednesday, October 21, 2026 at 03:05 PM"
     */
    long(time: LocalTime): string {
        return `${LocalTime.weekdayName(time)}, ${LocalTime.monthName(time, "long")} ${time.day}, ${time.year} at ${LocalTime.clock(time)}`;
    },

    /**
     * "03:05 PM"
     */
    clock(time: LocalTime): string {
        const h12 = time.hour % 12 === 0
            ? 12
            : time.hour % 12;
        const suffix = time.hour < 12
            ? "AM"
            : "PM";
        return `${String(h12).padStart(2, "0")}:${String(time.minute).padStart(2, "0")} ${suffix}`;
    },

    monthName(time: LocalTime, style: "short" | "long" = "short"): string {
        return style === "short"
            ? MONTH_NAMES[time.month - 1]!
            : new Date(Date.UTC(2000, time.month - 1, 1)).toLocaleString("en-US", { month: "long", timeZone: "UTC" });
    },

    /**
     * Parse "2026-10-24T14:00" (no offset) as wall-clock time in `timeZone`.
     * Returns null for anything that is not a local ISO date-time.
     */
    parseLocalIso(text: string, timeZone: string): Date | null {
        const m = /^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})/.exec(text.trim());
        if (m === null) {
            return null;
        }
        const [y, mo, d, h, mi] = m.slice(1).map(Number) as [number, number, number, number, number];
        // Guess the instant as if the wall clock were UTC, then correct by the zone's offset at that moment.
        let guess = Date.UTC(y, mo - 1, d, h, mi);
        for (let i = 0; i < 2; i++) {
            const local = LocalTime.of(new Date(guess), timeZone);
            const asUtc = Date.UTC(
                local.year,
                local.month - 1,
                local.day,
                local.hour,
                local.minute
            );
            guess += Date.UTC(y, mo - 1, d, h, mi) - asUtc;
        }
        return new Date(guess);
    },
} as const;
