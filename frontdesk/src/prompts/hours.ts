/**
 * Business-hours parsing and the facts derived from it (open now, next open day, 7-day schedule).
 *
 * Worked out in code and handed to the model as facts:
 * models are unreliable at date arithmetic and will happily offer "tomorrow" when tomorrow is closed.
 */

import { LocalTime, WEEKDAY_NAMES, type WeekdayName } from "../core/index.ts";

/**
 * `opens` / `closes` are minutes after midnight; both null means closed.
 * `byAppointment` marks days open only for booked visits.
 */
export type DayHours = {
    readonly text:          string;
    readonly opens:         number | null;
    readonly closes:        number | null;
    readonly byAppointment: boolean;
};

export type WeekHours = Readonly<Partial<Record<WeekdayName, DayHours>>>;

const DAY_ALIASES: Record<string, WeekdayName> = {
    mon: "Monday", monday: "Monday",
    tue: "Tuesday", tues: "Tuesday", tuesday: "Tuesday",
    wed: "Wednesday", weds: "Wednesday", wednesday: "Wednesday",
    thu: "Thursday", thur: "Thursday", thurs: "Thursday", thursday: "Thursday",
    fri: "Friday", friday: "Friday",
    sat: "Saturday", saturday: "Saturday",
    sun: "Sunday", sunday: "Sunday",
};
const MONDAY_FIRST: readonly WeekdayName[] = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];

const DAY = "(monday|tuesday|wednesday|thursday|friday|saturday|sunday|mon|tues|tue|weds|wed|thurs|thur|thu|fri|sat|sun)";
const TIME = "(\\d{1,2}(?::\\d{2})?\\s*(?:am|pm|a\\.m\\.|p\\.m\\.|a|p)?)";
const SEGMENT_SPLIT = new RegExp(`[,;|/\\n]+|(?<=[)\\sap]m)\\s+(?=${DAY}\\b)`, "i");
const RANGE_RE = new RegExp(`\\b${DAY}\\s*(?:-|–|to|thru|through)\\s*${DAY}\\b`, "i");
const TIME_RANGE_RE = new RegExp(`${TIME}\\s*(?:-|–|to)\\s*${TIME}`, "i");
const DAY_GLOBAL = new RegExp(`\\b${DAY}\\b`, "gi");

const CLOSED: DayHours = { text: "closed", opens: null, closes: null, byAppointment: false };

function minutes(token: string, end: boolean, start: number | null): number | null {
    const m = /^(\d{1,2})(?::(\d{2}))?\s*([ap])?/.exec(token.trim().toLowerCase().replaceAll(".", ""));
    if (m === null) {
        return null;
    }
    let hour = Number(m[1]);
    const minute = Number(m[2] ?? 0);
    const meridiem = m[3];
    if (meridiem === "p" && hour < 12) {
        hour += 12;
    } else if (meridiem === "a" && hour === 12) {
        hour = 0;
    } else if (meridiem === undefined) {
        // "8-5": a bare closing hour earlier than the opening hour is in the afternoon.
        if (end && start !== null && hour * 60 + minute <= start) {
            hour += 12;
        } else if (!end && hour < 6) {
            hour += 12;
        }
    }
    return hour * 60 + minute;
}

export const Hours = {
    isClosed(day: DayHours): boolean {
        return day.opens === null
            && !day.byAppointment;
    },

    /**
     * Parse strings like "Mon-Sat 9am-6pm, Sun closed" or "Saturday: by appointment only".
     * Days the text doesn't cover are absent from the result.
     */
    parse(text: string): WeekHours {
        const result: Partial<Record<WeekdayName, DayHours>> = {};
        for (const raw of (text || "").split(SEGMENT_SPLIT)) {
            const seg = (raw ?? "").trim().replace(/\.$/, "");
            if (!seg || seg.toLowerCase() in DAY_ALIASES) {
                continue;
            }
            let days: WeekdayName[];
            const range = RANGE_RE.exec(seg);
            if (/\b(daily|every\s*day|all\s*week|7\s*days)\b/i.test(seg)) {
                days = [...MONDAY_FIRST];
            } else if (range !== null) {
                const s = MONDAY_FIRST.indexOf(DAY_ALIASES[range[1]!.toLowerCase()]!);
                const e = MONDAY_FIRST.indexOf(DAY_ALIASES[range[2]!.toLowerCase()]!);
                const end = e >= s
                    ? e + 1
                    : e + 8;
                days = [];
                for (let i = s; i < end; i++) {
                    days.push(MONDAY_FIRST[i % 7]!);
                }
            } else {
                days = [...seg.matchAll(DAY_GLOBAL)].map(m => DAY_ALIASES[m[1]!.toLowerCase()]!);
            }
            if (days.length === 0) {
                continue;
            }

            let hours: DayHours;
            const times = TIME_RANGE_RE.exec(seg);
            if (/\bclosed\b/i.test(seg)) {
                hours = CLOSED;
            } else if (times !== null) {
                const opens = minutes(times[1]!, false, null);
                const closes = minutes(times[2]!, true, opens);
                hours = { text: `${times[1]!.trim()} to ${times[2]!.trim()}`, opens, closes, byAppointment: false };
            } else if (/appointment/i.test(seg)) {
                hours = { text: "by appointment only", opens: null, closes: null, byAppointment: true };
            } else {
                continue;
            }
            for (const day of days) {
                result[day] = hours;
            }
        }
        return result;
    },

    /**
     * True / false when today's hours are known, null when they aren't.
     */
    isOpenNow(hours: WeekHours, now: LocalTime): boolean | null {
        const today = hours[LocalTime.weekdayName(now)];
        if (today === undefined) {
            return null;
        }
        if (today.opens === null || today.closes === null) {
            return false;
        }
        return today.opens <= now.minuteOfDay
            && now.minuteOfDay < today.closes;
    },

    /**
     * The next day after today that is open (or by appointment), labelled "tomorrow" or by weekday name.
     */
    nextOpenDay(hours: WeekHours, now: LocalTime): { readonly label: string; readonly hours: DayHours } | null {
        for (let offset = 1; offset < 8; offset++) {
            const day = WEEKDAY_NAMES[(now.weekday + offset) % 7]!;
            const h = hours[day];
            if (h !== undefined && !Hours.isClosed(h)) {
                return {
                    label: offset === 1
                        ? "tomorrow"
                        : day,
                    hours: h,
                };
            }
        }
        return null;
    },

    /**
     * The next seven days, one line each, for the prompt.
     */
    scheduleBlock(hoursText: string, now: LocalTime): string {
        const hours = Hours.parse(hoursText);
        if (Object.keys(hours).length === 0) {
            return `- (could not parse hours; raw hours: ${hoursText || "not listed"})`;
        }
        const lines: string[] = [];
        for (let offset = 0; offset < 7; offset++) {
            const d = LocalTime.plusDays(now, offset);
            const day = LocalTime.weekdayName(d);
            const label = offset === 0
                ? "TODAY"
                : offset === 1
                    ? "TOMORROW"
                    : day;
            const h = hours[day];
            const status = h === undefined
                ? "hours not listed - follow the business notes for this day"
                : h.byAppointment
                    ? "BY APPOINTMENT ONLY - offer a specific booked time; don't tell them to just stop by"
                    : Hours.isClosed(h)
                        ? "CLOSED - never offer a time on this day"
                        : h.text;
            lines.push(`- ${label} (${day} ${LocalTime.monthName(d)} ${String(d.day).padStart(2, "0")}): ${status}`);
        }
        return lines.join("\n");
    },

    openStatusLine(hoursText: string, now: LocalTime): string {
        const hours = Hours.parse(hoursText);
        const state = Hours.isOpenNow(hours, now);
        const today = hours[LocalTime.weekdayName(now)];
        const next = Hours.nextOpenDay(hours, now);
        const laterToday = today !== undefined && today.opens !== null && now.minuteOfDay < today.opens;
        const nextPhrase = laterToday
            ? `later today (${today.text})`
            : next !== null
                ? `${next.label} (${next.hours.text})`
                : "the next day we're open";
        if (state === true) {
            return `OPEN/CLOSED RIGHT NOW: OPEN (today's hours: ${today?.text ?? "unknown"}). If asked whether we're open, say yes.`;
        }
        if (state === false) {
            return `OPEN/CLOSED RIGHT NOW: CLOSED (today: ${today?.text ?? "closed"}). `
                + "Don't volunteer it unless they ask about coming in now/today; when it is relevant, "
                + `turn it into a booking for the next open time: ${nextPhrase}. `
                + "Being closed never limits what you can answer.";
        }
        return "";
    },
} as const;
