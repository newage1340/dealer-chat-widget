/**
 * Reading posted HTML forms (Hono's `parseBody`), for the settings and onboarding pages.
 * Every reader trims, and treats a missing field like an empty one.
 */

import type { Context } from "hono";

export type FormBody = Record<string, string | File | (string | File)[]>;

/**
 * The posted form, with repeated fields (checkbox groups) kept as arrays.
 */
export async function formBody(c: Context): Promise<FormBody> {
    return await c.req.parseBody({ all: true });
}

/**
 * A field's (first) value, or "".
 */
export function one(body: FormBody, name: string): string {
    const value = body[name];
    const first = Array.isArray(value) ? value[0] : value;
    return typeof first === "string"
        ? first.trim()
        : "";
}

/**
 * A field's value exactly as typed, for passwords (where spaces count).
 */
export function untrimmed(body: FormBody, name: string): string {
    const value = body[name];
    return typeof value === "string"
        ? value
        : "";
}

/**
 * Every non-blank value of a repeated field (checkbox groups, multi-selects).
 */
export function many(body: FormBody, name: string): string[] {
    const value = body[name];
    const all = Array.isArray(value)
        ? value
        : value === undefined
            ? []
            : [value];
    return all
        .filter((v): v is string => typeof v === "string" && v.trim() !== "")
        .map(v => v.trim());
}

/**
 * Whether a checkbox was ticked.
 */
export function on(body: FormBody, name: string): boolean {
    return one(body, name) !== "";
}

/**
 * A number field, or undefined when it was left blank (so the current value stays).
 */
export function num(body: FormBody, name: string): number | undefined {
    const raw = one(body, name);
    return raw === ""
        ? undefined
        : Number(raw);
}

/**
 * Minutes after midnight from a time field ("13:30"), or null when it isn't a clock time.
 */
export function minuteOf(clock: string): number | null {
    const m = /^(\d{1,2}):(\d{2})$/.exec(clock);
    if (m === null || Number(m[1]) > 23 || Number(m[2]) > 59) {
        return null;
    }
    return Number(m[1]) * 60 + Number(m[2]);
}
