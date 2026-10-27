/**
 * Reading column values back into core types. JSON columns are parsed with zod, so a row that no longer
 * fits the code fails loudly at the edge instead of travelling on as a mistyped value.
 */

import { z } from "zod";

import {
    AppointmentNotice,
    AppointmentId,
    CallEndReason,
    CatalogItemId,
    ClosingReason,
    ConversationId,
    enumValue,
    Language,
    PhoneNumber,
    Purpose,
    SessionId,
    SessionOutcome,
    Sentiment,
    StaffNotice,
    Urgency,
    type ValueOf,
} from "../../core/index.ts";
import type { Row } from "../db.ts";

export const json = (value: unknown): string => JSON.stringify(value);
export const date = (value: unknown): Date => value instanceof Date ? value : new Date(String(value));
export const maybeDate = (value: unknown): Date | null => value === null || value === undefined ? null : date(value);
export const str = (value: unknown): string => value === null || value === undefined ? "" : String(value);

/**
 * The first row mapped by `map`, or null when the query found nothing.
 */
export function firstOrNull<T>(rows: readonly Row[], map: (row: Row) => T): T | null {
    const row = rows[0];
    return row === undefined
        ? null
        : map(row);
}

/**
 * A stored value of an `as const` enum, checked; a value the code doesn't know is a bug, not a row to guess at.
 */
export function member<E extends Readonly<Record<string, string>>>(values: E, raw: unknown): ValueOf<E> {
    const value = enumValue(values, raw);
    if (value === null) {
        throw new Error(`unexpected stored value '${str(raw)}' (expected one of ${Object.values(values).join(", ")})`);
    }
    return value;
}

/**
 * A JSON column parsed with `schema`. Drivers hand jsonb back either decoded or as text.
 */
export function jsonColumn<Schema extends z.ZodType>(schema: Schema, raw: unknown, column: string): z.output<Schema> {
    const parsed = schema.safeParse(typeof raw === "string" ? JSON.parse(raw) : raw);
    if (!parsed.success) {
        throw new Error(`stored ${column} doesn't fit: ${parsed.error.message}`);
    }
    return parsed.data;
}

// --- JSON column shapes --------------------------------------------------------------------

/**
 * Dates inside JSON are ISO strings.
 */
const instant = z.coerce.date();

const phone = z.string().transform((raw, ctx) => {
    const parsed = PhoneNumber.parse(raw);
    if (parsed === null) {
        ctx.addIssue({ code: "custom", message: `not a phone number: ${raw}` });
        return z.NEVER;
    }
    return parsed;
});

export const AddressJson = z.discriminatedUnion("kind", [
    z.object({ kind: z.literal("phone"), value: phone }),
    z.object({ kind: z.literal("email"), value: z.string() }),
    z.object({ kind: z.literal("web_session"), value: z.string() }),
]);

/**
 * A nested row (e.g. from `row_to_json`), mapped like any other row.
 */
export const RowJson = z.record(z.string(), z.unknown());

export const StringsJson = z.array(z.string());
export const InstantsJson = z.array(instant);
export const TextRecordJson = z.record(z.string(), z.string());
export const CatalogFieldsJson = z.record(z.string(), z.union([z.string(), z.number(), z.null()]));
export const NoticesJson = z.array(z.enum(AppointmentNotice));
export const ItemIdsJson = z.array(z.string().transform(CatalogItemId));

export const SessionMeterJson = z.object({
    turns:             z.number(),
    unproductiveTurns: z.number(),
    abusiveTurns:      z.number(),
    costUsd:           z.number(),
    startedAt:         instant,
    closing:           z.object({ reason: z.enum(ClosingReason), turnsLeft: z.number() }).nullable(),
});

export const CallStateJson = z.discriminatedUnion("status", [
    z.object({ status: z.literal("listening"), turns: z.number(), silentTurns: z.number() }),
    z.object({ status: z.literal("responding"), turns: z.number() }),
    z.object({ status: z.literal("transferring"), turns: z.number(), to: phone }),
    z.object({ status: z.literal("ended"), turns: z.number(), reason: z.enum(CallEndReason) }),
]);

export const ClassificationJson = z.object({
    sessionId:      z.string().transform(SessionId),
    outcome:        z.enum(SessionOutcome),
    topic:          z.string(),
    sentiment:      z.enum(Sentiment),
    urgency:        z.enum(Urgency),
    language:       z.enum(Language),
    followupNeeded: z.boolean(),
    gist:           z.string(),
    model:          z.string(),
    classifiedAt:   instant,
});

/**
 * A job's kind-specific fields (its tenant, dedupe key and run time are columns).
 */
export const JobBodyJson = z.discriminatedUnion("kind", [
    z.object({ kind: z.literal("send_sms"), to: phone, text: z.string(), purpose: z.enum(Purpose) }),
    z.object({ kind: z.literal("send_email"), to: z.string(), subject: z.string(), text: z.string(), purpose: z.enum(Purpose) }),
    z.object({ kind: z.literal("notify_staff"), notice: z.enum(StaffNotice), subject: z.string(), text: z.string() }),
    z.object({ kind: z.literal("transcribe_voicemail"), sessionId: z.string().transform(SessionId), recordingUrl: z.string() }),
    z.object({ kind: z.literal("classify_session"), sessionId: z.string().transform(SessionId) }),
    z.object({ kind: z.literal("deliver_transcript"), sessionId: z.string().transform(SessionId) }),
    z.object({ kind: z.literal("send_followup"), followup: z.enum(["confirm", "reminder"]), appointmentId: z.string().transform(AppointmentId) }),
    z.object({ kind: z.literal("cold_followup"), conversationId: z.string().transform(ConversationId), afterMessageAt: z.string() }),
    z.object({ kind: z.literal("record_fixed_costs"), month: z.string() }),
]);
