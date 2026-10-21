/**
 * Work done in the background, after the reply has been returned.
 * Jobs are stored in the same transaction as the events that cause them (an outbox),
 * so a crash never loses an alert and a retried webhook never sends one twice.
 */

import type { PhoneNumber } from "./channel.ts";
import type { AppointmentId, ConversationId, SessionId, TenantId } from "./ids.ts";
import type { Variant } from "./lang.ts";
import type { Purpose } from "./usage.ts";

export type JobBody =
    | { readonly kind: "send_sms";       readonly to: PhoneNumber; readonly text: string; readonly purpose: Purpose }
    | { readonly kind: "notify_staff";   readonly text: string; readonly reason: string }
    | { readonly kind: "summarize_call"; readonly sessionId: SessionId; readonly headline: string }
    | { readonly kind: "send_reminder";  readonly appointmentId: AppointmentId }
    | { readonly kind: "cold_followup";  readonly conversationId: ConversationId };

export type JobKind = JobBody["kind"];

/**
 * `dedupeKey` makes enqueueing idempotent: a second job with the same key is dropped.
 * `runAt` delays a job (reminders, follow-ups); null runs it as soon as a worker is free.
 */
export type JobMeta = {
    readonly tenantId:  TenantId;
    readonly dedupeKey: string;
    readonly runAt:     Date | null;
};

export type Job = JobMeta & JobBody;

export type JobOf<K extends JobKind> = JobMeta & Variant<JobBody, K>;
