/**
 * Work done in the background, after the reply has been returned.
 * Jobs are stored in the same transaction as the events that cause them (an outbox),
 * so a crash never loses an alert and a retried webhook never sends one twice.
 */

import type { PhoneNumber } from "./channel.ts";
import type { AppointmentId, ConversationId, SessionId, TenantId } from "./ids.ts";
import type { Variant } from "./lang.ts";
import type { StaffNotice } from "./tenant.ts";
import type { Purpose } from "./usage.ts";

export type JobBody =
    | { readonly kind: "send_sms";
        readonly to: PhoneNumber;
        readonly text: string;
        readonly purpose: Purpose
    }
    | { readonly kind: "send_email";
        readonly to: string;
        readonly subject: string;
        readonly text: string;
        readonly purpose: Purpose
    }
    | { readonly kind: "notify_staff";
        readonly notice: StaffNotice;
        readonly subject: string;
        readonly text: string
    }
    | { readonly kind: "transcribe_voicemail";
        readonly sessionId: SessionId;
        readonly recordingUrl: string
    }
    | { readonly kind: "classify_session";
        readonly sessionId: SessionId
    }
    | { readonly kind: "deliver_transcript";
        readonly sessionId: SessionId
    }
    | { readonly kind: "send_reminder";
        readonly appointmentId: AppointmentId
    }
    | { readonly kind: "cold_followup";
        readonly conversationId: ConversationId
    };

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
