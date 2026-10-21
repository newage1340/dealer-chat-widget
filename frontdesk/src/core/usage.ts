/**
 * Metering for every external (or self-hosted) service call, so cost decisions use data.
 */

import type { Channel } from "./channel.ts";
import type { TenantId } from "./ids.ts";
import type { ValueOf } from "./lang.ts";

export const ServiceKind = {
    Llm:                "llm",
    SpeechToText:       "stt",
    TextToSpeech:       "tts",
    Messaging:          "messaging",
    Telephony:          "telephony",
    VoiceOrchestration: "voice_orchestration",
    Email:              "email",
} as const;
export type ServiceKind = ValueOf<typeof ServiceKind>;

export const UsageUnit = {
    InputTokens:       "input_tokens",
    CachedInputTokens: "cached_input_tokens",
    OutputTokens:      "output_tokens",
    SmsSegments:       "sms_segments",
    CallSeconds:       "call_seconds",
    AudioSeconds:      "audio_seconds",
    Characters:        "characters",
    Requests:          "requests",
    NumberMonths:      "number_months",
} as const;
export type UsageUnit = ValueOf<typeof UsageUnit>;

/**
 * Why a service was called, so spend that serves no customer is still visible.
 */
export const Purpose = {
    Turn:                "turn",
    BookingConfirmation: "booking_confirmation",
    StaffAlert:          "staff_alert",
    Reminder:            "reminder",
    Followup:            "followup",
    Link:                "link",
    Classification:      "classification",
    Transcript:          "transcript",
    Voicemail:           "voicemail",
    Forwarding:          "forwarding",
    Authoring:           "authoring",
} as const;
export type Purpose = ValueOf<typeof Purpose>;

/**
 * One metered call.
 * `costUsd` is an estimate from the price table; null when the price is unknown.
 */
export type UsageRecord = {
    readonly provider:   string;
    readonly model:      string;
    readonly service:    ServiceKind;
    readonly purpose:    Purpose;
    readonly occurredAt: Date;
    readonly units:      Readonly<Partial<Record<UsageUnit, number>>>;
    readonly costUsd:    number | null;
    readonly latencyMs:  number | null;
    readonly tenantId:   TenantId | null;
    readonly channel:    Channel | null;
};

export const UsageRecord = {
    /**
     * The record of a call that just finished. `startedAt` is a `performance.now()` reading taken before it;
     * the price is filled in later from the price table.
     */
    measured(call: {
        readonly provider:  string;
        readonly model:     string;
        readonly service:   ServiceKind;
        readonly purpose:   Purpose;
        readonly units:     Readonly<Partial<Record<UsageUnit, number>>>;
        readonly tenantId:  TenantId | null;
        readonly channel:   Channel | null;
        readonly startedAt: number;
    }): UsageRecord
    {
        const { startedAt, ...rest } = call;
        return {
            ...rest,
            occurredAt: new Date(),
            costUsd:    null,
            latencyMs:  performance.now() - startedAt,
        };
    },
} as const;
