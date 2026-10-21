/**
 * Streaming speech interfaces, for running the voice pipeline ourselves
 * (self-hosted or per-use vendors) instead of paying a voice platform per minute.
 * Nothing implements these yet; they fix the shape adapters will meet.
 */

import type { Purpose, TenantId, UsageRecord } from "../core/index.ts";

export type AudioFormat = {
    readonly encoding:   "pcm16" | "mulaw";
    readonly sampleRate: number;
};

/**
 * `final` transcripts end a caller utterance; interim ones may still change.
 */
export type Transcript =
    | { readonly kind: "interim"; readonly text: string }
    | { readonly kind: "final";   readonly text: string; readonly confidence: number }
    | { readonly kind: "usage";   readonly usage: UsageRecord };

export type SpeechContext = {
    readonly tenantId: TenantId;
    readonly purpose:  Purpose;
    readonly hints:    readonly string[];
};

export interface SpeechToText {
    readonly name: string;
    transcribe(
        audio:   AsyncIterable<Uint8Array>,
        format:  AudioFormat,
        context: SpeechContext,
        signal?: AbortSignal,
    ): AsyncIterable<Transcript>;
}

export type SpeechChunk =
    | { readonly kind: "audio"; readonly bytes: Uint8Array }
    | { readonly kind: "usage"; readonly usage: UsageRecord };

export interface TextToSpeech {
    readonly name: string;
    synthesize(
        text:    AsyncIterable<string>,
        voice:   string,
        format:  AudioFormat,
        context: SpeechContext,
        signal?: AbortSignal,
    ): AsyncIterable<SpeechChunk>;
}
