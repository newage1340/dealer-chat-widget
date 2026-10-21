/**
 * Streaming speech interfaces, for running the voice pipeline ourselves
 * (self-hosted or per-use vendors) instead of paying a voice platform per minute.
 * Nothing implements these yet; they fix the shape adapters will meet.
 */

import type { Language, Purpose, TenantId, UsageRecord } from "../core/index.ts";

export type AudioFormat = {
    readonly encoding:   "pcm16" | "mulaw";
    readonly sampleRate: number;
};

/**
 * `final` results end a caller utterance; interim ones may still change.
 * `language` is set by recognizers that detect it, null otherwise.
 */
export type Recognition =
    | { readonly kind: "interim";
        readonly text: string
    }
    | { readonly kind: "final";
        readonly text: string;
        readonly confidence: number;
        readonly language: Language | null
    }
    | { readonly kind: "usage";
        readonly usage: UsageRecord
    };

/**
 * `languages` are the languages to listen or speak in;
 * more than one asks a multilingual recognizer to detect which is being spoken.
 */
export type SpeechContext = {
    readonly tenantId:  TenantId;
    readonly purpose:   Purpose;
    readonly languages: readonly Language[];
    readonly hints:     readonly string[];
};

export interface SpeechToText {
    readonly name: string;
    transcribe(
        audio:   AsyncIterable<Uint8Array>,
        format:  AudioFormat,
        context: SpeechContext,
        signal?: AbortSignal,
    ): AsyncIterable<Recognition>;
}

export type SpeechChunk =
    | { readonly kind: "audio";
        readonly bytes: Uint8Array
    }
    | { readonly kind: "usage";
        readonly usage: UsageRecord
    };

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

/**
 * A whole recording transcribed at once (voicemails).
 * `language` is the detected language, or null when it could not be told.
 */
export type BatchTranscript = {
    readonly text:     string;
    readonly language: Language | null;
    readonly usage:    UsageRecord;
};

export interface BatchTranscriber {
    readonly name: string;
    transcribe(
        audio:    Uint8Array,
        mimeType: string,
        context:  SpeechContext,
        signal?:  AbortSignal,
    ): Promise<BatchTranscript>;
}
