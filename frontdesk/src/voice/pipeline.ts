/**
 * Which voice pipeline calls use, and the speech services it needs.
 *
 * `twilio_gather`: Twilio listens (<Gather>, billed per listen) and speaks (<Say>, billed per character
 *   for neural voices). Nothing to run; the default.
 * `media_streams`: Twilio streams the call audio to us; Deepgram transcribes it live and a TTS provider
 *   speaks back. Several times cheaper per minute, faster replies, and callers can interrupt.
 * `media_streams_local`: the same, but Whisper on our own GPU machine transcribes (and optionally a local
 *   TTS speaks), with Deepgram as the backup when the machine is busy or down.
 */

import type { Language } from "../core/index.ts";
import type { SpeechToText, TextToSpeech } from "../providers/index.ts";

export const VoicePipelineKind = {
    TwilioGather:      "twilio_gather",
    MediaStreams:      "media_streams",
    MediaStreamsLocal: "media_streams_local",
} as const;
export type VoicePipelineKind = (typeof VoicePipelineKind)[keyof typeof VoicePipelineKind];

/**
 * A text-to-speech provider that says which languages it can speak.
 */
export type Voice = TextToSpeech & {
    supports(language: Language): boolean;
};

/**
 * `voices` are tried in order: the first that supports the language speaks, and the next takes over if it fails.
 */
export type VoicePipeline =
    | { readonly kind: "twilio_gather" }
    | { readonly kind: "media_streams" | "media_streams_local";
        readonly stt:    SpeechToText;
        readonly voices: readonly Voice[]
    };

export const VoicePipeline = {
    gather: { kind: "twilio_gather" } as VoicePipeline,

    /**
     * Voices that can speak `language`, best first; the full list when none claims it.
     */
    voicesFor(voices: readonly Voice[], language: Language): readonly Voice[] {
        const able = voices.filter(v => v.supports(language));
        return able.length > 0
            ? able
            : voices;
    },
} as const;
