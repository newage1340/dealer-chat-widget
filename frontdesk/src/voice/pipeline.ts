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

import type { Language, ValueOf } from "../core/index.ts";
import type { SpeechToText, TextToSpeech } from "../providers/index.ts";

export const VoicePipelineKind = {
    TwilioGather:      "twilio_gather",
    MediaStreams:      "media_streams",
    MediaStreamsLocal: "media_streams_local",
} as const;
export type VoicePipelineKind = ValueOf<typeof VoicePipelineKind>;

/**
 * The pipelines where Twilio streams call audio to our WebSocket.
 * `voices` are tried in order: the first that supports the language speaks, and the next takes over if it fails.
 */
export type StreamingPipeline = {
    readonly kind:   typeof VoicePipelineKind.MediaStreams | typeof VoicePipelineKind.MediaStreamsLocal;
    readonly stt:    SpeechToText;
    readonly voices: readonly TextToSpeech[];
};

export type VoicePipeline =
    | { readonly kind: typeof VoicePipelineKind.TwilioGather }
    | StreamingPipeline;

export const VoicePipeline = {
    gather: { kind: VoicePipelineKind.TwilioGather } satisfies VoicePipeline,

    /**
     * Whether calls stream audio to us (rather than Twilio's <Gather> listening for us).
     */
    streams(pipeline: VoicePipeline | undefined): pipeline is StreamingPipeline {
        return pipeline !== undefined
            && pipeline.kind !== VoicePipelineKind.TwilioGather;
    },

    /**
     * Voices that can speak `language`, best first; the full list when none claims it.
     */
    voicesFor(voices: readonly TextToSpeech[], language: Language): readonly TextToSpeech[] {
        const able = voices.filter(v => v.supports(language));
        return able.length > 0
            ? able
            : voices;
    },
} as const;
