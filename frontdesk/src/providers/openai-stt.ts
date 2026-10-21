/**
 * Whole-recording transcription through an OpenAI-compatible /v1/audio/transcriptions endpoint:
 * OpenAI (gpt-4o-mini-transcribe, about $0.003 a minute) for voicemails when Deepgram isn't set up,
 * or the same API on a self-hosted Whisper server.
 */

import { z } from "zod";

import { LanguageTag, UsageRecord } from "../core/index.ts";
import { checkStatus, providerFetch, readJson } from "./http.ts";
import type { BatchTranscriber, BatchTranscript, SpeechContext } from "./speech.ts";

/**
 * What /audio/transcriptions returns (`language` and `duration` only from some servers).
 */
export const TranscriptionReply = z.object({
    text:     z.string().optional(),
    language: z.string().optional(),
    duration: z.number().optional(),
});

/**
 * Audio length when the API doesn't report it: exact for WAV, and from the bitrate Twilio records MP3 at (32 kbps) otherwise.
 */
function estimateSeconds(audio: Uint8Array): number {
    const text = new TextDecoder().decode(audio.subarray(0, 4));
    if (text === "RIFF" && audio.length > 44) {
        const view = new DataView(audio.buffer, audio.byteOffset, audio.byteLength);
        const byteRate = view.getUint32(28, true);
        return byteRate > 0 ? (audio.length - 44) / byteRate : 0;
    }
    return (audio.length * 8) / 32_000;
}

export class OpenAiTranscriber implements BatchTranscriber {
    readonly name: string;
    readonly #baseUrl: string;
    readonly #apiKey: string;
    readonly #model: string;

    constructor(options: { readonly name?: string; readonly baseUrl?: string; readonly apiKey: string; readonly model?: string }) {
        this.name     = options.name ?? "openai-stt";
        this.#baseUrl = (options.baseUrl ?? "https://api.openai.com/v1").replace(/\/$/, "");
        this.#apiKey  = options.apiKey;
        this.#model   = options.model ?? "gpt-4o-mini-transcribe";
    }

    async transcribe(audio: Uint8Array, mimeType: string, context: SpeechContext, signal?: AbortSignal): Promise<BatchTranscript> {
        const extension = mimeType.includes("wav") ? "wav" : mimeType.includes("mpeg") || mimeType.includes("mp3") ? "mp3" : "audio";
        const body = new FormData();
        body.set("file", new Blob([new Uint8Array(audio)], { type: mimeType }), `recording.${extension}`);
        body.set("model", this.#model);
        body.set("response_format", "json");
        if (context.languages.length === 1) {
            body.set("language", context.languages[0]!);
        }
        const started = performance.now();
        const response = await providerFetch(this.name, `${this.#baseUrl}/audio/transcriptions`, {
            method:  "POST",
            headers: { Authorization: `Bearer ${this.#apiKey}` },
            body,
        }, { timeoutMs: 60_000, signal });
        checkStatus(this.name, response);
        const json = await readJson(this.name, response, TranscriptionReply);
        const usage = UsageRecord.measured({
            provider:  this.name,
            model:     this.#model,
            service:   "stt",
            purpose:   context.purpose,
            units:     { audio_seconds: json.duration ?? estimateSeconds(audio) },
            tenantId:  context.tenantId,
            channel:   "voice",
            startedAt: started,
        });
        return {
            text:     (json.text ?? "").trim(),
            language: json.language !== undefined ? LanguageTag.parse(json.language) : context.languages[0] ?? null,
            usage,
        };
    }
}
