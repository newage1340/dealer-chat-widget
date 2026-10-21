/**
 * Deepgram's pre-recorded transcription, for voicemails.
 */

import { LanguageTag, type UsageRecord } from "../core/index.ts";
import { ProviderError } from "./errors.ts";
import type { BatchTranscriber, BatchTranscript, SpeechContext } from "./speech.ts";

type DeepgramResponse = {
    readonly metadata?: { readonly duration?: number };
    readonly results?: {
        readonly channels?: readonly {
            readonly detected_language?: string;
            readonly alternatives?: readonly { readonly transcript?: string }[];
        }[];
    };
};

export class DeepgramTranscriber implements BatchTranscriber {
    readonly name = "deepgram";
    readonly #apiKey: string;
    readonly #model: string;

    constructor(apiKey: string, model = "nova-3") {
        this.#apiKey = apiKey;
        this.#model  = model;
    }

    async transcribe(
        audio: Uint8Array,
        mimeType: string,
        context: SpeechContext,
        signal?: AbortSignal
    ): Promise<BatchTranscript>
    {
        const params = new URLSearchParams({ model: this.#model, smart_format: "true", punctuate: "true" });
        if (context.languages.length === 1) {
            params.set("language", context.languages[0]!);
        } else {
            params.set("detect_language", "true");
        }
        const started = performance.now();
        let response: Response;
        try {
            response = await fetch(`https://api.deepgram.com/v1/listen?${params}`, {
                method:  "POST",
                headers: { Authorization: `Token ${this.#apiKey}`, "Content-Type": mimeType },
                body:    audio,
                signal:  signal ?? AbortSignal.timeout(60_000),
            });
        } catch (e) {
            throw new ProviderError(this.name, "unavailable", String(e), { cause: e });
        }
        if (!response.ok) {
            throw new ProviderError(this.name, response.status >= 500 ? "unavailable" : "bad_request", `HTTP ${response.status}`);
        }
        const json = await response.json() as DeepgramResponse;
        const channel = json.results?.channels?.[0];
        const seconds = json.metadata?.duration ?? 0;
        const usage: UsageRecord = {
            provider:   this.name,
            model:      this.#model,
            service:    "stt",
            purpose:    context.purpose,
            occurredAt: new Date(),
            units:      { audio_seconds: seconds },
            costUsd:    null,
            latencyMs:  performance.now() - started,
            tenantId:   context.tenantId,
            channel:    "voice",
        };
        return {
            text:     channel?.alternatives?.[0]?.transcript?.trim() ?? "",
            language: channel?.detected_language !== undefined
                ? LanguageTag.parse(channel.detected_language)
                : context.languages.length === 1
                    ? context.languages[0]!
                    : null,
            usage,
        };
    }
}
