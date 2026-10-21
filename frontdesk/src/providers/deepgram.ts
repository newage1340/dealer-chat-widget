/**
 * Deepgram: pre-recorded transcription (voicemails, and the fallback for local Whisper),
 * live transcription for streamed calls, and Aura text-to-speech.
 */

import { AsyncQueue, LanguageTag, type Language, type UsageRecord } from "../core/index.ts";
import { ProviderError } from "./errors.ts";
import type {
    AudioFormat,
    BatchTranscriber,
    BatchTranscript,
    Recognition,
    SpeechChunk,
    SpeechContext,
    SpeechToText,
    TextToSpeech,
} from "./speech.ts";

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

// --- streaming speech-to-text -------------------------------------------------------------

type LiveMessage = {
    readonly type?:         string;
    readonly is_final?:     boolean;
    readonly speech_final?: boolean;
    readonly channel?:      { readonly alternatives?: readonly { readonly transcript?: string; readonly confidence?: number }[] };
};

/**
 * Deepgram's live transcription over a WebSocket: audio in as it arrives, an utterance out
 * when Deepgram decides the caller finished (endpointing), and a signal when they start talking.
 */
export class DeepgramStreamingStt implements SpeechToText {
    readonly name = "deepgram";
    readonly #apiKey: string;
    readonly #model: string;
    readonly #endpointingMs: number;

    constructor(apiKey: string, options: { model?: string; endpointingMs?: number } = {}) {
        this.#apiKey        = apiKey;
        this.#model         = options.model ?? "nova-3";
        this.#endpointingMs = options.endpointingMs ?? 400;
    }

    async *transcribe(
        audio: AsyncIterable<Uint8Array>,
        format: AudioFormat,
        context: SpeechContext,
        signal?: AbortSignal
    ): AsyncIterable<Recognition>
    {
        const params = new URLSearchParams({
            model:            this.#model,
            encoding:         format.encoding === "mulaw" ? "mulaw" : "linear16",
            sample_rate:      String(format.sampleRate),
            channels:         "1",
            interim_results:  "true",
            endpointing:      String(this.#endpointingMs),
            utterance_end_ms: "1000",
            vad_events:       "true",
            smart_format:     "true",
            language:         context.languages.length === 1 ? context.languages[0]! : "multi",
        });
        for (const hint of context.hints.slice(0, 50)) {
            params.append("keyterm", hint);
        }
        const out = new AsyncQueue<Recognition>();
        // Browsers' WebSocket can't set headers; Deepgram takes the key as a subprotocol instead.
        const ws = new WebSocket(`wss://api.deepgram.com/v1/listen?${params}`, ["token", this.#apiKey]);
        ws.binaryType = "arraybuffer";
        let pending = "";
        let confidence = 0;
        let bytes = 0;
        const started = performance.now();
        const flush = () => {
            const text = pending.trim();
            pending = "";
            if (text) {
                out.push({ kind: "final", text, confidence, language: null });
            }
        };
        ws.addEventListener("message", event => {
            let message: LiveMessage;
            try {
                message = JSON.parse(String(event.data)) as LiveMessage;
            } catch {
                return;
            }
            if (message.type === "SpeechStarted") {
                out.push({ kind: "speech_started" });
            } else if (message.type === "UtteranceEnd") {
                flush();
            } else if (message.type === "Results") {
                const alt = message.channel?.alternatives?.[0];
                const text = alt?.transcript ?? "";
                if (message.is_final && text) {
                    pending += ` ${text}`;
                    confidence = alt?.confidence ?? confidence;
                } else if (text) {
                    out.push({ kind: "interim", text: `${pending} ${text}`.trim() });
                }
                if (message.speech_final) {
                    flush();
                }
            }
        });
        const finish = () => {
            flush();
            out.push({ kind: "usage", usage: {
                provider: this.name, model: `${this.#model}-streaming`, service: "stt", purpose: context.purpose, occurredAt: new Date(),
                units: { audio_seconds: bytes / (format.sampleRate * (format.encoding === "mulaw" ? 1 : 2)) },
                costUsd: null, latencyMs: performance.now() - started, tenantId: context.tenantId, channel: "voice",
            } });
            out.close();
        };
        ws.addEventListener("close", finish);
        ws.addEventListener("error", () => ws.close());
        signal?.addEventListener("abort", () => ws.close());

        const opened = new Promise<void>((resolve, reject) => {
            ws.addEventListener("open", () => resolve(), { once: true });
            ws.addEventListener("close", () => reject(new ProviderError(this.name, "unavailable", "live transcription closed before opening")), { once: true });
        });
        const pump = (async () => {
            await opened;
            for await (const chunk of audio) {
                if (ws.readyState !== WebSocket.OPEN) {
                    break;
                }
                bytes += chunk.byteLength;
                ws.send(chunk);
            }
            if (ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({ type: "CloseStream" }));
            }
        })();
        pump.catch(() => ws.close());
        yield* out;
    }
}

// --- text-to-speech -------------------------------------------------------------------------

/**
 * Deepgram Aura voices by language. Aura covers English and Spanish; other languages go to another provider.
 */
export const AURA_VOICES: Readonly<Partial<Record<Language, string>>> = {
    en: "aura-2-thalia-en",
    es: "aura-2-celeste-es",
};

/**
 * Deepgram Aura text-to-speech, one request per sentence, streamed back as phone audio.
 * `voice` is an Aura model name; "" picks the language's default from AURA_VOICES.
 */
export class DeepgramTts implements TextToSpeech {
    readonly name = "deepgram";
    readonly #apiKey: string;

    constructor(apiKey: string) {
        this.#apiKey = apiKey;
    }

    supports(language: Language): boolean {
        return AURA_VOICES[language] !== undefined;
    }

    async *synthesize(
        text: AsyncIterable<string>,
        voice: string,
        format: AudioFormat,
        context: SpeechContext,
        signal?: AbortSignal
    ): AsyncIterable<SpeechChunk>
    {
        const model = voice || AURA_VOICES[context.languages[0] ?? "en"] || AURA_VOICES.en!;
        const params = new URLSearchParams({
            model,
            encoding:    format.encoding === "mulaw" ? "mulaw" : "linear16",
            sample_rate: String(format.sampleRate),
            container:   "none",
        });
        for await (const piece of text) {
            if (!piece.trim()) {
                continue;
            }
            const started = performance.now();
            let response: Response;
            try {
                response = await fetch(`https://api.deepgram.com/v1/speak?${params}`, {
                    method:  "POST",
                    headers: { Authorization: `Token ${this.#apiKey}`, "Content-Type": "application/json" },
                    body:    JSON.stringify({ text: piece }),
                    signal:  signal ?? AbortSignal.timeout(15_000),
                });
            } catch (e) {
                throw new ProviderError(this.name, "unavailable", String(e), { cause: e });
            }
            if (!response.ok || response.body === null) {
                throw new ProviderError(this.name, response.status >= 500 ? "unavailable" : "bad_request", `speak HTTP ${response.status}`);
            }
            for await (const chunk of response.body) {
                yield { kind: "audio", bytes: chunk };
            }
            yield { kind: "usage", usage: {
                provider: this.name, model, service: "tts", purpose: context.purpose, occurredAt: new Date(),
                units: { characters: piece.length }, costUsd: null, latencyMs: performance.now() - started,
                tenantId: context.tenantId, channel: "voice",
            } };
        }
    }
}
