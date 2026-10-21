/**
 * Text-to-speech from any OpenAI-compatible /v1/audio/speech endpoint: OpenAI itself (gpt-4o-mini-tts,
 * many languages) or a self-hosted server such as Kokoro-FastAPI on the GPU machine.
 * Audio comes back as 24 kHz 16-bit PCM and is converted to 8 kHz mu-law for the phone line.
 */

import type { Language } from "../core/index.ts";
import { MuLaw, Pcm } from "./audio.ts";
import { ProviderError } from "./errors.ts";
import type { AudioFormat, SpeechChunk, SpeechContext, TextToSpeech } from "./speech.ts";

const SOURCE_RATE = 24_000;
const TIMEOUT_MS = 8_000;

export type OpenAiTtsOptions = {
    readonly name:      string;
    readonly baseUrl:   string;
    readonly apiKey:    string;
    readonly model:     string;
    readonly voice:     string;
    readonly languages: readonly Language[] | "all";
};

export class OpenAiCompatibleTts implements TextToSpeech {
    readonly name: string;
    readonly #o: OpenAiTtsOptions;

    constructor(options: OpenAiTtsOptions) {
        this.name = options.name;
        this.#o = options;
    }

    supports(language: Language): boolean {
        return this.#o.languages === "all" || this.#o.languages.includes(language);
    }

    async *synthesize(
        text: AsyncIterable<string>,
        voice: string,
        format: AudioFormat,
        context: SpeechContext,
        signal?: AbortSignal
    ): AsyncIterable<SpeechChunk>
    {
        for await (const piece of text) {
            if (!piece.trim()) {
                continue;
            }
            const started = performance.now();
            let response: Response;
            try {
                response = await fetch(`${this.#o.baseUrl.replace(/\/$/, "")}/audio/speech`, {
                    method:  "POST",
                    headers: { Authorization: `Bearer ${this.#o.apiKey || "none"}`, "Content-Type": "application/json" },
                    body:    JSON.stringify({ model: this.#o.model, voice: voice || this.#o.voice, input: piece, response_format: "pcm" }),
                    // On a call a slow voice must fail fast so the next one takes over; barge-in can cancel sooner.
                    signal:  signal === undefined ? AbortSignal.timeout(TIMEOUT_MS) : AbortSignal.any([signal, AbortSignal.timeout(TIMEOUT_MS)]),
                });
            } catch (e) {
                throw new ProviderError(this.name, "unavailable", String(e), { cause: e });
            }
            if (!response.ok || response.body === null) {
                throw new ProviderError(this.name, response.status >= 500 ? "unavailable" : "bad_request", `speech HTTP ${response.status}`);
            }
            // Convert as it streams; keep leftover bytes so samples and the 3:1 resample stay aligned.
            let carry = new Uint8Array(0);
            const step = 2 * (SOURCE_RATE / format.sampleRate);
            for await (const chunk of response.body) {
                const joined = new Uint8Array(carry.length + chunk.length);
                joined.set(carry);
                joined.set(chunk, carry.length);
                const usable = joined.length - (joined.length % step);
                carry = joined.slice(usable);
                if (usable === 0) {
                    continue;
                }
                const samples = Pcm.downsample(Pcm.fromBytes(joined.subarray(0, usable)), SOURCE_RATE, format.sampleRate);
                yield { kind: "audio", bytes: format.encoding === "mulaw" ? MuLaw.encode(samples) : new Uint8Array(samples.buffer) };
            }
            yield { kind: "usage", usage: {
                provider: this.name, model: this.#o.model, service: "tts", purpose: context.purpose, occurredAt: new Date(),
                units: { characters: piece.length }, costUsd: null, latencyMs: performance.now() - started,
                tenantId: context.tenantId, channel: "voice",
            } };
        }
    }
}
