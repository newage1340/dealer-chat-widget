/**
 * Self-hosted speech-to-text: our own voice-activity detection cuts the call audio into utterances,
 * and each one goes to an OpenAI-compatible /v1/audio/transcriptions server running Whisper on the GPU
 * machine (e.g. speaches or faster-whisper-server). When that server is busy, slow or down, the utterance
 * goes to the backup transcriber (Deepgram) instead, so a caller never waits on the GPU box.
 */

import { errorMessage, UsageRecord, type Language } from "../core/index.ts";
import { MuLaw, Pcm, VoiceActivity } from "./audio.ts";
import { checkStatus, providerFetch, readJson } from "./http.ts";
import { TranscriptionReply } from "./openai-stt.ts";
import type { AudioFormat, BatchTranscriber, Recognition, SpeechContext, SpeechToText } from "./speech.ts";

export type LocalWhisperOptions = {
    readonly baseUrl:        string;
    readonly apiKey:         string;
    readonly model:          string;
    /**
     * Utterances in flight at once before new ones go to the backup.
     */
    readonly maxConcurrency: number;
    /**
     * Longest wait for the local server before giving the utterance to the backup.
     */
    readonly timeoutMs:      number;
    readonly backup:         BatchTranscriber | null;
    readonly silenceMs?:     number;
};

export class LocalWhisperStt implements SpeechToText {
    readonly name = "local-whisper";
    readonly #o: LocalWhisperOptions;
    #inFlight = 0;

    constructor(options: LocalWhisperOptions) {
        this.#o = options;
    }

    async *transcribe(
        audio: AsyncIterable<Uint8Array>,
        format: AudioFormat,
        context: SpeechContext,
        signal?: AbortSignal
    ): AsyncIterable<Recognition>
    {
        const vad = new VoiceActivity({ silenceMs: this.#o.silenceMs ?? 700 });
        const frameSamples = Math.round(format.sampleRate / 50);
        let buffer = new Int16Array(0);
        for await (const chunk of audio) {
            if (signal?.aborted) {
                return;
            }
            const samples = format.encoding === "mulaw" ? MuLaw.decode(chunk) : Pcm.fromBytes(chunk);
            const joined = new Int16Array(buffer.length + samples.length);
            joined.set(buffer);
            joined.set(samples, buffer.length);
            let offset = 0;
            for (; offset + frameSamples <= joined.length; offset += frameSamples) {
                const result = vad.push(joined.subarray(offset, offset + frameSamples), format.sampleRate);
                if (result === "start") {
                    yield { kind: "speech_started" };
                } else if (result !== null) {
                    yield* this.#recognize(result, format.sampleRate, context);
                }
            }
            buffer = joined.slice(offset);
        }
    }

    async *#recognize(samples: Int16Array, sampleRate: number, context: SpeechContext): AsyncIterable<Recognition> {
        const wav = Pcm.wav(samples, sampleRate);
        const seconds = samples.length / sampleRate;
        const language = context.languages.length === 1 ? context.languages[0]! : null;
        if (this.#inFlight < this.#o.maxConcurrency) {
            this.#inFlight++;
            const started = performance.now();
            try {
                const text = await this.#local(wav, language, context.hints);
                yield { kind: "usage", usage: this.#usage(context, seconds, started) };
                if (text) {
                    yield { kind: "final", text, confidence: 1, language };
                }
                return;
            } catch (e) {
                console.warn(`local speech-to-text failed, using the backup: ${errorMessage(e)}`);
            } finally {
                this.#inFlight--;
            }
        }
        if (this.#o.backup === null) {
            return;
        }
        try {
            const result = await this.#o.backup.transcribe(wav, "audio/wav", context);
            yield { kind: "usage", usage: result.usage };
            if (result.text) {
                yield { kind: "final", text: result.text, confidence: 1, language: result.language };
            }
        } catch (e) {
            console.error(`backup speech-to-text failed too: ${errorMessage(e)}`);
        }
    }

    async #local(wav: Uint8Array, language: Language | null, hints: readonly string[]): Promise<string> {
        const body = new FormData();
        body.set("file", new Blob([new Uint8Array(wav)], { type: "audio/wav" }), "utterance.wav");
        body.set("model", this.#o.model);
        if (language !== null) {
            body.set("language", language);
        }
        if (hints.length > 0) {
            // Whisper uses the prompt as context, which nudges it toward these spellings.
            body.set("prompt", hints.slice(0, 40).join(", "));
        }
        const response = await providerFetch(this.name, `${this.#o.baseUrl.replace(/\/$/, "")}/audio/transcriptions`, {
            method:  "POST",
            headers: { Authorization: `Bearer ${this.#o.apiKey || "none"}` },
            body,
        }, { timeoutMs: this.#o.timeoutMs });
        checkStatus(this.name, response);
        const json = await readJson(this.name, response, TranscriptionReply);
        return (json.text ?? "").trim();
    }

    #usage(context: SpeechContext, seconds: number, started: number): UsageRecord {
        return UsageRecord.measured({
            provider:  this.name,
            model:     this.#o.model,
            service:   "stt",
            purpose:   context.purpose,
            units:     { audio_seconds: seconds },
            tenantId:  context.tenantId,
            channel:   "voice",
            startedAt: started,
        });
    }
}
