/**
 * One phone call over Twilio Media Streams: caller audio in, assistant audio out, on a WebSocket.
 *
 * Speech-to-text runs continuously. When the caller finishes a sentence the turn engine replies (streaming),
 * and each finished sentence of the reply is synthesized and played while the next is still being written,
 * so the caller hears the first words quickly. If the caller talks over the assistant, playback stops
 * (barge-in). Silence gets the tenant's "still there?" prompts, then a goodbye.
 *
 * Transfers and hang-ups: the reply plays out, the directive is left in PENDING_DIRECTIVES, and the socket
 * closes; Twilio then follows the <Redirect> after <Connect>, which reads the directive (see voice.ts).
 */

import {
    Address,
    AsyncQueue,
    errorMessage,
    LanguageGuess,
    Tenant,
    type CallDirective,
    type Inbound,
    type Language,
    type UsageRecord,
} from "../core/index.ts";
import type { DomainPack } from "../domains/index.ts";
import type { TurnEngine } from "../pipeline/engine.ts";
import type { Services } from "../pipeline/services.ts";
import { Speech } from "../prompts/speech.ts";
import { STRINGS, say } from "../prompts/strings.ts";
import type { AudioFormat, SpeechContext, TextToSpeech } from "../providers/index.ts";
import { VoicePipeline, type StreamingPipeline } from "./pipeline.ts";

const PHONE_AUDIO: AudioFormat = { encoding: "mulaw", sampleRate: 8000 };

/**
 * Bytes of mu-law per Twilio media message: 20 ms at 8 kHz.
 */
const FRAME_BYTES = 160;

/**
 * What to do once a streamed call's socket closes, by call id. Read (and removed) by /voice/after.
 */
export const PENDING_DIRECTIVES: Map<string, CallDirective> = new Map();

/**
 * Messages Twilio sends on a media stream (only the fields we use).
 */
type TwilioMessage =
    | { readonly event: "connected" }
    | { readonly event: "start";
        readonly start: {
            readonly streamSid: string;
            readonly callSid: string;
            readonly customParameters?: Readonly<Record<string, string>>
        }
    }
    | { readonly event: "media";
        readonly media: {
            readonly payload: string;
            readonly track?: string
        }
    }
    | { readonly event: "mark";
        readonly mark: { readonly name: string }
    }
    | { readonly event: "stop" }
    | { readonly event: "dtmf" };

export type StreamTransport = {
    send(message: object): void;
    close(): void;
};

export type StreamDeps = {
    readonly services: Services;
    readonly engine:   TurnEngine;
    readonly pipeline: StreamingPipeline;
};

/**
 * Splits streamed text into sentences worth speaking on their own.
 */
export class SentenceSplitter {
    #buffer = "";

    push(delta: string): string[] {
        this.#buffer += delta;
        const out: string[] = [];
        for (;;) {
            const m = /[.!?。！？]+["')\]]*\s+/.exec(this.#buffer);
            if (m === null) {
                return out;
            }
            const end = m.index + m[0].length;
            const sentence = this.#buffer.slice(0, end).trim();
            // Very short pieces ("Okay.") sound choppy alone; keep them with the next sentence.
            if (sentence.length < 12 && this.#buffer.length > end) {
                const next = /[.!?。！？]+["')\]]*\s+/.exec(this.#buffer.slice(end));
                if (next === null) {
                    return out;
                }
                const both = end + next.index + next[0].length;
                out.push(this.#buffer.slice(0, both).trim());
                this.#buffer = this.#buffer.slice(both);
                continue;
            }
            out.push(sentence);
            this.#buffer = this.#buffer.slice(end);
        }
    }

    flush(): string {
        const rest = this.#buffer.trim();
        this.#buffer = "";
        return rest;
    }
}

export class StreamCall {
    readonly #d: StreamDeps;
    readonly #t: StreamTransport;
    #streamSid = "";
    #callId = "";
    #caller: Address | null = null;
    #forwardedFrom: string | null = null;
    #tenant: Tenant | null = null;
    #pack: DomainPack | null = null;
    #language: Language = "en";
    #audio: AsyncQueue<Uint8Array> | null = null;
    #speech: AbortController | null = null;
    #marks = new Map<string, () => void>();
    #markSeq = 0;
    #busy = false;
    #queued: string[] = [];
    #silence: NodeJS.Timeout | null = null;
    #silentPrompts = 0;
    #turns = 0;
    #usage: UsageRecord[] = [];
    #startedAt = 0;
    #closed = false;

    constructor(deps: StreamDeps, transport: StreamTransport) {
        this.#d = deps;
        this.#t = transport;
    }

    async handle(raw: string): Promise<void> {
        let message: TwilioMessage;
        try {
            message = JSON.parse(raw) as TwilioMessage;
        } catch {
            return;
        }
        switch (message.event) {
            case "start":
                await this.#start(message.start);
                return;
            case "media":
                if (message.media.track === undefined || message.media.track === "inbound") {
                    this.#audio?.push(Buffer.from(message.media.payload, "base64"));
                }
                return;
            case "mark": {
                const done = this.#marks.get(message.mark.name);
                this.#marks.delete(message.mark.name);
                done?.();
                return;
            }
            case "stop":
                await this.finish();
                return;
            default:
                return;
        }
    }

    /**
     * The call is over (Twilio stopped the stream or the socket closed): stop listening and meter the call.
     */
    async finish(): Promise<void> {
        if (this.#closed) {
            return;
        }
        this.#closed = true;
        this.#clearSilence();
        this.#speech?.abort();
        this.#audio?.close();
        for (const done of this.#marks.values()) {
            done();
        }
        this.#marks.clear();
        if (this.#tenant === null) {
            return;
        }
        const seconds = (performance.now() - this.#startedAt) / 1000;
        const records: UsageRecord[] = [
            {
                provider: "twilio-media-streams", model: "", service: "telephony", purpose: "turn", occurredAt: new Date(),
                units: { call_seconds: seconds }, costUsd: null, latencyMs: null, tenantId: this.#tenant.id, channel: "voice",
            },
            ...this.#usage,
        ];
        for (const record of records) {
            await this.#d.services.store.usage.record(record).catch(e => console.error("could not record stream usage:", e));
        }
    }

    // --- setup ---------------------------------------------------------------------------

    async #start(start: Extract<TwilioMessage, { event: "start" }>["start"]): Promise<void> {
        const { store, registry } = this.#d.services;
        const params = start.customParameters ?? {};
        this.#streamSid = start.streamSid;
        this.#callId = params["call"] || start.callSid;
        this.#caller = Address.phone(params["from"] ?? "") ?? Address.webSession(`call-${this.#callId}`);
        this.#forwardedFrom = params["forwardedFrom"] || null;
        this.#startedAt = performance.now();
        const tenant = await store.tenants.byId((params["tenant"] ?? "") as Tenant["id"]);
        const session = tenant === null
            ? null
            : await store.conversations.findSession(tenant.id, { channel: "voice", externalId: this.#callId, vendor: "twilio", handling: "assistant" });
        if (tenant === null || session === null) {
            console.warn(`media stream for unknown call ${this.#callId}`);
            this.#t.close();
            return;
        }
        this.#tenant = tenant;
        this.#pack = registry.get(tenant.domain);
        this.#language = session.language;
        this.#listen();
        const greeting = [...await store.conversations.sessionMessages(session.id)].reverse().find(m => m.role === "assistant");
        // Don't hold up the message loop: the marks that say the greeting finished arrive on it.
        void this.#say(greeting?.text ?? say(STRINGS.reprompt, this.#language))
            .then(() => this.#armSilence())
            .catch(e => console.error(`greeting failed on call ${this.#callId}:`, e));
    }

    #context(): SpeechContext {
        return {
            tenantId:  this.#tenant!.id,
            purpose:   "turn",
            languages: [this.#language],
            hints:     this.#pack?.voice.speechHints ?? [],
        };
    }

    /**
     * Start (or restart, after a language change) speech-to-text on the incoming audio.
     */
    #listen(): void {
        this.#audio?.close();
        const audio = new AsyncQueue<Uint8Array>();
        this.#audio = audio;
        const recognitions = this.#d.pipeline.stt.transcribe(audio, PHONE_AUDIO, this.#context());
        void (async () => {
            try {
                for await (const r of recognitions) {
                    if (this.#closed) {
                        break;
                    }
                    switch (r.kind) {
                        case "speech_started":
                            this.#bargeIn();
                            break;
                        case "final":
                            this.#heard(r.text);
                            break;
                        case "usage":
                            this.#usage.push(r.usage);
                            break;
                        case "interim":
                            break;
                    }
                }
            } catch (e) {
                console.error(`speech-to-text failed on call ${this.#callId}:`, e);
            }
        })();
    }

    // --- turns -----------------------------------------------------------------------------

    #heard(text: string): void {
        this.#clearSilence();
        this.#silentPrompts = 0;
        if (this.#busy) {
            this.#queued.push(text);
            return;
        }
        void this.#turn(text).catch(e => console.error(`stream turn failed on call ${this.#callId}:`, e));
    }

    async #turn(text: string): Promise<void> {
        this.#busy = true;
        const tenant = this.#tenant!;
        const inbound: Inbound = {
            channel:           "voice",
            sender:            this.#caller!,
            text,
            receivedAt:        this.#d.services.clock(),
            dedupeKey:         `${this.#callId}:${++this.#turns}:${text.slice(0, 40)}`,
            externalSessionId: this.#callId,
            confidence:        null,
            language:          null,
            forwardedFrom:     this.#forwardedFrom as Inbound["forwardedFrom"],
        };
        const sentences = new AsyncQueue<string>();
        const splitter = new SentenceSplitter();
        const playing = this.#play(sentences);
        let directive: CallDirective = { kind: "continue" };
        let language = this.#language;
        try {
            for await (const part of this.#d.engine.run(inbound, tenant, { vendor: "twilio" })) {
                if (part.kind === "text") {
                    splitter.push(part.delta).forEach(s => sentences.push(s));
                } else {
                    directive = part.result.outcome.directive;
                    language = part.result.session.language;
                }
            }
        } catch (e) {
            // Say whatever was ready and keep listening; a turn that fails must not leave the call stuck as busy.
            console.error(`stream turn failed on call ${this.#callId}:`, e);
        } finally {
            const rest = splitter.flush();
            if (rest) {
                sentences.push(rest);
            }
            sentences.close();
        }
        await playing;
        if (language !== this.#language) {
            this.#language = language;
            this.#listen();
        }
        this.#busy = false;
        if (directive.kind !== "continue") {
            this.#hangUp(directive);
            return;
        }
        if (this.#queued.length > 0) {
            const next = this.#queued.splice(0).join(" ");
            await this.#turn(next);
            return;
        }
        this.#armSilence();
    }

    // --- speaking ----------------------------------------------------------------------------

    /**
     * Speak one piece of fixed text and wait until the caller has heard it.
     */
    async #say(text: string): Promise<void> {
        const q = new AsyncQueue<string>();
        q.push(text);
        q.close();
        await this.#play(q);
    }

    /**
     * Synthesize and send each sentence as it arrives, then wait for Twilio to finish playing it.
     * The voice is chosen from the first sentence's language, so a reply in a new language
     * isn't read by the old language's voice.
     */
    async #play(sentences: AsyncIterable<string>): Promise<void> {
        const controller = new AbortController();
        this.#speech = controller;
        let voices: readonly TextToSpeech[] | null = null;
        let language = this.#language;
        try {
            for await (const raw of sentences) {
                if (controller.signal.aborted || this.#closed) {
                    continue;
                }
                const text = Speech.forStreaming(raw, this.#pack!);
                if (!text.trim()) {
                    continue;
                }
                if (voices === null) {
                    const written = LanguageGuess.of(text);
                    language = written !== null && Tenant.offers(this.#tenant!, written) ? written : this.#language;
                    voices = VoicePipeline.voicesFor(this.#d.pipeline.voices, language);
                }
                await this.#synthesize(text, voices, language, controller.signal);
            }
            if (!controller.signal.aborted && !this.#closed) {
                await this.#mark();
            }
        } finally {
            if (this.#speech === controller) {
                this.#speech = null;
            }
        }
    }

    /**
     * One sentence through the first voice that works; a voice that fails before any audio hands over to the next.
     */
    async #synthesize(
        text: string,
        voices: readonly TextToSpeech[],
        language: Language,
        signal: AbortSignal
    ): Promise<void> {
        for (const voice of voices) {
            let sent = false;
            try {
                const once = new AsyncQueue<string>();
                once.push(text);
                once.close();
				const chunks = voice.synthesize(
				    once,
				    "",
				    PHONE_AUDIO,
				    { ...this.#context(), languages: [language] },
				    signal
				);
                for await (const chunk of chunks) {
                    if (signal.aborted || this.#closed) {
                        return;
                    }
                    if (chunk.kind === "usage") {
                        this.#usage.push(chunk.usage);
                        continue;
                    }
                    sent = true;
                    for (let i = 0; i < chunk.bytes.length; i += FRAME_BYTES) {
                        this.#t.send({
                            event: "media",
                            streamSid: this.#streamSid,
                            media: { payload: Buffer.from(chunk.bytes.subarray(i, i + FRAME_BYTES)).toString("base64") },
                        });
                    }
                }
                return;
            } catch (e) {
                if (signal.aborted || sent) {
                    return;
                }
                console.warn(`${voice.name} could not speak on call ${this.#callId}, trying the next voice: ${errorMessage(e)}`);
            }
        }
    }

    /**
     * Resolves when Twilio has played everything sent so far (it echoes our mark back), or on barge-in.
     */
    #mark(): Promise<void> {
        const name = `m${++this.#markSeq}`;
        const played = new Promise<void>(resolve => this.#marks.set(name, resolve));
        this.#t.send({
            event: "mark",
            streamSid: this.#streamSid,
            mark: { name }
        });
        return played;
    }

    /**
     * The caller started talking: stop the assistant mid-sentence and drop what Twilio has buffered.
     */
    #bargeIn(): void {
        this.#clearSilence();
        if (this.#speech === null) {
            return;
        }
        this.#speech.abort();
        this.#t.send({ event: "clear", streamSid: this.#streamSid });
        for (const done of this.#marks.values()) {
            done();
        }
        this.#marks.clear();
    }

    // --- silence and endings ----------------------------------------------------------------

    #armSilence(): void {
        this.#clearSilence();
        if (this.#closed || this.#tenant === null) {
            return;
        }
        const calls = this.#tenant.settings.calls;
        this.#silence = setTimeout(() => {
            void (async () => {
                if (this.#busy || this.#closed) {
                    return;
                }
                if (this.#silentPrompts < calls.stillTherePrompts) {
                    this.#silentPrompts++;
                    await this.#say(say(STRINGS.stillThere, this.#language));
                    this.#armSilence();
                    return;
                }
                await this.#say(say(STRINGS.silenceGoodbye, this.#language));
                this.#hangUp({ kind: "end" });
            })();
        }, calls.silenceSeconds * 1000);
    }

    #clearSilence(): void {
        if (this.#silence !== null) {
            clearTimeout(this.#silence);
            this.#silence = null;
        }
    }

    /**
     * Leave the transfer or hang-up for /voice/after and close the stream; Twilio moves on to the <Redirect>.
     */
    #hangUp(directive: CallDirective): void {
        PENDING_DIRECTIVES.set(this.#callId, directive);
        this.#t.close();
        void this.finish();
    }
}
