/**
 * Stand-ins for external services: scripted for tests, logging for running without credentials.
 */

import type { UsageRecord } from "../core/index.ts";
import type { EmailProvider, EmailReceipt, OutboundEmail } from "./email.ts";
import type { LlmProvider, LlmRequest, LlmStreamPart, ToolCall } from "./llm.ts";
import type { MessagingProvider, OutboundSms, SendReceipt } from "./messaging.ts";
import type { BatchTranscriber, BatchTranscript, SpeechContext } from "./speech.ts";

/**
 * What a scripted model does on one call: say `text`, then call `tools`.
 */
export type ScriptedReply = {
    readonly text:  string;
    readonly tools: readonly Omit<ToolCall, "id">[];
};

function usage(
    provider: string,
    service: UsageRecord["service"],
    purpose: UsageRecord["purpose"],
    units: UsageRecord["units"]
): UsageRecord
{
    return {
        provider,
        model:      "fake",
        service,
        purpose,
        occurredAt: new Date(),
        units,
        costUsd:    null,
        latencyMs:  0,
        tenantId:   null,
        channel:    null,
    };
}

/**
 * A model that answers from a script. The text streams word by word, like a real one.
 */
export class FakeLlm implements LlmProvider {
    readonly name = "fake";
    readonly requests: LlmRequest[] = [];
    readonly #script: (request: LlmRequest) => ScriptedReply | string;

    constructor(script: (request: LlmRequest) => ScriptedReply | string) {
        this.#script = script;
    }

    async *stream(request: LlmRequest): AsyncIterable<LlmStreamPart> {
        this.requests.push(request);
        const reply = this.#script(request);
        const { text, tools } = typeof reply === "string"
            ? { text: reply, tools: [] }
            : reply;
        for (const word of text.split(/(?<= )/)) {
            if (word) {
                yield { kind: "text", delta: word };
            }
        }
        for (const [i, call] of tools.entries()) {
            yield { kind: "tool_call", call: { id: `call_${this.requests.length}_${i}`, ...call } };
        }
        yield {
            kind:   "finish",
            reason: tools.length > 0
                ? "tool_calls"
                : "stop",
            usage:  { ...usage(this.name, "llm", request.purpose, { input_tokens: 100, output_tokens: 20 }), tenantId: request.tenantId },
        };
    }
}

export class LoggingMessaging implements MessagingProvider {
    readonly name = "log-messaging";
    readonly sent: OutboundSms[] = [];
    readonly #log: boolean;

    constructor(log = false) {
        this.#log = log;
    }

    async send(message: OutboundSms): Promise<SendReceipt> {
        this.sent.push(message);
        if (this.#log) {
            console.info(`[sms] ${message.from} -> ${message.to}: ${message.text}`);
        }
        return {
            providerMessageId: `SM${this.sent.length}`,
            usage:             { ...usage(this.name, "messaging", message.purpose, { sms_segments: 1 }), tenantId: message.tenantId },
        };
    }
}

export class LoggingEmail implements EmailProvider {
    readonly name = "log-email";
    readonly sent: OutboundEmail[] = [];
    readonly #log: boolean;

    constructor(log = false) {
        this.#log = log;
    }

    async send(email: OutboundEmail): Promise<EmailReceipt> {
        this.sent.push(email);
        if (this.#log) {
            console.info(`[email] ${email.to.join(", ")}: ${email.subject}\n${email.text}`);
        }
        return {
            providerMessageId: `EM${this.sent.length}`,
            usage:             { ...usage(this.name, "email", email.purpose, { requests: 1 }), tenantId: email.tenantId },
        };
    }
}

export class FakeTranscriber implements BatchTranscriber {
    readonly name = "fake-stt";
    readonly #text: string;

    constructor(text: string) {
        this.#text = text;
    }

    async transcribe(_audio: Uint8Array, _mimeType: string, context: SpeechContext): Promise<BatchTranscript> {
        return {
            text:     this.#text,
            language: context.languages[0] ?? null,
            usage:    { ...usage(this.name, "stt", context.purpose, { audio_seconds: 10 }), tenantId: context.tenantId },
        };
    }
}
