/**
 * Any OpenAI-compatible chat API: OpenAI itself, or a self-hosted server
 * (Ollama, llama.cpp, vLLM) at `baseUrl`.
 */

import OpenAI, {
    APIConnectionError,
    APIConnectionTimeoutError,
    APIError,
    AuthenticationError,
    BadRequestError,
    PermissionDeniedError,
    RateLimitError,
} from "openai";
import type {
    ChatCompletionChunk,
    ChatCompletionCreateParamsStreaming,
    ChatCompletionMessageParam,
    ChatCompletionTool,
} from "openai/resources/chat/completions";

import type { UsageRecord } from "../core/index.ts";
import { ProviderError } from "./errors.ts";
import type {
    ChatMessage,
    FinishReason,
    LlmProvider,
    LlmRequest,
    LlmStreamPart,
    ModelTier,
    ToolCall,
} from "./llm.ts";

export type OpenAiCompatibleOptions = {
    readonly name:    string;
    readonly models:  Readonly<Partial<Record<ModelTier, string>>>;
    readonly apiKey:  string;
    readonly baseUrl: string | null;
};

function toMessage(m: ChatMessage): ChatCompletionMessageParam {
    switch (m.role) {
        case "system":
            return { role: "system", content: m.text };
        case "user":
            return { role: "user", content: m.text };
        case "assistant":
            return m.toolCalls.length === 0
                ? { role: "assistant", content: m.text }
                : {
                    role:       "assistant",
                    content:    m.text || null,
                    tool_calls: m.toolCalls.map(c => ({
                        id:       c.id,
                        type:     "function" as const,
                        function: { name: c.name, arguments: JSON.stringify(c.arguments ?? {}) },
                    })),
                };
        case "tool":
            return { role: "tool", tool_call_id: m.toolCallId, content: m.output };
    }
}

function finishReason(raw: string | null | undefined): FinishReason {
    switch (raw) {
        case "tool_calls":
        case "function_call":
            return "tool_calls";
        case "length":
            return "length";
        default:
            return "stop";
    }
}

function classify(provider: string, e: unknown): ProviderError {
    const message = e instanceof Error
        ? e.message
        : String(e);
    if (e instanceof ProviderError) {
        return e;
    }
    if (e instanceof APIConnectionTimeoutError) {
        return new ProviderError(provider, "timeout", message, { cause: e });
    }
    if (e instanceof APIConnectionError) {
        return new ProviderError(provider, "unavailable", message, { cause: e });
    }
    if (e instanceof RateLimitError) {
        return new ProviderError(provider, "rate_limited", message, { cause: e });
    }
    if (e instanceof AuthenticationError || e instanceof PermissionDeniedError) {
        return new ProviderError(provider, "auth", message, { cause: e });
    }
    if (e instanceof BadRequestError) {
        return new ProviderError(provider, "bad_request", message, { cause: e });
    }
    if (e instanceof APIError && (e.status ?? 500) >= 500) {
        return new ProviderError(provider, "unavailable", message, { cause: e });
    }
    if (e instanceof Error && e.name === "AbortError") {
        return new ProviderError(provider, "timeout", message, { cause: e });
    }
    return new ProviderError(provider, "bad_response", message, { cause: e });
}

function parseArguments(raw: string): unknown {
    if (!raw.trim()) {
        return {};
    }
    try {
        return JSON.parse(raw);
    } catch {
        return { _unparsed: raw };
    }
}

export class OpenAiCompatibleLlm implements LlmProvider {
    readonly name: string;
    readonly #client: OpenAI;
    readonly #models: Readonly<Partial<Record<ModelTier, string>>>;

    constructor(options: OpenAiCompatibleOptions) {
        this.name = options.name;
        this.#models = options.models;
        this.#client = new OpenAI({
            apiKey:     options.apiKey || "not-needed",
            baseURL:    options.baseUrl ?? undefined,
            maxRetries: 0,
        });
    }

    model(tier: ModelTier): string {
        const model = this.#models[tier] ?? this.#models.fast ?? this.#models.capable ?? this.#models.small;
        if (model === undefined) {
            throw new ProviderError(this.name, "bad_request", `no model configured for tier '${tier}'`);
        }
        return model;
    }

    async *stream(request: LlmRequest, signal?: AbortSignal): AsyncIterable<LlmStreamPart> {
        const model = this.model(request.tier);
        const started = performance.now();
        const timeout = AbortSignal.timeout(request.timeoutMs);
        const combined = signal === undefined
            ? timeout
            : AbortSignal.any([signal, timeout]);

        const params: ChatCompletionCreateParamsStreaming = {
            model,
            messages:       request.messages.map(toMessage),
            max_tokens:     request.maxTokens,
            temperature:    request.temperature,
            stream:         true,
            stream_options: { include_usage: true },
        };
        // Local OpenAI-compatible servers may refuse fields they don't know, so only OpenAI gets the cache key.
        if (request.cacheKey !== undefined && this.name === "openai") {
            params.prompt_cache_key = request.cacheKey;
        }
        if (request.tools.length > 0) {
            params.tools = request.tools.map((t): ChatCompletionTool => ({
                type:     "function",
                function: { name: t.name, description: t.description, parameters: { ...t.parameters } },
            }));
        }
        if (request.format.kind === "json_schema") {
            params.response_format = {
                type:        "json_schema",
                json_schema: { name: request.format.name, schema: { ...request.format.schema }, strict: true },
            };
        }

        let chunks: AsyncIterable<ChatCompletionChunk>;
        try {
            chunks = await this.#client.chat.completions.create(params, { signal: combined });
        } catch (e) {
            throw classify(this.name, e);
        }

        const pending = new Map<number, { id: string; name: string; args: string }>();
        let reason: FinishReason = "stop";
        let usage: ChatCompletionChunk["usage"] = null;
        try {
            for await (const chunk of chunks) {
                usage = chunk.usage ?? usage;
                const choice = chunk.choices[0];
                if (choice === undefined) {
                    continue;
                }
                const delta = choice.delta;
                if (delta.content) {
                    yield { kind: "text", delta: delta.content };
                }
                for (const call of delta.tool_calls ?? []) {
                    const entry = pending.get(call.index) ?? { id: "", name: "", args: "" };
                    entry.id   += call.id ?? "";
                    entry.name += call.function?.name ?? "";
                    entry.args += call.function?.arguments ?? "";
                    pending.set(call.index, entry);
                }
                if (choice.finish_reason) {
                    reason = finishReason(choice.finish_reason);
                }
            }
        } catch (e) {
            throw classify(this.name, e);
        }

        for (const [index, entry] of [...pending.entries()].sort((a, b) => a[0] - b[0])) {
            const call: ToolCall = {
                id:        entry.id || `call_${index}`,
                name:      entry.name,
                arguments: parseArguments(entry.args),
            };
            yield { kind: "tool_call", call };
        }
        if (pending.size > 0) {
            reason = "tool_calls";
        }

        const cached = usage?.prompt_tokens_details?.cached_tokens ?? 0;
        const record: UsageRecord = {
            provider:   this.name,
            model,
            service:    "llm",
            purpose:    request.purpose,
            occurredAt: new Date(),
            units:      {
                input_tokens:        Math.max(0, (usage?.prompt_tokens ?? 0) - cached),
                cached_input_tokens: cached,
                output_tokens:       usage?.completion_tokens ?? 0,
            },
            costUsd:    null,
            latencyMs:  performance.now() - started,
            tenantId:   request.tenantId,
            channel:    request.channel,
        };
        yield { kind: "finish", reason, usage: record };
    }
}
