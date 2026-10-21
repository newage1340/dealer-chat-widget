/**
 * The vendor-neutral language model interface.
 *
 * Every provider streams, and every provider supports tools:
 * the model's spoken or written reply arrives as text deltas,
 * and anything machine-readable (bookings, profile details, call control) arrives as tool calls.
 * That split is what lets voice replies stream straight to text-to-speech.
 */

import type { Channel, Purpose, TenantId, UsageRecord, ValueOf } from "../core/index.ts";

/**
 * `Small` is for narrow, high-volume jobs with short outputs (classification, language detection),
 * the first candidates for a self-hosted model.
 */
export const ModelTier = {
    Small:   "small",
    Fast:    "fast",
    Capable: "capable",
} as const;
export type ModelTier = ValueOf<typeof ModelTier>;

export type ToolCall = {
    readonly id:        string;
    readonly name:      string;
    readonly arguments: unknown;
};

/**
 * `cacheable` marks a system message as part of the stable prompt prefix
 * (base rules, domain pack, business info), so providers that support explicit caching can mark it.
 */
export type ChatMessage =
    | { readonly role: "system";
        readonly text: string;
        readonly cacheable: boolean
    }
    | { readonly role: "user";
        readonly text: string
    }
    | { readonly role: "assistant";
        readonly text: string;
        readonly toolCalls: readonly ToolCall[]
    }
    | { readonly role: "tool";
        readonly toolCallId: string;
        readonly output: string
    };

/**
 * A tool as the model sees it. `parameters` is a JSON Schema object.
 */
export type ToolDefinition = {
    readonly name:        string;
    readonly description: string;
    readonly parameters:  Readonly<Record<string, unknown>>;
};

/**
 * `json_schema` constrains the whole reply to a JSON document matching `schema` (structured output).
 */
export type ResponseFormat =
    | { readonly kind: "text" }
    | { readonly kind: "json_schema";
        readonly name: string;
        readonly schema: Readonly<Record<string, unknown>>
    };

export type LlmRequest = {
    readonly messages:    readonly ChatMessage[];
    readonly tools:       readonly ToolDefinition[];
    readonly format:      ResponseFormat;
    readonly tier:        ModelTier;
    readonly maxTokens:   number;
    readonly temperature: number;
    readonly timeoutMs:   number;
    readonly purpose:     Purpose;
    readonly tenantId:    TenantId | null;
    readonly channel:     Channel | null;
};

export const FinishReason = {
    Stop:      "stop",
    ToolCalls: "tool_calls",
    Length:    "length",
} as const;
export type FinishReason = ValueOf<typeof FinishReason>;

/**
 * One piece of a streamed response.
 * A stream is any number of `text` and `tool_call` parts followed by exactly one `finish`.
 */
export type LlmStreamPart =
    | { readonly kind: "text";      readonly delta: string }
    | { readonly kind: "tool_call"; readonly call: ToolCall }
    | { readonly kind: "finish";    readonly reason: FinishReason; readonly usage: UsageRecord };

export interface LlmProvider {
    readonly name: string;

    /**
     * Throws ProviderError when the request cannot start or the stream breaks.
     */
    stream(request: LlmRequest, signal?: AbortSignal): AsyncIterable<LlmStreamPart>;
}

/**
 * A fully collected response, for callers that do not stream (summaries, authoring).
 */
export type LlmResponse = {
    readonly text:      string;
    readonly toolCalls: readonly ToolCall[];
    readonly reason:    FinishReason;
    readonly usage:     UsageRecord;
};

export const LlmProvider = {
    async complete(
        provider: LlmProvider,
        request: LlmRequest,
        signal?: AbortSignal
    ): Promise<LlmResponse>
	{
        let text = "";
        const toolCalls: ToolCall[] = [];
        for await (const part of provider.stream(request, signal)) {
            switch (part.kind) {
                case "text":
                    text += part.delta;
                    break;
                case "tool_call":
                    toolCalls.push(part.call);
                    break;
                case "finish":
                    return {
                        text,
                        toolCalls,
                        reason: part.reason,
                        usage: part.usage
                    };
            }
        }
        throw new Error(`${provider.name}: stream ended without a finish part`);
    },
} as const;
