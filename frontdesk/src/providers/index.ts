/**
 * Vendor-neutral interfaces for every external service.
 * Adapters live beside these files; nothing outside providers/ imports a vendor SDK.
 */

export {
    ProviderError,
    ProviderFailure,
} from "./errors.ts";
export {
    FinishReason,
    LlmProvider,
    ModelTier,
    type ChatMessage,
    type LlmRequest,
    type LlmResponse,
    type LlmStreamPart,
    type ToolCall,
    type ToolDefinition,
} from "./llm.ts";
export {
    type MessagingProvider,
    type OutboundSms,
    type SendReceipt,
} from "./messaging.ts";
export {
    type AudioFormat,
    type SpeechChunk,
    type SpeechContext,
    type SpeechToText,
    type TextToSpeech,
    type Transcript,
} from "./speech.ts";
export {
    PriceTable,
    type UsageSink,
} from "./usage.ts";
