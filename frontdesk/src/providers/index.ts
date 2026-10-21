/**
 * Vendor-neutral interfaces for every external service.
 * Adapters live beside these files; nothing outside providers/ imports a vendor SDK.
 */

export {
    type EmailProvider,
    type EmailReceipt,
    type OutboundEmail,
} from "./email.ts";
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
    type ResponseFormat,
    type ToolCall,
    type ToolDefinition,
} from "./llm.ts";
export {
    FakeLlm,
    FakeStt,
    FakeTranscriber,
    FakeTts,
    LoggingEmail,
    LoggingMessaging,
    type ScriptedReply,
} from "./fakes.ts";
export {
    type MessagingProvider,
    type OutboundSms,
    type SendReceipt,
} from "./messaging.ts";
export {
    type AudioFormat,
    type BatchTranscriber,
    type BatchTranscript,
    type Recognition,
    type SpeechChunk,
    type SpeechContext,
    type SpeechToText,
    type TextToSpeech,
} from "./speech.ts";
export {
    LlmRouter,
    type Route,
} from "./router.ts";
export {
    PriceTable,
    type UsageSink,
} from "./usage.ts";
