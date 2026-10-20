"""
Vendor-neutral interfaces to external services (LLM, messaging, speech).

Vendor SDKs are imported only inside adapter modules that implement these
protocols; nothing outside an adapter depends on a specific vendor.
"""

from .errors import ProviderError, ProviderTimeout, ProviderUnavailable
from .llm import ChatMessage, LLMProvider, LLMRequest, LLMResponse, ModelTier
from .messaging import MessagingProvider, OutboundMessage, SendReceipt
from .speech import (
    Audio,
    SpeechSynthesizer,
    SynthesisRequest,
    SynthesisResult,
    Transcriber,
    Transcript,
    TranscriptionRequest,
)
from .usage import ListUsageSink, PriceTable, UsageSink

__all__ = [
    "Audio", "ChatMessage", "ListUsageSink", "LLMProvider", "LLMRequest",
    "LLMResponse", "MessagingProvider", "ModelTier", "OutboundMessage",
    "PriceTable", "ProviderError", "ProviderTimeout", "ProviderUnavailable",
    "SendReceipt", "SpeechSynthesizer", "SynthesisRequest", "SynthesisResult",
    "Transcriber", "Transcript", "TranscriptionRequest", "UsageSink",
]
