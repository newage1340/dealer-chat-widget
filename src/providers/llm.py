"""
Language model completions.

Callers ask for a capability tier, never a vendor model name.
Which provider and model serve a tier is configuration, so swapping OpenAI
for a self-hosted model does not touch calling code.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from core import Channel, Role, TenantId, UsageRecord


class ModelTier(StrEnum):
    """
    FAST: short, latency-sensitive or simple work (classification, extraction, voice turns).
    CAPABLE: open-ended conversation where quality matters more than speed.
    """

    FAST    = "fast"
    CAPABLE = "capable"


@dataclass(frozen=True, slots=True)
class ChatMessage:
    role:    Role
    content: str


@dataclass(frozen=True, slots=True, kw_only=True)
class LLMRequest:
    messages:    Sequence[ChatMessage]
    tier:        ModelTier = ModelTier.FAST
    max_tokens:  int = 512
    temperature: float | None = None
    timeout_s:   float | None = None
    purpose:     str = "turn"
    tenant_id:   TenantId | None = None
    channel:     Channel | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class LLMResponse:
    text:          str
    provider:      str
    model:         str
    usage:         UsageRecord
    finish_reason: str = ""


class LLMProvider(Protocol):
    """
    Turns an LLMRequest into an LLMResponse, or raises ProviderError.

    Implementations do not retry or fall back;
    that policy belongs to the router so it is the same for every provider.
    """

    name: str

    def complete(self, request: LLMRequest) -> LLMResponse: ...

