"""Metering for every external service call, so cost decisions use data."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum

from .channels import Channel
from .ids import TenantId


class ServiceKind(StrEnum):
    LLM                 = "llm"
    SPEECH_TO_TEXT      = "stt"
    TEXT_TO_SPEECH      = "tts"
    MESSAGING           = "messaging"
    TELEPHONY           = "telephony"
    VOICE_ORCHESTRATION = "voice_orchestration"


class UsageUnit(StrEnum):
    INPUT_TOKENS        = "input_tokens"
    CACHED_INPUT_TOKENS = "cached_input_tokens"
    OUTPUT_TOKENS       = "output_tokens"
    SMS_SEGMENTS        = "sms_segments"
    CALL_SECONDS        = "call_seconds"
    AUDIO_SECONDS       = "audio_seconds"
    CHARACTERS          = "characters"
    REQUESTS            = "requests"


@dataclass(frozen=True, slots=True, kw_only=True)
class UsageRecord:
    """
    One metered call to an external (or self-hosted) service.

    `purpose` says why the call was made ("turn", "keep_warm", "followup", ...).
    This is so spend that serves no customer is still visible.
    `cost_usd` is an estimate from the provider's price table; None when unknown.
    """

    provider:    str
    service:     ServiceKind
    operation:   str
    occurred_at: datetime
    units:       Mapping[UsageUnit, float] = field(default_factory=dict)
    purpose:     str = "turn"
    cost_usd:    float | None = None
    latency_ms:  float | None = None
    tenant_id:   TenantId | None = None
    channel:     Channel | None = None
