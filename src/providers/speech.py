"""
Speech-to-text and text-to-speech as standalone services.

These cover pipelines where we handle audio ourselves (e.g. a self-hosted voice stack).
Telephony platforms that speak and listen on our behalf (Twilio <Say>/<Gather>, Vapi)
are modeled separately once the voice channel is ported.
"""

from dataclasses import dataclass
from typing import Protocol

from core import TenantId, UsageRecord


@dataclass(frozen=True, slots=True, kw_only=True)
class Audio:
    data: bytes
    mime_type: str
    duration_s: float | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class SynthesisRequest:
    text: str
    voice: str = ""  # provider-specific voice id; empty means the provider default
    purpose: str = "turn"
    tenant_id: TenantId | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class SynthesisResult:
    audio: Audio
    provider: str
    usage: UsageRecord


@dataclass(frozen=True, slots=True, kw_only=True)
class TranscriptionRequest:
    audio: Audio
    language: str = "en"
    purpose: str = "turn"
    tenant_id: TenantId | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class Transcript:
    text: str
    provider: str
    usage: UsageRecord
    confidence: float | None = None


class SpeechSynthesizer(Protocol):
    name: str

    def synthesize(self, request: SynthesisRequest) -> SynthesisResult: ...


class Transcriber(Protocol):
    name: str

    def transcribe(self, request: TranscriptionRequest) -> Transcript: ...
