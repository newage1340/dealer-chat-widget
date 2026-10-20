"""
In-memory providers for tests and offline development.
"""

from collections.abc import Callable, Iterable
from datetime import datetime, timezone

from core import ServiceKind, UsageRecord, UsageUnit

from .llm import LLMRequest, LLMResponse
from .messaging import OutboundMessage, SendReceipt


class FakeLLM:
    """
    Replies from a script: either a fixed sequence of strings or a function of the request.

    Every request is kept in `requests` so tests can assert on prompts.
    """

    def __init__(self, replies: Iterable[str] | Callable[[LLMRequest], str], *, name: str = "fake-llm"):
        self.name = name
        self.requests: list[LLMRequest] = []
        if callable(replies):
            self._reply = replies
        else:
            scripted = iter(replies)
            self._reply = lambda _request: next(scripted)

    def complete(self, request: LLMRequest) -> LLMResponse:
        self.requests.append(request)
        text = self._reply(request)
        prompt_chars = sum(len(m.content) for m in request.messages)
        usage = UsageRecord(
            provider=self.name,
            service=ServiceKind.LLM,
            operation="complete",
            occurred_at=datetime.now(timezone.utc),
            units={UsageUnit.INPUT_TOKENS: prompt_chars / 4, UsageUnit.OUTPUT_TOKENS: len(text) / 4},
            purpose=request.purpose,
            tenant_id=request.tenant_id,
            channel=request.channel,
        )
        return LLMResponse(text=text, provider=self.name, model=f"{request.tier}", usage=usage, finish_reason="stop")


class FakeMessaging:
    """
    Records outbound messages instead of sending them.
    """

    def __init__(self, *, name: str = "fake-messaging"):
        self.name = name
        self.sent: list[OutboundMessage] = []

    def send(self, message: OutboundMessage) -> SendReceipt:
        self.sent.append(message)
        usage = UsageRecord(
            provider=self.name,
            service=ServiceKind.MESSAGING,
            operation="send",
            occurred_at=datetime.now(timezone.utc),
            units={UsageUnit.SMS_SEGMENTS: 1},
            purpose=message.purpose,
            tenant_id=message.tenant_id,
            channel=message.channel,
        )
        return SendReceipt(provider=self.name, provider_message_id=f"fake-{len(self.sent)}", usage=usage)
