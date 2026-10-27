"""
Messaging adapters: Twilio for real sends, and a logging stand-in for development.
"""

import logging
from datetime import datetime, timezone

from core import ServiceKind, UsageRecord, UsageUnit

from .errors import ProviderError, ProviderUnavailable
from .messaging import OutboundMessage, SendReceipt

log = logging.getLogger(__name__)


def _usage(provider: str, message: OutboundMessage, segments: float) -> UsageRecord:
    return UsageRecord(
        provider    = provider,
        service     = ServiceKind.MESSAGING,
        operation   = "send",
        occurred_at = datetime.now(timezone.utc),
        units       = {UsageUnit.SMS_SEGMENTS: segments},
        purpose     = message.purpose,
        tenant_id   = message.tenant_id,
        channel     = message.channel,
    )


class TwilioMessaging:
    def __init__(self, *, account_sid: str, auth_token: str, messaging_service_sid: str = "", name: str = "twilio"):
        from twilio.rest import Client

        self.name = name
        self._client = Client(account_sid, auth_token)
        self._messaging_service_sid = messaging_service_sid

    def send(self, message: OutboundMessage) -> SendReceipt:
        from twilio.base.exceptions import TwilioRestException

        kwargs: dict = {"to": message.to.value, "body": message.text[:1500]}
        if message.media_urls:
            kwargs["media_url"] = list(message.media_urls)
        if self._messaging_service_sid:
            kwargs["messaging_service_sid"] = self._messaging_service_sid
        else:
            kwargs["from_"] = message.sender.value
        try:
            sent = self._client.messages.create(**kwargs)
        except TwilioRestException as e:
            raise ProviderError(self.name, str(e), retryable=(e.status or 0) >= 500) from e
        except Exception as e:
            raise ProviderUnavailable(self.name, str(e)) from e
        segments = float(getattr(sent, "num_segments", None) or 1)
        return SendReceipt(provider=self.name, provider_message_id=sent.sid, usage=_usage(self.name, message, segments))


class LoggingMessaging:
    """
    Logs outbound messages instead of sending them.
    Used when no messaging credentials are configured, so local development works end to end.
    """

    def __init__(self, *, name: str = "log-messaging"):
        self.name = name
        self.sent: list[OutboundMessage] = []

    def send(self, message: OutboundMessage) -> SendReceipt:
        self.sent.append(message)
        log.info("[%s] would send to %s from %s:\n%s", self.name, message.to, message.sender, message.text)
        return SendReceipt(
            provider            = self.name,
            provider_message_id = f"log-{len(self.sent)}",
            usage               = _usage(self.name, message, 1),
        )
