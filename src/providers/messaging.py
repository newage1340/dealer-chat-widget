"""
Sending text messages (SMS today; other text channels later).

Parsing inbound webhooks into InboundMessage is the channel adapter's job,
since it depends on the web framework as well as the vendor.
"""

from dataclasses import dataclass
from typing import Protocol

from core import Address, Channel, TenantId, UsageRecord


@dataclass(frozen=True, slots=True, kw_only=True)
class OutboundMessage:
    to:         Address
    sender:     Address
    text:       str
    channel:    Channel = Channel.SMS
    media_urls: tuple[str, ...] = ()
    purpose:    str = "turn"
    tenant_id:  TenantId | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class SendReceipt:
    provider:            str
    provider_message_id: str
    usage:               UsageRecord


class MessagingProvider(Protocol):
    name: str

    def send(self, message: OutboundMessage) -> SendReceipt: ...
