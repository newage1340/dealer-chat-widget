"""Inbound messages and stored conversation history."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

from .channels import Address, Channel
from .ids import ConversationKey


class Role(StrEnum):
    USER = "user"
    ASSISTANT = "assistant"
    SYSTEM = "system"


@dataclass(frozen=True, slots=True, kw_only=True)
class InboundMessage:
    """
    A message as received from a channel, before tenant/contact resolution.

    `recipient` is the tenant endpoint it was sent to
    (the number texted or called, or the widget's endpoint);
    Resolving it to a Tenant is a separate step so channel adapters stay ignorant of tenancy.
    """

    channel: Channel
    sender: Address
    recipient: Address
    text: str
    received_at: datetime
    session_id: str | None = None  # call SID, widget session, etc.
    raw: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True, kw_only=True)
class Message:
    """
    One turn of stored conversation history.
    """

    key: ConversationKey
    role: Role
    text: str
    channel: Channel
    created_at: datetime
    id: int | None = None
    meta: Mapping[str, Any] = field(default_factory=dict)
