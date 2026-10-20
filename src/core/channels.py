"""Channels a conversation can happen on, and the addresses that reach people."""

from dataclasses import dataclass
from enum import StrEnum


class Channel(StrEnum):
    SMS = "sms"
    WEB = "web"
    VOICE = "voice"


class AddressKind(StrEnum):
    PHONE = "phone"
    EMAIL = "email"
    WEB_SESSION = "web_session"


@dataclass(frozen=True, slots=True)
class Address:
    """A way to reach a person or a tenant endpoint.

    Values are expected to be normalized at the edge (E.164 for phones,
    lowercased for email) before an Address is constructed, so equality
    comparisons are meaningful everywhere else.
    """

    kind: AddressKind
    value: str

    def __str__(self) -> str:
        return f"{self.kind}:{self.value}"
