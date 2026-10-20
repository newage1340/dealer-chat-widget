"""
The unit of work: one inbound message handled by a chain of handlers.

Handlers never perform side effects directly.
They return an Outcome that describes a reply, state changes and Actions; a runner applies them.
This keeps handlers composable and testable without Twilio, OpenAI or a database.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

from .channels import Address, Channel
from .contacts import Contact
from .ids import ConversationKey
from .messages import InboundMessage, Message
from .tenant import Tenant


# --- Actions: side effects requested by a handler ---------------------------

@dataclass(frozen=True, slots=True, kw_only=True)
class SendMessage:
    """An extra outbound message, beyond the turn's reply."""

    to:      Address
    text:    str
    channel: Channel


@dataclass(frozen=True, slots=True, kw_only=True)
class NotifyStaff:
    text:   str
    reason: str


@dataclass(frozen=True, slots=True, kw_only=True)
class UpdateContact:
    changes: Mapping[str, Any]


@dataclass(frozen=True, slots=True, kw_only=True)
class ScheduleTask:
    kind:    str
    run_at:  datetime
    payload: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True, kw_only=True)
class TransferCall:
    to: Address


@dataclass(frozen=True, slots=True)
class EndSession:
    """Hang up a call or close a session after the reply is delivered."""


@dataclass(frozen=True, slots=True, kw_only=True)
class BookAppointment:
    """
    Record a confirmed booking, confirm it to the contact and alert staff.

    `rescheduled` marks a booking that replaces the contact's existing appointment.
    """

    time_text:   str
    subject:     str
    starts_at:   datetime | None = None
    location:    str = ""
    details:     Mapping[str, Any] = field(default_factory=dict)
    rescheduled: bool = False


type Action = (
    SendMessage
    | NotifyStaff
    | UpdateContact
    | ScheduleTask
    | TransferCall
    | EndSession
    | BookAppointment
)


# --- Results ----------------------------------------------------------------

@dataclass(frozen=True, slots=True, kw_only=True)
class Reply:
    """
    `data` carries channel-specific hints alongside the text
    (e.g. the web widget's `needs_profile` flag).
    """

    text:   str
    speech: str | None = None  # voice rendering, when it should differ from text
    data:   Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True, kw_only=True)
class Outcome:
    """
    What a handler decided.

    `handled=True` ends the chain for this turn. A handler can return
    `handled=False` with state updates or actions (e.g. capturing a name) to
    let later handlers run against the updated context.

    `state_updates` are merged into conversation state; `state_deletes`
    removes keys.
    """

    reply:         Reply | None = None
    actions:       tuple[Action, ...] = ()
    state_updates: Mapping[str, Any] = field(default_factory=dict)
    state_deletes: frozenset[str] = frozenset()
    handled:       bool = True


@dataclass(frozen=True, slots=True, kw_only=True)
class TurnContext:
    """Everything a handler may read while handling one inbound message."""

    tenant:  Tenant
    contact: Contact
    key:     ConversationKey
    inbound: InboundMessage
    history: tuple[Message, ...]
    state:   Mapping[str, Any]
    now:     datetime

    @property
    def channel(self) -> Channel:
        return self.inbound.channel

    @property
    def text(self) -> str:
        return self.inbound.text


class Handler(Protocol):
    """One step in the turn pipeline. Return None to pass without effect."""

    name: str

    def handle(self, ctx: TurnContext) -> Outcome | None: ...
