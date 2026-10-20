"""Appointments a contact books with a tenant."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from .ids import ConversationKey


class AppointmentStatus(StrEnum):
    CONFIRMED = "confirmed"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True, kw_only=True)
class Appointment:
    """
    A booked visit.

    `time_text` keeps the time as the customer said it ("tomorrow after work");
    `starts_at` is set only when it could be resolved to an instant.
    `sent_notices` records which notices have gone out,
    so features can add new kinds without a schema change.
    eg. (reminder, reconfirmation, follow-up, ...) 
    """

    key: ConversationKey
    time_text: str
    subject: str
    created_at: datetime
    starts_at: datetime | None = None
    item_ids: tuple[str, ...] = ()
    status: AppointmentStatus = AppointmentStatus.CONFIRMED
    sent_notices: frozenset[str] = frozenset()
    id: int | None = None
