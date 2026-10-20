"""Identifiers shared across the system."""

from dataclasses import dataclass
from typing import NewType

TenantId = NewType("TenantId", str)
ContactId = NewType("ContactId", str)


@dataclass(frozen=True, slots=True)
class ConversationKey:
    """
    Identifies one contact's ongoing relationship with one tenant.

    Deliberately channel-agnostic:
    a contact who texts and later calls is the same conversation.
    Each Message records the channel it arrived on.
    """

    tenant_id: TenantId
    contact_id: ContactId
