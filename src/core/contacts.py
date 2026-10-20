"""The people tenants talk to."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .channels import Address
from .ids import ContactId, TenantId


@dataclass(frozen=True, slots=True, kw_only=True)
class Contact:
    """
    What we know about a person, scoped to one tenant.

    `phone` is a real, reachable number.
    It can differ from the address the contact is writing from
    (e.g. a web-widget session that later supplies a phone number).
    """

    id:                ContactId
    tenant_id:         TenantId
    first_name:        str = ""
    last_name:         str = ""
    email:             str = ""
    phone:             str = ""
    addresses:         tuple[Address, ...] = ()
    terms_accepted_at: datetime | None = None
    attributes:        Mapping[str, Any] = field(default_factory=dict)

    @property
    def display_name(self) -> str:
        return " ".join(p for p in (self.first_name, self.last_name) if p)
