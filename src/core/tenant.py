"""A business using the system, and which domain it belongs to."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from .channels import Address, Channel
from .ids import TenantId


@dataclass(frozen=True, slots=True, kw_only=True)
class Branding:
    color: str = ""
    logo_url: str = ""


@dataclass(frozen=True, slots=True, kw_only=True)
class Tenant:
    """
    A business and everything core code needs to know about it.

    `domain` names the DomainConfig that supplies domain-specific behavior.
    `profile` holds the tenant's free-text answers to that domain's questions
    (e.g. "do you offer financing?"); its keys are defined by the domain, not by core.
    `settings` holds structured, domain-defined options.
    """

    id: TenantId
    name: str
    domain: str
    timezone: str
    endpoints: Mapping[Channel, tuple[Address, ...]] = field(default_factory=dict)
    staff_contacts: tuple[Address, ...] = ()
    branding: Branding = field(default_factory=Branding)
    hours_text: str = ""
    profile: Mapping[str, str] = field(default_factory=dict)
    settings: Mapping[str, Any] = field(default_factory=dict)

    def endpoint(self, channel: Channel) -> Address | None:
        """
        The tenant's primary address on a channel, if it has one.
        """
        addresses = self.endpoints.get(channel, ())
        return addresses[0] if addresses else None
