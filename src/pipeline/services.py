"""
The long-lived dependencies handlers, the runner and channels share.
"""

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from core import Tenant
from domains import DomainPack, DomainRegistry
from prompts import PromptComposer
from providers import LLMProvider, MessagingProvider, UsageSink
from storage import SqliteStore, utcnow
from tenants import TenantDirectory


def tenant_now(tenant: Tenant) -> datetime:
    """
    Tenant-local wall-clock time, timezone-naive (what prompts and hours logic expect).
    """
    return utcnow().astimezone(ZoneInfo(tenant.timezone)).replace(tzinfo=None)


@dataclass(frozen=True, slots=True, kw_only=True)
class Services:
    registry:  DomainRegistry
    directory: TenantDirectory
    store:     SqliteStore
    composer:  PromptComposer
    llm:       LLMProvider
    messaging: MessagingProvider
    usage:     UsageSink
    default_sender: str = ""  # fallback "from" number for outbound SMS when a tenant has none

    def pack(self, tenant: Tenant) -> DomainPack:
        return self.registry.get(tenant.domain)

    def local_now(self, tenant: Tenant) -> datetime:
        return tenant_now(tenant)
