"""
Tenant directory: businesses loaded from JSON files, plus their catalogs.

One file per tenant in the tenants directory (default: data/tenants/*.json).
The file shape is domain-neutral; `profile` keys and catalog fields come from the tenant's domain pack.
"""

import json
import logging
import threading
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from core import Address, AddressKind, Branding, CatalogItem, Channel, Tenant, TenantId
from domains import DomainRegistry
from domains.catalog import normalize_item

log = logging.getLogger(__name__)


def normalize_phone(raw: str) -> str:
    """
    E.164 for US numbers ("(317) 555-0100" -> "+13175550100"); other input passes through trimmed.
    """
    raw = (raw or "").strip()
    digits = "".join(c for c in raw if c.isdigit())
    if len(digits) == 10:
        return "+1" + digits
    if len(digits) == 11 and digits.startswith("1"):
        return "+" + digits
    if raw.startswith("+") and digits:
        return "+" + digits
    return raw


def phone_address(raw: str) -> Address | None:
    phone = normalize_phone(raw)
    return Address(AddressKind.PHONE, phone) if phone.startswith("+") else None


@dataclass(frozen=True, slots=True)
class TenantRecord:
    tenant: Tenant
    items:  tuple[CatalogItem, ...]
    source: Mapping[str, Any]
    path:   Path | None = None


def tenant_from_json(data: Mapping[str, Any], registry: DomainRegistry) -> TenantRecord:
    pack = registry.get(data["domain"])
    tenant_id = TenantId(str(data["id"]))

    endpoints: dict[Channel, tuple[Address, ...]] = {}
    for channel_name, numbers in (data.get("endpoints") or {}).items():
        addresses = tuple(a for a in (phone_address(n) for n in numbers) if a)
        if addresses:
            endpoints[Channel(channel_name)] = addresses

    staff = data.get("staff") or {}
    staff_contacts = tuple(a for a in (phone_address(n) for n in staff.get("phones", [])) if a)
    staff_contacts += tuple(Address(AddressKind.EMAIL, e.strip().lower()) for e in staff.get("emails", []) if e.strip())

    settings = dict(data.get("settings") or {})
    if data.get("links"):
        settings["links"] = data["links"]
    if data.get("transfer_to"):
        settings["transfer_to"] = normalize_phone(data["transfer_to"])

    branding = data.get("branding") or {}
    tenant = Tenant(
        id               = tenant_id,
        name             = data["name"],
        domain           = pack.id,
        timezone         = data.get("timezone") or "America/Indiana/Indianapolis",
        slug             = data.get("slug") or str(data["id"]),
        phone            = normalize_phone(data.get("phone", "")),
        address          = data.get("address", ""),
        website          = data.get("website", ""),
        agent_name       = data.get("agent_name", ""),
        voice_agent_name = data.get("voice_agent_name", ""),
        endpoints        = endpoints,
        staff_contacts   = staff_contacts,
        branding         = Branding(color=branding.get("color", ""), logo_url=branding.get("logo_url", "")),
        hours_text       = data.get("hours", ""),
        profile          = dict(data.get("profile") or {}),
        settings         = settings,
    )
    items = tuple(normalize_item(raw, pack.catalog, tenant_id, i) for i, raw in enumerate(data.get("catalog") or []))
    return TenantRecord(tenant=tenant, items=items, source=data)


class TenantDirectory:
    def __init__(self, records: Iterable[TenantRecord]):
        self._lock = threading.Lock()
        self._records: dict[TenantId, TenantRecord] = {r.tenant.id: r for r in records}

    @classmethod
    def from_directory(cls, directory: str | Path, registry: DomainRegistry) -> "TenantDirectory":
        records = []
        for path in sorted(Path(directory).glob("*.json")):
            try:
                record = tenant_from_json(json.loads(path.read_text(encoding="utf-8")), registry)
                records.append(replace(record, path=path))
            except Exception:
                log.exception("skipping tenant file %s", path)
        log.info("loaded %d tenant(s) from %s", len(records), directory)
        return cls(records)

    def all(self) -> list[TenantRecord]:
        with self._lock:
            return list(self._records.values())

    def get(self, tenant_id: str) -> TenantRecord | None:
        with self._lock:
            return self._records.get(TenantId(tenant_id))

    def by_slug(self, slug: str) -> TenantRecord | None:
        slug = (slug or "").strip().lower()
        return next((r for r in self.all() if r.tenant.slug.lower() == slug), None)

    def by_endpoint(self, channel: Channel, address: Address) -> TenantRecord | None:
        """
        The tenant that owns a number. A number listed only under one channel still matches the other
        phone channels, since one Twilio number usually serves both SMS and voice.
        """
        records = self.all()
        exact = next((r for r in records if address in r.tenant.endpoints.get(channel, ())), None)
        if exact:
            return exact
        return next((r for r in records if any(address in addrs for addrs in r.tenant.endpoints.values())), None)

    def replace_with(self, other: "TenantDirectory") -> None:
        """
        Adopt another directory's tenants in place (hot reload; the directory object is shared).
        """
        records = other.all()
        with self._lock:
            self._records = {r.tenant.id: r for r in records}

    def save_catalog(self, tenant_id: TenantId, raw_items: Sequence[Mapping[str, Any]], registry: DomainRegistry) -> int:
        """
        Replace a tenant's catalog from raw source rows and persist it to the tenant's file.
        """
        with self._lock:
            record = self._records.get(tenant_id)
            if record is None:
                raise KeyError(tenant_id)
            spec = registry.get(record.tenant.domain).catalog
            items = tuple(normalize_item(raw, spec, tenant_id, i) for i, raw in enumerate(raw_items))
            source = {**record.source, "catalog": list(raw_items)}
            if record.path is not None:
                tmp = record.path.with_suffix(".json.tmp")
                tmp.write_text(json.dumps(source, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
                tmp.replace(record.path)
            self._records[tenant_id] = replace(record, items=items, source=source)
        return len(items)


__all__ = [
    "TenantDirectory",
    "TenantRecord",
    "normalize_phone",
    "phone_address",
    "tenant_from_json",
]
