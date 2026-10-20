"""Things a tenant offers. Core knows the shape; domains define the fields."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

from .ids import TenantId


class FieldType(StrEnum):
    TEXT = "text"
    INTEGER = "integer"
    NUMBER = "number"
    MONEY = "money"
    URL = "url"


@dataclass(frozen=True, slots=True, kw_only=True)
class FieldSpec:
    name: str
    type: FieldType = FieldType.TEXT
    label: str = ""
    required: bool = False
    searchable: bool = False  # considered when matching customer queries


@dataclass(frozen=True, slots=True, kw_only=True)
class CatalogSchema:
    """Describes a domain's catalog items, e.g. vehicles or listings.

    `title_template` uses str.format fields named after FieldSpecs; missing
    values are dropped and whitespace collapsed, so "{year} {make} {model}"
    renders cleanly when year is unknown.
    """

    item_noun: str
    item_noun_plural: str
    fields: tuple[FieldSpec, ...]
    title_template: str
    id_field: str | None = None

    def field(self, name: str) -> FieldSpec | None:
        return next((f for f in self.fields if f.name == name), None)

    def title(self, item: "CatalogItem") -> str:
        values = {f.name: item.fields.get(f.name) or "" for f in self.fields}
        return " ".join(self.title_template.format_map(values).split())


@dataclass(frozen=True, slots=True, kw_only=True)
class CatalogItem:
    id: str
    tenant_id: TenantId
    fields: Mapping[str, Any] = field(default_factory=dict)
    updated_at: datetime | None = None

    def get(self, name: str, default: Any = None) -> Any:
        return self.fields.get(name, default)
