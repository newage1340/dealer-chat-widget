import dataclasses

import pytest

from core import (
    Address,
    AddressKind,
    CatalogItem,
    CatalogSchema,
    Channel,
    FieldSpec,
    Tenant,
    TenantId,
)


def make_tenant(**overrides):
    defaults = dict(
        id=TenantId("t1"),
        name="Acme",
        domain="test",
        timezone="America/Indiana/Indianapolis",
    )
    return Tenant(**(defaults | overrides))


def test_tenant_endpoint_returns_primary_address_or_none():
    sms = Address(AddressKind.PHONE, "+13175550100")
    tenant = make_tenant(endpoints={Channel.SMS: (sms, Address(AddressKind.PHONE, "+13175550101"))})

    assert tenant.endpoint(Channel.SMS) == sms
    assert tenant.endpoint(Channel.VOICE) is None


def test_types_are_immutable():
    with pytest.raises(dataclasses.FrozenInstanceError):
        make_tenant().name = "other"


def test_catalog_title_drops_missing_fields():
    schema = CatalogSchema(
        item_noun="item",
        item_noun_plural="items",
        fields=tuple(FieldSpec(name=n) for n in ("year", "brand", "model")),
        title_template="{year} {brand} {model}",
    )
    item = CatalogItem(id="A1", tenant_id=TenantId("t1"), fields={"brand": "Acme", "model": "X-1"})

    assert schema.title(item) == "Acme X-1"
