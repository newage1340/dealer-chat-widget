import re
from datetime import datetime
from pathlib import Path

import pytest

from core import Channel, Contact, ContactId
from domains import PACKS_DIR, DomainPackError, DomainRegistry, load_pack
from domains.pack import parse_pack
from prompts import PromptComposer, PromptInputs
from tenants import TenantDirectory

ROOT = Path(__file__).resolve().parent.parent
CAR_WORDS = re.compile(r"\b(vehicles?|cars?|dealers?|dealership|inventory|trade-?ins?|carfax|vin)\b", re.I)


@pytest.mark.parametrize("path", sorted(PACKS_DIR.glob("*.toml")), ids=lambda p: p.name)
def test_every_pack_and_the_template_validate(path):
    load_pack(path)


def test_validator_reports_all_problems():
    with pytest.raises(DomainPackError) as err:
        parse_pack({"domain": {"id": "x"}, "prompts": {"chat": "hi {nope}", "voice": ""}})
    problems = "\n".join(err.value.problems)
    assert "missing [catalog]" in problems
    assert "unknown placeholder {nope}" in problems


def _prompt(slug: str, channel: Channel, message: str) -> str:
    registry = DomainRegistry.from_directory()
    record = TenantDirectory.from_directory(ROOT / "data" / "tenants", registry).by_slug(slug)
    inputs = PromptInputs(
        tenant=record.tenant, pack=registry.get(record.tenant.domain), items=record.items,
        contact=Contact(id=ContactId("c"), tenant_id=record.tenant.id, first_name="Alex"),
        history=(), message=message, channel=channel, now=datetime(2026, 10, 20, 15, 0),
        caller_phone="+13175550199" if channel is Channel.VOICE else "",
    )
    composer = PromptComposer()
    messages = composer.voice_messages(inputs) if channel is Channel.VOICE else composer.chat_messages(inputs)
    return messages[0].content


@pytest.mark.parametrize("channel", [Channel.SMS, Channel.WEB, Channel.VOICE])
def test_plumbing_prompts_carry_no_automotive_content(channel):
    prompt = _prompt("demo-plumber", channel, "my water heater is leaking")
    assert "=== PLUMBING SERVICE SPECIFICS ===" in prompt
    assert not CAR_WORDS.findall(prompt)


def test_automotive_prompt_has_domain_section_toggles_and_matches():
    prompt = _prompt("hoosiers", Channel.SMS, "any SUVs?")
    assert "=== CAR DEALERSHIP SPECIFICS ===" in prompt
    assert "PLUS a $250 doc fee" in prompt                # fee toggle from tenant settings
    assert "every car on our lot is thoroughly inspected" not in prompt  # inspection toggle off
    assert "Trade-in:" in prompt                           # domain intake inside the base booking flow
    assert "(No close match found" not in prompt           # "SUVs" matched "Body: SUV"
