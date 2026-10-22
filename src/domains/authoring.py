"""
Generating new domain packs by prompting a model with the authoring "prompt leader".

The leader (authoring_prompt.md) carries the template shape and a complete example pack.
The model's TOML is validated with the same loader the app uses; validation errors are fed back
for one repair attempt.
"""

import re
import tomllib
from pathlib import Path

from core import Role
from providers import ChatMessage, LLMProvider, LLMRequest, ModelTier

from . import PACKS_DIR
from .pack import DomainPack, DomainPackError, parse_pack

LEADER_PATH = Path(__file__).parent / "authoring_prompt.md"
EXAMPLE_PACK = "plumbing.toml"


def authoring_prompt(business_type: str, extra_context: str = "") -> str:
    leader = LEADER_PATH.read_text(encoding="utf-8")
    return (leader
            .replace("{business_type}", business_type)
            .replace("{extra_context}", f"Additional context from the operator: {extra_context}" if extra_context else "")
            .replace("{template}", (PACKS_DIR / "_template.toml").read_text(encoding="utf-8"))
            .replace("{example}", (PACKS_DIR / EXAMPLE_PACK).read_text(encoding="utf-8")))


def _strip_fence(text: str) -> str:
    m = re.search(r"```(?:toml)?\s*\n(.*?)```", text, re.S)
    return (m.group(1) if m else text).strip() + "\n"


def _check(text: str) -> tuple[DomainPack | None, list[str]]:
    try:
        return parse_pack(tomllib.loads(text), "<generated>"), []
    except tomllib.TOMLDecodeError as e:
        return None, [f"TOML syntax error: {e}"]
    except DomainPackError as e:
        return None, e.problems


def generate_pack(llm: LLMProvider, business_type: str, extra_context: str = "", *, attempts: int = 2) -> tuple[str, DomainPack]:
    """
    Ask the model for a pack; returns (toml_text, parsed pack) or raises DomainPackError.
    """
    messages = [ChatMessage(Role.USER, authoring_prompt(business_type, extra_context))]
    problems: list[str] = []
    for _ in range(attempts):
        response = llm.complete(LLMRequest(messages=messages, tier=ModelTier.CAPABLE, max_tokens=8000,
                                           temperature=0.4, timeout_s=180, purpose="domain_authoring"))
        text = _strip_fence(response.text)
        pack, problems = _check(text)
        if pack is not None:
            return text, pack
        messages += [
            ChatMessage(Role.ASSISTANT, text),
            ChatMessage(Role.USER, "That pack failed validation:\n- " + "\n- ".join(problems)
                        + "\nReturn the corrected complete TOML document only."),
        ]
    raise DomainPackError("<generated>", problems)


def save_pack(text: str, pack: DomainPack, directory: Path = PACKS_DIR, *, overwrite: bool = False) -> Path:
    path = directory / f"{pack.id}.toml"
    if path.exists() and not overwrite:
        raise FileExistsError(f"{path} already exists")
    path.write_text(text, encoding="utf-8")
    return path
