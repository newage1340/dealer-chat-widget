"""
Parsing the machine-readable parts of a model reply.

The base prompts teach one protocol for every domain:
- META_JSON: {...}          a confirmed booking
- META_NAME/LAST_NAME/EMAIL/PHONE: value    profile facts learned this turn
- [TAKE_MESSAGE] / [TRANSFER] / [HANGUP]    end-of-call tokens (voice)
Everything machine-readable is removed from the text the customer sees or hears.
"""

import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

TOKENS = ("TAKE_MESSAGE", "TRANSFER", "HANGUP")

_META_JSON_RE = re.compile(r"META_JSON\s*:\s*", re.I)
_TOKEN_RE = re.compile(r"\[(" + "|".join(TOKENS) + r")\]", re.I)
_MD_LINK_RE = re.compile(r"\[([^\]]+)\]\((?:https?://|www\.|tel:)[^)]+\)", re.I)
_BARE_URL_RE = re.compile(r"\b(?:https?://|www\.)\S+", re.I)
_CODE_FENCE_RE = re.compile(r"```.*?```", re.S)
_MARKERS = {
    "first_name": "META_NAME",
    "last_name":  "META_LAST_NAME",
    "email":      "META_EMAIL",
    "phone":      "META_PHONE",
}


@dataclass(frozen=True, slots=True)
class ParsedReply:
    text:    str
    booking: dict[str, Any] | None = None
    profile: dict[str, str] = field(default_factory=dict)
    tokens:  frozenset[str] = frozenset()


def _extract_json(text: str) -> tuple[str, dict[str, Any] | None]:
    marker = _META_JSON_RE.search(text)
    if not marker:
        return text, None
    start = marker.end()
    depth, end = 0, None
    for i in range(start, len(text)):
        ch = text[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break
    if end is None:
        return text[:marker.start()].strip(), None
    try:
        meta = json.loads(text[start:end])
    except json.JSONDecodeError:
        meta = None
    return (text[:marker.start()] + text[end:]).strip(), meta if isinstance(meta, dict) else None


def scrub_urls(text: str) -> str:
    """
    Remove URLs the model wrote. Real links are sent by the system from catalog/tenant data;
    anything the model produces is a hallucination.
    """
    text = _MD_LINK_RE.sub(r"\1", text)
    text = _BARE_URL_RE.sub("", text)
    text = re.sub(r"\(\s*\)", "", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return re.sub(r"\s+([,.;:!?])", r"\1", text).strip()


def parse_reply(raw: str) -> ParsedReply:
    text, booking = _extract_json(raw or "")

    profile: dict[str, str] = {}
    for key, marker in _MARKERS.items():
        m = re.search(rf"{marker}\s*:\s*(.+?)(?:\n|$)", text, re.I)
        if m:
            profile[key] = m.group(1).strip()
            text = (text[:m.start()] + text[m.end():]).strip()

    tokens = frozenset(t.upper() for t in _TOKEN_RE.findall(text))
    text = _TOKEN_RE.sub("", text)
    text = _CODE_FENCE_RE.sub("", text)
    text = scrub_urls(text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()

    if booking:
        if booking.get("customer_name") and "first_name" not in profile:
            profile["first_name"] = str(booking["customer_name"]).strip().split(" ")[0]
        if booking.get("customer_email") and "email" not in profile:
            profile["email"] = str(booking["customer_email"]).strip()
    if "phone" in profile:
        digits = re.sub(r"\D", "", profile["phone"])[-10:]
        if len(digits) == 10:
            profile["phone"] = "+1" + digits
        else:
            profile.pop("phone")
    return ParsedReply(text=text, booking=booking, profile=profile, tokens=tokens)


def booking_subject(booking: dict[str, Any]) -> str:
    """
    The booking's subject, accepting the legacy `car_desc` key as well.
    """
    return str(booking.get("subject") or booking.get("car_desc") or "").strip()


def parse_local_iso(value: str, timezone_name: str) -> datetime | None:
    """
    Interpret the model's "YYYY-MM-DDTHH:MM:SS" (tenant-local, no offset) as an aware datetime.
    """
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).strip())
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo(timezone_name))
    return dt
