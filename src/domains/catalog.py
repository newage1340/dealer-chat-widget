"""
Catalog formatting and matching, driven entirely by a pack's CatalogSpec.
"""

import re
import string
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from core import CatalogItem, FieldSpec, FieldType, TenantId

from .pack import CatalogSpec

_WORD_RE = re.compile(r"[a-z0-9]+")
_STOPWORDS = frozenset(
    "a an and any are at be can do does for from have how i in is it me my of on or "
    "the that this to want what when which with you your we our got there".split()
)


def normalize_item(raw: Mapping[str, Any], spec: CatalogSpec, tenant_id: TenantId, index: int) -> CatalogItem:
    """
    Map a raw source row (scraped or hand-written) onto the schema's field names.
    """
    fields: dict[str, Any] = {}
    names = {f.name for f in spec.schema.fields}
    for key, value in raw.items():
        k = str(key).strip()
        target = spec.aliases.get(k.lower()) or (k.lower() if k.lower() in names else None)
        if target and value not in (None, ""):
            fields[target] = str(value).strip() if not isinstance(value, (int, float)) else value
    item_id = str(fields.get(spec.schema.id_field, "") if spec.schema.id_field else "") or f"item-{index}"
    return CatalogItem(id=item_id, tenant_id=tenant_id, fields=fields)


def format_value(spec: FieldSpec, value: Any) -> str:
    text = str(value).strip() if value is not None else ""
    if not text:
        return spec.empty_text
    if spec.type is FieldType.MONEY:
        digits = re.sub(r"[^\d.]", "", text)
        try:
            amount = float(digits)
        except ValueError:
            return text
        return f"${amount:,.0f}" if amount == int(amount) else f"${amount:,.2f}"
    if spec.type is FieldType.INTEGER and text.isdigit():
        return f"{int(text):,}" if len(text) > 4 else text
    return text


def _render_part(part: str, item: CatalogItem, spec: CatalogSpec) -> str:
    """
    Render one summary template; empty when every referenced field is blank.
    """
    names = [n for _, n, _, _ in string.Formatter().parse(part) if n]
    values = {}
    any_value = False
    for name in names:
        fs = spec.schema.field(name)
        rendered = format_value(fs, item.get(name)) if fs else str(item.get(name) or "")
        any_value = any_value or bool(rendered)
        values[name] = rendered
    if names and not any_value:
        return ""
    return part.format_map(values).strip()


def item_title(item: CatalogItem, spec: CatalogSpec) -> str:
    return spec.schema.title(item) or spec.schema.item_noun.title()


def item_summary(item: CatalogItem, spec: CatalogSpec) -> str:
    line = item_title(item, spec)
    parts = [p for p in (_render_part(part, item, spec) for part in spec.summary_parts) if p]
    if parts:
        line += " (" + ", ".join(parts) + ")"
    suffix = _render_part(spec.summary_suffix, item, spec) if spec.summary_suffix else ""
    return f"{line} {suffix}".strip()


def item_details(item: CatalogItem, spec: CatalogSpec) -> str:
    lines = [item_title(item, spec)]
    for fs in spec.schema.fields:
        if not fs.in_details:
            continue
        value = format_value(fs, item.get(fs.name))
        if value:
            lines.append(f"{fs.label or fs.name}: {value}")
    return "\n".join(lines)


def catalog_summary(items: Sequence[CatalogItem], spec: CatalogSpec, limit: int = 200) -> str:
    if not items:
        return spec.empty_text
    lines = [item_summary(i, spec) for i in items[:limit]]
    if len(items) > limit:
        lines.append(f"(+{len(items) - limit} more {spec.schema.item_noun_plural} not shown)")
    return "\n".join(lines)


def _stem(word: str) -> str:
    """
    Crude plural folding so "SUVs" matches "SUV" and "trucks" matches "truck".
    """
    if len(word) > 3 and word.endswith("s") and not word.endswith("ss"):
        return word[:-1]
    return word


def _words(text: str) -> set[str]:
    return {_stem(w) for w in _WORD_RE.findall(text.lower()) if w not in _STOPWORDS}


def _searchable_words(item: CatalogItem, spec: CatalogSpec) -> set[str]:
    words: set[str] = set()
    for fs in spec.schema.fields:
        if fs.searchable:
            words |= _words(str(item.get(fs.name) or ""))
    return words


def match_items(items: Iterable[CatalogItem], text: str, spec: CatalogSpec, top_k: int = 3) -> list[CatalogItem]:
    """
    Items whose searchable fields share the most words with `text`, best first.
    Title words count double, so naming an item beats a stray feature word.
    """
    query = _words(text)
    if not query:
        return []
    scored = []
    for index, item in enumerate(items):
        title_words = _words(item_title(item, spec))
        score = 2 * len(query & title_words) + len(query & _searchable_words(item, spec))
        if score:
            scored.append((score, -index, item))
    scored.sort(key=lambda s: (s[0], s[1]), reverse=True)
    return [item for _, _, item in scored[:top_k]]


def anchor_item(texts_newest_first: Iterable[str], items: Sequence[CatalogItem], spec: CatalogSpec) -> CatalogItem | None:
    """
    The item the conversation is focused on:
    the first text (newest first) that names exactly one item's title words.
    """
    titled = [(item, _words(item_title(item, spec))) for item in items]
    for text in texts_newest_first:
        words = _words(text)
        hits = [item for item, tw in titled if tw and len(tw & words) >= max(2, len(tw) - 1)]
        if len(hits) == 1:
            return hits[0]
    return None


def topic_visible(field: str | None, needles: Sequence[str], items: Sequence[CatalogItem]) -> bool:
    if not field:
        return True
    if not needles:
        return any(item.get(field) for item in items)
    lowered = [n.lower() for n in needles]
    return any(any(n in str(item.get(field) or "").lower() for n in lowered) for item in items)
