"""
Domain packs: everything that makes the assistant a car dealer, a plumber, a salon, ...

A pack is ONE TOML file with a fixed shape (see `_template.toml` and AUTHORING.md).
The universal base prompts and code never mention a domain;
a pack supplies the vocabulary, catalog shape, booking intake, widget topics,
voice hints and the domain-specific prompt sections that are appended to the base.
"""

import string
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from core import CatalogSchema, FieldSpec, FieldType


class DomainPackError(ValueError):
    """
    A pack failed to load. `problems` lists every issue found, not just the first.
    """

    def __init__(self, path: str, problems: list[str]):
        super().__init__(f"{path}:\n  - " + "\n  - ".join(problems))
        self.path = path
        self.problems = problems


@dataclass(frozen=True, slots=True, kw_only=True)
class ProfileQuestion:
    """
    A question each tenant answers when onboarding (e.g. "Do you offer financing?").
    Answers live in Tenant.profile[key] and are shown to the model under `label`.
    """

    key:      str
    label:    str
    question: str
    required: bool = False


@dataclass(frozen=True, slots=True, kw_only=True)
class IntakeField:
    """
    Something the agent collects before confirming a booking (e.g. trade-in, service address).
    `ask` tells the agent what to ask and when it can be skipped.
    """

    key:      str
    label:    str
    ask:      str
    required: bool = False
    channels: tuple[str, ...] = ("chat", "voice")


@dataclass(frozen=True, slots=True, kw_only=True)
class BookingSpec:
    """
    `location` is "business" when the customer comes in, "customer" when staff go to them.
    """

    visit_noun:       str
    subject_label:    str
    subject_hint:     str
    general_subject:  str
    location:         str = "business"
    skip_intake_when: str = ""
    intake:           tuple[IntakeField, ...] = ()


@dataclass(frozen=True, slots=True, kw_only=True)
class SendableField:
    """
    A catalog field the system texts on request instead of letting the model write it
    (listing URLs, history reports, ...). `trigger` is a regex matched against the customer's message.
    """

    field:   str
    label:   str
    trigger: str


@dataclass(frozen=True, slots=True, kw_only=True)
class CatalogSpec:
    """
    `aliases` maps raw source keys (e.g. scraped "CarfaxURL") to schema field names.
    `summary_parts` are format strings joined into the one-line summary;
    a part is dropped when every field it references is empty and has no `empty_text`.
    """

    schema:           CatalogSchema
    spoken_noun:      str
    heading:          str
    intro:            str
    empty_text:       str
    aliases:          Mapping[str, str] = field(default_factory=dict)
    summary_parts:    tuple[str, ...] = ()
    summary_suffix:   str = ""
    url_field:        str | None = None
    price_field:      str | None = None
    sendables:        tuple[SendableField, ...] = ()


@dataclass(frozen=True, slots=True, kw_only=True)
class WidgetTopic:
    """
    A sidebar shortcut. When `requires_field` is set, the topic is shown only if
    some catalog item's field contains one of `requires_any` (case-insensitive).
    """

    section:        str
    label:          str
    message:        str
    requires_field: str | None = None
    requires_any:   tuple[str, ...] = ()


@dataclass(frozen=True, slots=True, kw_only=True)
class WidgetSpec:
    header_title:      str
    tagline:           str
    input_placeholder: str
    icon:              str
    capabilities:      str
    topics:            tuple[WidgetTopic, ...] = ()


@dataclass(frozen=True, slots=True, kw_only=True)
class VoiceSpec:
    speech_hints:   str = ""
    speak_years:    bool = False
    pronunciations: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True, kw_only=True)
class PromptToggle:
    """
    A domain prompt section included only when a tenant setting is truthy (or falsy, via `else_*`).
    """

    setting:    str
    chat:       str = ""
    voice:      str = ""
    else_chat:  str = ""
    else_voice: str = ""


@dataclass(frozen=True, slots=True, kw_only=True)
class DomainPrompts:
    chat:          str
    voice:         str
    summary_focus: str = ""
    toggles:       tuple[PromptToggle, ...] = ()


@dataclass(frozen=True, slots=True, kw_only=True)
class DomainPack:
    id:                       str
    name:                     str
    description:              str
    business_noun:            str
    staff_noun:               str
    agent_role:               str
    voice_agent_role:         str
    default_agent_name:       str
    default_voice_agent_name: str
    goal:                     str
    upsell_topics:            str
    catalog:                  CatalogSpec
    booking:                  BookingSpec
    widget:                   WidgetSpec
    voice:                    VoiceSpec
    prompts:                  DomainPrompts
    profile:                  tuple[ProfileQuestion, ...] = ()
    followups:                Mapping[str, str] = field(default_factory=dict)
    source_path:              str = ""


# --- Placeholders -------------------------------------------------------------

# Fields any domain prompt text may reference. `setting_<key>` and `profile_<key>`
# are also allowed and resolve to the tenant's settings / profile answers.
PROMPT_FIELDS = frozenset({
    "business_name", "business_noun", "agent_name", "agent_role", "staff_noun",
    "item_noun", "item_noun_plural", "spoken_item_noun", "visit_noun", "general_subject",
    "subject_label", "phone", "address", "website",
})
_PREFIXES = ("setting_", "profile_")


# Extra fields available only to [followups] templates.
FOLLOWUP_FIELDS = frozenset({"first_name", "visit_time", "subject"})


def placeholder_problems(where: str, text: str, extra: frozenset[str] = frozenset()) -> list[str]:
    """
    Problems with `{placeholders}` in a piece of pack text (unknown names, unbalanced braces).
    """
    problems = []
    try:
        names = [f for _, f, _, _ in string.Formatter().parse(text) if f is not None]
    except ValueError as e:
        return [f"{where}: {e} (write literal braces as {{{{ and }}}})"]
    for name in names:
        base = name.split(".")[0].split("[")[0]
        if base not in PROMPT_FIELDS and base not in extra and not base.startswith(_PREFIXES):
            problems.append(f"{where}: unknown placeholder {{{name}}}")
    return problems


class PromptValues(dict):
    """
    Format-map values: unknown setting_/profile_ keys render as empty strings.
    """

    def __missing__(self, key: str) -> str:
        if key.startswith(_PREFIXES):
            return ""
        raise KeyError(key)


def render(text: str, values: Mapping[str, Any]) -> str:
    return text.format_map(values if isinstance(values, PromptValues) else PromptValues(values))


# --- Loading ------------------------------------------------------------------

class _Reader:
    """
    Reads typed values out of the parsed TOML while collecting problems.
    """

    def __init__(self, path: str):
        self.path = path
        self.problems: list[str] = []

    def table(self, data: Mapping, key: str, where: str, required: bool = True) -> Mapping:
        value = data.get(key)
        if value is None:
            if required:
                self.problems.append(f"missing [{where}]")
            return {}
        if not isinstance(value, Mapping):
            self.problems.append(f"[{where}] must be a table")
            return {}
        return value

    def tables(self, data: Mapping, key: str, where: str) -> list[Mapping]:
        value = data.get(key, [])
        if not isinstance(value, list) or not all(isinstance(v, Mapping) for v in value):
            self.problems.append(f"[[{where}]] must be an array of tables")
            return []
        return value

    def text(self, data: Mapping, key: str, where: str, default: str | None = None) -> str:
        value = data.get(key, default)
        if value is None:
            self.problems.append(f"missing {where}.{key}")
            return ""
        if not isinstance(value, str):
            self.problems.append(f"{where}.{key} must be a string")
            return ""
        return value

    def flag(self, data: Mapping, key: str, where: str, default: bool = False) -> bool:
        value = data.get(key, default)
        if not isinstance(value, bool):
            self.problems.append(f"{where}.{key} must be true or false")
            return default
        return value

    def texts(self, data: Mapping, key: str, where: str) -> tuple[str, ...]:
        value = data.get(key, [])
        if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
            self.problems.append(f"{where}.{key} must be a list of strings")
            return ()
        return tuple(value)


def load_pack(path: str | Path) -> DomainPack:
    path = Path(path)
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as e:
        raise DomainPackError(str(path), [str(e)]) from e
    return parse_pack(data, str(path))


def parse_pack(data: Mapping[str, Any], path: str = "<memory>") -> DomainPack:
    r = _Reader(path)

    d = r.table(data, "domain", "domain")
    cat = r.table(data, "catalog", "catalog")
    book = r.table(data, "booking", "booking")
    wid = r.table(data, "widget", "widget")
    voi = r.table(data, "voice", "voice", required=False)
    pr = r.table(data, "prompts", "prompts")

    field_specs = []
    for i, f in enumerate(r.tables(cat, "fields", "catalog.fields")):
        where = f"catalog.fields[{i}]"
        type_name = r.text(f, "type", where, "text")
        try:
            ftype = FieldType(type_name)
        except ValueError:
            r.problems.append(f"{where}.type must be one of {[t.value for t in FieldType]}")
            ftype = FieldType.TEXT
        field_specs.append(FieldSpec(
            name       = r.text(f, "name", where),
            type       = ftype,
            label      = r.text(f, "label", where, ""),
            required   = r.flag(f, "required", where),
            searchable = r.flag(f, "searchable", where),
            empty_text = r.text(f, "empty_text", where, ""),
            in_details = r.flag(f, "in_details", where, True),
        ))
    field_names = {f.name for f in field_specs}
    if not field_specs:
        r.problems.append("catalog needs at least one [[catalog.fields]] entry")

    schema = CatalogSchema(
        item_noun        = r.text(cat, "item_noun", "catalog"),
        item_noun_plural = r.text(cat, "item_noun_plural", "catalog"),
        fields           = tuple(field_specs),
        title_template   = r.text(cat, "title_template", "catalog"),
        id_field         = cat.get("id_field") or None,
    )

    def check_field(name: str | None, where: str):
        if name and name not in field_names:
            r.problems.append(f"{where} refers to unknown catalog field '{name}'")

    check_field(schema.id_field, "catalog.id_field")
    aliases = cat.get("aliases", {})
    if not isinstance(aliases, Mapping):
        r.problems.append("[catalog.aliases] must be a table of raw_key = \"field_name\"")
        aliases = {}
    for raw, target in aliases.items():
        check_field(target, f"catalog.aliases.{raw}")

    sendables = []
    for i, s in enumerate(r.tables(cat, "sendables", "catalog.sendables")):
        where = f"catalog.sendables[{i}]"
        sendable = SendableField(field=r.text(s, "field", where), label=r.text(s, "label", where), trigger=r.text(s, "trigger", where))
        check_field(sendable.field, f"{where}.field")
        sendables.append(sendable)

    catalog = CatalogSpec(
        schema         = schema,
        spoken_noun    = r.text(cat, "spoken_noun", "catalog", schema.item_noun),
        heading        = r.text(cat, "heading", "catalog", schema.item_noun_plural.upper()),
        intro          = r.text(cat, "intro", "catalog", ""),
        empty_text     = r.text(cat, "empty_text", "catalog", f"(No {schema.item_noun_plural} listed yet.)"),
        aliases        = {str(k).lower(): v for k, v in aliases.items()},
        summary_parts  = r.texts(cat, "summary_parts", "catalog"),
        summary_suffix = r.text(cat, "summary_suffix", "catalog", ""),
        url_field      = cat.get("url_field") or None,
        price_field    = cat.get("price_field") or None,
        sendables      = tuple(sendables),
    )
    check_field(catalog.url_field, "catalog.url_field")
    check_field(catalog.price_field, "catalog.price_field")
    for part in (*catalog.summary_parts, catalog.summary_suffix, schema.title_template):
        for _, name, _, _ in string.Formatter().parse(part):
            if name:
                check_field(name, f"catalog template '{part}'")

    intake = []
    for i, f in enumerate(r.tables(book, "intake", "booking.intake")):
        where = f"booking.intake[{i}]"
        intake.append(IntakeField(
            key      = r.text(f, "key", where),
            label    = r.text(f, "label", where),
            ask      = r.text(f, "ask", where),
            required = r.flag(f, "required", where),
            channels = r.texts(f, "channels", where) or ("chat", "voice"),
        ))
    booking = BookingSpec(
        visit_noun       = r.text(book, "visit_noun", "booking"),
        subject_label    = r.text(book, "subject_label", "booking"),
        subject_hint     = r.text(book, "subject_hint", "booking"),
        general_subject  = r.text(book, "general_subject", "booking"),
        location         = r.text(book, "location", "booking", "business"),
        skip_intake_when = r.text(book, "skip_intake_when", "booking", ""),
        intake           = tuple(intake),
    )
    if booking.location not in ("business", "customer"):
        r.problems.append("booking.location must be \"business\" or \"customer\"")

    topics = []
    for i, t in enumerate(r.tables(wid, "topics", "widget.topics")):
        where = f"widget.topics[{i}]"
        topic = WidgetTopic(
            section        = r.text(t, "section", where),
            label          = r.text(t, "label", where),
            message        = r.text(t, "message", where),
            requires_field = t.get("requires_field") or None,
            requires_any   = r.texts(t, "requires_any", where),
        )
        check_field(topic.requires_field, f"{where}.requires_field")
        topics.append(topic)
    widget = WidgetSpec(
        header_title      = r.text(wid, "header_title", "widget"),
        tagline           = r.text(wid, "tagline", "widget", "Instant answers, 24/7."),
        input_placeholder = r.text(wid, "input_placeholder", "widget"),
        icon              = r.text(wid, "icon", "widget", "💬"),
        capabilities      = r.text(wid, "capabilities", "widget"),
        topics            = tuple(topics),
    )

    pronunciations = []
    for i, p in enumerate(r.tables(voi, "pronunciations", "voice.pronunciations")):
        where = f"voice.pronunciations[{i}]"
        pronunciations.append((r.text(p, "pattern", where), r.text(p, "say", where)))
    voice = VoiceSpec(
        speech_hints   = r.text(voi, "speech_hints", "voice", ""),
        speak_years    = r.flag(voi, "speak_years", "voice"),
        pronunciations = tuple(pronunciations),
    )

    toggles = []
    for i, t in enumerate(r.tables(pr, "toggles", "prompts.toggles")):
        where = f"prompts.toggles[{i}]"
        toggles.append(PromptToggle(
            setting    = r.text(t, "setting", where),
            chat       = r.text(t, "chat", where, ""),
            voice      = r.text(t, "voice", where, ""),
            else_chat  = r.text(t, "else_chat", where, ""),
            else_voice = r.text(t, "else_voice", where, ""),
        ))
    prompts = DomainPrompts(
        chat          = r.text(pr, "chat", "prompts"),
        voice         = r.text(pr, "voice", "prompts"),
        summary_focus = r.text(pr, "summary_focus", "prompts", ""),
        toggles       = tuple(toggles),
    )

    profile = []
    for i, q in enumerate(r.tables(data, "profile", "profile")):
        where = f"profile[{i}]"
        profile.append(ProfileQuestion(
            key      = r.text(q, "key", where),
            label    = r.text(q, "label", where),
            question = r.text(q, "question", where),
            required = r.flag(q, "required", where),
        ))

    followups = data.get("followups", {})
    if not isinstance(followups, Mapping) or not all(isinstance(v, str) for v in followups.values()):
        r.problems.append("[followups] must be a table of name = \"template\"")
        followups = {}

    pack = DomainPack(
        id                       = r.text(d, "id", "domain"),
        name                     = r.text(d, "name", "domain"),
        description              = r.text(d, "description", "domain", ""),
        business_noun            = r.text(d, "business_noun", "domain"),
        staff_noun               = r.text(d, "staff_noun", "domain"),
        agent_role               = r.text(d, "agent_role", "domain"),
        voice_agent_role         = r.text(d, "voice_agent_role", "domain", "receptionist"),
        default_agent_name       = r.text(d, "default_agent_name", "domain"),
        default_voice_agent_name = r.text(d, "default_voice_agent_name", "domain"),
        goal                     = r.text(d, "goal", "domain"),
        upsell_topics            = r.text(d, "upsell_topics", "domain", ""),
        catalog                  = catalog,
        booking                  = booking,
        widget                   = widget,
        voice                    = voice,
        prompts                  = prompts,
        profile                  = tuple(profile),
        followups                = dict(followups),
        source_path              = path,
    )

    texts = {
        "domain.goal":           pack.goal,
        "prompts.chat":          prompts.chat,
        "prompts.voice":         prompts.voice,
        "prompts.summary_focus": prompts.summary_focus,
        "booking.skip_intake_when": booking.skip_intake_when,
        "widget.capabilities":   widget.capabilities,
        **{f"booking.intake[{i}].ask": f.ask for i, f in enumerate(intake)},
    }
    for i, t in enumerate(toggles):
        for attr in ("chat", "voice", "else_chat", "else_voice"):
            texts[f"prompts.toggles[{i}].{attr}"] = getattr(t, attr)
    for where, text in texts.items():
        r.problems.extend(placeholder_problems(where, text))
    for key, text in pack.followups.items():
        r.problems.extend(placeholder_problems(f"followups.{key}", text, FOLLOWUP_FIELDS))

    if r.problems:
        raise DomainPackError(path, r.problems)
    return pack
