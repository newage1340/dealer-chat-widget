"""
Builds model prompts as: universal base rules + the domain pack's sections + tenant facts + per-turn context.

Static content comes first and per-turn content last,
so providers with prefix caching (OpenAI) reuse the long static part across turns.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from core import Appointment, CatalogItem, Channel, Contact, Message, Role, Tenant
from domains.catalog import anchor_item, catalog_summary, item_details, item_title, match_items
from domains.pack import PROMPT_FIELDS, DomainPack, PromptValues, render
from providers import ChatMessage

from .hours import open_status_line, schedule_block

TEMPLATES_DIR = Path(__file__).parent / "templates"

MAX_HISTORY = 30


@dataclass(frozen=True, slots=True, kw_only=True)
class PromptInputs:
    tenant:       Tenant
    pack:         DomainPack
    items:        Sequence[CatalogItem]
    contact:      Contact
    history:      Sequence[Message]
    message:      str
    channel:      Channel
    now:          datetime  # tenant-local wall-clock time
    appointment:  Appointment | None = None
    caller_phone: str = ""


def _truthy(value) -> bool:
    if isinstance(value, str):
        return value.strip().lower() not in ("", "0", "false", "no", "none", "off")
    return bool(value)


def display_phone(phone: str) -> str:
    """
    "+13175550142" -> "(317) 555-0142"; anything else unchanged.
    """
    digits = "".join(c for c in phone if c.isdigit())
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}" if len(digits) == 10 else phone


def spoken_digits(phone: str) -> str:
    digits = "".join(c for c in phone if c.isdigit())[-10:]
    if len(digits) != 10:
        return phone
    return f"{'-'.join(digits[:3])}, {'-'.join(digits[3:6])}, {'-'.join(digits[6:])}"


class PromptComposer:
    def __init__(self, templates_dir: Path = TEMPLATES_DIR):
        self._base_chat = (templates_dir / "base_chat.md").read_text(encoding="utf-8")
        self._base_voice = (templates_dir / "base_voice.md").read_text(encoding="utf-8")
        self._summary = (templates_dir / "call_summary.md").read_text(encoding="utf-8")

    # --- shared values ------------------------------------------------------------

    def values(self, tenant: Tenant, pack: DomainPack, channel: Channel = Channel.SMS) -> PromptValues:
        """
        Placeholder values available to every template and domain prompt.
        """
        voice = channel is Channel.VOICE
        values = PromptValues({
            "business_name":    tenant.name,
            "business_noun":    pack.business_noun,
            "agent_name":       (tenant.voice_agent_name or pack.default_voice_agent_name) if voice
                                else (tenant.agent_name or pack.default_agent_name),
            "agent_role":       pack.voice_agent_role if voice else pack.agent_role,
            "staff_noun":       pack.staff_noun,
            "item_noun":        pack.catalog.schema.item_noun,
            "item_noun_plural": pack.catalog.schema.item_noun_plural,
            "spoken_item_noun": pack.catalog.spoken_noun,
            "visit_noun":       pack.booking.visit_noun,
            "general_subject":  pack.booking.general_subject,
            "subject_label":    pack.booking.subject_label,
            "phone":            display_phone(tenant.phone),
            "address":          tenant.address,
            "website":          tenant.website,
        })
        assert set(values) == PROMPT_FIELDS
        for key, value in tenant.settings.items():
            values[f"setting_{key}"] = value
        for key, value in tenant.profile.items():
            values[f"profile_{key}"] = value
        return values

    def domain_section(
        self,
        tenant: Tenant,
        pack: DomainPack,
        values: PromptValues,
        *,
        voice: bool
    ) -> str:
        parts = [render(pack.prompts.chat, values)]
        if voice:
            parts.append(render(pack.prompts.voice, values))
        for toggle in pack.prompts.toggles:
            on = _truthy(tenant.settings.get(toggle.setting))
            text = (toggle.voice if voice and toggle.voice else toggle.chat) if on else \
                   (toggle.else_voice if voice and toggle.else_voice else toggle.else_chat)
            if text:
                parts.append(render(text, values))
        body = "\n\n".join(p.strip() for p in parts if p.strip())
        return f"=== {pack.name.upper()} SPECIFICS ===\n{body}"

    # --- base templates -----------------------------------------------------------

    def _booking_values(self, pack: DomainPack, voice: bool) -> dict[str, str]:
        b = pack.booking
        channel_name = "voice" if voice else "chat"
        intake = [f for f in b.intake if channel_name in f.channels]
        customer_site = b.location == "customer"

        if voice:
            steps = "".join(f"  b{i}. {f.label}: {f.ask}\n" for i, f in enumerate(intake, 1))
            if b.skip_intake_when:
                steps += f"  (Skip the b-steps when {b.skip_intake_when}.)\n"
        elif intake:
            steps = "\n".join(f"- {f.label}: {f.ask}" for f in intake)
            if b.skip_intake_when:
                steps += f"\n- SKIP this step entirely when {b.skip_intake_when}."
        else:
            steps = "- Nothing extra to ask for this business - go straight to STEP 2."

        location_step = location_voice_step = ""
        if customer_site:
            location_step = (
                f"\nSTEP 0.5 - Where the {b.visit_noun} happens\n"
                "- The team comes to the customer. "
                f"Before confirming, get the full address for the {b.visit_noun} "
                "(street and city) unless it is already in the conversation. "
                "Include it as \"location\" in META_JSON.\n"
            )
            location_voice_step = f"  a2. Get the address for the {b.visit_noun} (street and city) if you don't have it.\n"

        details = ", ".join(f'"{f.key}": "<{f.label.lower()} or empty>"' for f in b.intake)
        return {
            "intake_steps":        steps,
            "voice_intake_steps":  steps if voice else "",
            "location_step":       location_step,
            "voice_location_step": location_voice_step,
            "location_hint":       f"address for the {b.visit_noun}" if customer_site else "leave empty",
            "subject_hint":        b.subject_hint,
            "subject_phrase":      f" for [{b.subject_hint}]",
            "see_phrase":          "helping you" if customer_site else "seeing you",
            "details_example":     details,
        }

    def _base(self, template: str, tenant: Tenant, pack: DomainPack, channel: Channel) -> str:
        values = self.values(tenant, pack, channel)
        values.update(self._booking_values(pack, channel is Channel.VOICE))
        values.update({
            "channel_label":       {Channel.SMS: "SMS", Channel.WEB: "web chat", Channel.VOICE: "phone"}[channel],
            "catalog_heading":     pack.catalog.heading,
            "phone_or_unlisted":   display_phone(tenant.phone) or "(phone not listed)",
            "goal":                pack.goal,
            "upsell_clause":       f" - {pack.upsell_topics} -" if pack.upsell_topics else "",
            "business_noun_title": pack.business_noun.title(),
        })
        return render(template, values)

    # --- per-tenant and per-turn blocks -------------------------------------------

    def _business_info(self, tenant: Tenant, pack: DomainPack) -> str:
        lines = [
            "=== BUSINESS INFO ===",
            f"Name: {tenant.name}",
            f"Address: {tenant.address or '(not listed)'}",
            f"Hours: {tenant.hours_text or '(not listed)'}",
            f"Phone (use exactly, never invent): {display_phone(tenant.phone) or '(not listed)'}",
        ]
        if tenant.website:
            lines.append(f"Website: {tenant.website}")
        for q in pack.profile:
            lines.append(f"{q.label}: {tenant.profile.get(q.key) or '(not listed)'}")
        return "\n".join(lines)

    def _catalog(self, inputs: PromptInputs) -> str:
        spec = inputs.pack.catalog
        count = len(inputs.items)
        noun = spec.schema.item_noun_plural.upper()
        head = [f"=== {spec.heading} ==="]
        if spec.intro:
            head.append(spec.intro)
        head.append(
            f"THERE ARE EXACTLY {count} {noun} LISTED. " +
            f"If asked how many, say {count} - " +
            "don't count the list yourself."
        )
        return "\n".join(head) + "\n" + catalog_summary(inputs.items, spec)

    def _focus(self, inputs: PromptInputs) -> str:
        spec = inputs.pack.catalog
        own = match_items(inputs.items, inputs.message, spec)
        assistant_texts = [m.text for m in reversed(inputs.history) if m.role is Role.ASSISTANT]
        anchor = anchor_item(assistant_texts[:3], inputs.items, spec)
        recent = " ".join(m.text for m in list(inputs.history)[-2:])

        if anchor is not None and (not own or anchor in own):
            title = item_title(anchor, spec)
            return (
                "=== TOP MATCHING DETAILS ===\n(Use ONLY these facts - do NOT guess anything not shown here)\n"
                f"{item_details(anchor, spec)}\n\n"
                f"=== FOCUS (CRITICAL) ===\nThe conversation has been focused on the \"{title}\". "
                f"Refer to it exactly; do not drift to a different {spec.schema.item_noun} or invent one."
            )
        matches = own or match_items(inputs.items, f"{recent} {inputs.message}", spec)
        details = "\n\n---\n\n".join(item_details(i, spec) for i in matches) if matches else \
            "(No close match found - what the customer asked about may not be something we have.)"
        return f"=== TOP MATCHING DETAILS ===\n(Use ONLY these facts - do NOT guess anything not shown here)\n{details}"

    def _customer(self, inputs: PromptInputs) -> str:
        c = inputs.contact
        known = [
            f"- First name: {c.first_name}" if c.first_name else "",
            f"- Last name: {c.last_name}" if c.last_name else "",
            f"- Email: {c.email}" if c.email else "",
            f"- Phone (for follow-up texts): {c.phone}" if c.phone else "",
        ]
        known = [k for k in known if k]
        block = "Already collected:\n" + "\n".join(known) if known else "No customer details collected yet."
        rules = []
        if c.first_name:
            rules.append("Use the first name naturally when it fits. Never ask for it again.")
        if c.phone:
            rules.append("The phone number is ALREADY ON FILE - never ask for it.")
        rules.append("Email is asked only during STEP 2 of the booking flow. LAST NAME IS NEVER REQUIRED - never ask for it.")
        return "=== CUSTOMER PROFILE ===\n" + block + "\n" + " ".join(rules)

    def _appointment(self, inputs: PromptInputs) -> str:
        a = inputs.appointment
        if a is None:
            return "=== CONFIRMED APPOINTMENT ===\nNo appointment confirmed yet."
        what = f"{a.time_text} for {a.subject}"
        starts_local = a.starts_at.astimezone(ZoneInfo(inputs.tenant.timezone)).replace(tzinfo=None) if a.starts_at else None
        if starts_local is not None and starts_local < inputs.now:
            return (
                "=== CONFIRMED APPOINTMENT ===\n"
                f"This customer had an appointment {what}, but that time has ALREADY PASSED. "
                "Don't talk about it as upcoming; acknowledge it and gently offer to reschedule if it fits."
            )
        return (
            "=== CONFIRMED APPOINTMENT ===\n"
           f"This customer has a confirmed appointment {what}. "
           "Don't push for a booking - they're booked. \n"
           "Answer their questions naturally."
        )

    def _time(self, inputs: PromptInputs) -> str:
        now = inputs.now
        return (
            "=== CURRENT DATE & TIME (use to interpret \"tomorrow\", \"Friday\", etc.) ===\n"
            f"Today is {now.strftime('%A, %B %d, %Y at %I:%M %p')}.\n\n"
            "=== WHICH DAYS WE ARE ACTUALLY OPEN (already worked out - do NOT calculate this yourself) ===\n"
            f"{schedule_block(inputs.tenant.hours_text, now)}\n"
            "NEVER offer, suggest, or confirm a time on a day marked CLOSED.\n"
            "If the day they asked for is closed, say so and offer the next open day by name.\n"
            "If TOMORROW is closed, don't use the word \"tomorrow\" for a booking - name the next open day instead.\n"
            "Quote the opening time listed for THAT day."
        )

    def _real_world(self, inputs: PromptInputs) -> str:
        now = inputs.now
        status = open_status_line(inputs.tenant.hours_text, now)
        return (
            "=== REAL-WORLD FACTS (USE THESE - DO NOT GUESS) ===\n"
            f"CURRENT TIME: {now.strftime('%A %B %d, %Y at %I:%M %p')}\n"
            f"HOURS: {inputs.tenant.hours_text or '(not listed)'}\n"
            + (status + "\n" if status else "") +
            "-   Being closed never limits what you answer; it only affects coming in right now.\n"
            "-   Only bring up open/closed when the caller asks about now/today.\n"
            "    Don't announce it during a booking for a future day.\n"
            "-   Never say we're open until a time that has already passed.\n"
            "    Only suggest future times within the hours."
        )

    def _caller_number(self, inputs: PromptInputs) -> str:
        if not inputs.caller_phone:
            return ""
        return (
            "=== CALLER'S NUMBER (from caller ID - already on file) ===\n"
            f"{spoken_digits(inputs.caller_phone)}.\n"
            "Never ask for it and never read it back; use it silently."
        )

    def _history_messages(self, history: Sequence[Message]) -> list[ChatMessage]:
        return [ChatMessage(m.role, m.text) for m in list(history)[-MAX_HISTORY:]
                if m.role in (Role.USER, Role.ASSISTANT) and m.text.strip()]

    # --- public builders ----------------------------------------------------------

    def chat_messages(self, inputs: PromptInputs) -> list[ChatMessage]:
        t, p = inputs.tenant, inputs.pack
        values = self.values(t, p, inputs.channel)
        system = "\n\n".join([
            self._base(self._base_chat, t, p, inputs.channel),
            self.domain_section(t, p, values, voice=False),
            self._business_info(t, p),
            self._catalog(inputs),
            self._focus(inputs),
            self._customer(inputs),
            self._appointment(inputs),
            self._time(inputs),
            "Write ONE reply now.",
        ])
        return [
            ChatMessage(Role.SYSTEM, system), *self._history_messages(inputs.history),
            ChatMessage(Role.USER, inputs.message),
        ]

    def voice_messages(self, inputs: PromptInputs) -> list[ChatMessage]:
        t, p = inputs.tenant, inputs.pack
        values = self.values(t, p, Channel.VOICE)
        system = "\n\n".join(s for s in [
            self._base(self._base_voice, t, p, Channel.VOICE),
            self.domain_section(t, p, values, voice=True),
            self._business_info(t, p),
            self._catalog(inputs),
            self._focus(inputs),
            self._customer(inputs),
            self._appointment(inputs),
            self._real_world(inputs),
            self._time(inputs),
            self._caller_number(inputs),
        ] if s)
        stamp = f"[NOW: {inputs.now.strftime('%A %I:%M %p')}]"
        return [
            ChatMessage(Role.SYSTEM, system), *self._history_messages(inputs.history),
            ChatMessage(Role.USER, f"{stamp}\n{inputs.message}"),
        ]

    def summary_messages(
        self,
        tenant: Tenant,
        pack: DomainPack,
        transcript: str,
        caller_info: str
    ) -> list[ChatMessage]:
        values = self.values(tenant, pack, Channel.VOICE)
        intake_sections = "".join(f"\n{f.label.upper()}\n  One line, if it came up.\n" for f in pack.booking.intake)
        values.update({
            "subject_hint":    pack.booking.subject_hint,
            "location_line":   f", plus the address for the {pack.booking.visit_noun}"
                               if pack.booking.location == "customer" else "",
            "intake_sections": intake_sections,
            "summary_focus":   f"\nWHAT MATTERS MOST FOR THIS BUSINESS\n  {render(pack.prompts.summary_focus, values)}\n"
                               if pack.prompts.summary_focus else "",
        })
        system = render(self._summary, values)
        user = f"CALL TRANSCRIPT:\n{transcript}\n\nKNOWN CALLER INFO:\n{caller_info or '(none yet)'}"
        return [ChatMessage(Role.SYSTEM, system), ChatMessage(Role.USER, user)]

    def followup_text(
        self,
        tenant: Tenant,
        pack: DomainPack,
        kind: str,
        contact: Contact,
        **extra: str
    ) -> str | None:
        template = pack.followups.get(kind)
        if not template:
            return None
        values = self.values(tenant, pack)
        values.update({"first_name": contact.first_name or "there", **extra})
        return template.format_map(values)
