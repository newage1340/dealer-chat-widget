"""
The turn handlers. Each looks at a TurnContext and either passes (None) or returns an Outcome.

Order matters: compliance first (opt-out), then gates, then the model conversation.
New behavior (domain-specific shortcuts, deterministic answers) is added as another handler,
not by editing the conversation handler.
"""

import logging
import re
from collections.abc import Sequence

from core import (
    Action,
    AddressKind,
    BookAppointment,
    CatalogItem,
    Channel,
    EndSession,
    NotifyStaff,
    Outcome,
    Reply,
    Role,
    SendMessage,
    TransferCall,
    TurnContext,
    UpdateContact,
)
from domains import DomainPack
from domains.catalog import anchor_item, item_title, match_items
from prompts import PromptInputs
from providers import LLMRequest, ModelTier, ProviderError
from tenants import phone_address

from .meta import booking_subject, parse_local_iso, parse_reply
from .services import Services

log = logging.getLogger(__name__)

FALLBACK_TEXT = "Sorry, I had trouble processing that. Could you try again?"
FALLBACK_VOICE = "Sorry, I had a hiccup there - what were you asking about?"

_AFFIRMATIVE_RE = re.compile(r"^\s*(yes|yeah|yep|yup|sure|ok|okay|please|absolutely|definitely|go ahead|send it|sounds good)\b", re.I)
_SENDING_RE = re.compile(r"\b(send|sending|text|texting|on (?:its|the) way|shoot)\b", re.I)


def _truthy(value) -> bool:
    if isinstance(value, str):
        return value.strip().lower() not in ("", "0", "false", "no", "off")
    return bool(value)


class OptOutHandler:
    """
    SMS compliance keywords (STOP / START / HELP). Opted-out contacts get no replies.
    """

    name = "opt_out"
    _STOP = frozenset({"stop", "stopall", "unsubscribe", "cancel", "end", "quit"})
    _START = frozenset({"start", "unstop", "resume"})

    def handle(self, ctx: TurnContext) -> Outcome | None:
        if ctx.channel is not Channel.SMS:
            return None
        word = re.sub(r"[^a-z]", "", ctx.text.lower())
        name = ctx.tenant.name
        if word in self._STOP:
            return Outcome(
                reply   = Reply(text=f"You've been unsubscribed from {name} messages. Reply START to resubscribe."),
                actions = (UpdateContact(changes={"opted_out": True}),),
            )
        if word in self._START:
            return Outcome(
                reply   = Reply(text=f"You're resubscribed to {name} messages. Reply STOP to opt out anytime."),
                actions = (UpdateContact(changes={"opted_out": False}),),
            )
        if word == "help":
            phone = f" or call {ctx.tenant.phone}" if ctx.tenant.phone else ""
            return Outcome(reply=Reply(text=f"{name}: reply here with any question{phone}. Msg & data rates may apply. Reply STOP to opt out."))
        if ctx.contact.attributes.get("opted_out"):
            return Outcome(reply=None)
        return None


class WebProfileGate:
    """
    Web-widget visitors give a first name and phone before the assistant answers,
    so confirmations and follow-ups can reach a real number.
    Tenants can opt out with settings.skip_profile_gate (e.g. a demo site).
    """

    name = "profile_gate"
    TEXT = ("Sure, I can help with that. Before I do, could I get your first name and 10-digit phone number? "
            "We use these so we can text you appointment confirmations and follow up if you have any questions.")

    def handle(self, ctx: TurnContext) -> Outcome | None:
        if ctx.channel is not Channel.WEB or _truthy(ctx.tenant.settings.get("skip_profile_gate")):
            return None
        missing_name = not ctx.contact.first_name
        missing_phone = not ctx.contact.phone
        if not (missing_name or missing_phone):
            return None
        return Outcome(reply=Reply(text=self.TEXT, data={
            "needs_profile":   True,
            "missing":         {"name": missing_name, "phone": missing_phone},
            "pending_message": ctx.text,
            "ephemeral":       True,
        }))


def strip_spoken_name(text: str, first_name: str) -> str:
    """
    Remove the caller's first name from direct-address positions in a spoken reply
    ("Got it, Evan!" -> "Got it!"); speech-to-text mishears names too often to say them.
    """
    if not text or len((first_name or "").strip()) < 2:
        return text
    n = re.escape(first_name.strip())
    t = re.sub(rf"\b(hey|hi|hello|oh|yeah|sure|okay|ok|awesome|great|perfect|alright|thanks|thank you|got it)\s+{n}\b",
               r"\1", text, flags=re.I)
    t = re.sub(rf",\s*{n}\b(?=\s*(?:[!.?,—-]|and\b|$))", "", t, flags=re.I)
    t = re.sub(rf"^\s*{n}\s*,\s*", "", t, flags=re.I)
    t = re.sub(r"\s{2,}", " ", t)
    return re.sub(r"\s+([,!.?])", r"\1", t).strip() or text


class ConversationHandler:
    """
    The model-driven reply: compose base + domain prompt, call the LLM router,
    then turn the reply's machine-readable parts into Actions.
    """

    name = "conversation"

    def __init__(self, services: Services):
        self.s = services

    def handle(self, ctx: TurnContext) -> Outcome | None:
        record = self.s.directory.get(ctx.tenant.id)
        items = record.items if record else ()
        pack = self.s.pack(ctx.tenant)
        voice = ctx.channel is Channel.VOICE
        appointment = self.s.store.latest_appointment(ctx.key)

        inputs = PromptInputs(
            tenant       = ctx.tenant,
            pack         = pack,
            items        = items,
            contact      = ctx.contact,
            history      = ctx.history,
            message      = ctx.text,
            channel      = ctx.channel,
            now          = ctx.now,
            appointment  = appointment,
            caller_phone = ctx.inbound.sender.value if voice and ctx.inbound.sender.kind is AddressKind.PHONE else "",
        )
        messages = self.s.composer.voice_messages(inputs) if voice else self.s.composer.chat_messages(inputs)
        via_vapi = ctx.inbound.raw.get("via") == "vapi"
        request = LLMRequest(
            messages    = messages,
            tier        = ModelTier.FAST if voice else ModelTier.CAPABLE,
            max_tokens  = 400 if voice else 600,
            temperature = 0.4,
            timeout_s   = (25 if via_vapi else 12) if voice else 40,
            purpose     = "turn",
            tenant_id   = ctx.tenant.id,
            channel     = ctx.channel,
        )
        try:
            response = self.s.llm.complete(request)
        except ProviderError as e:
            log.error("LLM turn failed for %s: %s", ctx.tenant.id, e)
            return Outcome(reply=Reply(text=FALLBACK_VOICE if voice else FALLBACK_TEXT))

        parsed = parse_reply(response.text)
        text = parsed.text or (FALLBACK_VOICE if voice else FALLBACK_TEXT)
        actions: list[Action] = []
        state: dict = {}

        profile = dict(parsed.profile)
        if ctx.channel is not Channel.WEB:
            profile.pop("phone", None)  # SMS/voice already know the real number
        if profile:
            actions.append(UpdateContact(changes=profile))

        booking = parsed.booking
        if booking and booking.get("confirmed") and (booking.get("visit_time") or booking.get("visit_time_iso")):
            actions.append(BookAppointment(
                time_text   = str(booking.get("visit_time") or booking.get("visit_time_iso")),
                subject     = booking_subject(booking) or pack.booking.general_subject,
                starts_at   = parse_local_iso(str(booking.get("visit_time_iso") or ""), ctx.tenant.timezone),
                location    = str(booking.get("location") or ""),
                details     = booking.get("details") if isinstance(booking.get("details"), dict) else {},
                rescheduled = bool(booking.get("reschedule")) or appointment is not None,
            ))

        sends = self._links_to_send(ctx, pack, items, text)
        if sends:
            already = list(ctx.state.get("sent_urls", []))
            fresh = [(label, url) for label, url in sends if url not in already]
            if fresh:
                state["sent_urls"] = already + [url for _, url in fresh]
                if voice:
                    for label, url in fresh:
                        actions.append(SendMessage(to=ctx.inbound.sender, channel=Channel.SMS,
                                                   text=f"Here's the {label} from {ctx.tenant.name}: {url}"))
                else:
                    text += "\n\n" + "\n".join(f"{label[:1].upper()}{label[1:]}: {url}" for label, url in fresh)

        if voice:
            text = strip_spoken_name(text, ctx.contact.first_name)
            actions.extend(self._call_control(ctx, pack, parsed.tokens, text, booked=any(isinstance(a, BookAppointment) for a in actions)))

        return Outcome(reply=Reply(text=text), actions=tuple(actions), state_updates=state)

    # --- links ----------------------------------------------------------------------

    def _links_to_send(self, ctx: TurnContext, pack: DomainPack, items: Sequence[CatalogItem], reply: str) -> list[tuple[str, str]]:
        """
        URLs the system should send this turn: catalog sendables for the item in focus,
        and tenant links (settings.links) whose trigger matches.
        """
        user = ctx.text
        last_bot = next((m.text for m in reversed(ctx.history) if m.role is Role.ASSISTANT), "")
        said_yes = bool(_AFFIRMATIVE_RE.match(user))
        promising = bool(_SENDING_RE.search(reply))

        def wanted(trigger: str) -> bool:
            if re.search(trigger, user, re.I):
                return True
            return said_yes and bool(re.search(trigger, last_bot, re.I))

        sends: list[tuple[str, str]] = []
        spec = pack.catalog
        if spec.sendables and items:
            focus = anchor_item([reply, last_bot], items, spec) or next(iter(match_items(items, user, spec, 1)), None)
            if focus is not None:
                for sendable in spec.sendables:
                    url = str(focus.get(sendable.field) or "")
                    if url and wanted(sendable.trigger):
                        sends.append((f"{sendable.label} for the {item_title(focus, spec)}", url))

        for link in ctx.tenant.settings.get("links") or []:
            url, trigger = link.get("url", ""), link.get("trigger", "")
            if url and trigger and wanted(trigger) and (promising or said_yes):
                sends.append((link.get("label", "link"), url))
        return sends

    # --- voice call control ---------------------------------------------------------

    def _call_control(self, ctx: TurnContext, pack: DomainPack, tokens: frozenset[str], reply: str, *, booked: bool) -> list[Action]:
        actions: list[Action] = []
        if "TRANSFER" in tokens:
            target = phone_address(str(ctx.tenant.settings.get("transfer_to") or ctx.tenant.phone or ""))
            if target is not None:
                actions.append(NotifyStaff(text=self._summary(ctx, pack, reply, "Transferred call"), reason="call_transfer"))
                actions.append(TransferCall(to=target))
                return actions
            tokens = tokens | {"TAKE_MESSAGE"}
        if "TAKE_MESSAGE" in tokens:
            if not booked:
                actions.append(NotifyStaff(text=self._summary(ctx, pack, reply, "Call lead"), reason="call_handoff"))
            actions.append(EndSession())
        elif "HANGUP" in tokens:
            actions.append(EndSession())
        return actions

    def _summary(self, ctx: TurnContext, pack: DomainPack, reply: str, headline: str) -> str:
        call = [m for m in ctx.history if m.meta.get("session_id") == ctx.inbound.session_id] or list(ctx.history)[-20:]
        lines = [f"{'caller' if m.role is Role.USER else 'agent'}: {m.text}" for m in call]
        lines += [f"caller: {ctx.text}", f"agent: {reply}"]
        caller = ctx.inbound.sender.value
        name = ctx.contact.display_name
        info = "\n".join(x for x in (f"name: {name}" if name else "", f"phone: {caller}") if x)
        messages = self.s.composer.summary_messages(ctx.tenant, pack, "\n".join(lines), info)
        try:
            body = self.s.llm.complete(LLMRequest(
                messages=messages, tier=ModelTier.CAPABLE, max_tokens=550, temperature=0.2,
                purpose="call_summary", tenant_id=ctx.tenant.id, channel=ctx.channel,
            )).text
        except ProviderError as e:
            log.warning("call summary failed: %s", e)
            body = "(summary unavailable - see conversation history)"
        who = f"{name} - {caller}" if name else caller
        return f"[{ctx.tenant.name} AI - {headline}]\nCall back: {who}\n\n{body}"
