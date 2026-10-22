"""
Twilio voice: speech <Gather> loop driven by the same turn pipeline as chat and SMS.
"""

import logging
import random
from datetime import datetime, timezone

from flask import Blueprint, Response, request
from twilio.twiml.voice_response import Gather, VoiceResponse

from core import Address, AddressKind, Channel, ConversationKey, EndSession, InboundMessage, Role, TransferCall
from domains import DomainPack
from pipeline import TurnResult
from tenants import TenantRecord, phone_address

from . import speech
from .context import ChannelContext

log = logging.getLogger(__name__)

MAX_TURNS = 30
MAX_SILENT_TURNS = 3
LOW_CONFIDENCE = 0.45
STILL_THERE = "Hey, you still there?"
GOODBYE = "Alright, I'll let the team know you reached out. Take it easy!"
REPROMPT = "Sorry, I didn't quite catch that - mind saying it one more time?"
WRAPUP = "Thanks for the time - I'll pass everything along and someone will follow up. Take care!"


def greeting(record: TenantRecord, pack: DomainPack, now: datetime) -> str:
    """
    A varied, time-aware opener that asks for the caller's name up front,
    so even early hang-ups reach the team with a name.
    """
    t = record.tenant
    agent = t.voice_agent_name or pack.default_voice_agent_name
    part = "Good morning" if now.hour < 12 else "Good afternoon" if now.hour < 17 else "Good evening"
    return random.choice([
        f"{part}, thank you for calling {t.name}. This is {agent} - and who do I have the pleasure of speaking with?",
        f"Thank you for calling {t.name}, this is {agent}. May I ask who I'm speaking with?",
        f"{part}, this is {agent} over at {t.name}. Who do I have the pleasure of speaking with today?",
        f"Thank you for calling {t.name}, this is {agent}. And may I have your name to get started?",
    ])


def caller_address(caller_phone: str, call_id: str) -> Address:
    """
    The caller's phone, or a per-call stand-in when caller ID is blocked or the call is web-based.
    """
    return phone_address(caller_phone) or Address(AddressKind.WEB_SESSION, f"call-{call_id}")


def start_call(app: ChannelContext, record: TenantRecord, caller_phone: str, call_id: str) -> str:
    """
    Record the start of a call and return the greeting. Shared by Twilio and Vapi.
    """
    s = app.services
    sender = caller_address(caller_phone, call_id)
    pack = s.pack(record.tenant)
    text = greeting(record, pack, s.local_now(record.tenant))
    phone = sender.value if sender.kind is AddressKind.PHONE else ""
    contact = s.store.contact_for_address(record.tenant.id, sender, phone=phone)
    key = ConversationKey(record.tenant.id, contact.id)
    s.store.add_message(key, Role.ASSISTANT, text, Channel.VOICE, session_id=call_id, meta={"session_id": call_id})
    s.store.update_state(key, {"call": {"id": call_id, "turns": 0, "silent": 0}})
    return text


def call_state(app: ChannelContext, record: TenantRecord, caller_phone: str, call_id: str) -> tuple[ConversationKey, dict]:
    sender = caller_address(caller_phone, call_id)
    phone = sender.value if sender.kind is AddressKind.PHONE else ""
    contact = app.services.store.contact_for_address(record.tenant.id, sender, phone=phone)
    key = ConversationKey(record.tenant.id, contact.id)
    return key, dict(app.services.store.get_state(key).get("call") or {})


def run_voice_turn(app: ChannelContext, record: TenantRecord, caller_phone: str, called: str,
                   speech_text: str, call_id: str, *, via: str) -> TurnResult:
    inbound = InboundMessage(
        channel     = Channel.VOICE,
        sender      = caller_address(caller_phone, call_id),
        recipient   = phone_address(called) or Address(AddressKind.PHONE, called),
        text        = speech_text,
        received_at = datetime.now(timezone.utc),
        session_id  = call_id,
        raw         = {"via": via},
    )
    return app.runner.run(inbound, record=record)


def create_blueprint(app: ChannelContext) -> Blueprint:
    bp = Blueprint("voice", __name__)
    s = app.services
    cfg = app.config

    def action_url(call_id: str) -> str:
        path = f"/voice/handle?call_sid={call_id}"
        return f"{cfg.public_base_url}{path}" if cfg.public_base_url else path

    def gather(vr: VoiceResponse, text: str, call_id: str, pack: DomainPack, timeout: int = 8) -> Gather:
        g = Gather(
            input="speech", action=action_url(call_id), method="POST", speech_timeout="auto", timeout=timeout,
            language="en-US", speech_model="experimental_conversations", enhanced=True,
            hints=pack.voice.speech_hints or None, barge_in=True, action_on_empty_result=True,
        )
        if text:
            g.say(speech.for_polly(text, pack), voice=cfg.twilio_voice)
        vr.append(g)
        return g

    def listen(text: str, call_id: str, pack: DomainPack) -> Response:
        """
        Speak inside a Gather (so the caller can barge in), re-prompt once on silence, then wrap up.
        """
        vr = VoiceResponse()
        gather(vr, text, call_id, pack)
        gather(vr, STILL_THERE, call_id, pack, timeout=10)
        vr.say(GOODBYE, voice=cfg.twilio_voice)
        vr.hangup()
        return Response(str(vr), mimetype="application/xml")

    def finish(text: str, pack: DomainPack, transfer_to: str | None = None) -> Response:
        vr = VoiceResponse()
        if text:
            vr.say(speech.for_polly(text, pack), voice=cfg.twilio_voice)
        if transfer_to:
            vr.dial(transfer_to)
        else:
            vr.hangup()
        return Response(str(vr), mimetype="application/xml")

    def unconfigured() -> Response:
        vr = VoiceResponse()
        vr.say("Sorry, this number isn't set up yet. Goodbye.", voice=cfg.twilio_voice)
        vr.hangup()
        return Response(str(vr), mimetype="application/xml")

    def lookup() -> TenantRecord | None:
        called = phone_address(request.form.get("To", ""))
        return s.directory.by_endpoint(Channel.VOICE, called) if called else None

    @bp.post("/voice")
    def voice():
        call_id = request.form.get("CallSid", "")
        record = lookup()
        if record is None or not call_id:
            return unconfigured()
        text = start_call(app, record, request.form.get("From", ""), call_id)
        vr = VoiceResponse()
        vr.pause(length=random.randint(1, 2))
        pack = s.pack(record.tenant)
        gather(vr, text, call_id, pack)
        gather(vr, STILL_THERE, call_id, pack, timeout=10)
        vr.say(GOODBYE, voice=cfg.twilio_voice)
        vr.hangup()
        return Response(str(vr), mimetype="application/xml")

    @bp.post("/voice/handle")
    def handle():
        call_id = (request.args.get("call_sid") or request.form.get("CallSid") or "").strip()
        caller = request.form.get("From", "")
        said = (request.form.get("SpeechResult") or "").strip()
        try:
            confidence = float(request.form.get("Confidence") or 0)
        except ValueError:
            confidence = 0.0
        record = lookup()
        if record is None:
            return unconfigured()
        pack = s.pack(record.tenant)
        key, state = call_state(app, record, caller, call_id)

        if not said:
            state["silent"] = state.get("silent", 0) + 1
            s.store.update_state(key, {"call": state})
            if state["silent"] >= MAX_SILENT_TURNS:
                return finish(GOODBYE, pack)
            return listen(STILL_THERE, call_id, pack)
        if confidence and confidence < LOW_CONFIDENCE and len(said.split()) <= 2:
            return listen(REPROMPT, call_id, pack)

        state.update(silent=0, turns=state.get("turns", 0) + 1)
        s.store.update_state(key, {"call": state})
        if state["turns"] > MAX_TURNS:
            return finish(WRAPUP, pack)

        try:
            result = run_voice_turn(app, record, caller, request.form.get("To", ""), said, call_id, via="twilio")
        except Exception:
            log.exception("voice turn failed")
            return listen("Sorry, I had a hiccup there - what were you asking about?", call_id, pack)

        text = result.reply.text if result.reply else ""
        transfer = next((a for a in result.actions if isinstance(a, TransferCall)), None)
        if transfer is not None:
            return finish(text, pack, transfer_to=transfer.to.value)
        if any(isinstance(a, EndSession) for a in result.actions):
            return finish(text, pack)
        return listen(text, call_id, pack)

    return bp
