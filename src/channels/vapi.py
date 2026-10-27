"""
Vapi custom-LLM endpoint: Vapi runs the call (telephony, STT, ElevenLabs TTS, barge-in)
and POSTs an OpenAI-style chat-completions request each turn. We answer from the same pipeline.

End-of-call and transfer are returned as calls to the assistant's Vapi tools (endCall / transferCall).
"""

import json
import logging
import time

from flask import Blueprint, Response, request

from core import Channel, EndSession, TransferCall
from tenants import normalize_phone, phone_address

from . import speech
from .context import ChannelContext
from .voice import run_voice_turn, start_call

log = logging.getLogger(__name__)


def _fields(payload: dict) -> tuple[str, str, str, str, bool]:
    """
    (speech, caller, called, call_id, has_user_turn) from Vapi's request.
    """
    said, has_user = "", False
    for m in reversed(payload.get("messages") or []):
        if isinstance(m, dict) and m.get("role") == "user":
            said, has_user = (m.get("content") or "").strip(), True
            break
    call = payload.get("call") or {}
    caller = normalize_phone((call.get("customer") or payload.get("customer") or {}).get("number") or "")
    called = normalize_phone((call.get("phoneNumber") or payload.get("phoneNumber") or {}).get("number") or "")
    return said, caller, called, (call.get("id") or payload.get("id") or "vapi-call").strip(), has_user


def _tool_calls(action: str, destination: str, tool_names: list[str]) -> list[dict] | None:
    """
    Match the assistant's configured tool names by keyword, falling back to Vapi's defaults.
    """
    def find(*keywords: str) -> str | None:
        return next((n for n in tool_names if all(k in n.lower() for k in keywords)), None)

    if action == "hangup":
        name = find("end", "call") or find("hangup") or "endCall"
        return [{"index": 0, "id": "call_end", "type": "function", "function": {"name": name, "arguments": "{}"}}]
    if action == "transfer" and destination:
        name = find("transfer") or "transferCall"
        return [{"index": 0, "id": "call_xfer", "type": "function",
                 "function": {"name": name, "arguments": json.dumps({"destination": destination})}}]
    return None


def _chunk(cid: str, *, text: str | None = None, tool_calls: list | None = None, finish: str | None = None) -> str:
    delta: dict = {}
    if text is not None:
        delta["content"] = text
    if tool_calls is not None:
        delta["tool_calls"] = tool_calls
    body = {"id": cid, "object": "chat.completion.chunk", "created": int(time.time()), "model": "front-desk",
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]}
    return f"data: {json.dumps(body)}\n\n"


def create_blueprint(app: ChannelContext) -> Blueprint:
    bp = Blueprint("vapi", __name__)
    s = app.services

    @bp.post("/vapi")
    @bp.post("/vapi/chat/completions")
    def completions():
        payload = request.get_json(silent=True) or {}
        stream = bool(payload.get("stream", True))
        said, caller, called, call_id, has_user = _fields(payload)
        tool_names = [((t.get("function") or {}).get("name") or t.get("type") or "")
                      for t in (payload.get("tools") or []) if isinstance(t, dict)]

        called_addr = phone_address(called)
        record = s.directory.by_endpoint(Channel.VOICE, called_addr) if called_addr else None
        action, destination = "continue", ""
        if record is None:
            text, action = "Sorry, this number isn't set up yet. Goodbye.", "hangup"
            pack = None
        else:
            pack = s.pack(record.tenant)
            try:
                if not has_user:
                    text = start_call(app, record, caller, call_id)
                else:
                    result = run_voice_turn(app, record, caller, called, said, call_id, via="vapi")
                    text = result.reply.text if result.reply else ""
                    transfer = next((a for a in result.actions if isinstance(a, TransferCall)), None)
                    if transfer is not None:
                        action, destination = "transfer", transfer.to.value
                    elif any(isinstance(a, EndSession) for a in result.actions):
                        action = "hangup"
            except Exception:
                log.exception("vapi turn failed")
                text = "Sorry, I had a hiccup there - what were you asking about?"

        text = speech.for_elevenlabs(text, pack) if pack else text
        text = text or "Sorry, could you say that again?"
        tool_calls = _tool_calls(action, destination, tool_names)
        finish = "tool_calls" if tool_calls else "stop"
        cid = f"chatcmpl-{call_id}"

        if stream:
            def generate():
                yield _chunk(cid, text=text)
                if tool_calls:
                    yield _chunk(cid, tool_calls=tool_calls)
                yield _chunk(cid, finish=finish)
                yield "data: [DONE]\n\n"
            return Response(generate(), content_type="text/event-stream")

        message: dict = {"role": "assistant", "content": text}
        if tool_calls:
            message["tool_calls"] = tool_calls
        body = {"id": cid, "object": "chat.completion", "created": int(time.time()), "model": "front-desk",
                "choices": [{"index": 0, "message": message, "finish_reason": finish}]}
        return Response(json.dumps(body), content_type="application/json")

    return bp
