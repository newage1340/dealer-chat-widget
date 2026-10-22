"""
Twilio SMS webhook.
"""

import logging
from datetime import datetime, timezone

from flask import Blueprint, Response, request
from twilio.twiml.messaging_response import MessagingResponse

from core import Channel, ConversationKey, InboundMessage
from tenants import phone_address

from .context import ChannelContext

log = logging.getLogger(__name__)


def create_blueprint(app: ChannelContext) -> Blueprint:
    bp = Blueprint("sms", __name__)
    s = app.services

    def twiml(text: str | None) -> Response:
        resp = MessagingResponse()
        if text:
            resp.message(text)
        return Response(str(resp), mimetype="application/xml")

    @bp.post("/sms")
    def sms():
        sender = phone_address(request.form.get("From", ""))
        recipient = phone_address(request.form.get("To", ""))
        body = (request.form.get("Body") or "").strip()
        if sender is None or recipient is None:
            return twiml(None)
        record = s.directory.by_endpoint(Channel.SMS, recipient)
        if record is None:
            log.warning("SMS to unconfigured number %s", recipient.value)
            return twiml(None)

        contact = s.store.contact_for_address(record.tenant.id, sender, phone=sender.value)
        key = ConversationKey(record.tenant.id, contact.id)
        first_contact = not s.store.notice_sent(key, "sms_primer")

        inbound = InboundMessage(
            channel     = Channel.SMS,
            sender      = sender,
            recipient   = recipient,
            text        = body,
            received_at = datetime.now(timezone.utc),
            session_id  = request.form.get("MessageSid"),
        )
        try:
            result = app.runner.run(inbound, record=record)
        except Exception:
            log.exception("SMS turn failed")
            return twiml("Sorry, I had trouble processing that. Could you try again?")

        text = result.reply.text if result.reply else None
        if text and first_contact:
            pack = s.pack(record.tenant)
            text += (f"\n\nFYI - I can help with {pack.widget.capabilities}. By texting this number you agree to our "
                     f"Terms of Service ({app.config.terms_url}). Msg & data rates may apply. Reply STOP to opt out.")
            s.store.mark_notice(key, "sms_primer")
        return twiml(text)

    return bp
