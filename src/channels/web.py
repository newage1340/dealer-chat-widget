"""
Web chat widget: the page, its JSON API, and the embeddable loader.

Endpoints and payloads match the existing static/script.js and static/embed.js.
Each browser session is its own contact (a WEB_SESSION address); the visitor's real phone
is stored on the contact once they give it.
"""

import logging
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

from flask import Blueprint, abort, jsonify, make_response, redirect, render_template, request, send_from_directory, session

from core import Address, AddressKind, Channel, ConversationKey, InboundMessage, Role
from domains.catalog import topic_visible
from tenants import TenantRecord

from .context import ChannelContext

log = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).resolve().parent.parent.parent / "static"


def _session_address(session_id: str) -> Address:
    return Address(AddressKind.WEB_SESSION, re.sub(r"[^a-zA-Z0-9_-]", "", session_id)[:64])


def _us_phone(raw: str) -> str:
    digits = re.sub(r"\D", "", raw or "")
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return "+1" + digits if len(digits) == 10 else ""


def create_blueprint(app: ChannelContext) -> Blueprint:
    bp = Blueprint("web", __name__, template_folder="templates")
    s = app.services

    def tenant_for(slug: str) -> TenantRecord | None:
        return s.directory.by_slug(slug) if slug else (s.directory.by_slug(app.config.default_slug) if app.config.default_slug else None)

    def json_body() -> tuple[dict, str, str]:
        data = request.get_json(silent=True) or {}
        return data, (data.get("session_id") or "").strip(), (data.get("slug") or "").strip()

    def contact_key(record: TenantRecord, session_id: str):
        contact = s.store.contact_for_address(record.tenant.id, _session_address(session_id))
        return contact, ConversationKey(record.tenant.id, contact.id)

    @bp.get("/")
    def index():
        if app.config.default_slug:
            return redirect(f"/widget/{app.config.default_slug}", code=302)
        links = "".join(
            f'<li><a href="/widget/{r.tenant.slug}">{r.tenant.name}</a> <small>({r.tenant.domain})</small></li>'
            for r in sorted(s.directory.all(), key=lambda r: r.tenant.name)
        )
        return f"<h1>Front Desk</h1><p>Widgets:</p><ul>{links}</ul>"

    @bp.get("/widget/<slug>")
    def widget(slug: str):
        record = s.directory.by_slug(slug)
        if record is None:
            abort(404, f"No business with slug '{slug}'.")
        pack = s.pack(record.tenant)
        sections: dict[str, list] = {}
        for topic in pack.widget.topics:
            if topic_visible(topic.requires_field, topic.requires_any, record.items):
                sections.setdefault(topic.section, []).append(topic)
        t = record.tenant
        return render_template(
            "widget.html",
            business_name     = t.name,
            brand_color       = t.branding.color or "#4a90e2",
            logo_url          = t.branding.logo_url,
            slug              = t.slug,
            icon              = pack.widget.icon,
            tagline           = pack.widget.tagline,
            header_title      = pack.widget.header_title,
            input_placeholder = pack.widget.input_placeholder,
            sections          = list(sections.items()),
            terms_url         = app.config.terms_url,
            privacy_url       = app.config.privacy_url,
        )

    @bp.post("/chat")
    def chat():
        data, session_id, slug = json_body()
        message = (data.get("message") or "").strip()
        record = tenant_for(slug)
        if record is None:
            return jsonify({"error": "no business resolved (missing or unknown slug)"}), 400
        if not message:
            return jsonify({"error": "empty message"}), 400
        if not session_id:
            session_id = session.get("sid") or uuid.uuid4().hex
            session["sid"] = session_id

        # Self-heal: the widget echoes the phone it cached, in case the server lost it.
        cached_phone = _us_phone(data.get("real_phone") or "")
        contact, _ = contact_key(record, session_id)
        if cached_phone and not contact.phone:
            s.store.update_contact(contact, {"phone": cached_phone})

        inbound = InboundMessage(
            channel     = Channel.WEB,
            sender      = _session_address(session_id),
            recipient   = Address(AddressKind.WEB_SESSION, record.tenant.slug),
            text        = message,
            received_at = datetime.now(timezone.utc),
            session_id  = session_id,
        )
        try:
            result = app.runner.run(inbound, record=record)
        except Exception:
            log.exception("chat turn failed")
            return jsonify({"error": "processing error"}), 500

        reply = result.reply
        if reply is not None and reply.data.get("needs_profile"):
            return jsonify({
                "reply":           reply.text,
                "needs_profile":   True,
                "missing":         reply.data.get("missing"),
                "pending_message": reply.data.get("pending_message"),
                "session_id":      session_id,
            })
        return jsonify({
            "reply":      reply.text if reply else "",
            "primer":     None,
            "silent":     reply is None,
            "session_id": session_id,
        })

    @bp.post("/widget/welcome")
    def welcome():
        _, session_id, slug = json_body()
        record = tenant_for(slug)
        if not session_id or record is None:
            return jsonify({"error": "missing session_id or unknown slug"}), 400
        contact, key = contact_key(record, session_id)
        if s.store.notice_sent(key, "welcome"):
            return jsonify({"welcome": ""})
        t, pack = record.tenant, s.pack(record.tenant)
        agent = t.agent_name or pack.default_agent_name
        welcome_text = (
            f"Hi, my name is {agent} with {t.name}. I can help with {pack.widget.capabilities}.\n\n"
            f"By communicating with our assistant, you agree to our [terms]({app.config.terms_url}). "
            "If you receive and respond to SMS follow-ups, msg frequency varies and msg & data rates may apply. "
            "Reply HELP for help, STOP to opt out anytime."
        )
        followup = "How can I help you?"
        s.store.add_message(key, Role.ASSISTANT, welcome_text, Channel.WEB, session_id=session_id)
        s.store.add_message(key, Role.ASSISTANT, followup, Channel.WEB, session_id=session_id)
        s.store.mark_notice(key, "welcome")
        s.store.update_contact(contact, {"terms_accepted_at": datetime.now(timezone.utc)})
        return jsonify({"welcome": welcome_text, "welcome_followup": followup})

    @bp.post("/widget/history")
    def history():
        _, session_id, slug = json_body()
        record = tenant_for(slug)
        if not session_id or record is None:
            return jsonify({"messages": []})
        _, key = contact_key(record, session_id)
        return jsonify({"messages": [{"role": str(m.role), "content": m.text} for m in s.store.history(key, 40)]})

    @bp.post("/widget/profile")
    def profile():
        data, session_id, slug = json_body()
        name = (data.get("name") or "").strip()
        phone = _us_phone(data.get("phone") or "")
        record = tenant_for(slug)
        if not session_id or record is None:
            return jsonify({"error": "missing session_id or unknown slug"}), 400
        if not name:
            return jsonify({"error": "missing name"}), 400
        if not phone:
            return jsonify({"error": "phone must be 10 digits"}), 400
        contact, _ = contact_key(record, session_id)
        s.store.update_contact(contact, {"first_name": name.split()[0], "last_name": " ".join(name.split()[1:]), "phone": phone})
        return jsonify({"ok": True})

    @bp.post("/widget/register-phone")
    def register_phone():
        data, session_id, slug = json_body()
        phone = _us_phone(data.get("phone") or "")
        record = tenant_for(slug)
        if not session_id or record is None or not phone:
            return jsonify({"error": "missing session_id, slug, or valid phone"}), 400
        contact, _ = contact_key(record, session_id)
        s.store.update_contact(contact, {"phone": phone})
        return jsonify({"ok": True, "phone": phone})

    @bp.post("/widget/clear-session")
    def clear_session():
        """
        Wipe this visitor's web conversations (every browser session tied to the same phone),
        so a reset customer isn't followed up about a conversation they deleted.
        SMS and voice conversations are left alone.
        """
        data, session_id, slug = json_body()
        record = tenant_for(slug)
        if not session_id or record is None:
            return jsonify({"error": "missing session_id or unknown slug"}), 400
        tenant_id = record.tenant.id
        contact, key = contact_key(record, session_id)
        phone = contact.phone or _us_phone(data.get("real_phone") or "")
        targets = {contact.id}
        if phone:
            for contact_id in s.store.contacts_with_phone(tenant_id, phone):
                sibling = s.store.get_contact(tenant_id, contact_id)
                if sibling and sibling.addresses and all(a.kind is AddressKind.WEB_SESSION for a in sibling.addresses):
                    targets.add(contact_id)
        for contact_id in targets:
            s.store.delete_contact(ConversationKey(tenant_id, contact_id))
        return jsonify({"ok": True})

    @bp.get("/embed.js")
    def embed_js():
        resp = make_response(send_from_directory(STATIC_DIR, "embed.js"))
        resp.headers["Content-Type"] = "application/javascript; charset=utf-8"
        resp.headers["Cache-Control"] = "public, max-age=3600"
        return resp

    @bp.get("/widget-config")
    def widget_config():
        record = tenant_for((request.args.get("dealer") or request.args.get("business") or "").strip())
        if record is None:
            resp = jsonify({"error": "business not found"})
            resp.headers["Access-Control-Allow-Origin"] = "*"
            return resp, 404
        t = record.tenant
        resp = jsonify({"dealer_name": t.name, "business_name": t.name, "brand_color": t.branding.color,
                        "logo_url": t.branding.logo_url, "slug": t.slug})
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp

    return bp
