"""
Operational endpoints: health, reloading tenants/packs, usage, and prompt previews.

Everything except /health requires `?token=` (or an X-Admin-Token header) matching ADMIN_TOKEN.
With no ADMIN_TOKEN configured, admin endpoints are disabled.
"""

from datetime import timedelta

from flask import Blueprint, Response, abort, jsonify, request

from core import Channel, Contact, ContactId
from tenants import phone_address
from domains import DomainRegistry
from prompts import PromptInputs
from storage import utcnow
from tenants import TenantDirectory

from .context import ChannelContext


def create_blueprint(app: ChannelContext) -> Blueprint:
    bp = Blueprint("admin", __name__)
    s = app.services

    def require_token():
        token = request.args.get("token") or request.headers.get("X-Admin-Token") or ""
        if not app.config.admin_token or token != app.config.admin_token:
            abort(403)

    @bp.get("/health")
    def health():
        return jsonify({"ok": True, "tenants": len(s.directory.all()), "domains": s.registry.ids()})

    @bp.post("/admin/reload")
    def reload():
        """
        Re-read domain packs and tenant files without a restart.
        """
        require_token()
        registry = DomainRegistry.from_directory()
        directory = TenantDirectory.from_directory(app.config.tenants_dir, registry)
        s.registry.replace_with(registry)
        s.directory.replace_with(directory)
        return jsonify({"ok": True, "tenants": len(s.directory.all()), "domains": s.registry.ids()})

    @bp.post("/admin/catalog-upload")
    @bp.post("/admin/inventory-upload")
    def catalog_upload():
        """
        Replace a tenant's catalog (e.g. from the scraper job). Identify the tenant by "slug"
        or by "twilio_number"; send rows as "items" (or the legacy "vehicles").
        An empty list is refused so a failed scrape can never wipe a live catalog.
        """
        data = request.get_json(silent=True) or {}
        token = request.headers.get("X-Upload-Token") or data.get("token") or ""
        if not token or token not in {app.config.upload_token, app.config.admin_token} - {""}:
            return jsonify({"error": "unauthorized"}), 401
        record = s.directory.by_slug(data.get("slug", "")) if data.get("slug") else None
        if record is None and (number := phone_address(data.get("twilio_number", ""))):
            record = s.directory.by_endpoint(Channel.SMS, number)
        if record is None:
            return jsonify({"error": "unknown tenant"}), 404
        rows = data.get("items") or data.get("vehicles") or []
        if not isinstance(rows, list) or not rows:
            return jsonify({"error": "no items - refusing to wipe the existing catalog"}), 400
        count = s.directory.save_catalog(record.tenant.id, rows, s.registry)
        return jsonify({"ok": True, "tenant": record.tenant.id, "items": count})

    @bp.get("/admin/usage")
    def usage():
        require_token()
        hours = int(request.args.get("hours", "24"))
        return jsonify(s.store.usage_summary(utcnow() - timedelta(hours=hours)))

    @bp.get("/admin/prompt/<slug>")
    def prompt_preview(slug: str):
        """
        The exact system prompt a turn would get, e.g. /admin/prompt/gov?channel=voice&message=any+trucks
        """
        require_token()
        record = s.directory.by_slug(slug)
        if record is None:
            abort(404)
        channel = Channel(request.args.get("channel", "sms"))
        inputs = PromptInputs(
            tenant   = record.tenant,
            pack     = s.pack(record.tenant),
            items    = record.items,
            contact  = Contact(id=ContactId("preview"), tenant_id=record.tenant.id, first_name="Alex", phone="+13175550199"),
            history  = (),
            message  = request.args.get("message", "Hi, what do you have?"),
            channel  = channel,
            now      = s.local_now(record.tenant),
            caller_phone = "+13175550199" if channel is Channel.VOICE else "",
        )
        messages = s.composer.voice_messages(inputs) if channel is Channel.VOICE else s.composer.chat_messages(inputs)
        return Response("\n\n".join(f"##### {m.role}\n{m.content}" for m in messages), mimetype="text/plain")

    return bp
