"""
Applies the Actions handlers request. This is the only place turn-time side effects happen.

Call-control actions (TransferCall, EndSession) are left to the voice channels,
which read them from the TurnResult.
"""

import logging
from collections.abc import Iterable

from core import (
    Action,
    Address,
    AddressKind,
    Appointment,
    BookAppointment,
    Channel,
    Contact,
    NotifyStaff,
    ScheduleTask,
    SendMessage,
    Tenant,
    TurnContext,
    UpdateContact,
)
from providers import OutboundMessage, ProviderError
from storage import utcnow
from tenants import normalize_phone

from .services import Services

log = logging.getLogger(__name__)


class ActionExecutor:
    def __init__(self, services: Services):
        self.s = services

    def sender_for(self, tenant: Tenant) -> Address | None:
        endpoint = tenant.endpoint(Channel.SMS) or tenant.endpoint(Channel.VOICE)
        if endpoint is not None:
            return endpoint
        fallback = normalize_phone(self.s.default_sender)
        return Address(AddressKind.PHONE, fallback) if fallback else None

    def send_sms(self, tenant: Tenant, to: Address, text: str, *, purpose: str = "turn") -> bool:
        if to.kind is not AddressKind.PHONE:
            return False
        sender = self.sender_for(tenant) or Address(AddressKind.PHONE, "")
        try:
            receipt = self.s.messaging.send(OutboundMessage(
                to=to, sender=sender, text=text, channel=Channel.SMS, purpose=purpose, tenant_id=tenant.id,
            ))
        except ProviderError as e:
            log.warning("SMS to %s failed: %s", to.value, e)
            return False
        self.s.usage.record(receipt.usage)
        return True

    def notify_staff(self, tenant: Tenant, text: str, *, reason: str) -> None:
        phones = [a for a in tenant.staff_contacts if a.kind is AddressKind.PHONE]
        if not phones:
            log.warning("No staff phones for %s; %s alert not sent:\n%s", tenant.id, reason, text)
            return
        for phone in phones:
            self.send_sms(tenant, phone, text, purpose=f"staff_{reason}")
        emails = [a.value for a in tenant.staff_contacts if a.kind is AddressKind.EMAIL]
        if emails:
            log.info("Email notifications not configured; skipped %s for %s", emails, reason)

    def apply(self, ctx: TurnContext, actions: Iterable[Action]) -> Contact:
        contact = ctx.contact
        for action in actions:
            try:
                match action:
                    case UpdateContact(changes=changes):
                        contact = self.s.store.update_contact(contact, changes)
                    case SendMessage(to=to, text=text):
                        self.send_sms(ctx.tenant, to, text)
                    case NotifyStaff(text=text, reason=reason):
                        self.notify_staff(ctx.tenant, text, reason=reason)
                    case BookAppointment():
                        self._book(ctx, contact, action)
                    case ScheduleTask():
                        log.info("ScheduleTask not supported yet: %s", action)
                    case _:
                        pass  # TransferCall / EndSession: handled by the voice channels
            except Exception:
                log.exception("action %s failed for %s", type(action).__name__, ctx.tenant.id)
        return contact

    def _book(self, ctx: TurnContext, contact: Contact, action: BookAppointment) -> None:
        pack = self.s.pack(ctx.tenant)
        appointment = self.s.store.save_appointment(Appointment(
            key        = ctx.key,
            time_text  = action.time_text,
            subject    = action.subject,
            created_at = utcnow(),
            starts_at  = action.starts_at,
            location   = action.location,
            details    = action.details,
        ), replace_existing=action.rescheduled)
        log.info("Booked appointment %s for %s: %s / %s", appointment.id, ctx.tenant.id, action.time_text, action.subject)

        # The SMS reply already confirms on SMS; other channels get a text confirmation.
        phone = contact.phone or (ctx.inbound.sender.value if ctx.inbound.sender.kind is AddressKind.PHONE else "")
        if phone and ctx.channel is not Channel.SMS:
            text = self.s.composer.followup_text(ctx.tenant, pack, "confirm", contact,
                                                 visit_time=action.time_text, subject=action.subject)
            if text:
                self.send_sms(ctx.tenant, Address(AddressKind.PHONE, phone), text, purpose="booking_confirmation")

        b = pack.booking
        lines = [
            f"[{ctx.tenant.name} AI] {b.visit_noun.title()} {'rescheduled' if action.rescheduled else 'booked'}",
            f"Customer: {contact.display_name or '(name not given)'} - {phone or ctx.inbound.sender.value}",
        ]
        if contact.email:
            lines.append(f"Email: {contact.email}")
        lines.append(f"Time: {action.time_text}")
        lines.append(f"{b.subject_label}: {action.subject}")
        if action.location:
            lines.append(f"Location: {action.location}")
        for f in b.intake:
            value = action.details.get(f.key)
            if value:
                lines.append(f"{f.label}: {value}")
        lines.append(f"Channel: {ctx.channel}")
        self.notify_staff(ctx.tenant, "\n".join(lines), reason="booking")
