"""
Background jobs: appointment reminders and cold follow-ups.

Message wording comes from each domain pack's [followups] templates.
"""

import logging
from datetime import timedelta

from core import Address, AddressKind, Channel, ConversationKey, Role
from pipeline import ActionExecutor, Services
from storage import utcnow

log = logging.getLogger(__name__)

COLD_DEFAULT_MINUTES = 30
COLD_MAX_AGE_HOURS = 72


def send_reminders(services: Services, executor: ActionExecutor, lead_minutes: int) -> None:
    for appt in services.store.upcoming_appointments(utcnow() + timedelta(minutes=lead_minutes)):
        if "reminder" in appt.sent_notices:
            continue
        record = services.directory.get(appt.key.tenant_id)
        contact = services.store.get_contact(appt.key.tenant_id, appt.key.contact_id)
        if record is None or contact is None or not contact.phone or contact.attributes.get("opted_out"):
            continue
        text = services.composer.followup_text(record.tenant, services.pack(record.tenant), "reminder", contact,
                                               visit_time=appt.time_text, subject=appt.subject)
        if text and executor.send_sms(record.tenant, Address(AddressKind.PHONE, contact.phone), text, purpose="reminder"):
            services.store.mark_appointment_notice(appt.id, "reminder")
            log.info("Sent reminder for appointment %s", appt.id)


def send_cold_followups(services: Services, executor: ActionExecutor) -> None:
    """
    One follow-up text to customers who went quiet after the assistant's last reply,
    unless they booked or already got one.
    """
    now = utcnow()
    for key, last_role, last_at, last_user_at in services.store.conversations_since(now - timedelta(hours=COLD_MAX_AGE_HOURS)):
        if last_role is not Role.ASSISTANT or last_user_at is None:
            continue
        record = services.directory.get(key.tenant_id)
        if record is None:
            continue
        minutes = int(record.tenant.settings.get("cold_followup_minutes") or COLD_DEFAULT_MINUTES)
        if now - last_user_at < timedelta(minutes=minutes):
            continue
        if services.store.notice_sent(key, "cold_followup") or services.store.latest_appointment(key) is not None:
            continue
        contact = services.store.get_contact(key.tenant_id, key.contact_id)
        if contact is None or not contact.phone or contact.attributes.get("opted_out"):
            continue
        text = services.composer.followup_text(record.tenant, services.pack(record.tenant), "cold", contact)
        if not text:
            continue
        services.store.mark_notice(key, "cold_followup")
        if executor.send_sms(record.tenant, Address(AddressKind.PHONE, contact.phone), text, purpose="cold_followup"):
            services.store.add_message(ConversationKey(key.tenant_id, key.contact_id), Role.ASSISTANT, text,
                                       _channel_of_last(services, key))
            log.info("Sent cold follow-up to %s for %s", contact.phone, key.tenant_id)


def _channel_of_last(services: Services, key: ConversationKey) -> Channel:
    history = services.store.history(key, 1)
    return history[-1].channel if history else Channel.SMS


def start(services: Services, executor: ActionExecutor, *, reminder_lead_minutes: int):
    from apscheduler.schedulers.background import BackgroundScheduler

    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(send_reminders, "interval", minutes=5, args=(services, executor, reminder_lead_minutes),
                      id="reminders", replace_existing=True)
    scheduler.add_job(send_cold_followups, "interval", minutes=1, args=(services, executor),
                      id="cold_followups", replace_existing=True)
    scheduler.start()
    log.info("Scheduler started")
    return scheduler
