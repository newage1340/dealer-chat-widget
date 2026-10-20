"""
Runs one inbound message through the handler chain and persists the result.
"""

import logging
from collections.abc import Sequence
from dataclasses import dataclass, replace

from core import (
    Action,
    AddressKind,
    Contact,
    ConversationKey,
    Handler,
    InboundMessage,
    Outcome,
    Reply,
    Role,
    Tenant,
    TurnContext,
    UpdateContact,
)
from tenants import TenantRecord

from .executor import ActionExecutor
from .services import Services

log = logging.getLogger(__name__)

HISTORY_LIMIT = 40


class UnknownTenant(LookupError):
    pass


@dataclass(frozen=True, slots=True, kw_only=True)
class TurnResult:
    tenant:  Tenant
    contact: Contact
    key:     ConversationKey
    reply:   Reply | None
    actions: tuple[Action, ...]


class TurnRunner:
    def __init__(self, services: Services, handlers: Sequence[Handler], executor: ActionExecutor):
        self.s = services
        self.handlers = tuple(handlers)
        self.executor = executor

    def resolve(self, inbound: InboundMessage, record: TenantRecord | None = None) -> tuple[TenantRecord, Contact]:
        record = record or self.s.directory.by_endpoint(inbound.channel, inbound.recipient)
        if record is None:
            raise UnknownTenant(f"no tenant for {inbound.channel} {inbound.recipient}")
        phone = inbound.sender.value if inbound.sender.kind is AddressKind.PHONE else ""
        contact = self.s.store.contact_for_address(record.tenant.id, inbound.sender, phone=phone)
        return record, contact

    def run(self, inbound: InboundMessage, *, record: TenantRecord | None = None) -> TurnResult:
        record, contact = self.resolve(inbound, record)
        tenant = record.tenant
        key = ConversationKey(tenant.id, contact.id)
        ctx = TurnContext(
            tenant  = tenant,
            contact = contact,
            key     = key,
            inbound = inbound,
            history = self.s.store.history(key, HISTORY_LIMIT),
            state   = self.s.store.get_state(key),
            now     = self.s.local_now(tenant),
        )

        actions: list[Action] = []
        updates: dict = {}
        deletes: set[str] = set()
        final: Outcome | None = None
        for handler in self.handlers:
            outcome = handler.handle(ctx)
            if outcome is None:
                continue
            actions.extend(outcome.actions)
            updates.update(outcome.state_updates)
            deletes |= outcome.state_deletes
            if outcome.handled:
                final = outcome
                break
            # Let later handlers see what earlier ones learned.
            state = {k: v for k, v in {**ctx.state, **outcome.state_updates}.items() if k not in outcome.state_deletes}
            contact_changes = {k: v for a in outcome.actions if isinstance(a, UpdateContact) for k, v in a.changes.items()}
            ctx = replace(ctx, state=state, contact=replace(ctx.contact, **{
                k: v for k, v in contact_changes.items() if k in ("first_name", "last_name", "email", "phone")
            }))

        reply = final.reply if final else None
        ephemeral = bool(reply and reply.data.get("ephemeral"))
        meta = {"session_id": inbound.session_id} if inbound.session_id else None
        if not ephemeral:
            if inbound.text:
                self.s.store.add_message(key, Role.USER, inbound.text, inbound.channel, session_id=inbound.session_id, meta=meta)
            if reply and reply.text:
                self.s.store.add_message(key, Role.ASSISTANT, reply.text, inbound.channel, session_id=inbound.session_id, meta=meta)
            # A customer who writes back is no longer waiting on a follow-up.
            if inbound.text:
                self.s.store.clear_notice(key, "cold_followup")
        if updates or deletes:
            self.s.store.update_state(key, updates, frozenset(deletes))

        contact = self.executor.apply(ctx, actions)
        return TurnResult(tenant=tenant, contact=contact, key=key, reply=reply, actions=tuple(actions))
