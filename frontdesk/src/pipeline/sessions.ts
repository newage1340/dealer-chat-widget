/**
 * Ending sessions. Every way a session ends (a call hangs up, a chat goes idle, a wind-down finishes)
 * goes through `closeSession`, so classification, transcript delivery and abuse strikes always happen.
 */

import { Abuse, type EventBody, type SessionCloseReason, type SessionId } from "../core/index.ts";
import { Subscriber } from "../turn/index.ts";
import { stamp, type Services } from "./services.ts";

/**
 * Close a session if it is still open. Returns false when it was already closed.
 */
export async function closeSession(
    services: Services,
    sessionId: SessionId,
    reason: SessionCloseReason
): Promise<boolean>
{
    const { store } = services;
    const now = services.clock();
    const session = await store.conversations.closeSession(sessionId, now);
    if (session === null) {
        return false;
    }
    const tenant = await store.tenants.byId(session.tenantId);
    const conversation = await store.conversations.conversation(session.conversationId);
    if (tenant === null || conversation === null) {
        return true;
    }
    let contact = await store.conversations.contact(conversation.contactId);
    const bodies: EventBody[] = [{ kind: "session_closed", reason, turns: session.meter.turns }];

    const closing = session.meter.closing;
    if (contact !== null && closing !== null && Abuse.isStrike(closing.reason)) {
        const changes = Abuse.strike(contact, tenant.settings.abuse, now);
        contact = await store.conversations.updateContact(contact.id, changes);
        if (changes.block) {
            bodies.push({ kind: "contact_blocked", contactId: contact.id, block: changes.block });
        }
    }

    const events = stamp(bodies, {
        tenantId:       tenant.id,
        conversationId: conversation.id,
        sessionId:      session.id,
        occurredAt:     now,
        causedBy:       null,
    });
    const jobs = Subscriber.jobsFor(services.subscribers, events, {
        tenant,
        pack:    services.registry.get(tenant.domain),
        contact,
        channel: session.channel,
        now,
    });
    await store.outbox.publish(events, jobs);
    return true;
}

/**
 * Close web and SMS sessions that have gone quiet.
 */
export async function sweepIdleSessions(services: Services): Promise<number> {
    const now = services.clock();
    const idle = await services.store.conversations.idleSessions(new Date(now.getTime() - services.smsIdleMs), 100);
    let closed = 0;
    for (const session of idle) {
        if (await closeSession(services, session.id, "idle")) {
            closed++;
        }
    }
    return closed;
}
