/**
 * The long-lived dependencies the engine, worker and channels share.
 */

import { randomUUID } from "node:crypto";

import {
    EventId,
    type Channel,
    type Contact,
    type ContactId,
    type ConversationId,
    type DomainEvent,
    type EventBody,
    type Job,
    type Role,
    type Session,
    type SessionId,
    type Tenant,
    type TenantId,
} from "../core/index.ts";
import type { DomainRegistry } from "../domains/index.ts";
import type { PromptComposer } from "../prompts/composer.ts";
import type { LlmProvider } from "../providers/index.ts";
import type { Store } from "../storage/index.ts";
import { Subscriber, type Policy } from "../turn/index.ts";

/**
 * `smsIdleMs` is how long an SMS or web session stays open without messages before it closes
 * (and its transcript is classified and delivered).
 */
export type Services = {
    readonly store:       Store;
    readonly registry:    DomainRegistry;
    readonly composer:    PromptComposer;
    readonly llm:         LlmProvider;
    readonly policies:    readonly Policy[];
    readonly subscribers: readonly Subscriber[];
    readonly clock:       () => Date;
    readonly smsIdleMs:   number;
};

/**
 * Give event bodies their ids and context.
 */
export function stamp(
    bodies: readonly EventBody[],
    meta: {
        readonly tenantId:       TenantId;
        readonly conversationId: ConversationId;
        readonly sessionId:      SessionId | null;
        readonly occurredAt:     Date;
        readonly causedBy:       string | null;
    }
): DomainEvent[]
{
    return bodies.map(body => ({ ...meta, ...body, id: EventId(randomUUID()) }) as DomainEvent);
}

/**
 * Who and where a batch of events is about.
 * `causedBy` is the dedupe key of the inbound message behind them (null outside a turn).
 */
export type EventScope = {
    readonly tenant:         Tenant;
    readonly conversationId: ConversationId;
    readonly sessionId:      SessionId | null;
    readonly contact:        Contact | null;
    readonly channel:        Channel;
    readonly causedBy:       string | null;
};

/**
 * Stamp event bodies and work out the jobs subscribers want for them,
 * for storing together with the rest of a write.
 */
export function eventsAndJobs(
    services: Services,
    scope: EventScope,
    bodies: readonly EventBody[],
    now: Date
): { readonly events: DomainEvent[]; readonly jobs: Job[] }
{
    const events = stamp(bodies, {
        tenantId:       scope.tenant.id,
        conversationId: scope.conversationId,
        sessionId:      scope.sessionId,
        occurredAt:     now,
        causedBy:       scope.causedBy,
    });
    const jobs = Subscriber.jobsFor(services.subscribers, events, {
        tenant:  scope.tenant,
        pack:    services.registry.get(scope.tenant.domain),
        contact: scope.contact,
        channel: scope.channel,
        now,
    });
    return { events, jobs };
}

/**
 * Record events (and the jobs they cause) outside a turn.
 */
export async function publishEvents(
    services: Services,
    scope: EventScope,
    bodies: readonly EventBody[]
): Promise<void>
{
    const { events, jobs } = eventsAndJobs(services, scope, bodies, services.clock());
    await services.store.outbox.publish(events, jobs);
}

/**
 * Record one message no turn produced (a call greeting, a follow-up text, a voicemail),
 * so the model sees it in later turns. `dedupeKey` makes it safe to repeat.
 */
export async function recordMessage(
    services: Services,
    session: Session,
    contactId: ContactId,
    role: Role,
    text: string,
    dedupeKey: string
): Promise<void>
{
    await services.store.turns.commit({
        dedupeKey,
        reply:          role === "assistant" ? text : "",
        contactId,
        contactChanges: {},
        session,
        messages:       [{ conversationId: session.conversationId, sessionId: session.id, role, text, channel: session.channel }],
        events:         [],
        jobs:           [],
        at:             services.clock(),
    });
}
