/**
 * The long-lived dependencies the engine, worker and channels share.
 */

import { randomUUID } from "node:crypto";

import {
    EventId,
    type ConversationId,
    type DomainEvent,
    type EventBody,
    type SessionId,
    type TenantId,
} from "../core/index.ts";
import type { DomainRegistry } from "../domains/index.ts";
import type { PromptComposer } from "../prompts/composer.ts";
import type { LlmProvider } from "../providers/index.ts";
import type { Store } from "../storage/index.ts";
import type { Policy, Subscriber } from "../turn/index.ts";

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
