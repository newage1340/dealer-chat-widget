/**
 * Reactions to events, decided at commit time.
 *
 * A subscriber turns an event into jobs (a booking into a confirmation text and a staff alert).
 * It runs inside the commit, so the event and its jobs are stored together or not at all;
 * the jobs themselves run later, in a worker.
 */

import type { Channel, Contact, DomainEvent, EventKind, EventOf, Job, Tenant } from "../core/index.ts";
import type { DomainPack } from "../domains/index.ts";

/**
 * What a subscriber may know besides the event. `contact` and `channel` are null
 * for events recorded outside a conversation turn when they are not known.
 */
export type SubscriberContext = {
    readonly tenant:  Tenant;
    readonly pack:    DomainPack;
    readonly contact: Contact | null;
    readonly channel: Channel | null;
    readonly now:     Date;
};

export interface Subscriber<K extends EventKind = EventKind> {
    readonly name:   string;
    readonly events: readonly K[];
    react(event: EventOf<K>, ctx: SubscriberContext): readonly Job[];
}

export const Subscriber = {
    define<K extends EventKind>(subscriber: Subscriber<K>): Subscriber<K> {
        return subscriber;
    },

    /**
     * Every job the given subscribers want for some events.
     */
    jobsFor(
        subscribers: readonly Subscriber[],
        events: readonly DomainEvent[],
        ctx: SubscriberContext
    ): Job[]
    {
        return events.flatMap(event => subscribers
            .filter(s => s.events.includes(event.kind))
            .flatMap(s => s.react(event as EventOf<EventKind>, ctx)));
    },
} as const;
