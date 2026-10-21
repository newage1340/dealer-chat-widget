/**
 * Reactions to events, decided at commit time.
 *
 * A subscriber turns an event into jobs (a booking into a confirmation text and a staff alert).
 * It runs inside the commit, so the event and its jobs are stored together or not at all;
 * the jobs themselves run later, in a worker.
 */

import type { DomainEvent, EventKind, EventOf, Job, Tenant } from "../core/index.ts";

export interface Subscriber<K extends EventKind = EventKind> {
    readonly name:   string;
    readonly events: readonly K[];
    react(event: EventOf<K>, tenant: Tenant): readonly Job[];
}

export const Subscriber = {
    define<K extends EventKind>(subscriber: Subscriber<K>): Subscriber<K> {
        return subscriber;
    },

    /**
     * Every job the given subscribers want for one event.
     */
    jobsFor(subscribers: readonly Subscriber[], event: DomainEvent, tenant: Tenant): Job[] {
        return subscribers
            .filter(s => s.events.includes(event.kind))
            .flatMap(s => s.react(event as EventOf<EventKind>, tenant));
    },
} as const;
