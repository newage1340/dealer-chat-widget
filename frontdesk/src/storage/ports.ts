/**
 * What the application needs from storage, independent of the database behind it.
 * Postgres implements these in production; an in-memory version backs the tests.
 */

import type {
	Address, Appointment, CallVendor, CatalogItem, Channel, Contact,
	ContactChanges, Conversation, ConversationId, DomainEvent,
	Job, JobId, Message, Session, Tenant, TenantId } from "../core/index.ts";
import type { UsageSink } from "../providers/index.ts";

export interface TenantDirectory {
    bySlug(slug: string): Promise<Tenant | null>;
    byEndpoint(channel: Channel, address: Address): Promise<Tenant | null>;
    catalog(tenantId: TenantId): Promise<readonly CatalogItem[]>;
    replaceCatalog(tenantId: TenantId, items: readonly CatalogItem[]): Promise<void>;
}

/**
 * Identifies which session an inbound message belongs to.
 * Voice and web sessions are found by their vendor id;
 * SMS reuses the conversation's open SMS session until it goes idle.
 */
export type SessionKey =
    | { readonly channel: "web";
        readonly externalId: string
    }
    | { readonly channel: "sms" }
    | { readonly channel: "voice";
        readonly externalId: string;
        readonly vendor: CallVendor
    };

export type Participants = {
    readonly contact:      Contact;
    readonly conversation: Conversation;
};

export interface ConversationStore {
    /**
     * The contact and conversation for a sender, created on first contact.
     */
    resolve(tenantId: TenantId, sender: Address): Promise<Participants>;
    session(conversation: Conversation, key: SessionKey, now: Date): Promise<Session>;
    history(conversationId: ConversationId, limit: number): Promise<readonly Message[]>;
    latestAppointment(conversationId: ConversationId): Promise<Appointment | null>;
}

export type NewMessage = Omit<Message, "id" | "createdAt">;

/**
 * Everything one turn writes, applied atomically.
 * `dedupeKey` and `reply` are recorded so a retried webhook gets the same answer without re-running the turn.
 * `jobs` are the subscribers' reactions to `events`, enqueued in the same transaction.
 */
export type TurnCommit = {
    readonly dedupeKey:      string;
    readonly reply:          string;
    readonly contact:        Contact;
    readonly contactChanges: ContactChanges;
    readonly session:        Session;
    readonly messages:       readonly NewMessage[];
    readonly events:         readonly DomainEvent[];
    readonly jobs:           readonly Job[];
};

/**
 * A previously committed turn, found by its dedupe key.
 */
export type CommittedTurn = {
    readonly reply:   string;
    readonly session: Session;
};

export interface TurnLog {
    find(dedupeKey: string): Promise<CommittedTurn | null>;
    commit(turn: TurnCommit): Promise<void>;
}

export type ClaimedJob = Job & {
    readonly id:       JobId;
    readonly attempts: number;
};

/**
 * A durable job queue. `claim` leases jobs so two workers never run the same one;
 * a lease that is neither completed nor failed expires and the job is claimed again.
 */
export interface JobQueue {
    claim(limit: number, leaseMs: number): Promise<readonly ClaimedJob[]>;
    complete(id: JobId): Promise<void>;
    fail(id: JobId, error: string, retryAt: Date | null): Promise<void>;
}

export interface Store {
    readonly tenants:       TenantDirectory;
    readonly conversations: ConversationStore;
    readonly turns:         TurnLog;
    readonly jobs:          JobQueue;
    readonly usage:         UsageSink;
}
