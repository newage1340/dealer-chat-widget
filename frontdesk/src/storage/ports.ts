/**
 * What the application needs from storage, independent of the database behind it.
 * Postgres implements these (postgres.ts); tests run the same code against an in-process Postgres.
 */

import type {
	Address, Appointment, AppointmentId, AppointmentNotice, Block, CallHandling, CallVendor,
	CatalogItem, CatalogSyncStatus, Channel, Classification, Contact, ContactChanges, ContactId, Conversation,
	ConversationId, DomainEvent, Job, JobId, Language, Message, Session, SessionId,
	SessionOutcome, Tenant, TenantId, Transcript, Voicemail,
	EvalRecord, EvalSummary, QualityRow, TurnMetric, Account } from "../core/index.ts";
import type { UsageSink } from "../providers/index.ts";

export interface TenantDirectory {
    byId(id: TenantId): Promise<Tenant | null>;
    bySlug(slug: string): Promise<Tenant | null>;
    byEndpoint(channel: Channel, address: Address): Promise<Tenant | null>;
    all(): Promise<readonly Tenant[]>;
    /**
     * Create or replace a tenant, including its endpoint index.
     */
    save(tenant: Tenant): Promise<void>;
    catalog(tenantId: TenantId): Promise<readonly CatalogItem[]>;
    replaceCatalog(tenantId: TenantId, items: readonly CatalogItem[]): Promise<void>;
    /**
     * How the latest fetch from the tenant's catalog source went, or null if it never ran.
     */
    syncStatus(tenantId: TenantId): Promise<CatalogSyncStatus | null>;
    recordSync(tenantId: TenantId, status: CatalogSyncStatus): Promise<void>;
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
        readonly vendor: CallVendor;
        readonly handling: CallHandling
    };

export type Participants = {
    readonly contact:      Contact;
    readonly conversation: Conversation;
};

/**
 * `language` is what a new session starts in; `smsIdleMs` is how long an SMS session stays open.
 */
export type SessionDefaults = {
    readonly language:  Language;
    readonly smsIdleMs: number;
};

export interface ConversationStore {
    /**
     * The contact and conversation for a sender, created on first contact.
     * A phone sender's number is recorded as the contact's reachable phone.
     */
    resolve(tenantId: TenantId, sender: Address, now: Date): Promise<Participants>;
    /**
     * The open session for `key`, or a new one.
     */
    session(conversation: Conversation, key: SessionKey, defaults: SessionDefaults, now: Date): Promise<Session>;
    sessionById(id: SessionId): Promise<Session | null>;
    findSession(tenantId: TenantId, key: SessionKey): Promise<Session | null>;
    saveSession(session: Session): Promise<void>;
    /**
     * Mark a session ended. Returns the session if this call closed it, null if it was already closed.
     */
    closeSession(id: SessionId, at: Date): Promise<Session | null>;
    /**
     * Open web and SMS sessions with no activity since `before` (and calls that never reported an end
     * for 90 minutes beyond that), for the idle sweep.
     */
    idleSessions(before: Date, limit: number): Promise<readonly Session[]>;
    /**
     * Voice sessions the assistant is handling right now for a tenant (for the line limit).
     */
    activeCalls(tenantId: TenantId, excluding: SessionId | null): Promise<number>;
    history(conversationId: ConversationId, limit: number): Promise<readonly Message[]>;
    sessionMessages(sessionId: SessionId): Promise<readonly Message[]>;
    lastMessage(conversationId: ConversationId): Promise<Message | null>;
    conversation(id: ConversationId): Promise<Conversation | null>;
    contact(id: ContactId): Promise<Contact | null>;
    contactByAddress(tenantId: TenantId, address: Address): Promise<Contact | null>;
    updateContact(id: ContactId, changes: ContactChanges): Promise<Contact>;
    /**
     * Link another address (e.g. a phone given in the web widget) to a contact.
     */
    addAddress(contactId: ContactId, address: Address): Promise<void>;
    /**
     * Forget every contact's preferred language (or one contact's), and put their open conversations
     * back in `language`. For a business owner fixing a language that stuck, or after changing the offered languages.
     * Returns how many contacts changed.
     */
    resetLanguages(tenantId: TenantId, contactId: ContactId | null, language: Language): Promise<number>;
    /**
     * Remove a contact and everything about them (web widget "clear session", privacy requests).
     */
    deleteContact(id: ContactId): Promise<void>;
}

export interface AppointmentStore {
    latest(conversationId: ConversationId): Promise<Appointment | null>;
    byId(id: AppointmentId): Promise<Appointment | null>;
    markNotice(id: AppointmentId, notice: AppointmentNotice): Promise<void>;
}

export type NewMessage = Omit<Message, "id" | "createdAt">;

/**
 * Everything one turn writes, applied atomically.
 * `dedupeKey` and `reply` are recorded so a retried webhook gets the same answer without re-running the turn.
 * `jobs` are the subscribers' reactions to `events`, enqueued in the same transaction.
 * Appointments booked during the turn are saved from their `appointment_booked` events.
 */
export type TurnCommit = {
    readonly dedupeKey:      string;
    readonly reply:          string;
    readonly contactId:      ContactId;
    readonly contactChanges: ContactChanges;
    readonly session:        Session;
    readonly messages:       readonly NewMessage[];
    readonly events:         readonly DomainEvent[];
    readonly jobs:           readonly Job[];
    readonly at:             Date;
};

/**
 * A previously committed turn, found by its dedupe key.
 */
export type CommittedTurn = {
    readonly reply:     string;
    readonly sessionId: SessionId;
};

export interface TurnLog {
    find(dedupeKey: string): Promise<CommittedTurn | null>;
    /**
     * Returns false (and writes nothing) when a turn with the same dedupe key was already committed.
     */
    commit(turn: TurnCommit): Promise<boolean>;
}

/**
 * Events and jobs recorded outside a turn (webhook callbacks, sweeps, workers), atomically.
 */
export interface Outbox {
    publish(events: readonly DomainEvent[], jobs: readonly Job[]): Promise<void>;
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
    enqueue(jobs: readonly Job[]): Promise<void>;
    claim(limit: number, leaseMs: number, now: Date): Promise<readonly ClaimedJob[]>;
    complete(id: JobId): Promise<void>;
    /**
     * `retryAt` null gives up on the job for good.
     */
    fail(id: JobId, error: string, retryAt: Date | null): Promise<void>;
}

/**
 * Filters for listing transcripts (the staff web app, metrics).
 */
export type TranscriptQuery = {
    readonly tenantId: TenantId;
    readonly since:    Date | null;
    readonly until:    Date | null;
    readonly outcomes: readonly SessionOutcome[];
    readonly limit:    number;
};

export interface TranscriptStore {
    transcript(sessionId: SessionId): Promise<Transcript | null>;
    list(query: TranscriptQuery): Promise<readonly Transcript[]>;
    saveClassification(classification: Classification): Promise<void>;
}

export interface VoicemailStore {
    save(voicemail: Voicemail): Promise<void>;
    bySession(sessionId: SessionId): Promise<Voicemail | null>;
}

export type UsageTotal = {
    readonly provider: string;
    readonly purpose:  string;
    readonly calls:    number;
    readonly costUsd:  number;
};

/**
 * Spend aggregated from the usage table.
 */
export interface SpendLedger {
    /**
     * The tenant's estimated service cost since the start of the current month, in its timezone.
     */
    monthToDate(tenant: Tenant, now: Date): Promise<number>;
    /**
     * Totals per provider and purpose since `since`, for the usage report.
     */
    summary(tenantId: TenantId | null, since: Date): Promise<readonly UsageTotal[]>;
}

/**
 * Staff actions on bans, from the web app.
 */
export interface BlockStore {
    block(contactId: ContactId, block: Block): Promise<void>;
    unblock(contactId: ContactId): Promise<void>;
    blocked(tenantId: TenantId, now: Date): Promise<readonly Contact[]>;
}

/**
 * Filters for the quality report.
 */
export type QualityQuery = {
    readonly since:    Date;
    readonly tenantId: TenantId | null;
    readonly domain:   string | null;
};

/**
 * Per-turn measurements and eval results, kept for comparing prompt variants and models.
 */
export interface QualityLog {
    recordTurn(metric: TurnMetric): Promise<void>;
    recordEval(record: EvalRecord): Promise<void>;
    report(query: QualityQuery): Promise<readonly QualityRow[]>;
    evals(since: Date): Promise<readonly EvalSummary[]>;
}

/**
 * Business-owner logins, and which onboarded businesses still need a look from the platform team.
 */
export interface AccountStore {
    byEmail(email: string): Promise<Account | null>;
    byId(id: string): Promise<Account | null>;
    forTenant(tenantId: TenantId): Promise<readonly Account[]>;
    /**
     * Returns false when the email is already taken.
     */
    create(account: Account): Promise<boolean>;
    setPassword(id: string, passwordHash: string): Promise<void>;
    touchLogin(id: string, at: Date): Promise<void>;
    remove(id: string): Promise<void>;
    markSubmitted(tenantId: TenantId, at: Date): Promise<void>;
    markReviewed(tenantId: TenantId, at: Date): Promise<void>;
    /**
     * Tenants that came in through onboarding and haven't been marked reviewed.
     */
    unreviewed(): Promise<readonly TenantId[]>;
}

export interface Store {
    readonly tenants:       TenantDirectory;
    readonly conversations: ConversationStore;
    readonly appointments:  AppointmentStore;
    readonly turns:         TurnLog;
    readonly outbox:        Outbox;
    readonly transcripts:   TranscriptStore;
    readonly voicemails:    VoicemailStore;
    readonly jobs:          JobQueue;
    readonly usage:         UsageSink;
    readonly spend:         SpendLedger;
    readonly blocks:        BlockStore;
    readonly quality:       QualityLog;
    readonly accounts:      AccountStore;
    close(): Promise<void>;
}
