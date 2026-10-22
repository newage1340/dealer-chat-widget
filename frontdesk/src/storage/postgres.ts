/**
 * The Store on Postgres (a real server in production, PGlite in tests and local development).
 */

import { randomUUID } from "node:crypto";

import {
    Address,
    AppointmentId,
    AppointmentStatus,
    assertNever,
    BlockSource,
    CallHandling,
    CallState,
    CallVendor,
    CatalogItemId,
    Channel,
    ContactId,
    ConversationId,
    enumValue,
    JobId,
    Language,
    LocalTime,
    MessageId,
    PhoneNumber,
    Role,
    SessionId,
    SessionMeter,
    TenantId,
    type Account,
    type Appointment,
    type AppointmentNotice,
    type Block,
    type CatalogItem,
    type Classification,
    type Contact,
    type ContactChanges,
    type Conversation,
    type DomainEvent,
    type EvalSummary,
    type FieldValue,
    type Job,
    type Message,
    type QualityRow,
    type Session,
    type Transcript,
    type UsageRecord,
    type ValueOf,
    type Voicemail,
} from "../core/index.ts";
import { PriceTable } from "../providers/usage.ts";
import { StoredTenant } from "../tenants/schema.ts";
import type { Db, Row } from "./db.ts";
import type {
    AccountStore,
    AppointmentStore,
    BlockStore,
    ClaimedJob,
    CommittedTurn,
    ConversationStore,
    JobQueue,
    Outbox,
    Participants,
    QualityLog,
    SessionKey,
    SpendLedger,
    Store,
    TenantDirectory,
    TranscriptQuery,
    TranscriptStore,
    TurnCommit,
    TurnLog,
    UsageTotal,
    VoicemailStore,
    WebhookLog,
} from "./ports.ts";

const json = (value: unknown) => JSON.stringify(value);
const date = (value: unknown): Date => value instanceof Date ? value : new Date(String(value));
const maybeDate = (value: unknown): Date | null => value === null || value === undefined ? null : date(value);
const str = (value: unknown): string => value === null || value === undefined ? "" : String(value);
const parsed = <T>(value: unknown): T => (typeof value === "string" ? JSON.parse(value) : value) as T;

/**
 * The first row mapped by `map`, or null when the query found nothing.
 */
function firstOrNull<T>(rows: readonly Row[], map: (row: Row) => T): T | null {
    const row = rows[0];
    return row === undefined
        ? null
        : map(row);
}

/**
 * A stored value of an `as const` enum, checked; a value the code doesn't know is a bug, not a row to guess at.
 */
function member<E extends Readonly<Record<string, string>>>(values: E, raw: unknown): ValueOf<E> {
    const value = enumValue(values, raw);
    if (value === null) {
        throw new Error(`unexpected stored value '${str(raw)}' (expected one of ${Object.values(values).join(", ")})`);
    }
    return value;
}

// --- Row mapping --------------------------------------------------------------------

const CONTACT_COLUMNS = `c.*, (
    select coalesce(json_agg(json_build_object('kind', a.kind, 'value', a.value)), '[]'::json)
    from contact_addresses a where a.contact_id = c.id
) as addresses`;

function toContact(r: Row): Contact {
    const block: Block | null = r["block_source"] === null || r["block_source"] === undefined
        ? null
        : {
            until:     maybeDate(r["block_until"]),
            source:    member(BlockSource, r["block_source"]),
            reason:    str(r["block_reason"]),
            createdAt: date(r["block_created_at"]),
        };
    return {
        id:                ContactId(str(r["id"])),
        tenantId:          TenantId(str(r["tenant_id"])),
        firstName:         str(r["first_name"]),
        lastName:          str(r["last_name"]),
        email:             str(r["email"]),
        phone:             PhoneNumber.parse(str(r["phone"])),
        addresses:         parsed<Address[]>(r["addresses"] ?? []),
        optedOut:          Boolean(r["opted_out"]),
        termsAcceptedAt:   maybeDate(r["terms_accepted_at"]),
        preferredLanguage: r["preferred_language"] === null || r["preferred_language"] === undefined
            ? null
            : member(Language, r["preferred_language"]),
        block,
        abuseStrikes:      parsed<string[]>(r["abuse_strikes"] ?? []).map(date),
        attributes:        parsed<Record<string, string>>(r["attributes"] ?? {}),
    };
}

function toConversation(r: Row): Conversation {
    return {
        id:            ConversationId(str(r["id"])),
        tenantId:      TenantId(str(r["tenant_id"])),
        contactId:     ContactId(str(r["contact_id"])),
        createdAt:     date(r["created_at"]),
        lastMessageAt: maybeDate(r["last_message_at"]),
    };
}

function toSession(r: Row): Session {
    const meter = parsed<SessionMeter & { startedAt: string }>(r["meter"]);
    const base = {
        id:             SessionId(str(r["id"])),
        conversationId: ConversationId(str(r["conversation_id"])),
        tenantId:       TenantId(str(r["tenant_id"])),
        startedAt:      date(r["started_at"]),
        endedAt:        maybeDate(r["ended_at"]),
        sentUrls:       parsed<string[]>(r["sent_urls"] ?? []),
        language:       member(Language, r["language"]),
        meter:          { ...meter, startedAt: date(meter.startedAt) },
    };
    const channel = member(Channel, r["channel"]);
    switch (channel) {
        case "web":
            return { ...base, channel: "web", externalId: str(r["external_id"]) };
        case "sms":
            return { ...base, channel: "sms", externalId: null };
        case "voice":
            return {
                ...base,
                channel:    "voice",
                externalId: str(r["external_id"]),
                vendor:     member(CallVendor, r["vendor"]),
                handling:   r["handling"] === null || r["handling"] === undefined
                    ? CallHandling.Assistant
                    : member(CallHandling, r["handling"]),
                call:       parsed<CallState>(r["call"] ?? CallState.start()),
            };
        default:
            return assertNever(channel);
    }
}

function toMessage(r: Row): Message {
    return {
        id:             MessageId(str(r["id"])),
        conversationId: ConversationId(str(r["conversation_id"])),
        sessionId:      SessionId(str(r["session_id"])),
        role:           member(Role, r["role"]),
        text:           str(r["text"]),
        channel:        member(Channel, r["channel"]),
        createdAt:      date(r["created_at"]),
    };
}

function toAppointment(r: Row): Appointment {
    return {
        id:             AppointmentId(str(r["id"])),
        tenantId:       TenantId(str(r["tenant_id"])),
        contactId:      ContactId(str(r["contact_id"])),
        conversationId: ConversationId(str(r["conversation_id"])),
        timeText:       str(r["time_text"]),
        startsAt:       maybeDate(r["starts_at"]),
        subject:        str(r["subject"]),
        location:       str(r["location"]),
        details:        parsed<Record<string, string>>(r["details"] ?? {}),
        itemIds:        parsed<string[]>(r["item_ids"] ?? []).map(CatalogItemId),
        status:         member(AppointmentStatus, r["status"]),
        sentNotices:    parsed<AppointmentNotice[]>(r["sent_notices"] ?? []),
        createdAt:      date(r["created_at"]),
    };
}

function toClassification(r: Row): Classification {
    const data = parsed<Classification & { classifiedAt: string }>(r["data"]);
    return { ...data, classifiedAt: date(data.classifiedAt) };
}

function toJob(r: Row): ClaimedJob {
    const body = parsed<Record<string, unknown>>(r["body"]);
    return {
        ...body,
        id:        JobId(str(r["id"])),
        attempts:  Number(r["attempts"]),
        tenantId:  TenantId(str(r["tenant_id"])),
        dedupeKey: str(r["dedupe_key"]),
        runAt:     maybeDate(r["run_at"]),
    } as ClaimedJob;
}

/**
 * Column assignments for a contact update, as SQL fragments and parameters.
 */
function contactAssignments(changes: ContactChanges, firstParam: number): { sets: string[]; params: unknown[] } {
    const sets: string[] = [];
    const params: unknown[] = [];
    const set = (column: string, value: unknown, cast = "") => {
        params.push(value);
        sets.push(`${column} = $${firstParam + params.length - 1}${cast}`);
    };
    if (changes.firstName !== undefined) set("first_name", changes.firstName);
    if (changes.lastName !== undefined) set("last_name", changes.lastName);
    if (changes.email !== undefined) set("email", changes.email);
    if (changes.phone !== undefined) set("phone", changes.phone);
    if (changes.optedOut !== undefined) set("opted_out", changes.optedOut);
    if (changes.termsAcceptedAt !== undefined) set("terms_accepted_at", changes.termsAcceptedAt);
    if (changes.preferredLanguage !== undefined) set("preferred_language", changes.preferredLanguage);
    if (changes.abuseStrikes !== undefined) set("abuse_strikes", json(changes.abuseStrikes), "::jsonb");
    if (changes.attributes !== undefined) set("attributes", json(changes.attributes), "::jsonb");
    if (changes.block !== undefined) {
        set("block_until", changes.block?.until ?? null);
        set("block_source", changes.block?.source ?? null);
        set("block_reason", changes.block?.reason ?? null);
        set("block_created_at", changes.block?.createdAt ?? null);
    }
    return { sets, params };
}

function sessionKeyExternal(key: SessionKey): string | null {
    return key.channel === "sms"
        ? null
        : key.externalId;
}

// --- Store --------------------------------------------------------------------------

export class PostgresStore implements Store {
    readonly tenants:       TenantDirectory;
    readonly conversations: ConversationStore;
    readonly appointments:  AppointmentStore;
    readonly turns:         TurnLog;
    readonly outbox:        Outbox;
    readonly transcripts:   TranscriptStore;
    readonly voicemails:    VoicemailStore;
    readonly jobs:          JobQueue;
    readonly usage:         { record(usage: UsageRecord): Promise<void> };
    readonly spend:         SpendLedger;
    readonly blocks:        BlockStore;
    readonly quality:       QualityLog;
    readonly accounts:      AccountStore;
    readonly webhooks:      WebhookLog;
    readonly #db: Db;
    readonly #prices: PriceTable;

    /**
     * `prices` estimates the cost of usage records that arrive without one (texts, calls, transcription).
     */
    constructor(db: Db, prices: PriceTable = {}) {
        this.#db = db;
        this.#prices = prices;
        this.tenants       = this.#tenants();
        this.conversations = this.#conversations();
        this.appointments  = this.#appointments();
        this.turns         = this.#turns();
        this.outbox        = { publish: (events, jobs) => db.transaction(tx => writeOutbox(tx, events, jobs)) };
        this.transcripts   = this.#transcripts();
        this.voicemails    = this.#voicemails();
        this.jobs          = this.#jobs();
        this.usage         = { record: u => this.#recordUsage(u) };
        this.spend         = this.#spend();
        this.blocks        = this.#blocks();
        this.quality       = this.#quality();
        this.accounts      = this.#accounts();
        this.webhooks      = {
            claim: async (source, eventId, at) => (await db.query(
                "insert into webhook_events (source, event_id, received_at) values ($1, $2, $3) on conflict do nothing returning event_id",
                [source, eventId, at],
            )).length > 0,
        };
    }

    async close(): Promise<void> {
        await this.#db.close();
    }

    #tenants(): TenantDirectory {
        const db = this.#db;
        const one = async (sql: string, params: unknown[]) => {
            const rows = await db.query(sql, params);
            return firstOrNull(rows, r => StoredTenant.read(parsed(r["data"])));
        };
        return {
            byId:       id => one("select data from tenants where id = $1", [id]),
            bySlug:     slug => one("select data from tenants where slug = $1", [slug]),
            byEndpoint: (channel, address) => one(
                `select t.data from tenant_endpoints e join tenants t on t.id = e.tenant_id
                 where e.channel = $1 and e.address = $2`,
                [channel, Address.key(address)],
            ),
            all:        async () => (await db.query("select data from tenants order by slug")).map(r => StoredTenant.read(parsed(r["data"]))),
            save:       tenant => db.transaction(async tx => {
                await tx.query(
                    `insert into tenants (id, slug, data, updated_at) values ($1, $2, $3::jsonb, now())
                     on conflict (id) do update set slug = excluded.slug, data = excluded.data, updated_at = now()`,
                    [tenant.id, tenant.slug, json(StoredTenant.write(tenant))],
                );
                await tx.query("delete from tenant_endpoints where tenant_id = $1", [tenant.id]);
                for (const [channel, addresses] of Object.entries(tenant.endpoints)) {
                    for (const address of addresses ?? []) {
                        await tx.query(
                            `insert into tenant_endpoints (channel, address, tenant_id) values ($1, $2, $3)
                             on conflict (channel, address) do update set tenant_id = excluded.tenant_id`,
                            [channel, Address.key(address), tenant.id],
                        );
                    }
                }
            }),
            catalog:    async tenantId => (await db.query(
                "select * from catalog_items where tenant_id = $1 order by position", [tenantId],
            )).map((r): CatalogItem => ({
                id:        CatalogItemId(str(r["id"])),
                tenantId:  TenantId(str(r["tenant_id"])),
                fields:    parsed<Record<string, FieldValue>>(r["fields"]),
                updatedAt: date(r["updated_at"]),
            })),
            syncStatus: async tenantId => {
                const row = (await db.query("select * from catalog_syncs where tenant_id = $1", [tenantId]))[0];
                return row === undefined
                    ? null
                    : { syncedAt: date(row["synced_at"]), items: Number(row["items"]), error: row["error"] === null ? null : str(row["error"]) };
            },
            recordSync: async (tenantId, status) => {
                await db.query(
                    `insert into catalog_syncs (tenant_id, synced_at, items, error) values ($1, $2, $3, $4)
                     on conflict (tenant_id) do update set synced_at = excluded.synced_at, items = excluded.items, error = excluded.error`,
                    [tenantId, status.syncedAt, status.items, status.error],
                );
            },
            replaceCatalog: (tenantId, items) => db.transaction(async tx => {
                await tx.query("delete from catalog_items where tenant_id = $1", [tenantId]);
                const seen = new Set<string>();
                let position = 0;
                for (const item of items) {
                    const id = seen.has(item.id)
                        ? `${item.id}-${position}`
                        : item.id;
                    seen.add(id);
                    await tx.query(
                        "insert into catalog_items (tenant_id, id, position, fields, updated_at) values ($1, $2, $3, $4::jsonb, $5)",
                        [tenantId, id, position++, json(item.fields), item.updatedAt],
                    );
                }
            }),
        };
    }

    #conversations(): ConversationStore {
        const db = this.#db;
        const contactById = async (id: string, q: Db = db) => {
            const rows = await q.query(`select ${CONTACT_COLUMNS} from contacts c where c.id = $1`, [id]);
            return firstOrNull(rows, toContact);
        };
        const sessionById = async (id: string) => {
            const rows = await db.query("select * from sessions where id = $1", [id]);
            return firstOrNull(rows, toSession);
        };
        const findSession = async (tenantId: string, key: SessionKey, conversationId: string | null, idleSince: Date | null) => {
            const rows = key.channel === "sms"
                ? await db.query(
                    `select * from sessions where conversation_id = $1 and channel = 'sms' and ended_at is null
                     and last_activity_at >= $2 order by started_at desc limit 1`,
                    [conversationId, idleSince ?? new Date(0)],
                )
                : await db.query(
                    "select * from sessions where tenant_id = $1 and channel = $2 and external_id = $3 and ended_at is null",
                    [tenantId, key.channel, key.externalId],
                );
            return firstOrNull(rows, toSession);
        };

        return {
            resolve: async (tenantId, sender, now) => {
                const key = Address.key(sender);
                const lookup = async (): Promise<Participants | null> => {
                    const rows = await db.query(
                        `select ${CONTACT_COLUMNS}, row_to_json(v) as conversation
                         from contact_addresses ca
                         join contacts c on c.id = ca.contact_id
                         join conversations v on v.contact_id = c.id
                         where ca.tenant_id = $1 and ca.address = $2`,
                        [tenantId, key],
                    );
                    const r = rows[0];
                    return r === undefined
                        ? null
                        : { contact: toContact(r), conversation: toConversation(parsed<Row>(r["conversation"])) };
                };
                const existing = await lookup();
                if (existing !== null) {
                    return existing;
                }
                await db.transaction(async tx => {
                    const contactId = randomUUID();
                    const claimed = await tx.query(
                        `insert into contact_addresses (tenant_id, address, kind, value, contact_id)
                         values ($1, $2, $3, $4, $5) on conflict do nothing returning contact_id`,
                        [tenantId, key, sender.kind, sender.value, contactId],
                    );
                    if (claimed.length === 0) {
                        return;
                    }
                    await tx.query(
                        `insert into contacts (id, tenant_id, phone, created_at) values ($1, $2, $3, $4)`,
                        [contactId, tenantId, sender.kind === "phone" ? sender.value : null, now],
                    );
                    await tx.query(
                        "insert into conversations (id, tenant_id, contact_id, created_at) values ($1, $2, $3, $4)",
                        [randomUUID(), tenantId, contactId, now],
                    );
                });
                const created = await lookup();
                if (created === null) {
                    throw new Error(`could not create contact for ${key}`);
                }
                return created;
            },

            session: async (conversation, key, defaults, now) => {
                const existing = await findSession(
                    conversation.tenantId, key, conversation.id, new Date(now.getTime() - defaults.smsIdleMs),
                );
                if (existing !== null) {
                    return existing;
                }
                const id = randomUUID();
                const rows = await db.query(
                    `insert into sessions (id, conversation_id, tenant_id, channel, external_id, vendor, handling, call,
                                           language, meter, started_at, last_activity_at)
                     values ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10::jsonb, $11, $11)
                     on conflict do nothing returning *`,
                    [
                        id, conversation.id, conversation.tenantId, key.channel, sessionKeyExternal(key),
                        key.channel === "voice" ? key.vendor : null,
                        key.channel === "voice" ? key.handling : null,
                        key.channel === "voice" ? json(CallState.start()) : null,
                        defaults.language, json(SessionMeter.start(now)), now,
                    ],
                );
                if (rows[0] !== undefined) {
                    return toSession(rows[0]);
                }
                const raced = await findSession(conversation.tenantId, key, conversation.id, null);
                if (raced === null) {
                    throw new Error("session insert conflicted but no session found");
                }
                return raced;
            },

            sessionById,
            findSession: (tenantId, key) => findSession(tenantId, key, null, null),

            saveSession: async session => {
                await db.query(
                    `update sessions set language = $2, meter = $3::jsonb, sent_urls = $4::jsonb, call = $5::jsonb,
                     handling = $6, last_activity_at = greatest(last_activity_at, now()) where id = $1`,
                    [
                        session.id, session.language, json(session.meter), json(session.sentUrls),
                        session.channel === "voice" ? json(session.call) : null,
                        session.channel === "voice" ? session.handling : null,
                    ],
                );
            },

            closeSession: async (id, at) => {
                const rows = await db.query(
                    "update sessions set ended_at = $2 where id = $1 and ended_at is null returning *", [id, at],
                );
                return firstOrNull(rows, toSession);
            },

            idleSessions: async (before, limit) => (await db.query(
                `select * from sessions where ended_at is null and (
                     (channel in ('web', 'sms') and last_activity_at < $1)
                     or (channel = 'voice' and last_activity_at < $1::timestamptz - interval '90 minutes')
                 ) order by last_activity_at limit $2`,
                [before, limit],
            )).map(toSession),

            activeCalls: async (tenantId, excluding) => {
                const rows = await db.query<{ n: number }>(
                    `select count(*)::int as n from sessions where tenant_id = $1 and channel = 'voice'
                     and handling = 'assistant' and ended_at is null and id <> coalesce($2, '')
                     and started_at > now() - interval '2 hours'`,
                    [tenantId, excluding],
                );
                return Number(rows[0]?.n ?? 0);
            },

            history: async (conversationId, limit) => (await db.query(
                `select * from (select * from messages where conversation_id = $1 order by created_at desc limit $2) m
                 order by created_at`,
                [conversationId, limit],
            )).map(toMessage),

            sessionMessages: async sessionId => (await db.query(
                "select * from messages where session_id = $1 order by created_at", [sessionId],
            )).map(toMessage),

            lastMessage: async conversationId => {
                const rows = await db.query(
                    "select * from messages where conversation_id = $1 order by created_at desc limit 1", [conversationId],
                );
                return firstOrNull(rows, toMessage);
            },

            conversation: async id => {
                const rows = await db.query("select * from conversations where id = $1", [id]);
                return firstOrNull(rows, toConversation);
            },

            contact: id => contactById(id),

            contactByAddress: async (tenantId, address) => {
                const rows = await db.query(
                    `select ${CONTACT_COLUMNS} from contact_addresses ca join contacts c on c.id = ca.contact_id
                     where ca.tenant_id = $1 and ca.address = $2`,
                    [tenantId, Address.key(address)],
                );
                return firstOrNull(rows, toContact);
            },

            updateContact: async (id, changes) => {
                const { sets, params } = contactAssignments(changes, 2);
                if (sets.length > 0) {
                    await db.query(`update contacts set ${sets.join(", ")} where id = $1`, [id, ...params]);
                }
                const contact = await contactById(id);
                if (contact === null) {
                    throw new Error(`no contact ${id}`);
                }
                return contact;
            },

            addAddress: async (contactId, address) => {
                await db.query(
                    `insert into contact_addresses (tenant_id, address, kind, value, contact_id)
                     select tenant_id, $2, $3, $4, id from contacts where id = $1
                     on conflict (tenant_id, address) do nothing`,
                    [contactId, Address.key(address), address.kind, address.value],
                );
            },

            resetLanguages: (tenantId, contactId, language) => db.transaction(async tx => {
                const rows = await tx.query(
                    `update contacts set preferred_language = null
                     where tenant_id = $1 and ($2::text is null or id = $2) and preferred_language is not null
                     returning id`,
                    [tenantId, contactId],
                );
                await tx.query(
                    `update sessions s set language = $3
                     from conversations c
                     where s.conversation_id = c.id and s.tenant_id = $1 and s.ended_at is null
                       and ($2::text is null or c.contact_id = $2)`,
                    [tenantId, contactId, language],
                );
                return rows.length;
            }),

            deleteContact: async id => {
                await db.query("delete from contacts where id = $1", [id]);
            },
        };
    }

    #appointments(): AppointmentStore {
        const db = this.#db;
        const one = async (sql: string, params: unknown[]) => {
            const rows = await db.query(sql, params);
            return firstOrNull(rows, toAppointment);
        };
        return {
            latest:     conversationId => one(
                `select * from appointments where conversation_id = $1 and status = 'confirmed'
                 order by created_at desc limit 1`,
                [conversationId],
            ),
            byId:       id => one("select * from appointments where id = $1", [id]),
            markNotice: async (id, notice) => {
                await db.query(
                    `update appointments set sent_notices = sent_notices || $2::jsonb
                     where id = $1 and not sent_notices ? $3`,
                    [id, json([notice]), notice],
                );
            },
        };
    }

    #turns(): TurnLog {
        const db = this.#db;
        return {
            find: async dedupeKey => {
                const rows = await db.query("select reply, session_id from turns where dedupe_key = $1", [dedupeKey]);
                const r = rows[0];
                return r === undefined
                    ? null
                    : { reply: str(r["reply"]), sessionId: SessionId(str(r["session_id"])) } satisfies CommittedTurn;
            },

            commit: turn => db.transaction(async tx => {
                const claimed = await tx.query(
                    `insert into turns (dedupe_key, reply, session_id, created_at) values ($1, $2, $3, $4)
                     on conflict do nothing returning dedupe_key`,
                    [turn.dedupeKey, turn.reply, turn.session.id, turn.at],
                );
                if (claimed.length === 0) {
                    return false;
                }
                await writeTurn(tx, turn);
                return true;
            }),
        };
    }

    #transcripts(): TranscriptStore {
        const db = this.#db;
        const assemble = async (sessionRows: Row[]): Promise<Transcript[]> => {
            if (sessionRows.length === 0) {
                return [];
            }
            const ids = sessionRows.map(r => str(r["id"]));
            const messages = (await db.query(
                "select * from messages where session_id = any($1) order by created_at", [ids],
            )).map(toMessage);
            const classifications = new Map((await db.query(
                "select * from classifications where session_id = any($1)", [ids],
            )).map(r => [str(r["session_id"]), toClassification(r)]));
            return sessionRows.map(r => ({
                session:        toSession(r),
                messages:       messages.filter(m => m.sessionId === str(r["id"])),
                classification: classifications.get(str(r["id"])) ?? null,
            }));
        };
        return {
            transcript: async sessionId => (await assemble(
                await db.query("select * from sessions where id = $1", [sessionId]),
            ))[0] ?? null,

            list: async (query: TranscriptQuery) => assemble(await db.query(
                `select s.* from sessions s left join classifications c on c.session_id = s.id
                 where s.tenant_id = $1 and s.ended_at is not null
                 and ($2::timestamptz is null or s.started_at >= $2)
                 and ($3::timestamptz is null or s.started_at < $3)
                 and (cardinality($4::text[]) = 0 or c.outcome = any($4))
                 order by s.started_at desc limit $5`,
                [query.tenantId, query.since, query.until, [...query.outcomes], query.limit],
            )),

            saveClassification: async c => {
                await db.query(
                    `insert into classifications (session_id, outcome, data) values ($1, $2, $3::jsonb)
                     on conflict (session_id) do update set outcome = excluded.outcome, data = excluded.data`,
                    [c.sessionId, c.outcome, json(c)],
                );
            },
        };
    }

    #voicemails(): VoicemailStore {
        const db = this.#db;
        return {
            save: async v => {
                await db.query(
                    `insert into voicemails (session_id, tenant_id, conversation_id, sender, recording_url, duration_seconds, received_at)
                     values ($1, $2, $3, $4::jsonb, $5, $6, $7) on conflict (session_id) do nothing`,
                    [v.sessionId, v.tenantId, v.conversationId, json(v.from), v.recordingUrl, v.durationSeconds, v.receivedAt],
                );
            },
            bySession: async sessionId => {
                const rows = await db.query("select * from voicemails where session_id = $1", [sessionId]);
                const r = rows[0];
                return r === undefined
                    ? null
                    : {
                        sessionId:       SessionId(str(r["session_id"])),
                        tenantId:        TenantId(str(r["tenant_id"])),
                        conversationId:  ConversationId(str(r["conversation_id"])),
                        from:            parsed<Address>(r["sender"]),
                        recordingUrl:    str(r["recording_url"]),
                        durationSeconds: Number(r["duration_seconds"]),
                        receivedAt:      date(r["received_at"]),
                    } satisfies Voicemail;
            },
        };
    }

    #jobs(): JobQueue {
        const db = this.#db;
        return {
            enqueue: jobs => db.transaction(tx => insertJobs(tx, jobs)),

            claim: async (limit, leaseMs, now) => (await db.query(
                `update jobs set status = 'running', attempts = attempts + 1, lease_until = $3
                 where id in (
                     select id from jobs
                     where (status = 'pending' and (run_at is null or run_at <= $2))
                        or (status = 'running' and lease_until < $2)
                     order by coalesce(run_at, created_at)
                     limit $1
                     for update skip locked
                 )
                 returning *`,
                [limit, now, new Date(now.getTime() + leaseMs)],
            )).map(toJob),

            complete: async id => {
                await db.query("update jobs set status = 'done', lease_until = null where id = $1", [id]);
            },

            fail: async (id, error, retryAt) => {
                await db.query(
                    `update jobs set status = $2, run_at = coalesce($3, run_at), last_error = $4, lease_until = null
                     where id = $1`,
                    [id, retryAt === null ? "failed" : "pending", retryAt, error.slice(0, 2000)],
                );
            },
        };
    }

    async #recordUsage(record: UsageRecord): Promise<void> {
        const u = record.costUsd === null
            ? { ...record, costUsd: PriceTable.estimate(this.#prices, record) }
            : record;
        await this.#db.query(
            `insert into usage (provider, model, service, purpose, occurred_at, units, cost_usd, latency_ms, tenant_id, channel)
             values ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, $10)`,
            [u.provider, u.model, u.service, u.purpose, u.occurredAt, json(u.units), u.costUsd, u.latencyMs, u.tenantId, u.channel],
        );
    }

    #spend(): SpendLedger {
        const db = this.#db;
        return {
            monthToDate: async (tenant, now) => {
                const local = LocalTime.of(now, tenant.timezone);
                const monthStart = LocalTime.parseLocalIso(
                    `${LocalTime.isoMonth(local)}-01T00:00`, tenant.timezone,
                ) ?? new Date(0);
                const rows = await db.query<{ total: number | null }>(
                    "select coalesce(sum(cost_usd), 0)::float8 as total from usage where tenant_id = $1 and occurred_at >= $2",
                    [tenant.id, monthStart],
                );
                return Number(rows[0]?.total ?? 0);
            },
            summary: async (tenantId, since) => (await db.query(
                `select provider, purpose, count(*)::int as calls, coalesce(sum(cost_usd), 0)::float8 as cost
                 from usage where ($1::text is null or tenant_id = $1) and occurred_at >= $2
                 group by provider, purpose order by cost desc`,
                [tenantId, since],
            )).map((r): UsageTotal => ({
                provider: str(r["provider"]),
                purpose:  str(r["purpose"]),
                calls:    Number(r["calls"]),
                costUsd:  Number(r["cost"]),
            })),
        };
    }

    #accounts(): AccountStore {
        const db = this.#db;
        const toAccount = (r: Row): Account => ({
            id:           str(r["id"]),
            tenantId:     TenantId(str(r["tenant_id"])),
            email:        str(r["email"]),
            name:         str(r["name"]),
            passwordHash: str(r["password_hash"]),
            createdAt:    date(r["created_at"]),
            lastLoginAt:  r["last_login_at"] === null ? null : date(r["last_login_at"]),
        });
        const one = async (sql: string, params: unknown[]) => {
            return firstOrNull(await db.query(sql, params), toAccount);
        };
        return {
            byEmail:   email => one("select * from accounts where email = $1", [email.trim().toLowerCase()]),
            byId:      id => one("select * from accounts where id = $1", [id]),
            forTenant: async tenantId => (await db.query("select * from accounts where tenant_id = $1 order by created_at", [tenantId])).map(toAccount),
            create:    async a => (await db.query(
                `insert into accounts (id, tenant_id, email, name, password_hash, created_at) values ($1, $2, $3, $4, $5, $6)
                 on conflict (email) do nothing returning id`,
                [a.id, a.tenantId, a.email.trim().toLowerCase(), a.name, a.passwordHash, a.createdAt],
            )).length > 0,
            setPassword: async (id, hash) => {
                await db.query("update accounts set password_hash = $2 where id = $1", [id, hash]);
            },
            touchLogin: async (id, at) => {
                await db.query("update accounts set last_login_at = $2 where id = $1", [id, at]);
            },
            remove: async id => {
                await db.query("delete from accounts where id = $1", [id]);
            },
            markSubmitted: async (tenantId, at) => {
                await db.query(
                    "insert into onboarding_submissions (tenant_id, submitted_at) values ($1, $2) on conflict (tenant_id) do nothing",
                    [tenantId, at],
                );
            },
            markReviewed: async (tenantId, at) => {
                await db.query("update onboarding_submissions set reviewed_at = $2 where tenant_id = $1", [tenantId, at]);
            },
            unreviewed: async () => (await db.query(
                "select tenant_id from onboarding_submissions where reviewed_at is null order by submitted_at",
            )).map(r => TenantId(str(r["tenant_id"]))),
        };
    }

    #quality(): QualityLog {
        const db = this.#db;
        return {
            recordTurn: async m => {
                await db.query(
                    `insert into turn_metrics (tenant_id, domain, session_id, channel, variant, model, occurred_at, latency_ms,
                        first_text_ms, rounds, tools, tool_failures, corrected, fallback, input_tokens, cached_tokens,
                        output_tokens, cost_usd, reply_chars, language, switched)
                     values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)`,
                    [m.tenantId, m.domain, m.sessionId, m.channel, m.variant, m.model, m.occurredAt, Math.round(m.latencyMs),
                        m.firstTextMs === null ? null : Math.round(m.firstTextMs), m.rounds, json(m.tools), m.toolFailures,
                        m.corrected, m.fallback, m.inputTokens, m.cachedTokens, m.outputTokens, m.costUsd, m.replyChars,
                        m.language, m.switched],
                );
            },
            recordEval: async r => {
                await db.query(
                    `insert into eval_runs (run_id, scenario, variant, model, passed, failures, cost_usd, ran_at)
                     values ($1, $2, $3, $4, $5, $6::jsonb, $7, $8)`,
                    [r.runId, r.scenario, r.variant, r.model, r.passed, json(r.failures), r.costUsd, r.ranAt],
                );
            },
            report: async q => (await db.query(
                `select m.variant, m.model, m.channel,
                        count(*)::int as turns,
                        count(distinct m.session_id)::int as sessions,
                        avg(m.latency_ms)::float as avg_latency,
                        avg(m.first_text_ms)::float as avg_first_text,
                        avg(case when m.corrected then 1 else 0 end)::float as corrected_rate,
                        avg(case when m.tool_failures > 0 then 1 else 0 end)::float as failure_rate,
                        avg(case when m.fallback then 1 else 0 end)::float as fallback_rate,
                        coalesce(sum(m.cached_tokens)::float / nullif(sum(m.cached_tokens + m.input_tokens), 0), 0)::float as cached_share,
                        avg(m.cost_usd)::float as cost_per_turn,
                        coalesce((select avg(case when c.outcome = 'booked' then 1 else 0 end)::float from classifications c
                            where c.session_id in (select distinct x.session_id from turn_metrics x
                                where x.variant = m.variant and x.model = m.model and x.channel = m.channel and x.occurred_at >= $1
                                  and ($2::text is null or x.tenant_id = $2) and ($3::text is null or x.domain = $3))), 0)::float as booked_rate,
                        coalesce((select avg(case when c.outcome in ('booked', 'lead') then 1 else 0 end)::float from classifications c
                            where c.session_id in (select distinct x.session_id from turn_metrics x
                                where x.variant = m.variant and x.model = m.model and x.channel = m.channel and x.occurred_at >= $1
                                  and ($2::text is null or x.tenant_id = $2) and ($3::text is null or x.domain = $3))), 0)::float as lead_rate
                 from turn_metrics m
                 where m.occurred_at >= $1 and ($2::text is null or m.tenant_id = $2) and ($3::text is null or m.domain = $3)
                 group by m.variant, m.model, m.channel
                 order by m.variant, m.model, m.channel`,
                [q.since, q.tenantId, q.domain],
            )).map((r): QualityRow => ({
                variant:        str(r["variant"]),
                model:          str(r["model"]),
                channel:        member(Channel, r["channel"]),
                turns:          Number(r["turns"]),
                sessions:       Number(r["sessions"]),
                avgLatencyMs:   Number(r["avg_latency"]),
                avgFirstTextMs: r["avg_first_text"] === null ? null : Number(r["avg_first_text"]),
                correctedRate:  Number(r["corrected_rate"]),
                failureRate:    Number(r["failure_rate"]),
                fallbackRate:   Number(r["fallback_rate"]),
                cachedShare:    Number(r["cached_share"]),
                costPerTurn:    Number(r["cost_per_turn"]),
                bookedRate:     Number(r["booked_rate"]),
                leadRate:       Number(r["lead_rate"]),
            })),
            evals: async since => (await db.query(
                `select scenario, variant, model, count(*)::int as runs,
                        avg(case when passed then 1 else 0 end)::float as pass_rate, max(ran_at) as last_run
                 from eval_runs where ran_at >= $1
                 group by scenario, variant, model
                 order by pass_rate, scenario`,
                [since],
            )).map((r): EvalSummary => ({
                scenario: str(r["scenario"]),
                variant:  str(r["variant"]),
                model:    str(r["model"]),
                runs:     Number(r["runs"]),
                passRate: Number(r["pass_rate"]),
                lastRun:  date(r["last_run"]),
            })),
        };
    }

    #blocks(): BlockStore {
        const conversations = this.conversations;
        const db = this.#db;
        return {
            block:   async (contactId, block) => { await conversations.updateContact(contactId, { block }); },
            unblock: async contactId => { await conversations.updateContact(contactId, { block: null, abuseStrikes: [] }); },
            blocked: async (tenantId, now) => (await db.query(
                `select ${CONTACT_COLUMNS} from contacts c where c.tenant_id = $1 and c.block_source is not null
                 and (c.block_until is null or c.block_until > $2) order by c.block_created_at desc`,
                [tenantId, now],
            )).map(toContact),
        };
    }
}

async function insertJobs(tx: Db, jobs: readonly Job[]): Promise<void> {
    for (const job of jobs) {
        const { tenantId, dedupeKey, runAt, ...body } = job;
        await tx.query(
            `insert into jobs (id, tenant_id, kind, body, dedupe_key, run_at) values ($1, $2, $3, $4::jsonb, $5, $6)
             on conflict (dedupe_key) do nothing`,
            [randomUUID(), tenantId, body.kind, json(body), dedupeKey, runAt],
        );
    }
}

async function insertEvents(tx: Db, events: readonly DomainEvent[]): Promise<void> {
    for (const event of events) {
        const { id, tenantId, conversationId, sessionId, occurredAt, causedBy, ...body } = event;
        await tx.query(
            `insert into events (id, tenant_id, conversation_id, session_id, kind, body, occurred_at, caused_by)
             values ($1, $2, $3, $4, $5, $6::jsonb, $7, $8) on conflict (id) do nothing`,
            [id, tenantId, conversationId, sessionId, body.kind, json(body), occurredAt, causedBy],
        );
        if (body.kind === "appointment_booked") {
            const a = body.appointment;
            if (body.replaces !== null) {
                await tx.query("update appointments set status = 'cancelled' where id = $1", [body.replaces]);
            }
            await tx.query(
                `insert into appointments (id, tenant_id, contact_id, conversation_id, time_text, starts_at, subject,
                                           location, details, item_ids, status, sent_notices, created_at)
                 values ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb, $11, $12::jsonb, $13)
                 on conflict (id) do nothing`,
                [a.id, a.tenantId, a.contactId, a.conversationId, a.timeText, a.startsAt, a.subject, a.location,
                    json(a.details), json(a.itemIds), a.status, json(a.sentNotices), a.createdAt],
            );
        }
        if (body.kind === "appointment_cancelled") {
            await tx.query("update appointments set status = 'cancelled' where id = $1", [body.appointmentId]);
        }
    }
}

async function writeOutbox(tx: Db, events: readonly DomainEvent[], jobs: readonly Job[]): Promise<void> {
    await insertEvents(tx, events);
    await insertJobs(tx, jobs);
}

async function writeTurn(tx: Db, turn: TurnCommit): Promise<void> {
    const { sets, params } = contactAssignments(turn.contactChanges, 2);
    if (sets.length > 0) {
        await tx.query(`update contacts set ${sets.join(", ")} where id = $1`, [turn.contactId, ...params]);
    }
    const s = turn.session;
    await tx.query(
        `update sessions set language = $2, meter = $3::jsonb, sent_urls = $4::jsonb, call = $5::jsonb,
         handling = $6, last_activity_at = $7 where id = $1`,
        [
            s.id, s.language, json(s.meter), json(s.sentUrls),
            s.channel === "voice" ? json(s.call) : null,
            s.channel === "voice" ? s.handling : null,
            turn.at,
        ],
    );
    let offset = 0;
    for (const m of turn.messages) {
        await tx.query(
            `insert into messages (id, conversation_id, session_id, role, text, channel, created_at)
             values ($1, $2, $3, $4, $5, $6, $7)`,
            [randomUUID(), m.conversationId, m.sessionId, m.role, m.text, m.channel, new Date(turn.at.getTime() + offset++)],
        );
    }
    if (turn.messages.length > 0) {
        await tx.query("update conversations set last_message_at = $2 where id = $1", [s.conversationId, turn.at]);
    }
    await writeOutbox(tx, turn.events, turn.jobs);
}
