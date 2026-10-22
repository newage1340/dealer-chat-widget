/**
 * Contacts (and the addresses that reach them), conversations, sessions and messages.
 */

import { randomUUID } from "node:crypto";

import { Address, CallState, SessionMeter } from "../../core/index.ts";
import type { Db } from "../db.ts";
import type { ConversationStore, Participants, SessionKey } from "../ports.ts";
import { firstOrNull, json, jsonColumn, RowJson } from "./columns.ts";
import { CONTACT_COLUMNS, sessionKeyExternal, toContact, toConversation, toMessage, toSession } from "./rows.ts";
import { updateContact, updateSession } from "./writes.ts";

export function conversationStore(db: Db): ConversationStore {
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
                    : { contact: toContact(r), conversation: toConversation(jsonColumn(RowJson, r["conversation"], "conversation")) };
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

        saveSession: session => updateSession(db, session, null),

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
            const rows = await db.query(
                `select count(*)::int as n from sessions where tenant_id = $1 and channel = 'voice'
                 and handling = 'assistant' and ended_at is null and id <> coalesce($2, '')
                 and started_at > now() - interval '2 hours'`,
                [tenantId, excluding],
            );
            return Number(rows[0]?.["n"] ?? 0);
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
            await updateContact(db, id, changes);
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
