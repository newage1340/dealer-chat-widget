/**
 * Writes more than one store makes, each run inside the caller's transaction.
 */

import { randomUUID } from "node:crypto";

import type { ContactChanges, ContactId, DomainEvent, Job, Session } from "../../core/index.ts";
import type { Db } from "../db.ts";
import type { TurnCommit } from "../ports.ts";
import { json } from "./columns.ts";
import { contactAssignments } from "./rows.ts";

export async function updateContact(q: Db, id: ContactId, changes: ContactChanges): Promise<void> {
    const { sets, params } = contactAssignments(changes, 2);
    if (sets.length > 0) {
        await q.query(`update contacts set ${sets.join(", ")} where id = $1`, [id, ...params]);
    }
}

/**
 * Save a session's mutable state. `activityAt` null marks it active now (never moving the time backward).
 */
export async function updateSession(q: Db, session: Session, activityAt: Date | null): Promise<void> {
    await q.query(
        `update sessions set language = $2, meter = $3::jsonb, sent_urls = $4::jsonb, call = $5::jsonb,
         handling = $6, last_activity_at = coalesce($7, greatest(last_activity_at, now())) where id = $1`,
        [
            session.id, session.language, json(session.meter), json(session.sentUrls),
            session.channel === "voice" ? json(session.call) : null,
            session.channel === "voice" ? session.handling : null,
            activityAt,
        ],
    );
}

export async function insertJobs(q: Db, jobs: readonly Job[]): Promise<void> {
    for (const job of jobs) {
        const { tenantId, dedupeKey, runAt, ...body } = job;
        await q.query(
            `insert into jobs (id, tenant_id, kind, body, dedupe_key, run_at) values ($1, $2, $3, $4::jsonb, $5, $6)
             on conflict (dedupe_key) do nothing`,
            [randomUUID(), tenantId, body.kind, json(body), dedupeKey, runAt],
        );
    }
}

/**
 * Record events, and apply the ones that change appointments.
 */
async function insertEvents(q: Db, events: readonly DomainEvent[]): Promise<void> {
    for (const event of events) {
        const { id, tenantId, conversationId, sessionId, occurredAt, causedBy, ...body } = event;
        await q.query(
            `insert into events (id, tenant_id, conversation_id, session_id, kind, body, occurred_at, caused_by)
             values ($1, $2, $3, $4, $5, $6::jsonb, $7, $8) on conflict (id) do nothing`,
            [id, tenantId, conversationId, sessionId, body.kind, json(body), occurredAt, causedBy],
        );
        if (body.kind === "appointment_booked") {
            const a = body.appointment;
            if (body.replaces !== null) {
                await q.query("update appointments set status = 'cancelled' where id = $1", [body.replaces]);
            }
            await q.query(
                `insert into appointments (id, tenant_id, contact_id, conversation_id, time_text, starts_at, subject,
                                           location, details, item_ids, status, sent_notices, created_at)
                 values ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb, $11, $12::jsonb, $13)
                 on conflict (id) do nothing`,
                [a.id, a.tenantId, a.contactId, a.conversationId, a.timeText, a.startsAt, a.subject, a.location,
                    json(a.details), json(a.itemIds), a.status, json(a.sentNotices), a.createdAt],
            );
        }
        if (body.kind === "appointment_cancelled") {
            await q.query("update appointments set status = 'cancelled' where id = $1", [body.appointmentId]);
        }
    }
}

export async function writeOutbox(q: Db, events: readonly DomainEvent[], jobs: readonly Job[]): Promise<void> {
    await insertEvents(q, events);
    await insertJobs(q, jobs);
}

/**
 * Everything a turn established, after its dedupe key has been claimed.
 */
export async function writeTurn(q: Db, turn: TurnCommit): Promise<void> {
    await updateContact(q, turn.contactId, turn.contactChanges);
    await updateSession(q, turn.session, turn.at);
    let offset = 0;
    for (const m of turn.messages) {
        await q.query(
            `insert into messages (id, conversation_id, session_id, role, text, channel, created_at)
             values ($1, $2, $3, $4, $5, $6, $7)`,
            // A millisecond apart, so a turn's messages keep their order.
            [randomUUID(), m.conversationId, m.sessionId, m.role, m.text, m.channel, new Date(turn.at.getTime() + offset++)],
        );
    }
    if (turn.messages.length > 0) {
        await q.query("update conversations set last_message_at = $2 where id = $1", [turn.session.conversationId, turn.at]);
    }
    await writeOutbox(q, turn.events, turn.jobs);
}
