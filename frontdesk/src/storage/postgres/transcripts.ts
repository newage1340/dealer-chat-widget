/**
 * Finished sessions with their messages and classification, and voicemails.
 */

import { ConversationId, SessionId, TenantId, type Transcript } from "../../core/index.ts";
import type { Db, Row } from "../db.ts";
import type { TranscriptQuery, TranscriptStore, VoicemailStore } from "../ports.ts";
import { AddressJson, date, firstOrNull, json, jsonColumn, str } from "./columns.ts";
import { toClassification, toMessage, toSession } from "./rows.ts";

export function transcriptStore(db: Db): TranscriptStore {
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

export function voicemailStore(db: Db): VoicemailStore {
    return {
        save: async v => {
            await db.query(
                `insert into voicemails (session_id, tenant_id, conversation_id, sender, recording_url, duration_seconds, received_at)
                 values ($1, $2, $3, $4::jsonb, $5, $6, $7) on conflict (session_id) do nothing`,
                [v.sessionId, v.tenantId, v.conversationId, json(v.from), v.recordingUrl, v.durationSeconds, v.receivedAt],
            );
        },
        bySession: async sessionId => firstOrNull(
            await db.query("select * from voicemails where session_id = $1", [sessionId]),
            r => ({
                sessionId:       SessionId(str(r["session_id"])),
                tenantId:        TenantId(str(r["tenant_id"])),
                conversationId:  ConversationId(str(r["conversation_id"])),
                from:            jsonColumn(AddressJson, r["sender"], "voicemail sender"),
                recordingUrl:    str(r["recording_url"]),
                durationSeconds: Number(r["duration_seconds"]),
                receivedAt:      date(r["received_at"]),
            }),
        ),
    };
}
