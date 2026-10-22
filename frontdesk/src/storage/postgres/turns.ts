/**
 * Committed turns (by dedupe key, so a retried webhook is answered from the first result)
 * and the outbox for events and jobs recorded outside a turn.
 */

import { SessionId } from "../../core/index.ts";
import type { Db } from "../db.ts";
import type { Outbox, TurnLog } from "../ports.ts";
import { firstOrNull, str } from "./columns.ts";
import { writeOutbox, writeTurn } from "./writes.ts";

export function turnLog(db: Db): TurnLog {
    return {
        find: async dedupeKey => firstOrNull(
            await db.query("select reply, session_id from turns where dedupe_key = $1", [dedupeKey]),
            r => ({ reply: str(r["reply"]), sessionId: SessionId(str(r["session_id"])) }),
        ),

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

export function outbox(db: Db): Outbox {
    return {
        publish: (events, jobs) => db.transaction(tx => writeOutbox(tx, events, jobs)),
    };
}
