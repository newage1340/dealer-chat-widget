/**
 * Webhook events already applied, so a vendor's retried delivery is applied once.
 */

import type { Db } from "../db.ts";
import type { WebhookLog } from "../ports.ts";

export function webhookLog(db: Db): WebhookLog {
    return {
        claim: async (source, eventId, at) => (await db.query(
            "insert into webhook_events (source, event_id, received_at) values ($1, $2, $3) on conflict do nothing returning event_id",
            [source, eventId, at],
        )).length > 0,
    };
}
