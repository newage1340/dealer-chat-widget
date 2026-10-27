/**
 * Booked appointments (written by turns, from their `appointment_booked` events) and the notices sent about them.
 */

import type { Db } from "../db.ts";
import type { AppointmentStore } from "../ports.ts";
import { firstOrNull, json } from "./columns.ts";
import { toAppointment } from "./rows.ts";

export function appointmentStore(db: Db): AppointmentStore {
    const one = async (sql: string, params: unknown[]) => firstOrNull(await db.query(sql, params), toAppointment);
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
