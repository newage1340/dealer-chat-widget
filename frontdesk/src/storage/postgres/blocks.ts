/**
 * Banned contacts. A block is part of the contact row; this is the staff-facing view of it.
 */

import type { Db } from "../db.ts";
import type { BlockStore } from "../ports.ts";
import { CONTACT_COLUMNS, toContact } from "./rows.ts";
import { updateContact } from "./writes.ts";

export function blockStore(db: Db): BlockStore {
    return {
        block:   (contactId, block) => updateContact(db, contactId, { block }),
        unblock: contactId => updateContact(db, contactId, { block: null, abuseStrikes: [] }),
        blocked: async (tenantId, now) => (await db.query(
            `select ${CONTACT_COLUMNS} from contacts c where c.tenant_id = $1 and c.block_source is not null
             and (c.block_until is null or c.block_until > $2) order by c.block_created_at desc`,
            [tenantId, now],
        )).map(toContact),
    };
}
