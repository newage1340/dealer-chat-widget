/**
 * The Store on Postgres (a real server in production, PGlite in tests and local development).
 * Each port lives in its own file under postgres/; this wires them to one connection.
 */

import type { PriceTable, UsageSink } from "../providers/index.ts";
import type { Db } from "./db.ts";
import type {
    AccountStore,
    AppointmentStore,
    BlockStore,
    ConversationStore,
    JobQueue,
    Outbox,
    QualityLog,
    SpendLedger,
    Store,
    TenantDirectory,
    TranscriptStore,
    TurnLog,
    VoicemailStore,
    WebhookLog,
} from "./ports.ts";
import { accountStore } from "./postgres/accounts.ts";
import { appointmentStore } from "./postgres/appointments.ts";
import { blockStore } from "./postgres/blocks.ts";
import { conversationStore } from "./postgres/conversations.ts";
import { jobQueue } from "./postgres/jobs.ts";
import { qualityLog } from "./postgres/quality.ts";
import { tenantDirectory } from "./postgres/tenants.ts";
import { transcriptStore, voicemailStore } from "./postgres/transcripts.ts";
import { outbox, turnLog } from "./postgres/turns.ts";
import { spendLedger, usageSink } from "./postgres/usage.ts";
import { webhookLog } from "./postgres/webhooks.ts";

export class PostgresStore implements Store {
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
    readonly webhooks:      WebhookLog;
    readonly #db: Db;

    /**
     * `prices` estimates the cost of usage records that arrive without one (texts, calls, transcription).
     */
    constructor(db: Db, prices: PriceTable = {}) {
        this.#db           = db;
        this.tenants       = tenantDirectory(db);
        this.conversations = conversationStore(db);
        this.appointments  = appointmentStore(db);
        this.turns         = turnLog(db);
        this.outbox        = outbox(db);
        this.transcripts   = transcriptStore(db);
        this.voicemails    = voicemailStore(db);
        this.jobs          = jobQueue(db);
        this.usage         = usageSink(db, prices);
        this.spend         = spendLedger(db);
        this.blocks        = blockStore(db);
        this.quality       = qualityLog(db);
        this.accounts      = accountStore(db);
        this.webhooks      = webhookLog(db);
    }

    async close(): Promise<void> {
        await this.#db.close();
    }
}
