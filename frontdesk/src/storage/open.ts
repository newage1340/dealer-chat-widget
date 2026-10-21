/**
 * Open the database the configuration points at, bring its schema up to date, and wrap it in a Store.
 */

import { connectEmbedded, connectServer } from "./db.ts";
import { migrate } from "./migrations.ts";
import { PostgresStore } from "./postgres.ts";

/**
 * `databaseUrl` set: a Postgres server (production). Otherwise an embedded database in `embeddedDir`,
 * or in memory when that is null (tests). The embedded database is for development only:
 * on a host with a wiped disk (like Render without a persistent disk) its data would not survive a deploy.
 */
export async function openStore(databaseUrl: string, embeddedDir: string | null): Promise<PostgresStore> {
    const db = databaseUrl
        ? connectServer(databaseUrl)
        : await connectEmbedded(embeddedDir);
    await migrate(db);
    return new PostgresStore(db);
}
