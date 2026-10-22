/**
 * The little the store needs from a Postgres connection, with two implementations:
 * a real server (postgres.js, production) and PGlite, Postgres compiled to WebAssembly
 * (in-process, for tests and for local development without installing Postgres).
 */

import { PGlite } from "@electric-sql/pglite";
import postgres from "postgres";

export type Row = Record<string, unknown>;

export interface Db {
    query(text: string, params?: readonly unknown[]): Promise<Row[]>;
    transaction<T>(fn: (tx: Db) => Promise<T>): Promise<T>;
    /**
     * Run a script of several statements without parameters (migrations).
     */
    exec(script: string): Promise<void>;
    close(): Promise<void>;
}

type PgSql = postgres.Sql | postgres.TransactionSql;

function serverDb(sql: PgSql, root: postgres.Sql): Db {
    return {
        async query(text: string, params: readonly unknown[] = []) {
            return await sql.unsafe(text, params as postgres.ParameterOrJSON<never>[]);
        },
        async transaction<T>(fn: (tx: Db) => Promise<T>) {
            if ("savepoint" in sql) {
                return await fn(serverDb(sql, root));
            }
            return await root.begin(tx => fn(serverDb(tx, root))) as T;
        },
        async exec(script: string) {
            await sql.unsafe(script);
        },
        async close() {
            await root.end({ timeout: 5 });
        },
    };
}

export function connectServer(url: string): Db {
    const sql = postgres(url, {
        max:       5,
        onnotice:  () => {},
        transform: { undefined: null },
    });
    return serverDb(sql, sql);
}

type PgliteLike = Pick<PGlite, "query" | "exec">;

function pgliteDb(db: PgliteLike, root: PGlite): Db {
    return {
        async query(text: string, params: readonly unknown[] = []) {
            return (await db.query<Row>(text, [...params])).rows;
        },
        async transaction<T>(fn: (tx: Db) => Promise<T>) {
            if (db !== root) {
                return await fn(pgliteDb(db, root));
            }
            return await root.transaction(tx => fn(pgliteDb(tx, root)));
        },
        async exec(script: string) {
            await db.exec(script);
        },
        async close() {
            await root.close();
        },
    };
}

/**
 * `dataDir` null keeps everything in memory (tests).
 */
export async function connectEmbedded(dataDir: string | null): Promise<Db> {
    const db = dataDir === null
        ? new PGlite()
        : new PGlite(dataDir);
    await db.waitReady;
    return pgliteDb(db, db);
}
