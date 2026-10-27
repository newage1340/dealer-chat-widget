/**
 * Business owner logins, and which onboarding signups are waiting for review.
 */

import { TenantId, type Account } from "../../core/index.ts";
import type { Db, Row } from "../db.ts";
import type { AccountStore } from "../ports.ts";
import { date, firstOrNull, maybeDate, str } from "./columns.ts";

function toAccount(r: Row): Account {
    return {
        id:           str(r["id"]),
        tenantId:     TenantId(str(r["tenant_id"])),
        email:        str(r["email"]),
        name:         str(r["name"]),
        passwordHash: str(r["password_hash"]),
        createdAt:    date(r["created_at"]),
        lastLoginAt:  maybeDate(r["last_login_at"]),
    };
}

/**
 * Emails are stored and looked up lowercased, so sign-in doesn't depend on how someone typed theirs.
 */
function normalEmail(email: string): string {
    return email.trim().toLowerCase();
}

export function accountStore(db: Db): AccountStore {
    const one = async (sql: string, params: unknown[]) => firstOrNull(await db.query(sql, params), toAccount);
    return {
        byEmail:   email => one("select * from accounts where email = $1", [normalEmail(email)]),
        byId:      id => one("select * from accounts where id = $1", [id]),
        forTenant: async tenantId => (await db.query("select * from accounts where tenant_id = $1 order by created_at", [tenantId])).map(toAccount),
        create:    async a => (await db.query(
            `insert into accounts (id, tenant_id, email, name, password_hash, created_at) values ($1, $2, $3, $4, $5, $6)
             on conflict (email) do nothing returning id`,
            [a.id, a.tenantId, normalEmail(a.email), a.name, a.passwordHash, a.createdAt],
        )).length > 0,
        setPassword: async (id, hash) => {
            await db.query("update accounts set password_hash = $2 where id = $1", [id, hash]);
        },
        touchLogin: async (id, at) => {
            await db.query("update accounts set last_login_at = $2 where id = $1", [id, at]);
        },
        remove: async id => {
            await db.query("delete from accounts where id = $1", [id]);
        },
        markSubmitted: async (tenantId, at) => {
            await db.query(
                "insert into onboarding_submissions (tenant_id, submitted_at) values ($1, $2) on conflict (tenant_id) do nothing",
                [tenantId, at],
            );
        },
        markReviewed: async (tenantId, at) => {
            await db.query("update onboarding_submissions set reviewed_at = $2 where tenant_id = $1", [tenantId, at]);
        },
        unreviewed: async () => (await db.query(
            "select tenant_id from onboarding_submissions where reviewed_at is null order by submitted_at",
        )).map(r => TenantId(str(r["tenant_id"]))),
    };
}
