/**
 * Metered usage, priced on the way in when the provider didn't know the cost, and the spend totals built from it.
 */

import { LocalTime } from "../../core/index.ts";
import { PriceTable, type UsageSink } from "../../providers/usage.ts";
import type { Db } from "../db.ts";
import type { SpendLedger } from "../ports.ts";
import { json, str } from "./columns.ts";

export function usageSink(db: Db, prices: PriceTable): UsageSink {
    return {
        record: async record => {
            const u = record.costUsd === null
                ? { ...record, costUsd: PriceTable.estimate(prices, record) }
                : record;
            await db.query(
                `insert into usage (provider, model, service, purpose, occurred_at, units, cost_usd, latency_ms, tenant_id, channel)
                 values ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, $10)`,
                [u.provider, u.model, u.service, u.purpose, u.occurredAt, json(u.units), u.costUsd, u.latencyMs, u.tenantId, u.channel],
            );
        },
    };
}

export function spendLedger(db: Db): SpendLedger {
    return {
        monthToDate: async (tenant, now) => {
            const local = LocalTime.of(now, tenant.timezone);
            const monthStart = LocalTime.parseLocalIso(`${LocalTime.isoMonth(local)}-01T00:00`, tenant.timezone) ?? new Date(0);
            const rows = await db.query(
                "select coalesce(sum(cost_usd), 0)::float8 as total from usage where tenant_id = $1 and occurred_at >= $2",
                [tenant.id, monthStart],
            );
            return Number(rows[0]?.["total"] ?? 0);
        },

        summary: async (tenantId, since) => (await db.query(
            `select provider, purpose, count(*)::int as calls, coalesce(sum(cost_usd), 0)::float8 as cost
             from usage where ($1::text is null or tenant_id = $1) and occurred_at >= $2
             group by provider, purpose order by cost desc`,
            [tenantId, since],
        )).map(r => ({
            provider: str(r["provider"]),
            purpose:  str(r["purpose"]),
            calls:    Number(r["calls"]),
            costUsd:  Number(r["cost"]),
        })),
    };
}
