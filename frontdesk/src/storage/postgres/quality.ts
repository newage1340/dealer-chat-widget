/**
 * Turn metrics and eval runs, reported per prompt variant, model and channel.
 */

import { Channel, type EvalSummary, type QualityRow } from "../../core/index.ts";
import type { Db } from "../db.ts";
import type { QualityLog } from "../ports.ts";
import { date, json, member, str } from "./columns.ts";

export function qualityLog(db: Db): QualityLog {
    return {
        recordTurn: async m => {
            await db.query(
                `insert into turn_metrics (tenant_id, domain, session_id, channel, variant, model, occurred_at, latency_ms,
                    first_text_ms, rounds, tools, tool_failures, corrected, fallback, input_tokens, cached_tokens,
                    output_tokens, cost_usd, reply_chars, language, switched)
                 values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)`,
                [m.tenantId, m.domain, m.sessionId, m.channel, m.variant, m.model, m.occurredAt, Math.round(m.latencyMs),
                    m.firstTextMs === null ? null : Math.round(m.firstTextMs), m.rounds, json(m.tools), m.toolFailures,
                    m.corrected, m.fallback, m.inputTokens, m.cachedTokens, m.outputTokens, m.costUsd, m.replyChars,
                    m.language, m.switched],
            );
        },
        recordEval: async r => {
            await db.query(
                `insert into eval_runs (run_id, scenario, variant, model, passed, failures, cost_usd, ran_at)
                 values ($1, $2, $3, $4, $5, $6::jsonb, $7, $8)`,
                [r.runId, r.scenario, r.variant, r.model, r.passed, json(r.failures), r.costUsd, r.ranAt],
            );
        },
        report: async q => (await db.query(
            `select m.variant, m.model, m.channel,
                    count(*)::int as turns,
                    count(distinct m.session_id)::int as sessions,
                    avg(m.latency_ms)::float as avg_latency,
                    avg(m.first_text_ms)::float as avg_first_text,
                    avg(case when m.corrected then 1 else 0 end)::float as corrected_rate,
                    avg(case when m.tool_failures > 0 then 1 else 0 end)::float as failure_rate,
                    avg(case when m.fallback then 1 else 0 end)::float as fallback_rate,
                    coalesce(sum(m.cached_tokens)::float / nullif(sum(m.cached_tokens + m.input_tokens), 0), 0)::float as cached_share,
                    avg(m.cost_usd)::float as cost_per_turn,
                    coalesce((select avg(case when c.outcome = 'booked' then 1 else 0 end)::float from classifications c
                        where c.session_id in (select distinct x.session_id from turn_metrics x
                            where x.variant = m.variant and x.model = m.model and x.channel = m.channel and x.occurred_at >= $1
                              and ($2::text is null or x.tenant_id = $2) and ($3::text is null or x.domain = $3))), 0)::float as booked_rate,
                    coalesce((select avg(case when c.outcome in ('booked', 'lead') then 1 else 0 end)::float from classifications c
                        where c.session_id in (select distinct x.session_id from turn_metrics x
                            where x.variant = m.variant and x.model = m.model and x.channel = m.channel and x.occurred_at >= $1
                              and ($2::text is null or x.tenant_id = $2) and ($3::text is null or x.domain = $3))), 0)::float as lead_rate
             from turn_metrics m
             where m.occurred_at >= $1 and ($2::text is null or m.tenant_id = $2) and ($3::text is null or m.domain = $3)
             group by m.variant, m.model, m.channel
             order by m.variant, m.model, m.channel`,
            [q.since, q.tenantId, q.domain],
        )).map((r): QualityRow => ({
            variant:        str(r["variant"]),
            model:          str(r["model"]),
            channel:        member(Channel, r["channel"]),
            turns:          Number(r["turns"]),
            sessions:       Number(r["sessions"]),
            avgLatencyMs:   Number(r["avg_latency"]),
            avgFirstTextMs: r["avg_first_text"] === null ? null : Number(r["avg_first_text"]),
            correctedRate:  Number(r["corrected_rate"]),
            failureRate:    Number(r["failure_rate"]),
            fallbackRate:   Number(r["fallback_rate"]),
            cachedShare:    Number(r["cached_share"]),
            costPerTurn:    Number(r["cost_per_turn"]),
            bookedRate:     Number(r["booked_rate"]),
            leadRate:       Number(r["lead_rate"]),
        })),
        evals: async since => (await db.query(
            `select scenario, variant, model, count(*)::int as runs,
                    avg(case when passed then 1 else 0 end)::float as pass_rate, max(ran_at) as last_run
             from eval_runs where ran_at >= $1
             group by scenario, variant, model
             order by pass_rate, scenario`,
            [since],
        )).map((r): EvalSummary => ({
            scenario: str(r["scenario"]),
            variant:  str(r["variant"]),
            model:    str(r["model"]),
            runs:     Number(r["runs"]),
            passRate: Number(r["pass_rate"]),
            lastRun:  date(r["last_run"]),
        })),
    };
}
