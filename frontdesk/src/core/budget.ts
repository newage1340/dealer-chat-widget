/**
 * Spending limits that protect against abuse and runaway cost.
 *
 * Limits nest: a session (one call or chat), a contact (per day), and a tenant (per day and month).
 * They are checked before every turn, deterministically and for free,
 * so a caller who keeps talking nonsense is cut off by budget even if no classifier notices.
 */

import type { ValueOf } from "./lang.ts";

/**
 * `maxUnproductiveTurns` counts consecutive turns the model flagged as off-topic or abusive;
 * any productive turn resets the count.
 */
export type SessionLimits = {
    readonly maxTurns:             number;
    readonly maxSilentTurns:       number;
    readonly maxDurationSeconds:   number;
    readonly maxCostUsd:           number;
    readonly maxUnproductiveTurns: number;
};

export type ContactLimits = {
    readonly maxSessionsPerDay: number;
    readonly maxCostUsdPerDay:  number;
};

export type TenantLimits = {
    readonly maxCostUsdPerDay:   number;
    readonly maxCostUsdPerMonth: number;
};

export type Limits = {
    readonly session: SessionLimits;
    readonly contact: ContactLimits;
    readonly tenant:  TenantLimits;
};

/**
 * Running totals for one session, kept on the Session and updated by every commit.
 */
export type SessionMeter = {
    readonly turns:             number;
    readonly unproductiveTurns: number;
    readonly costUsd:           number;
    readonly startedAt:         Date;
};

/**
 * Spend outside the current session, read from the usage table before the turn.
 */
export type Spend = {
    readonly contactSessionsToday: number;
    readonly contactCostToday:     number;
    readonly tenantCostToday:      number;
    readonly tenantCostThisMonth:  number;
};

/**
 * How the model judged a turn, reported through the `flag_conversation` tool.
 * Turns the model does not flag count as `productive`.
 */
export const TurnAssessment = {
    Productive: "productive",
    OffTopic:   "off_topic",
    Abusive:    "abusive",
} as const;
export type TurnAssessment = ValueOf<typeof TurnAssessment>;

export const BudgetReason = {
    TurnLimit:     "turn_limit",
    Duration:      "duration",
    SessionCost:   "session_cost",
    Unproductive:  "unproductive",
    ContactDaily:  "contact_daily",
    TenantDaily:   "tenant_daily",
    TenantMonthly: "tenant_monthly",
    Blocked:       "blocked",
} as const;
export type BudgetReason = ValueOf<typeof BudgetReason>;

/**
 * `warn` lets the turn run but tells the model to steer back or say the conversation will end.
 * `end` closes the session after a short canned goodbye, without calling the model.
 * `refuse` answers with a canned line and no model call at all (blocked caller, tenant over budget).
 */
export type BudgetVerdict =
    | { readonly kind: "ok" }
    | { readonly kind: "warn";
		readonly reason: BudgetReason
	}
    | { readonly kind: "end";
		readonly reason: BudgetReason
	}
    | { readonly kind: "refuse";
		readonly reason: BudgetReason
	};

export const SessionMeter = {
    start(now: Date): SessionMeter {
        return {
            turns:             0,
            unproductiveTurns: 0,
            costUsd:           0,
            startedAt:         now,
        };
    },

    /**
     * The meter after one more turn.
     */
    record(
        meter: SessionMeter,
        assessment: TurnAssessment,
        costUsd: number
    ): SessionMeter
    {
        return {
            ...meter,
            turns:             meter.turns + 1,
            unproductiveTurns: assessment === "productive"
                ? 0
                : meter.unproductiveTurns + 1,
            costUsd:           meter.costUsd + costUsd,
        };
    },
} as const;

export const Budget = {
    /**
     * Whether a new session may start for this contact.
     */
    admit(
        spend: Spend,
        limits: Limits,
        blockedUntil: Date | null,
        now: Date
    ): BudgetVerdict
    {
        if (blockedUntil !== null && blockedUntil > now) {
            return { kind: "refuse", reason: "blocked" };
        }
        if (spend.tenantCostThisMonth >= limits.tenant.maxCostUsdPerMonth) {
            return { kind: "refuse", reason: "tenant_monthly" };
        }
        if (spend.tenantCostToday >= limits.tenant.maxCostUsdPerDay) {
            return { kind: "refuse", reason: "tenant_daily" };
        }
        if (
            spend.contactSessionsToday >= limits.contact.maxSessionsPerDay
            || spend.contactCostToday >= limits.contact.maxCostUsdPerDay
        ) {
            return { kind: "refuse", reason: "contact_daily" };
        }
        return { kind: "ok" };
    },

    /**
     * Whether the next turn of a running session may call the model.
     * Hard limits end the session; reaching the last allowed unproductive turn only warns.
     */
    check(
        meter: SessionMeter,
        spend: Spend,
        limits: Limits,
        now: Date
    ): BudgetVerdict
    {
        const s = limits.session;
        const elapsed = (now.getTime() - meter.startedAt.getTime()) / 1000;
        if (meter.turns >= s.maxTurns) {
            return { kind: "end", reason: "turn_limit" };
        }
        if (elapsed >= s.maxDurationSeconds) {
            return { kind: "end", reason: "duration" };
        }
        if (meter.costUsd >= s.maxCostUsd) {
            return { kind: "end", reason: "session_cost" };
        }
        if (meter.unproductiveTurns >= s.maxUnproductiveTurns) {
            return { kind: "end", reason: "unproductive" };
        }
        if (
            spend.contactCostToday + meter.costUsd >= limits.contact.maxCostUsdPerDay
        ) {
            return { kind: "end", reason: "contact_daily" };
        }
        if (meter.unproductiveTurns === s.maxUnproductiveTurns - 1) {
            return { kind: "warn", reason: "unproductive" };
        }
        return { kind: "ok" };
    },
} as const;
