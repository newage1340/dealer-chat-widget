/**
 * Limits on how long conversations run and how much a tenant can spend.
 *
 * Sessions are never cut off mid-sentence.
 * When a limit is reached the session enters a wind-down: the model gets a few closing turns
 * to wrap up politely (business hours, thanks for calling), and only then does the session end.
 * The tenant's monthly cost cap (plan.ts) is the one hard limit, because it keeps every tenant profitable.
 * Calls also only start inside the tenant's schedule and up to the lines they pay for.
 */

import type { Channel } from "./channel.ts";
import { Block, type Contact, type ContactChanges } from "./contact.ts";
import type { ValueOf } from "./lang.ts";
import { Plan } from "./plan.ts";
import { LocalClock, ServiceSchedule } from "./schedule.ts";

/**
 * `maxSessionSeconds` is how long a call or chat may run; tenants can change it.
 * The wind-down starts `closingLeadSeconds` before that, and lasts `closingTurns` turns.
 * `maxUnproductiveTurns` consecutive off-topic turns also start the wind-down.
 */
export type SessionLimits = {
    readonly maxSessionSeconds:    number;
    readonly closingLeadSeconds:   number;
    readonly closingTurns:         number;
    readonly maxSilentTurns:       number;
    readonly maxUnproductiveTurns: number;
};

/**
 * `strikesToBlock` sessions ended for abuse within `strikeWindowDays` block the caller for `blockHours`.
 */
export type AbusePolicy = {
    readonly strikesToBlock:   number;
    readonly strikeWindowDays: number;
    readonly blockHours:       number;
};

export const DEFAULT_SESSION_LIMITS: SessionLimits = {
    maxSessionSeconds:    30 * 60,
    closingLeadSeconds:   2 * 60,
    closingTurns:         3,
    maxSilentTurns:       3,
    maxUnproductiveTurns: 3,
};

export const DEFAULT_ABUSE_POLICY: AbusePolicy = {
    strikesToBlock:   2,
    strikeWindowDays: 30,
    blockHours:       24,
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

export const ClosingReason = {
    Duration:     "duration",
    ScheduleEnd:  "schedule_end",
    Unproductive: "unproductive",
    Abusive:      "abusive",
    TenantCap:    "tenant_cap",
} as const;
export type ClosingReason = ValueOf<typeof ClosingReason>;

/**
 * A wind-down in progress. `turnsLeft` counts the closing turns not yet used.
 */
export type Closing = {
    readonly reason:    ClosingReason;
    readonly turnsLeft: number;
};

/**
 * Running totals for one session, kept on the Session and updated by every commit.
 */
export type SessionMeter = {
    readonly turns:             number;
    readonly unproductiveTurns: number;
    readonly abusiveTurns:      number;
    readonly costUsd:           number;
    readonly startedAt:         Date;
    readonly closing:           Closing | null;
};

/**
 * `closing` lets the model answer, told it has `turnsLeft` turns (this one included) to wrap up.
 * `end` means the wind-down is used up: say a canned goodbye without calling the model.
 */
export type BudgetVerdict =
    | { readonly kind: "ok" }
    | { readonly kind: "closing";
		readonly reason: ClosingReason;
		readonly turnsLeft: number
	}
    | { readonly kind: "end";
		readonly reason: ClosingReason
	};

/**
 * Why a new session may not use the assistant.
 * Calls then get the fallback `Coverage.fallbackFor` picks (voicemail, forward or reject);
 * texts get a canned reply, or none for blocked contacts.
 * `allowance_used` means the assistant's share of the cap is gone but the voicemail reserve is not;
 * `tenant_cap` means the whole cap is gone; `billing_inactive` that the tenant's subscription lapsed.
 */
export const RefusalReason = {
    Blocked:         "blocked",
    BillingInactive: "billing_inactive",
    AllowanceUsed: "allowance_used",
    TenantCap:     "tenant_cap",
    OffSchedule:   "off_schedule",
    LineBusy:      "line_busy",
} as const;
export type RefusalReason = ValueOf<typeof RefusalReason>;

export type Admission =
    | { readonly kind: "admit" }
    | { readonly kind: "refuse";
		readonly reason: RefusalReason
	};

/**
 * What admission and per-turn checks need to know about the tenant right now.
 * `activeCalls` counts the tenant's calls already in progress, not including this one.
 */
export type BudgetInput = {
    readonly channel:       Channel;
    readonly plan:          Plan;
    readonly schedule:      ServiceSchedule;
    readonly timeZone:      string;
    readonly textAlwaysOn:  boolean;
    readonly costThisMonth: number;
    readonly activeCalls:   number;
    readonly billingActive: boolean;
    readonly now:           Date;
};

/**
 * Where a tenant stands against its monthly cap.
 * `warning` starts at 80% of the assistant's share; `assistant_used` leaves only the voicemail reserve.
 */
export const CapStatus = {
    Ok:            "ok",
    Warning:       "warning",
    AssistantUsed: "assistant_used",
    Exceeded:      "exceeded",
} as const;
export type CapStatus = ValueOf<typeof CapStatus>;

const CAP_WARNING_SHARE = 0.8;

export const SessionMeter = {
    start(now: Date): SessionMeter {
        return {
            turns:             0,
            unproductiveTurns: 0,
            abusiveTurns:      0,
            costUsd:           0,
            startedAt:         now,
            closing:           null,
        };
    },

    /**
     * The meter after a turn that ran under `verdict`.
     */
    record(
        meter: SessionMeter,
        verdict: BudgetVerdict,
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
            abusiveTurns:      assessment === "abusive"
                ? meter.abusiveTurns + 1
                : meter.abusiveTurns,
            costUsd:           meter.costUsd + costUsd,
            closing:           verdict.kind === "closing"
                ? { reason: verdict.reason, turnsLeft: verdict.turnsLeft - 1 }
                : meter.closing,
        };
    },
} as const;

export const Budget = {
    capStatus(costThisMonth: number, plan: Plan): CapStatus {
        const assistantCap = Plan.assistantCapUsd(plan);
        if (costThisMonth >= Plan.costCapUsd(plan)) {
            return "exceeded";
        }
        if (costThisMonth >= assistantCap) {
            return "assistant_used";
        }
        return costThisMonth >= assistantCap * CAP_WARNING_SHARE
            ? "warning"
            : "ok";
    },

    /**
     * Whether a new session may use the assistant at all.
     */
    admit(block: Block | null, input: BudgetInput): Admission {
        if (Block.isActive(block, input.now)) {
            return { kind: "refuse", reason: "blocked" };
        }
        if (!input.billingActive) {
            return { kind: "refuse", reason: "billing_inactive" };
        }
        const cap = Budget.capStatus(input.costThisMonth, input.plan);
        if (cap === "exceeded") {
            return { kind: "refuse", reason: "tenant_cap" };
        }
        if (cap === "assistant_used") {
            return { kind: "refuse", reason: "allowance_used" };
        }
        if (
            scheduled(input)
            && !ServiceSchedule.isOpen(input.schedule, LocalClock.of(input.now, input.timeZone))
        ) {
            return { kind: "refuse", reason: "off_schedule" };
        }
        if (
            input.channel === "voice"
            && input.activeCalls >= Plan.maxConcurrentCalls(input.plan)
        ) {
            return { kind: "refuse", reason: "line_busy" };
        }
        return { kind: "admit" };
    },

    /**
     * How the next turn of a running session may proceed.
     */
    check(
        meter: SessionMeter,
        limits: SessionLimits,
        input: BudgetInput
    ): BudgetVerdict
    {
        if (meter.closing !== null) {
            return meter.closing.turnsLeft > 0
                ? { kind: "closing", ...meter.closing }
                : { kind: "end", reason: meter.closing.reason };
        }
        const reason = closingReason(meter, limits, input);
        return reason === null
            ? { kind: "ok" }
            : { kind: "closing", reason, turnsLeft: limits.closingTurns };
    },
} as const;

/**
 * Whether the call schedule applies to this channel.
 */
function scheduled(input: BudgetInput): boolean {
    return input.channel === "voice"
        || !input.textAlwaysOn;
}

function closingReason(
    meter: SessionMeter,
    limits: SessionLimits,
    input: BudgetInput
): ClosingReason | null
{
    const elapsed = (input.now.getTime() - meter.startedAt.getTime()) / 1000;
    if (input.costThisMonth + meter.costUsd >= Plan.assistantCapUsd(input.plan)) {
        return "tenant_cap";
    }
    if (meter.abusiveTurns > 0) {
        return "abusive";
    }
    if (meter.unproductiveTurns >= limits.maxUnproductiveTurns) {
        return "unproductive";
    }
    if (elapsed >= limits.maxSessionSeconds - limits.closingLeadSeconds) {
        return "duration";
    }
    if (scheduled(input)) {
        const left = ServiceSchedule.minutesLeft(input.schedule, LocalClock.of(input.now, input.timeZone));
        if (left === null || left * 60 <= limits.closingLeadSeconds) {
            return "schedule_end";
        }
    }
    return null;
}

const DAY_MS  = 24 * 60 * 60 * 1000;
const HOUR_MS = 60 * 60 * 1000;

export const Abuse = {
    /**
     * Whether a session that wound down for `reason` counts as an abuse strike.
     */
    isStrike(reason: ClosingReason): boolean {
        return reason === "abusive"
            || reason === "unproductive";
    },

    /**
     * Record a strike against a contact, blocking them once they reach the policy's threshold.
     * Strikes older than the window are dropped; an existing staff block is never shortened.
     */
    strike(
        contact: Contact,
        policy: AbusePolicy,
        now: Date
    ): ContactChanges
    {
        const windowStart = now.getTime() - policy.strikeWindowDays * DAY_MS;
        const strikes = [...contact.abuseStrikes, now].filter(t => t.getTime() >= windowStart);
        if (strikes.length < policy.strikesToBlock || contact.block?.source === "staff") {
            return { abuseStrikes: strikes };
        }
        return {
            abuseStrikes: [],
            block: {
                until:     new Date(now.getTime() + policy.blockHours * HOUR_MS),
                source:    "auto",
                reason:    `${strikes.length} abusive sessions in ${policy.strikeWindowDays} days`,
                createdAt: now,
            },
        };
    },
} as const;
