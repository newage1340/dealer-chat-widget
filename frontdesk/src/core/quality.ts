/**
 * Quality metering: what each turn cost and how it went, per prompt variant and model,
 * so a prompt or model change can be compared with numbers instead of impressions.
 * Records are kept indefinitely; they are the data for A/B tests and for tuning.
 */

import { createHash } from "node:crypto";

import type { Channel } from "./channel.ts";
import type { ConversationId, DomainId, SessionId, TenantId } from "./ids.ts";
import type { Language } from "./language.ts";

/**
 * One turn, measured. `corrected` means the reply claimed an action without its tool and needed
 * the corrective round; `fallback` means the customer got a canned line because the model failed.
 * `firstTextMs` is when the first words were ready (what a caller waits for).
 */
export type TurnMetric = {
    readonly tenantId:     TenantId;
    readonly domain:       DomainId;
    readonly sessionId:    SessionId;
    readonly channel:      Channel;
    readonly variant:      string;
    readonly model:        string;
    readonly occurredAt:   Date;
    readonly latencyMs:    number;
    readonly firstTextMs:  number | null;
    readonly rounds:       number;
    readonly tools:        readonly string[];
    readonly toolFailures: number;
    readonly corrected:    boolean;
    readonly fallback:     boolean;
    readonly inputTokens:  number;
    readonly cachedTokens: number;
    readonly outputTokens: number;
    readonly costUsd:      number;
    readonly replyChars:   number;
    readonly language:     Language;
    readonly switched:     boolean;
};

/**
 * One eval scenario run, kept so pass rates can be tracked per model and prompt variant over time.
 */
export type EvalRecord = {
    readonly runId:    string;
    readonly scenario: string;
    readonly variant:  string;
    readonly model:    string;
    readonly passed:   boolean;
    readonly failures: readonly string[];
    readonly costUsd:  number;
    readonly ranAt:    Date;
};

/**
 * A version of the prompts to test against the others. `templates` replaces base template files
 * (by name, e.g. "base_chat.md") with files in src/prompts/variants/; `temperature` and `tier` override the turn's.
 * `weight` is its share of conversations, relative to the other variants.
 */
export type PromptVariant = {
    readonly id:           string;
    readonly weight:       number;
    readonly description:  string;
    readonly templates:    Readonly<Record<string, string>>;
    readonly temperature:  number | null;
    readonly tier:         "fast" | "capable" | null;
};

export const CONTROL_VARIANT: PromptVariant = {
    id:          "control",
    weight:      100,
    description: "The current prompts.",
    templates:   {},
    temperature: null,
    tier:        null,
};

export const PromptVariant = {
    /**
     * The variant a conversation belongs to. Stable for the conversation's whole life,
     * so one customer never sees the assistant change mid-relationship, and spread by weight.
     */
    assign(variants: readonly PromptVariant[], experiment: string, conversationId: ConversationId): PromptVariant {
        const live = variants.filter(v => v.weight > 0);
        const total = live.reduce((sum, v) => sum + v.weight, 0);
        if (live.length === 0 || total <= 0) {
            return CONTROL_VARIANT;
        }
        const hash = createHash("sha256").update(`${experiment}:${conversationId}`).digest().readUInt32BE(0);
        let point = (hash / 0x1_0000_0000) * total;
        for (const v of live) {
            point -= v.weight;
            if (point < 0) {
                return v;
            }
        }
        return live[live.length - 1]!;
    },
} as const;

/**
 * Aggregates for the quality report, per variant, model and channel.
 * Rates are shares of turns (or of classified sessions, for outcomes), from 0 to 1.
 */
export type QualityRow = {
    readonly variant:        string;
    readonly model:          string;
    readonly channel:        Channel;
    readonly turns:          number;
    readonly sessions:       number;
    readonly avgLatencyMs:   number;
    readonly avgFirstTextMs: number | null;
    readonly correctedRate:  number;
    readonly failureRate:    number;
    readonly fallbackRate:   number;
    readonly cachedShare:    number;
    readonly costPerTurn:    number;
    readonly bookedRate:     number;
    readonly leadRate:       number;
};

export type EvalSummary = {
    readonly scenario: string;
    readonly variant:  string;
    readonly model:    string;
    readonly runs:     number;
    readonly passRate: number;
    readonly lastRun:  Date;
};
