/**
 * Deterministic checks that run before the model:
 * compliance (STOP/START/HELP), the web profile gate, rate limits.
 * A policy either lets the turn continue or answers it outright.
 */

import type { TurnContext } from "./context.ts";
import type { TurnOutcome } from "./outcome.ts";

/**
 * `answer` replies without calling the model.
 * `ephemeral` answers are shown but not stored in history (e.g. "what's your name first?").
 * `silence` stores the inbound message but sends nothing (e.g. an opted-out contact).
 * `data` is passed through to the channel (e.g. the web widget's "ask for name and phone" form).
 */
export type PolicyDecision =
    | { readonly kind: "continue" }
    | { readonly kind: "answer";
        readonly outcome: TurnOutcome;
        readonly ephemeral: boolean;
        readonly data: Readonly<Record<string, unknown>>
    }
    | { readonly kind: "silence" };

export interface Policy {
    readonly name: string;
    check(ctx: TurnContext): PolicyDecision | Promise<PolicyDecision>;
}

export const PolicyDecision = {
    continue: { kind: "continue" } satisfies PolicyDecision,
    silence:  { kind: "silence" }  satisfies PolicyDecision,

    answer(
        outcome: TurnOutcome,
        ephemeral = false,
        data: Readonly<Record<string, unknown>> = {}
    ): PolicyDecision
    {
        return { kind: "answer", outcome, ephemeral, data };
    },
} as const;
