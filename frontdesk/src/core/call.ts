/**
 * The lifecycle of a phone call, as an explicit state machine.
 * Every voice vendor adapter drives calls through `CallState.next`,
 * so silence limits, transfers and hang-ups behave the same on Twilio and Vapi.
 * Time limits and the polite wind-down are the budget's job (budget.ts), not the call's.
 */

import type { SessionLimits } from "./budget.ts";
import type { PhoneNumber } from "./channel.ts";
import type { ValueOf } from "./lang.ts";
import { Result } from "./result.ts";

export const CallEndReason = {
    Completed:    "completed",
    CallerHungUp: "caller_hung_up",
    Silence:      "silence",
    Transferred:  "transferred",
    Budget:       "budget",
    Error:        "error",
} as const;
export type CallEndReason = ValueOf<typeof CallEndReason>;

/**
 * What the assistant wants to happen after it speaks.
 */
export type CallDirective =
    | { readonly kind: "continue" }
    | { readonly kind: "transfer";
		readonly to: PhoneNumber
	}
    | { readonly kind: "end" };

export type CallState =
    | { readonly status: "listening";
		readonly turns: number;
		readonly silentTurns: number
	}
    | { readonly status: "responding";
		readonly turns: number
	}
    | { readonly status: "transferring";
		readonly turns: number;
		readonly to: PhoneNumber
	}
    | { readonly status: "ended";
		readonly turns: number;
		readonly reason: CallEndReason
	};

export type CallEvent =
    | { readonly kind: "heard";
		readonly text: string
	}
    | { readonly kind: "silence" }
    | { readonly kind: "replied";
		readonly directive: CallDirective
	}
    | { readonly kind: "hung_up" }
    | { readonly kind: "failed" };

export type CallLimits = Pick<SessionLimits, "maxSilentTurns">;

export type InvalidCallTransition = {
    readonly from:  CallState["status"];
    readonly event: CallEvent["kind"];
};

const DEFAULT_LIMITS: CallLimits = {
    maxSilentTurns: 3,
};

export const CallState = {
    /**
     * A call that has been answered and greeted, waiting for the caller to speak.
     */
    start(): CallState {
        return {
            status: "listening",
            turns: 0,
            silentTurns: 0
        };
    },

    isLive(state: CallState): boolean {
        return state.status === "listening"
			|| state.status === "responding";
    },

    next(
        state: CallState,
        event: CallEvent,
        limits: CallLimits = DEFAULT_LIMITS
    ): Result<CallState, InvalidCallTransition>
	{
        const invalid = Result.err({
            from: state.status,
            event: event.kind
        });
        const { turns } = state;

        if (state.status === "ended") {
            return invalid;
        }
        if (event.kind === "hung_up") {
            return Result.ok({
                status: "ended",
                turns,
                reason: state.status === "transferring"
					? "transferred"
					: "caller_hung_up"
            });
        }
        if (event.kind === "failed") {
            return Result.ok({
                status: "ended",
                turns,
                reason: "error"
            });
        }

        switch (state.status) {
            case "listening":
                if (event.kind === "heard") {
                    return Result.ok({
                        status: "responding",
                        turns: turns + 1
                    });
                }
                if (event.kind === "silence") {
                    const silentTurns = state.silentTurns + 1;
                    return Result.ok(silentTurns >= limits.maxSilentTurns
                        ? { status: "ended", turns, reason: "silence" }
                        : { status: "listening", turns, silentTurns });
                }
                return invalid;

            case "responding":
                if (event.kind !== "replied") {
                    return invalid;
                }
                switch (event.directive.kind) {
                    case "continue": return Result.ok({
                        status: "listening",
                        turns,
                        silentTurns: 0
                    });
                    case "transfer": return Result.ok({
                        status: "transferring",
                        turns,
                        to: event.directive.to
                    });
                    case "end": return Result.ok({
                        status: "ended",
                        turns,
                        reason: "completed"
                    });
                }

            case "transferring":
                return invalid;
        }
    },
} as const;
