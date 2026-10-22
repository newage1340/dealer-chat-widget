/**
 * What happens to a call the assistant cannot take, so no call that reaches us goes unanswered.
 *
 * The business's own phone service decides which calls reach us (forward everything,
 * forward when nobody picks up, forward while closed, or publish our number);
 * we never need to know which, because calls staff answer never pass through us at all.
 */

import type { RefusalReason } from "./budget.ts";
import type { PhoneNumber } from "./channel.ts";
import { assertNever, type ValueOf } from "./lang.ts";

/**
 * What a call gets when the assistant cannot take it.
 * `voicemail` is our own: a short greeting, a recording, a transcript emailed to staff (~1.1 cents a minute).
 * `forward` dials the business (~2.2 cents a minute for the whole call, their voicemail included).
 * `reject` plays a busy signal and costs nothing.
 */
export const CallFallback = {
    Voicemail: "voicemail",
    Forward:   "forward",
    Reject:    "reject",
} as const;
export type CallFallback = ValueOf<typeof CallFallback>;

/**
 * The tenant's choices for the two situations where they have a real choice.
 */
export type CallFallbacks = {
    readonly offSchedule: CallFallback;
    readonly lineBusy:    CallFallback;
};

/**
 * How a call was handled, recorded on its session.
 */
export const CallHandling = {
    Assistant: "assistant",
    Voicemail: "voicemail",
    Forwarded: "forwarded",
    Rejected:  "rejected",
} as const;
export type CallHandling = ValueOf<typeof CallHandling>;

export const DEFAULT_CALL_FALLBACKS: CallFallbacks = {
    offSchedule: "voicemail",
    lineBusy:    "voicemail",
};

/**
 * What we know about where a call came from, for deciding whether forwarding is safe.
 * `forwardedFrom` is the number that forwarded the call to us, when the carrier reports one.
 */
export type CallRoute = {
    readonly forwardTo:     PhoneNumber | null;
    readonly forwardedFrom: PhoneNumber | null;
};

export const Coverage = {
    /**
     * Whether forwarding this call is possible and cannot loop.
     * A call some phone already forwarded to us is never sent back out:
     * it most likely came from the business, whose phone just went unanswered.
     */
    canForward(route: CallRoute): boolean {
        return route.forwardTo !== null
            && route.forwardedFrom === null;
    },

    /**
     * What a refused call actually gets.
     * Blocked callers and tenants with nothing left are rejected (free);
     * once the assistant's allowance is used up, the reserve still pays for voicemail.
     * A forward that is not possible becomes voicemail.
     */
    fallbackFor(
        fallbacks: CallFallbacks,
        reason: RefusalReason,
        route: CallRoute
    ): CallFallback
    {
        const chosen = (() => {
            switch (reason) {
                case "blocked":        return "reject";
                case "billing_inactive": return "reject";
                case "tenant_cap":     return "reject";
                case "allowance_used": return "voicemail";
                case "off_schedule":   return fallbacks.offSchedule;
                case "line_busy":      return fallbacks.lineBusy;
                default:               return assertNever(reason);
            }
        })();
        return chosen === "forward" && !Coverage.canForward(route)
            ? "voicemail"
            : chosen;
    },
} as const;
