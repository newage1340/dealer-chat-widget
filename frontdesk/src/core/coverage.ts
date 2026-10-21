/**
 * How calls reach the assistant, and what happens when it cannot take one.
 *
 * In every mode the business's own phone service decides which calls come to us,
 * so calls staff answer never pass through Twilio and never cost us anything:
 * - `after_hours`: the business forwards its line to us while it is closed.
 * - `overflow`:    the business forwards calls nobody answered in time (carrier "forward on no answer / busy").
 * - `always`:      every call comes straight to us and the assistant answers immediately.
 */

import type { RefusalReason } from "./budget.ts";
import type { ValueOf } from "./lang.ts";

export const CoverageMode = {
    AfterHours: "after_hours",
    Overflow:   "overflow",
    Always:     "always",
} as const;
export type CoverageMode = ValueOf<typeof CoverageMode>;

/**
 * What a call gets when the assistant cannot take it.
 * `voicemail` is our own: a short greeting, a recording, a transcript emailed to staff (~1.1 cents a minute).
 * `forward` dials the business (~2.2 cents a minute for the whole call, voicemail included).
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
 * How a call the assistant is not answering was handled, recorded on its session.
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

export const Coverage = {
    /**
     * Whether calls may be forwarded back to the business.
     * In `after_hours` and `overflow` the business forwarded the call to us,
     * so sending it back would ring an empty phone and bounce straight back.
     */
    canForward(mode: CoverageMode): boolean {
        return mode === "always";
    },

    /**
     * What a refused call actually gets.
     * Blocked callers and tenants with nothing left are rejected (free);
     * once the assistant's allowance is used up, the reserve still pays for voicemail.
     */
    fallbackFor(
        mode: CoverageMode,
        fallbacks: CallFallbacks,
        reason: RefusalReason
    ): CallFallback
    {
        const chosen = (() => {
            switch (reason) {
                case "blocked":        return "reject";
                case "tenant_cap":     return "reject";
                case "allowance_used": return "voicemail";
                case "off_schedule":   return fallbacks.offSchedule;
                case "line_busy":      return fallbacks.lineBusy;
            }
        })();
        return chosen === "forward" && !Coverage.canForward(mode)
            ? "voicemail"
            : chosen;
    },
} as const;
