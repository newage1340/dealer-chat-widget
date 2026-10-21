/**
 * What a turn produces: the reply, and the facts it established.
 * Nothing here has happened yet; the turn engine commits it in one transaction.
 */

import type { CallDirective, ContactChanges, EventBody } from "../core/index.ts";

export type TurnOutcome = {
    readonly reply:          string;
    readonly events:         readonly EventBody[];
    readonly contactChanges: ContactChanges;
    readonly sentUrls:       readonly string[];
    readonly directive:      CallDirective;
};

/**
 * The streamed form of a turn: reply text as it is generated, then the full outcome.
 * Channels that cannot stream (SMS, Twilio <Say>) wait for `done`.
 */
export type TurnPart =
    | { readonly kind: "text"; readonly delta: string }
    | { readonly kind: "done"; readonly outcome: TurnOutcome };

export const TurnOutcome = {
    reply(text: string): TurnOutcome {
        return {
            reply:          text,
            events:         [],
            contactChanges: {},
            sentUrls:       [],
            directive:      { kind: "continue" },
        };
    },
} as const;
