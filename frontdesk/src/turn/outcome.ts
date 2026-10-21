/**
 * What a turn produces: the reply, and the facts it established.
 * Nothing here has happened yet; the turn engine commits it in one transaction.
 */

import type { CallDirective, ContactChanges, EventBody, Language, TurnAssessment } from "../core/index.ts";

/**
 * `language` is the language the reply was written in; it becomes the session's language.
 * `assessment` is how the model judged the turn, which feeds the session's budget meter.
 */
export type TurnOutcome = {
    readonly reply:          string;
    readonly language:       Language;
    readonly assessment:     TurnAssessment;
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
    | { readonly kind: "text";
        readonly delta: string
    }
    | { readonly kind: "done";
        readonly outcome: TurnOutcome
    };

export const TurnOutcome = {
    reply(text: string, language: Language): TurnOutcome {
        return {
            reply:          text,
            language,
            assessment:     "productive",
            events:         [],
            contactChanges: {},
            sentUrls:       [],
            directive:      { kind: "continue" },
        };
    },
} as const;
