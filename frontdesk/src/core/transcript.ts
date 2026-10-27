/**
 * Full transcripts of closed sessions, and the cheap classification attached to each.
 *
 * The classification replaces the old free-form call summary:
 * a handful of fixed fields from a small model (or a local classifier), short output, low cost.
 * Staff read the transcript itself; the classification feeds metrics, routing and the email subject line.
 */

import type { Message, Session } from "./conversation.ts";
import type { SessionId } from "./ids.ts";
import type { ValueOf } from "./lang.ts";
import type { Language } from "./language.ts";

export const SessionOutcome = {
    Booked:      "booked",
    Lead:        "lead",
    Answered:    "answered",
    Transferred: "transferred",
    Abandoned:   "abandoned",
    Spam:        "spam",
    Abusive:     "abusive",
} as const;
export type SessionOutcome = ValueOf<typeof SessionOutcome>;

export const Sentiment = {
    Positive: "positive",
    Neutral:  "neutral",
    Negative: "negative",
} as const;
export type Sentiment = ValueOf<typeof Sentiment>;

export const Urgency = {
    Low:    "low",
    Normal: "normal",
    Urgent: "urgent",
} as const;
export type Urgency = ValueOf<typeof Urgency>;

/**
 * `topic` is one of the domain pack's topic ids (e.g. "water_heater", "test_drive").
 * `gist` is one or two sentences in the tenant's staff language, not a structured summary.
 * `language` is the language the customer actually used.
 */
export type Classification = {
    readonly sessionId:      SessionId;
    readonly outcome:        SessionOutcome;
    readonly topic:          string;
    readonly sentiment:      Sentiment;
    readonly urgency:        Urgency;
    readonly language:       Language;
    readonly followupNeeded: boolean;
    readonly gist:           string;
    readonly model:          string;
    readonly classifiedAt:   Date;
};

export type Transcript = {
    readonly session:        Session;
    readonly messages:       readonly Message[];
    readonly classification: Classification | null;
};
