/**
 * Everything a turn can read. Built once per inbound message, never mutated.
 */

import type {
    Appointment,
    CatalogItem,
    Contact,
    Conversation,
    Inbound,
    Message,
    Session,
    Spend,
    Tenant,
} from "../core/index.ts";

/**
 * `now` is the instant the turn started; tenant-local rendering happens where it is needed.
 * `appointment` is the conversation's latest confirmed appointment, if any.
 * `spend` is what this contact and tenant have already spent, for budget checks.
 */
export type TurnContext = {
    readonly inbound:      Inbound;
    readonly tenant:       Tenant;
    readonly catalog:      readonly CatalogItem[];
    readonly contact:      Contact;
    readonly conversation: Conversation;
    readonly session:      Session;
    readonly history:      readonly Message[];
    readonly appointment:  Appointment | null;
    readonly spend:        Spend;
    readonly now:          Date;
};
