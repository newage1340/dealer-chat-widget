/**
 * The people tenants talk to.
 */

import type { Address, PhoneNumber } from "./channel.ts";
import type { ContactId, TenantId } from "./ids.ts";
import type { Language } from "./language.ts";

/**
 * What we know about a person, scoped to one tenant.
 * `phone` is a real, reachable number.
 * It can differ from the addresses the contact writes from
 * (e.g. a web-widget session that later supplies a phone number).
 * `preferredLanguage` is the language they last spoke, used to greet them in it next time.
 * `blockedUntil` is set after repeated abusive sessions; until then they get no model replies.
 * `attributes` holds domain-defined facts the core does not interpret.
 */
export type Contact = {
    readonly id:                ContactId;
    readonly tenantId:          TenantId;
    readonly firstName:         string;
    readonly lastName:          string;
    readonly email:             string;
    readonly phone:             PhoneNumber | null;
    readonly addresses:         readonly Address[];
    readonly optedOut:          boolean;
    readonly termsAcceptedAt:   Date | null;
    readonly preferredLanguage: Language | null;
    readonly blockedUntil:      Date | null;
    readonly attributes:        Readonly<Record<string, string>>;
};

/**
 * The fields a turn may change on a contact.
 */
export type ContactChanges = Partial<Pick<Contact,
    | "firstName"
    | "lastName"
    | "email"
    | "phone"
    | "optedOut"
    | "termsAcceptedAt"
    | "preferredLanguage"
    | "blockedUntil"
    | "attributes"
>>;

export const Contact = {
    displayName(contact: Contact): string {
        return [contact.firstName, contact.lastName].filter(Boolean).join(" ");
    },

    apply(contact: Contact, changes: ContactChanges): Contact {
        return { ...contact, ...changes };
    },
} as const;
