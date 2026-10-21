/**
 * The people tenants talk to.
 */

import type { Address, PhoneNumber } from "./channel.ts";
import type { ContactId, TenantId } from "./ids.ts";
import type { Language } from "./language.ts";

/**
 * A ban on a contact. `until` null means indefinitely.
 * Staff can add, extend or lift blocks from the web app; automatic blocks never override a staff one.
 */
export type Block = {
    readonly until:     Date | null;
    readonly source:    "auto" | "staff";
    readonly reason:    string;
    readonly createdAt: Date;
};

export const Block = {
    isActive(block: Block | null, now: Date): boolean {
        return block !== null
            && (block.until === null || block.until > now);
    },
} as const;

/**
 * What we know about a person, scoped to one tenant.
 * `phone` is a real, reachable number.
 * It can differ from the addresses the contact writes from
 * (e.g. a web-widget session that later supplies a phone number).
 * `preferredLanguage` is the language they last spoke, used to greet them in it next time.
 * `block` stops the assistant from answering them (set automatically after repeated abuse, or by staff);
 * `abuseStrikes` are the times of recent sessions that ended for abuse.
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
    readonly block:             Block | null;
    readonly abuseStrikes:      readonly Date[];
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
    | "block"
    | "abuseStrikes"
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
