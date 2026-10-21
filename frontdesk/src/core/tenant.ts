/**
 * A business using the system, and which domain it belongs to.
 */

import type { Address, Channel, PhoneNumber } from "./channel.ts";
import type { DomainId, TenantId } from "./ids.ts";

export type Branding = {
    readonly color:   string;
    readonly logoUrl: string;
};

/**
 * A link the assistant may send when the customer asks for it.
 * `trigger` is a case-insensitive pattern matched against what the customer said.
 */
export type TenantLink = {
    readonly label:   string;
    readonly url:     string;
    readonly trigger: string;
};

/**
 * Options every tenant can set, regardless of domain.
 * `coldFollowupMinutes` null means the tenant does not send cold follow-ups.
 */
export type TenantSettings = {
    readonly skipProfileGate:     boolean;
    readonly transferTo:          PhoneNumber | null;
    readonly coldFollowupMinutes: number | null;
    readonly links:               readonly TenantLink[];
};

/**
 * A business and everything core code needs to know about it.
 * `domain` names the domain pack that supplies business-type behavior.
 * `profile` holds the tenant's answers to that domain's questions
 * (e.g. "do you offer financing?"); its keys are defined by the pack, not by core.
 * `agentName` / `voiceAgentName` override the pack's default persona names when set.
 */
export type Tenant = {
    readonly id:             TenantId;
    readonly slug:           string;
    readonly name:           string;
    readonly domain:         DomainId;
    readonly timezone:       string;
    readonly phone:          PhoneNumber | null;
    readonly address:        string;
    readonly website:        string;
    readonly agentName:      string;
    readonly voiceAgentName: string;
    readonly endpoints:      Readonly<Partial<Record<Channel, readonly Address[]>>>;
    readonly staffContacts:  readonly Address[];
    readonly branding:       Branding;
    readonly hoursText:      string;
    readonly profile:        Readonly<Record<string, string>>;
    readonly settings:       TenantSettings;
};

export const Tenant = {
    /**
     * The tenant's primary address on a channel, if it has one.
     */
    endpoint(tenant: Tenant, channel: Channel): Address | null {
        return tenant.endpoints[channel]?.[0] ?? null;
    },

    staffPhones(tenant: Tenant): PhoneNumber[] {
        return tenant.staffContacts.flatMap(a => a.kind === "phone" ? [a.value] : []);
    },
} as const;
