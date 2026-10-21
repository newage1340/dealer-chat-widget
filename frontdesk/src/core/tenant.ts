/**
 * A business using the system, and which domain it belongs to.
 */

import type { AbusePolicy, SessionLimits } from "./budget.ts";
import type { Address, Channel, PhoneNumber } from "./channel.ts";
import type { DomainId, TenantId } from "./ids.ts";
import type { ValueOf } from "./lang.ts";
import type { Language, LocalizedText } from "./language.ts";

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
 * Things staff are told about.
 */
export const StaffNotice = {
    Transcript:  "transcript",
    Booking:     "booking",
    Handoff:     "handoff",
    BudgetAlert: "budget_alert",
} as const;
export type StaffNotice = ValueOf<typeof StaffNotice>;

export const NotifyVia = {
    Email: "email",
    Sms:   "sms",
} as const;
export type NotifyVia = ValueOf<typeof NotifyVia>;

/**
 * What the tenant pays for, and what we let them spend.
 * `monthlyCostCapUsd` caps our service costs (models, telephony, texts) for the tenant each month,
 * keeping the plan profitable; once reached, the assistant stops answering until the month rolls over.
 * `smsNotifications` is the paid add-on for staff texts. Settings may ask for SMS without it;
 * the plan wins and those notices go by email.
 */
export type Plan = {
    readonly priceUsd:          number;
    readonly monthlyCostCapUsd: number;
    readonly smsNotifications:  boolean;
};

/**
 * A spoken or written notice that conversations are recorded and transcribed,
 * for places that require consent from everyone on a call. Off unless the tenant turns it on.
 */
export type RecordingDisclosure = {
    readonly enabled: boolean;
    readonly text:    LocalizedText;
};

/**
 * Options every tenant can set, regardless of domain.
 * `coldFollowupMinutes` null means the tenant does not send cold follow-ups.
 * `notifications` lists, per notice, how staff want to receive it (email by default).
 * `limits` and `abuse` have system defaults applied; tenants adjust them from the web app.
 */
export type TenantSettings = {
    readonly skipProfileGate:     boolean;
    readonly transferTo:          PhoneNumber | null;
    readonly coldFollowupMinutes: number | null;
    readonly links:               readonly TenantLink[];
    readonly notifications:       Readonly<Record<StaffNotice, readonly NotifyVia[]>>;
    readonly limits:              SessionLimits;
    readonly abuse:               AbusePolicy;
    readonly recordingDisclosure: RecordingDisclosure;
};

/**
 * A business and everything core code needs to know about it.
 * `domain` names the domain pack that supplies business-type behavior.
 * `profile` holds the tenant's answers to that domain's questions
 * (e.g. "do you offer financing?"); its keys are defined by the pack, not by core.
 * `agentName` / `voiceAgentName` override the pack's default persona names when set.
 * `defaultLanguage` greets callers the system knows nothing about;
 * customers can switch to any supported language by asking, or just by speaking it.
 * `staffLanguage` is the language transcript gists and staff notices are written in.
 */
export type Tenant = {
    readonly id:              TenantId;
    readonly slug:            string;
    readonly name:            string;
    readonly domain:          DomainId;
    readonly timezone:        string;
    readonly phone:           PhoneNumber | null;
    readonly address:         string;
    readonly website:         string;
    readonly agentName:       string;
    readonly voiceAgentName:  string;
    readonly endpoints:       Readonly<Partial<Record<Channel, readonly Address[]>>>;
    readonly staffContacts:   readonly Address[];
    readonly branding:        Branding;
    readonly hoursText:       string;
    readonly defaultLanguage: Language;
    readonly staffLanguage:   Language;
    readonly profile:         Readonly<Record<string, string>>;
    readonly plan:            Plan;
    readonly settings:        TenantSettings;
};

export const Tenant = {
    /**
     * The tenant's primary address on a channel, if it has one.
     */
    endpoint(tenant: Tenant, channel: Channel): Address | null {
        return tenant.endpoints[channel]?.[0] ?? null;
    },

    /**
     * How staff receive a notice, after the plan is applied.
     */
    notifyVia(tenant: Tenant, notice: StaffNotice): readonly NotifyVia[] {
        const allowed = tenant.settings.notifications[notice].filter(
            via => via !== "sms" || tenant.plan.smsNotifications
        );
        return allowed.length > 0
            ? allowed
            : ["email"];
    },

    staffPhones(tenant: Tenant): PhoneNumber[] {
        return tenant.staffContacts.flatMap(
			a => a.kind === "phone"
				? [a.value]
				: []
		);
    },
} as const;
