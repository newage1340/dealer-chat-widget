/**
 * A business using the system, and which domain it belongs to.
 */

import { NO_BILLING, type BillingState } from "./billing.ts";
import { Bound, PLATFORM_BOUNDS } from "./bounds.ts";
import type { AbusePolicy, SessionLimits } from "./budget.ts";
import { DEFAULT_CATALOG_SETTINGS, type CatalogSettings } from "./catalog.ts";
import type { Address, Channel, PhoneNumber } from "./channel.ts";
import type { CallFallbacks } from "./coverage.ts";
import type { DomainId, TenantId } from "./ids.ts";
import type { ValueOf } from "./lang.ts";
import { Language, type LocalizedText } from "./language.ts";
import type { Plan } from "./plan.ts";
import { DEFAULT_PRIVACY, type PrivacySettings } from "./privacy.ts";
import type { ServiceSchedule } from "./schedule.ts";

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
 * A spoken or written notice that conversations are recorded and transcribed,
 * for places that require consent from everyone on a call. Off unless the tenant turns it on.
 */
export type RecordingDisclosure = {
    readonly enabled: boolean;
    readonly text:    LocalizedText;
};

/**
 * How formal the assistant sounds, from a relaxed neighborhood shop to a clinical office.
 */
export const Professionalism = {
    Casual:   "casual",
    Friendly: "friendly",
    Polished: "polished",
    Formal:   "formal",
} as const;
export type Professionalism = ValueOf<typeof Professionalism>;

/**
 * `professionalism` and `honestAboutAi` (whether the assistant says it is a virtual assistant when asked,
 * instead of staying in character) fall back to the domain pack's defaults when null.
 */
export type AgentStyle = {
    readonly professionalism: Professionalism | null;
    readonly honestAboutAi:   boolean | null;
};

/**
 * How calls avoid paying for dead air, within PLATFORM_BOUNDS.
 * `silenceSeconds` is how long to wait for the caller to speak; `stillTherePrompts` how many times to ask
 * "are you still there?" before hanging up. `hiddenCallerId` is what calls without caller ID get
 * (robocalls usually hide it): the assistant, our voicemail, or no answer.
 */
export type CallSettings = {
    readonly silenceSeconds:    number;
    readonly stillTherePrompts: number;
    readonly hiddenCallerId:    "assistant" | "voicemail" | "reject";
};

export const DEFAULT_CALL_SETTINGS: CallSettings = {
    silenceSeconds:    PLATFORM_BOUNDS.silenceSeconds.default,
    stillTherePrompts: PLATFORM_BOUNDS.stillTherePrompts.default,
    hiddenCallerId:    "assistant",
};

/**
 * Options every tenant can set, regardless of domain.
 * `coldFollowupMinutes` null means the tenant does not send cold follow-ups.
 * `notifications` lists, per notice, how staff want to receive it (email by default).
 *   SMS needs the plan's add-on; without it those notices go by email.
 * `callFallbacks` say what calls get when the assistant cannot take them (our voicemail by default).
 * `textAlwaysOn` keeps web chat and SMS answering outside the call schedule (they cost far less than calls).
 * `limits` and `abuse` have system defaults applied; tenants adjust them from the web app.
 * `languages` are the languages the assistant offers (always including the default language);
 *   customers asking for another are told politely which ones are available.
 */
export type TenantSettings = {
    readonly skipProfileGate:     boolean;
    readonly transferTo:          PhoneNumber | null;
    readonly coldFollowupMinutes: number | null;
    readonly links:               readonly TenantLink[];
    readonly textAlwaysOn:        boolean;
    readonly callFallbacks:       CallFallbacks;
    readonly notifications:       Readonly<Record<StaffNotice, readonly NotifyVia[]>>;
    readonly limits:              SessionLimits;
    readonly abuse:               AbusePolicy;
    readonly recordingDisclosure: RecordingDisclosure;
    readonly languages:           readonly Language[];
    readonly privacy:             PrivacySettings;
    readonly style:               AgentStyle;
    readonly catalog:             CatalogSettings;
    readonly calls:               CallSettings;
};

/**
 * A business and everything core code needs to know about it.
 * `domain` names the domain pack that supplies business-type behavior.
 * `profile` holds the tenant's answers to that domain's questions
 * (e.g. "do you offer financing?"); its keys are defined by the pack, not by core.
 * `options` are the pack's per-tenant switches and values (e.g. `fee_phrase`, `free_estimates`),
 * used by `{setting_<key>}` placeholders and prompt toggles.
 * `agentName` / `voiceAgentName` override the pack's default persona names when set.
 * `defaultLanguage` greets callers the system knows nothing about;
 * customers can switch to any supported language by asking, or just by speaking it.
 * `staffLanguage` is the language transcript gists and staff notices are written in.
 * `billing` is the subscription as the payment provider reports it ("none" until the tenant subscribes).
 * `schedule` is when the assistant answers the calls that reach us (always, unless the tenant narrows it);
 * outside it they get the off-schedule fallback.
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
    readonly options:         Readonly<Record<string, string | number | boolean>>;
    readonly plan:            Plan;
    readonly billing:         BillingState;
    readonly schedule:        ServiceSchedule;
    readonly settings:        TenantSettings;
};

export const Tenant = {
    /**
     * A tenant read back from storage, with settings added since it was saved filled in with their defaults.
     */
    upgrade(stored: Tenant): Tenant {
        const s: Partial<TenantSettings> = stored.settings;
        return {
            ...stored,
            billing:  stored.billing ?? NO_BILLING,
            settings: {
                ...stored.settings,
                languages: s.languages ?? Object.values(Language),
                privacy:   { ...DEFAULT_PRIVACY, ...s.privacy },
                style:     s.style ?? { professionalism: null, honestAboutAi: null },
                catalog:   s.catalog ?? DEFAULT_CATALOG_SETTINGS,
                calls:     {
                    ...DEFAULT_CALL_SETTINGS,
                    ...s.calls,
                    silenceSeconds:    Bound.clamp(PLATFORM_BOUNDS.silenceSeconds, s.calls?.silenceSeconds ?? DEFAULT_CALL_SETTINGS.silenceSeconds),
                    stillTherePrompts: Bound.clamp(PLATFORM_BOUNDS.stillTherePrompts, s.calls?.stillTherePrompts ?? DEFAULT_CALL_SETTINGS.stillTherePrompts),
                },
                limits:    {
                    ...stored.settings.limits,
                    maxSessionSeconds: 60 * Bound.clamp(PLATFORM_BOUNDS.maxSessionMinutes, stored.settings.limits.maxSessionSeconds / 60),
                },
            },
        };
    },

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
            via => via !== "sms" || tenant.plan.subscription.smsNotifications
        );
        return allowed.length > 0
            ? allowed
            : ["email"];
    },

    /**
     * Whether the assistant may speak `language` for this tenant.
     */
    offers(tenant: Tenant, language: Language): boolean {
        return language === tenant.defaultLanguage || tenant.settings.languages.includes(language);
    },

    /**
     * The language to start a conversation in: the customer's last one if it is still offered.
     */
    startLanguage(tenant: Tenant, preferred: Language | null): Language {
        return preferred !== null && Tenant.offers(tenant, preferred)
            ? preferred
            : tenant.defaultLanguage;
    },

    staffPhones(tenant: Tenant): PhoneNumber[] {
        return tenant.staffContacts.flatMap(
			a => a.kind === "phone"
				? [a.value]
				: []
		);
    },
} as const;
