/**
 * The values `{placeholder}`s are filled with, from the pack alone or from the pack and the tenant.
 * The field names are listed in domains/template.ts, which is what pack files are checked against;
 * the record types here make the compiler insist every listed field gets a value.
 */

import { PhoneNumber, type Channel, type Tenant } from "../core/index.ts";
import type { DomainPack, PackField, PromptField } from "../domains/index.ts";

/**
 * Any set of placeholder values (templates also get section-specific ones, like `intake_steps`).
 */
export type Values = Readonly<Record<string, string>>;

export type PackValues = Readonly<Record<PackField, string>>;

/**
 * Every prompt field, plus the tenant's `profile_<key>` answers and `setting_<key>` options.
 */
export type TenantValues = Readonly<Record<PromptField, string>> & Values;

export const PromptValues = {
    /**
     * Values from the pack alone: safe in text every tenant of the business type shares.
     */
    pack(pack: DomainPack, channel: Channel): PackValues {
        const voice = channel === "voice";
        return {
            business_noun:    pack.businessNoun,
            agent_role:       voice ? pack.voiceAgentRole : pack.agentRole,
            staff_noun:       pack.staffNoun,
            item_noun:        pack.catalog.itemNoun,
            item_noun_plural: pack.catalog.itemNounPlural,
            spoken_item_noun: pack.catalog.spokenNoun,
            visit_noun:       pack.booking.visitNoun,
            general_subject:  pack.booking.generalSubject,
            subject_label:    pack.booking.subjectLabel,
        };
    },

    /**
     * Values available to every template and domain prompt for this tenant.
     */
    tenant(tenant: Tenant, pack: DomainPack, channel: Channel): TenantValues {
        const voice = channel === "voice";
        const profile = Object.entries(tenant.profile).map(([key, value]) => [`profile_${key}`, value]);
        const options = Object.entries(tenant.options).map(([key, value]) => [`setting_${key}`, String(value)]);
        return {
            ...PromptValues.pack(pack, channel),
            business_name: tenant.name,
            agent_name:    voice
                ? tenant.voiceAgentName || pack.defaultVoiceAgentName
                : tenant.agentName || pack.defaultAgentName,
            phone:         tenant.phone === null
                ? ""
                : PhoneNumber.display(tenant.phone),
            address:       tenant.address,
            website:       tenant.website,
            ...Object.fromEntries(profile),
            ...Object.fromEntries(options),
        };
    },
} as const;
