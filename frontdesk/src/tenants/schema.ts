/**
 * The JSON form of a tenant, as written by hand, imported, or sent by the admin API.
 * Friendlier than the stored Tenant: phone numbers as plain strings, most fields optional with defaults.
 */

import { z } from "zod";

import {
    Address,
    Tenant,
    DEFAULT_ABUSE_POLICY,
    DEFAULT_CALL_FALLBACKS,
    DEFAULT_PRICING,
    DEFAULT_PRIVACY,
    DEFAULT_SESSION_LIMITS,
    DomainId,
    Language,
    PhoneNumber,
    PiiKind,
    PLATFORM_BOUNDS,
    Professionalism,
    ServiceSchedule,
    TenantId,
    type Bound,
    type CatalogSource,
    type LocalizedText,
} from "../core/index.ts";

const LANGUAGES = Object.values(Language) as [Language, ...Language[]];
const phone = z.string().transform((raw, ctx) => {
    const parsed = PhoneNumber.parse(raw);
    if (parsed === null) {
        ctx.addIssue({ code: "custom", message: `not a phone number: '${raw}'` });
        return z.NEVER;
    }
    return parsed;
});
const optionalPhone = z.string().optional().transform((raw, ctx) => {
    if (!raw) {
        return null;
    }
    const parsed = PhoneNumber.parse(raw);
    if (parsed === null) {
        ctx.addIssue({ code: "custom", message: `not a phone number: '${raw}'` });
        return z.NEVER;
    }
    return parsed;
});
const PII_KINDS = Object.values(PiiKind) as [PiiKind, ...PiiKind[]];
const LEVELS = Object.values(Professionalism) as [Professionalism, ...Professionalism[]];
const notices = z.array(z.enum(["email", "sms"])).min(1);
const fallback = z.enum(["voicemail", "forward", "reject"]);

const DEFAULT_DISCLOSURE: LocalizedText = {
    en: "Just so you know, this call is recorded and transcribed.",
    es: "Le informamos que esta llamada se graba y se transcribe.",
};

const refresh = z.number().int().min(5).max(7 * 24 * 60).default(60);
const CatalogSourceFile = z.discriminatedUnion("kind", [
    z.strictObject({ kind: z.literal("manual") }),
    z.strictObject({ kind: z.literal("dataset"), name: z.string().regex(/^[a-z0-9][a-z0-9_.-]*$/, "a file name in datasets/") }),
    z.strictObject({ kind: z.literal("csv_url"), url: z.url(), refreshMinutes: refresh }),
    z.strictObject({ kind: z.literal("json_url"), url: z.url(), itemsPath: z.string().default(""), refreshMinutes: refresh }),
]);

/**
 * A whole number inside a platform bound (see core/bounds.ts), with its default.
 */
function bounded(bound: Bound) {
    return z.number().int().min(bound.min, `at least ${bound.min}`).max(bound.max, `at most ${bound.max}`).default(bound.default);
}

const Window = z.strictObject({
    day:         z.number().int().min(0).max(6),
    startMinute: z.number().int().min(0).max(1440),
    endMinute:   z.number().int().min(0).max(1440),
});

export const TenantFile = z.strictObject({
    id:             z.string().min(1),
    slug:           z.string().regex(/^[a-z0-9][a-z0-9-]*$/, "lowercase letters, digits and -"),
    name:           z.string().min(1),
    domain:         z.string().min(1),
    timezone:       z.string().refine(isTimeZone, "unknown timezone"),
    phone:          optionalPhone,
    address:        z.string().default(""),
    website:        z.string().default(""),
    agentName:      z.string().default(""),
    voiceAgentName: z.string().default(""),
    endpoints:      z.strictObject({
        sms:   z.array(phone).default([]),
        voice: z.array(phone).default([]),
    }).prefault({}),
    staff:          z.strictObject({
        phones: z.array(phone).default([]),
        emails: z.array(z.email()).default([]),
    }).prefault({}),
    branding:       z.strictObject({
        color:   z.string().default(""),
        logoUrl: z.string().default(""),
    }).prefault({}),
    hoursText:       z.string().default(""),
    defaultLanguage: z.enum(LANGUAGES).default("en"),
    staffLanguage:   z.enum(LANGUAGES).default("en"),
    profile:         z.record(z.string(), z.string()).default({}),
    options:         z.record(z.string(), z.union([z.string(), z.number(), z.boolean()])).default({}),
    plan:            z.strictObject({
        extraLines:       z.number().int().min(0).default(0),
        smsNotifications: z.boolean().default(false),
        topUpsUsd:        z.number().min(0).default(0),
    }).prefault({}),
    schedule:        z.union([
        z.literal("always"),
        z.array(Window).min(1),
    ]).default("always"),
    settings:        z.strictObject({
        skipProfileGate:     z.boolean().default(false),
        transferTo:          optionalPhone,
        coldFollowupMinutes: z.number().int().positive().nullable().default(null),
        links:               z.array(z.strictObject({ label: z.string(), url: z.string(), trigger: z.string() })).default([]),
        textAlwaysOn:        z.boolean().default(true),
        callFallbacks:       z.strictObject({ offSchedule: fallback, lineBusy: fallback }).default(DEFAULT_CALL_FALLBACKS),
        notifications:       z.strictObject({
            transcript:   notices.default(["email"]),
            booking:      notices.default(["email"]),
            handoff:      notices.default(["email"]),
            budget_alert: notices.default(["email"]),
        }).prefault({}),
        maxSessionMinutes:   bounded(PLATFORM_BOUNDS.maxSessionMinutes),
        recordingDisclosure: z.boolean().default(false),
        languages:           z.array(z.enum(LANGUAGES)).min(1).default(LANGUAGES),
        privacy:             z.strictObject({
            redact:    z.array(z.enum(PII_KINDS)).default([...DEFAULT_PRIVACY.redact]),
            sensitive:     z.boolean().default(false),
            crisisSupport: z.boolean().default(false),
        }).prefault({}),
        style:               z.strictObject({
            professionalism: z.enum(LEVELS).nullable().default(null),
            honestAboutAi:   z.boolean().nullable().default(null),
        }).prefault({}),
        calls:               z.strictObject({
            silenceSeconds:    bounded(PLATFORM_BOUNDS.silenceSeconds),
            stillTherePrompts: bounded(PLATFORM_BOUNDS.stillTherePrompts),
            hiddenCallerId:    z.enum(["assistant", "voicemail", "reject"]).default("assistant"),
        }).prefault({}),
        catalog:             z.strictObject({
            source:   CatalogSourceFile.default({ kind: "manual" }),
            fieldMap: z.record(z.string(), z.string()).default({}),
        }).prefault({}),
    }).prefault({}),
});
export type TenantFile = z.input<typeof TenantFile>;

function isTimeZone(zone: string): boolean {
    try {
        new Intl.DateTimeFormat("en-US", { timeZone: zone });
        return true;
    } catch {
        return false;
    }
}

export function toTenant(file: z.output<typeof TenantFile>): Tenant {
    const s = file.settings;
    return {
        id:              TenantId(file.id),
        slug:            file.slug,
        name:            file.name,
        domain:          DomainId(file.domain),
        timezone:        file.timezone,
        phone:           file.phone,
        address:         file.address,
        website:         file.website,
        agentName:       file.agentName,
        voiceAgentName:  file.voiceAgentName,
        endpoints:       {
            sms:   file.endpoints.sms.map(value => ({ kind: "phone" as const, value })),
            voice: file.endpoints.voice.map(value => ({ kind: "phone" as const, value })),
        },
        staffContacts:   [
            ...file.staff.phones.map(value => ({ kind: "phone" as const, value })),
            ...file.staff.emails.flatMap(e => {
                const a = Address.email(e);
                return a === null
                    ? []
                    : [a];
            }),
        ],
        branding:        file.branding,
        hoursText:       file.hoursText,
        defaultLanguage: file.defaultLanguage,
        staffLanguage:   file.staffLanguage,
        profile:         file.profile,
        options:         file.options,
        plan:            { pricing: DEFAULT_PRICING, subscription: file.plan },
        schedule:        file.schedule === "always"
            ? ServiceSchedule.always
            : { kind: "windows", windows: file.schedule.map(w => ({ ...w, day: w.day as 0 | 1 | 2 | 3 | 4 | 5 | 6 })) },
        settings:        {
            skipProfileGate:     s.skipProfileGate,
            transferTo:          s.transferTo,
            coldFollowupMinutes: s.coldFollowupMinutes,
            links:               s.links,
            textAlwaysOn:        s.textAlwaysOn,
            callFallbacks:       s.callFallbacks,
            notifications:       s.notifications,
            limits:              { ...DEFAULT_SESSION_LIMITS, maxSessionSeconds: s.maxSessionMinutes * 60 },
            abuse:               DEFAULT_ABUSE_POLICY,
            recordingDisclosure: { enabled: s.recordingDisclosure, text: DEFAULT_DISCLOSURE },
            languages:           [...new Set([file.defaultLanguage, ...s.languages])],
            privacy:             s.privacy,
            style:               s.style,
            catalog:             s.catalog,
            calls:               s.calls,
        },
    };
}

/**
 * The file form of a stored tenant: what the settings page edits and sends back through `parseTenant`.
 * Settings without a file form (abuse policy, pricing, disclosure wording) keep their defaults.
 */
export function fromTenant(tenant: Tenant) {
    const s = tenant.settings;
    const phones = (channel: "sms" | "voice") => (tenant.endpoints[channel] ?? []).map(a => a.value);
    return {
        id:              tenant.id,
        slug:            tenant.slug,
        name:            tenant.name,
        domain:          tenant.domain,
        timezone:        tenant.timezone,
        phone:           tenant.phone ?? "",
        address:         tenant.address,
        website:         tenant.website,
        agentName:       tenant.agentName,
        voiceAgentName:  tenant.voiceAgentName,
        endpoints:       { sms: phones("sms"), voice: phones("voice") },
        staff:           {
            phones: Tenant.staffPhones(tenant),
            emails: tenant.staffContacts.flatMap(a => a.kind === "email" ? [a.value] : []),
        },
        branding:        { ...tenant.branding },
        hoursText:       tenant.hoursText,
        defaultLanguage: tenant.defaultLanguage,
        staffLanguage:   tenant.staffLanguage,
        profile:         { ...tenant.profile },
        options:         { ...tenant.options },
        plan:            { ...tenant.plan.subscription },
        schedule:        tenant.schedule.kind === "always"
            ? "always"
            : tenant.schedule.windows.map(w => ({ ...w })),
        settings:        {
            skipProfileGate:     s.skipProfileGate,
            transferTo:          s.transferTo ?? "",
            coldFollowupMinutes: s.coldFollowupMinutes,
            links:               s.links.map(l => ({ ...l })),
            textAlwaysOn:        s.textAlwaysOn,
            callFallbacks:       { ...s.callFallbacks },
            notifications:       {
                transcript:   [...s.notifications.transcript],
                booking:      [...s.notifications.booking],
                handoff:      [...s.notifications.handoff],
                budget_alert: [...s.notifications.budget_alert],
            },
            maxSessionMinutes:   Math.round(s.limits.maxSessionSeconds / 60),
            recordingDisclosure: s.recordingDisclosure.enabled,
            languages:           [...s.languages],
            privacy:             { redact: [...s.privacy.redact], sensitive: s.privacy.sensitive, crisisSupport: s.privacy.crisisSupport },
            style:               { ...s.style },
            catalog:             { source: { ...s.catalog.source } as CatalogSource, fieldMap: { ...s.catalog.fieldMap } },
            calls:               { ...s.calls },
        },
    } satisfies z.input<typeof TenantFile>;
}

/**
 * A tenant in its editable file form.
 */
export type TenantDraft = ReturnType<typeof fromTenant>;

export type TenantProblem = {
    readonly path:    string;
    readonly message: string;
};

export function parseTenant(data: unknown): { ok: true; tenant: Tenant } | { ok: false; problems: TenantProblem[] } {
    const parsed = TenantFile.safeParse(data);
    if (!parsed.success) {
        return {
            ok:       false,
            problems: parsed.error.issues.map(i => ({ path: i.path.join("."), message: i.message })),
        };
    }
    return { ok: true, tenant: toTenant(parsed.data) };
}
