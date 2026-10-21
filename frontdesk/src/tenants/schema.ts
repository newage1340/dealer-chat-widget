/**
 * The JSON form of a tenant, as written by hand, imported, or sent by the admin API.
 * Friendlier than the stored Tenant: phone numbers as plain strings, most fields optional with defaults.
 */

import { z } from "zod";

import {
    Address,
    DEFAULT_ABUSE_POLICY,
    DEFAULT_CALL_FALLBACKS,
    DEFAULT_PRICING,
    DEFAULT_SESSION_LIMITS,
    DomainId,
    Language,
    PhoneNumber,
    ServiceSchedule,
    TenantId,
    type LocalizedText,
    type Tenant,
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
const notices = z.array(z.enum(["email", "sms"])).min(1);
const fallback = z.enum(["voicemail", "forward", "reject"]);

const DEFAULT_DISCLOSURE: LocalizedText = {
    en: "Just so you know, this call is recorded and transcribed.",
    es: "Le informamos que esta llamada se graba y se transcribe.",
};

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
        maxSessionMinutes:   z.number().int().min(5).max(120).default(DEFAULT_SESSION_LIMITS.maxSessionSeconds / 60),
        recordingDisclosure: z.boolean().default(false),
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
        },
    };
}

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
