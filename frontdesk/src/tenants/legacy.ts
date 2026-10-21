/**
 * Converting the Python version's tenant files (data/tenants/*.json) to the new tenant format.
 */

import type { TenantFile } from "./schema.ts";

type Legacy = {
    readonly id?:          string;
    readonly slug?:        string;
    readonly name?:        string;
    readonly domain?:      string;
    readonly timezone?:    string;
    readonly phone?:       string;
    readonly address?:     string;
    readonly website?:     string;
    readonly hours?:       string;
    readonly endpoints?:   { readonly sms?: readonly string[]; readonly voice?: readonly string[] };
    readonly staff?:       { readonly phones?: readonly string[]; readonly emails?: readonly string[] };
    readonly transfer_to?: string;
    readonly branding?:    { readonly color?: string; readonly logo_url?: string };
    readonly profile?:     Readonly<Record<string, string>>;
    readonly settings?:    Readonly<Record<string, string | number | boolean>>;
    readonly links?:       readonly { readonly label?: string; readonly url?: string; readonly trigger?: string }[];
    readonly catalog?:     readonly Record<string, unknown>[];
};

export function fromLegacy(data: unknown): { tenant: TenantFile; catalog: readonly Record<string, unknown>[] } {
    const d = data as Legacy;
    const { cold_followup_minutes: cold, ...options } = d.settings ?? {};
    const tenant: TenantFile = {
        id:        d.id ?? d.slug ?? "",
        slug:      d.slug ?? d.id ?? "",
        name:      d.name ?? "",
        domain:    d.domain ?? "",
        timezone:  d.timezone ?? "America/Indiana/Indianapolis",
        address:   d.address ?? "",
        website:   d.website ?? "",
        hoursText: d.hours ?? "",
        endpoints: { sms: [...(d.endpoints?.sms ?? [])], voice: [...(d.endpoints?.voice ?? [])] },
        staff:     { phones: [...(d.staff?.phones ?? [])], emails: [...(d.staff?.emails ?? [])] },
        branding:  { color: d.branding?.color ?? "", logoUrl: d.branding?.logo_url ?? "" },
        profile:   { ...(d.profile ?? {}) },
        options,
        settings:  {
            ...(typeof cold === "number" && cold > 0 ? { coldFollowupMinutes: cold } : {}),
            ...(d.transfer_to ? { transferTo: d.transfer_to } : {}),
            links: (d.links ?? []).flatMap(l => l.url && l.trigger ? [{ label: l.label ?? "link", url: l.url, trigger: l.trigger }] : []),
        },
        ...(d.phone ? { phone: d.phone } : {}),
    };
    return { tenant, catalog: d.catalog ?? [] };
}
