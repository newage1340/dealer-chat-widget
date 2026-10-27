/**
 * Converting the Python version's tenant files (data/tenants/*.json) to the new tenant format.
 */

import { z } from "zod";

import { Problem, Result } from "../core/index.ts";
import type { TenantFile } from "./schema.ts";

const text = z.string().optional();
const texts = z.array(z.string()).optional();

/**
 * The Python version's tenant file. Every field is optional; the new schema decides what is required.
 */
const Legacy = z.object({
    id:          text,
    slug:        text,
    name:        text,
    domain:      text,
    timezone:    text,
    phone:       text,
    address:     text,
    website:     text,
    hours:       text,
    endpoints:   z.object({ sms: texts, voice: texts }).optional(),
    staff:       z.object({ phones: texts, emails: texts }).optional(),
    transfer_to: text,
    branding:    z.object({ color: text, logo_url: text }).optional(),
    profile:     z.record(z.string(), z.string()).optional(),
    settings:    z.record(z.string(), z.union([z.string(), z.number(), z.boolean()])).optional(),
    links:       z.array(z.object({ label: text, url: text, trigger: text })).optional(),
});

/**
 * Whether a tenant file is in the Python version's format (it has fields only that format uses).
 */
export function isLegacy(data: Readonly<Record<string, unknown>>): boolean {
    return "hours" in data
        || "transfer_to" in data
        || "_source" in data;
}

export function fromLegacy(data: unknown): Result<TenantFile, Problem[]> {
    const parsed = Legacy.safeParse(data);
    if (!parsed.success) {
        return Result.err(Problem.fromIssues(parsed.error.issues));
    }
    const d = parsed.data;
    const { cold_followup_minutes: cold, ...options } = d.settings ?? {};
    return Result.ok({
        id:        d.id ?? d.slug ?? "",
        slug:      d.slug ?? d.id ?? "",
        name:      d.name ?? "",
        domain:    d.domain ?? "",
        timezone:  d.timezone ?? "America/Indiana/Indianapolis",
        address:   d.address ?? "",
        website:   d.website ?? "",
        hoursText: d.hours ?? "",
        endpoints: { sms: d.endpoints?.sms ?? [], voice: d.endpoints?.voice ?? [] },
        staff:     { phones: d.staff?.phones ?? [], emails: d.staff?.emails ?? [] },
        branding:  { color: d.branding?.color ?? "", logoUrl: d.branding?.logo_url ?? "" },
        profile:   d.profile ?? {},
        options,
        settings:  {
            ...(typeof cold === "number" && cold > 0 ? { coldFollowupMinutes: cold } : {}),
            ...(d.transfer_to ? { transferTo: d.transfer_to } : {}),
            links: (d.links ?? []).flatMap(l => l.url && l.trigger ? [{ label: l.label ?? "link", url: l.url, trigger: l.trigger }] : []),
        },
        ...(d.phone ? { phone: d.phone } : {}),
    });
}
