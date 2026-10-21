/**
 * Checks on a saved tenant for setups that are valid but won't work well: things to fix before going live.
 * Shown on the settings page and by `pnpm check-demo`.
 */

import { Tenant, type CatalogItem } from "../core/index.ts";
import type { DomainPack } from "../domains/index.ts";
import { Hours } from "../prompts/hours.ts";
import { parsePrice } from "../tools/index.ts";

export function lintTenant(tenant: Tenant, pack: DomainPack, items: readonly CatalogItem[]): string[] {
    const warnings: string[] = [];
    const days = Object.keys(Hours.parse(tenant.hoursText));
    if (days.length < 7) {
        warnings.push(tenant.hoursText
            ? `The hours only cover ${days.length} of 7 days, so the assistant can't tell when you're open the rest. Write them like "Monday-Friday: 9am to 6pm, Saturday: 10am to 2pm, Sunday: closed".`
            : "No hours are set, so the assistant can't say when you're open or check booking times.");
    }
    for (const q of pack.profile.filter(q => q.required && !tenant.profile[q.key]?.trim())) {
        warnings.push(`"${q.label}" is not answered; the assistant will say it doesn't know.`);
    }
    if (tenant.phone === null) {
        warnings.push("No main phone number, so the assistant can't give customers a number to call.");
    }
    if ((tenant.endpoints.voice ?? []).length > 0 && tenant.settings.transferTo === null && tenant.phone === null) {
        warnings.push("Calls can't be transferred to a person: set a transfer number.");
    }
    if (!tenant.staffContacts.some(a => a.kind === "email")) {
        warnings.push("No staff email, so transcripts, bookings and callback requests go nowhere.");
    }
    if (items.length === 0) {
        warnings.push(`No ${pack.catalog.itemNounPlural} listed, so the assistant can only answer from the business details.`);
    }
    const field = pack.catalog.priceField;
    if (field !== null) {
        const spec = pack.catalog.fields.find(f => f.name === field);
        const unreadable = items.filter(i => {
            const value = String(i.fields[field] ?? "").trim();
            return value !== "" && parsePrice(value) === null;
        });
        if (unreadable.length > 0) {
            warnings.push(`${unreadable.length} ${pack.catalog.itemNounPlural} have a ${spec?.label || field} that isn't a dollar amount (e.g. "${String(unreadable[0]!.fields[field])}"), so totals skip them.`);
        }
    }
    if (!Tenant.offers(tenant, tenant.staffLanguage) && tenant.staffLanguage !== "en") {
        warnings.push("Summaries are written in a language customers aren't offered; that's fine, just make sure it's intended.");
    }
    return warnings;
}
