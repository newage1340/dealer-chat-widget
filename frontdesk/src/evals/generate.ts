/**
 * Eval scenarios written from each demo tenant's own data, so every pack gets the same checks
 * without hand-writing them: a price from its catalog, an item it doesn't have, a closed day,
 * a prank, and switching into and back out of its second language.
 * A change to the shared prompts that breaks one business type shows up here.
 */

import { CatalogItem, Language, LANGUAGE_NAMES, Tenant, type Tenant as TenantType } from "../core/index.ts";
import { Catalog, PriceRange, type DomainRegistry } from "../domains/index.ts";
import { Hours } from "../prompts/hours.ts";
import type { Store } from "../storage/index.ts";
import type { Scenario } from "./run.ts";

/**
 * A greeting and a question per language, for the language-switch check.
 */
const HELLO: Readonly<Partial<Record<Language, string>>> = {
    es: "Hola, ¿qué servicios tienen y cuánto cuestan?",
    zh: "你好，你们有什么服务？价格是多少？",
    vi: "Xin chào, bạn có những dịch vụ gì và giá bao nhiêu?",
    fr: "Bonjour, quels services proposez-vous et à quel prix?",
    pt: "Olá, quais serviços vocês têm e quanto custam?",
    tl: "Kumusta po, anong mga serbisyo ang meron kayo at magkano po?",
    ko: "안녕하세요, 어떤 서비스가 있고 가격은 얼마인가요?",
};

function priceDigits(amount: number): string {
    return Number.isInteger(amount)
        ? String(amount)
        : amount.toFixed(2);
}

/**
 * `foreign` is a real item from another business type's catalog: plausible to ask for, but not offered here.
 */
function scenarios(tenant: TenantType, items: readonly CatalogItem[], registry: DomainRegistry, foreign: string): Scenario[] {
    const pack = registry.get(tenant.domain);
    const spec = pack.catalog;
    const name = (what: string) => `${pack.id} (generated): ${what}`;
    const base = { tenant: tenant.slug, channel: "sms" as const, profile: { firstName: "Sam" }, privacy: {} };
    const expect = (e: Partial<Scenario["turns"][number]["expect"]>) => ({ events: [], noEvents: [], replyIncludes: [], replyExcludes: [], ...e });
    const out: Scenario[] = [];

    // A price straight from the catalog.
    const field = spec.priceField;
    const priced = field === null
        ? undefined
        : items.find(i => {
            const p = PriceRange.parse(CatalogItem.text(i, field));
            return p !== null && p.low > 0 && !p.minimum;
        });
    if (priced !== undefined && field !== null) {
        const p = PriceRange.parse(CatalogItem.text(priced, field))!;
        const digits = priceDigits(p.low).replace(/\B(?=(\d{3})+(?!\d))/g, ",?");
        out.push({ ...base, name: name("quotes a catalog price"), turns: [
            { say: `How much is the ${Catalog.title(priced, spec)}?`, expect: expect({ replyMatches: digits }) },
        ] });
    }

    // Something the business doesn't have: never claim it, and don't book it.
    out.push({ ...base, name: name("says so when something isn't offered"), turns: [
        { say: `Hi, do you offer ${foreign}? How much is it?`, expect: expect({
            replyMatches:  "(don't|do not|doesn't|does not|not (currently )?(offer|have|carry|list|provide)|unfortunately|only|specializ|focus|no tenemos|no ofrecemos)",
            replyExcludes: ["yes, we", "yes we", "sí, tenemos", "sí, ofrecemos"],
            noEvents:      ["appointment_booked"],
        }) },
    ] });

    // A closed day is never offered.
    const hours = Hours.parse(tenant.hoursText);
    const closed = Object.entries(hours).find(([, h]) => h !== undefined && Hours.isClosed(h))?.[0];
    if (closed !== undefined) {
        out.push({ ...base, name: name(`won't book a closed ${closed}`), turns: [
            { say: `Can I come in this ${closed} at 11am?`, expect: expect({ replyMatches: "closed|not open|aren't open|don't open|cerrad|no abrimos", noEvents: ["appointment_booked"] }) },
        ] });
    }

    // A prank gets flagged, politely.
    out.push({ ...base, name: name("flags a prank"), turns: [
        { say: "lol are you a potato? my cat wants to order a moon pizza", expect: expect({ events: ["turn_flagged"], noEvents: ["appointment_booked"] }) },
    ] });

    // Into the second language and back out, without the first one lingering.
    const second = tenant.settings.languages.find(l => l !== tenant.defaultLanguage && Tenant.offers(tenant, l) && HELLO[l] !== undefined);
    if (second !== undefined && tenant.defaultLanguage === "en") {
        out.push({ ...base, name: name(`switches to ${LANGUAGE_NAMES[second]} and back`), turns: [
            { say: HELLO[second]!, expect: expect({ language: second }) },
            { say: "Sorry, can we switch back to English please? What are your hours?", expect: expect({ language: "en" }) },
        ] });
    }
    return out;
}

/**
 * Generated scenarios for every demo tenant in the store (slugs starting "demo-"), one pack each.
 */
export async function generatedScenarios(store: Store, registry: DomainRegistry): Promise<Scenario[]> {
    const demos: { tenant: TenantType; items: readonly CatalogItem[] }[] = [];
    for (const tenant of await store.tenants.all()) {
        if (tenant.slug.startsWith("demo-") && !demos.some(d => d.tenant.domain === tenant.domain) && registry.ids().includes(tenant.domain)) {
            demos.push({ tenant, items: await store.tenants.catalog(tenant.id) });
        }
    }
    return demos.flatMap((d, i) => {
        // Borrow an item from the next business type over, e.g. a brake job for the nail salon.
        // Skip types whose items are addresses (real estate listings): "do you offer 3410 Weston Pointe Dr?" isn't a question.
        const others = [...demos.slice(i + 1), ...demos.slice(0, i)]
            .filter(o => o.items.some(x => !/^\d/.test(Catalog.title(x, registry.get(o.tenant.domain).catalog))));
        const other = others[0] ?? d;
        const candidates = other.items.filter(x => !/^\d/.test(Catalog.title(x, registry.get(other.tenant.domain).catalog)));
        const item = candidates[Math.floor(candidates.length / 2)];
        const foreign = item === undefined || other === d
            ? "scuba diving lessons"
            : Catalog.title(item, registry.get(other.tenant.domain).catalog).toLowerCase();
        return scenarios(d.tenant, d.items, registry, foreign);
    });
}
