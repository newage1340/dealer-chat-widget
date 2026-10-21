/**
 * Catalog formatting and matching, driven entirely by a pack's CatalogSpec.
 */

import {
    CatalogItem,
    CatalogItemId,
    type FieldSpec,
    type FieldValue,
    type TenantId,
} from "../core/index.ts";
import type { CatalogSpec, WidgetTopic } from "./pack.ts";
import { Template } from "./template.ts";

const WORD_RE = /[a-z0-9]+/g;
const STOPWORDS: ReadonlySet<string> = new Set(
    ("a an and any are at be can do does for from have how i in is it me my of on or "
        + "the that this to want what when which with you your we our got there").split(" ")
);

function fieldSpec(spec: CatalogSpec, name: string): FieldSpec | undefined {
    return spec.fields.find(f => f.name === name);
}

/**
 * Crude plural folding so "SUVs" matches "SUV" and "trucks" matches "truck".
 */
function stem(word: string): string {
    return word.length > 3 && word.endsWith("s") && !word.endsWith("ss")
        ? word.slice(0, -1)
        : word;
}

function words(text: string): Set<string> {
    return new Set(
        (text.toLowerCase().match(WORD_RE) ?? [])
            .filter(w => !STOPWORDS.has(w))
            .map(stem)
    );
}

function overlap(a: Set<string>, b: Set<string>): number {
    let n = 0;
    for (const w of a) {
        if (b.has(w)) {
            n++;
        }
    }
    return n;
}

export const Catalog = {
    /**
     * Map a raw source row (scraped or hand-written) onto the pack's field names.
     */
    normalize(
        raw: Readonly<Record<string, unknown>>,
        spec: CatalogSpec,
        tenantId: TenantId,
        index: number,
        now: Date
    ): CatalogItem
    {
        const names = new Set(spec.fields.map(f => f.name));
        const fields: Record<string, FieldValue> = {};
        for (const [key, value] of Object.entries(raw)) {
            const k = key.trim().toLowerCase();
            const target = spec.aliases[k] ?? (names.has(k) ? k : undefined);
            if (target === undefined || value === null || value === undefined || value === "") {
                continue;
            }
            fields[target] = typeof value === "number"
                ? value
                : String(value).trim();
        }
        const id = spec.idField !== null && fields[spec.idField] !== undefined
            ? String(fields[spec.idField])
            : `item-${index}`;
        return { id: CatalogItemId(id), tenantId, fields, updatedAt: now };
    },

    formatValue(spec: FieldSpec, value: FieldValue | undefined): string {
        const text = value === null || value === undefined
            ? ""
            : String(value).trim();
        if (!text) {
            return spec.emptyText;
        }
        if (spec.type === "money") {
            const amount = Number(text.replace(/[^\d.]/g, ""));
            if (!Number.isFinite(amount) || text.replace(/[^\d.]/g, "") === "") {
                return text;
            }
            return Number.isInteger(amount)
                ? `$${amount.toLocaleString("en-US")}`
                : `$${amount.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
        }
        if (spec.type === "integer" && /^\d+$/.test(text)) {
            return text.length > 4
                ? Number(text).toLocaleString("en-US")
                : text;
        }
        return text;
    },

    title(item: CatalogItem, spec: CatalogSpec): string {
        const values = Object.fromEntries(spec.fields.map(f => [f.name, CatalogItem.text(item, f.name)]));
        const title = Template.render(spec.titleTemplate, values).split(/\s+/).filter(Boolean).join(" ");
        return title || spec.itemNoun.charAt(0).toUpperCase() + spec.itemNoun.slice(1);
    },

    /**
     * One summary template, or "" when every field it references is blank.
     */
    renderPart(part: string, item: CatalogItem, spec: CatalogSpec): string {
        const names = Template.fields(part);
        if (!names.ok) {
            return "";
        }
        const values: Record<string, string> = {};
        let anyValue = false;
        for (const name of names.value) {
            const fs = fieldSpec(spec, name);
            const rendered = fs !== undefined
                ? Catalog.formatValue(fs, item.fields[name])
                : CatalogItem.text(item, name);
            anyValue = anyValue || rendered !== "";
            values[name] = rendered;
        }
        if (names.value.length > 0 && !anyValue) {
            return "";
        }
        return Template.render(part, values).trim();
    },

    summary(item: CatalogItem, spec: CatalogSpec): string {
        let line = Catalog.title(item, spec);
        const parts = spec.summaryParts.map(p => Catalog.renderPart(p, item, spec)).filter(Boolean);
        if (parts.length > 0) {
            line += ` (${parts.join(", ")})`;
        }
        const suffix = spec.summarySuffix
            ? Catalog.renderPart(spec.summarySuffix, item, spec)
            : "";
        return `${line} ${suffix}`.trim();
    },

    details(item: CatalogItem, spec: CatalogSpec): string {
        const lines = [Catalog.title(item, spec)];
        for (const fs of spec.fields) {
            if (!fs.inDetails) {
                continue;
            }
            const value = Catalog.formatValue(fs, item.fields[fs.name]);
            if (value) {
                lines.push(`${fs.label || fs.name}: ${value}`);
            }
        }
        return lines.join("\n");
    },

    overview(items: readonly CatalogItem[], spec: CatalogSpec, limit = 200): string {
        if (items.length === 0) {
            return spec.emptyText;
        }
        const lines = items.slice(0, limit).map(i => Catalog.summary(i, spec));
        if (items.length > limit) {
            lines.push(`(+${items.length - limit} more ${spec.itemNounPlural} not shown)`);
        }
        return lines.join("\n");
    },

    /**
     * Items whose searchable fields share the most words with `text`, best first.
     * Title words count double, so naming an item beats a stray feature word.
     */
    match(items: readonly CatalogItem[], text: string, spec: CatalogSpec, topK = 3): CatalogItem[] {
        const query = words(text);
        if (query.size === 0) {
            return [];
        }
        const searchable = spec.fields.filter(f => f.searchable);
        const scored: { score: number; index: number; item: CatalogItem }[] = [];
        items.forEach((item, index) => {
            const titleWords = words(Catalog.title(item, spec));
            const fieldWords = words(searchable.map(f => CatalogItem.text(item, f.name)).join(" "));
            const score = 2 * overlap(query, titleWords) + overlap(query, fieldWords);
            if (score > 0) {
                scored.push({ score, index, item });
            }
        });
        scored.sort((a, b) => b.score - a.score || a.index - b.index);
        return scored.slice(0, topK).map(s => s.item);
    },

    /**
     * The item the conversation is focused on:
     * the first text (newest first) that names exactly one item's title words.
     */
    anchor(textsNewestFirst: readonly string[], items: readonly CatalogItem[], spec: CatalogSpec): CatalogItem | null {
        const titled = items.map(item => ({ item, title: words(Catalog.title(item, spec)) }));
        for (const text of textsNewestFirst) {
            const said = words(text);
            const hits = titled.filter(
                t => t.title.size > 0
                    && overlap(t.title, said) >= Math.max(2, t.title.size - 1)
            );
            if (hits.length === 1) {
                return hits[0]!.item;
            }
        }
        return null;
    },

    /**
     * Whether a widget topic applies to this tenant's catalog.
     */
    topicVisible(topic: WidgetTopic, items: readonly CatalogItem[]): boolean {
        const field = topic.requiresField;
        if (field === null) {
            return true;
        }
        if (topic.requiresAny.length === 0) {
            return items.some(item => CatalogItem.text(item, field) !== "");
        }
        const needles = topic.requiresAny.map(n => n.toLowerCase());
        return items.some(item => {
            const value = CatalogItem.text(item, field).toLowerCase();
            return needles.some(n => value.includes(n));
        });
    },
} as const;
