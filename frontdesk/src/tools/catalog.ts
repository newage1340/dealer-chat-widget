/**
 * Searching the tenant's catalog and pricing what the customer wants from it.
 */

import { z } from "zod";

import { Catalog, PriceRange } from "../domains/index.ts";
import { Tool, ToolResult, type TurnContext } from "../turn/index.ts";

export function searchCatalog(ctx: TurnContext): Tool {
    const spec = ctx.pack.catalog;
    return Tool.define({
        name:        "search_catalog",
        description: `Find ${spec.itemNounPlural} matching what the customer described.`,
        followUp:    true,
        parameters:  z.object({ query: z.string().describe("What they're looking for, in a few words") }),
        run: args => {
            const matches = Catalog.match(ctx.catalog, args.query, spec, 5);
            return ToolResult.output(matches.length > 0
                ? { matches: matches.map(i => Catalog.details(i, spec)) }
                : { matches: [], note: `No ${spec.itemNounPlural} match that. Tell the customer we don't currently have it.` });
        },
    });
}

/**
 * Adds up what the customer wants from the catalog's own prices, so totals are computed, never estimated.
 */
export function priceItems(ctx: TurnContext): Tool {
    const spec = ctx.pack.catalog;
    return Tool.define({
        name:        "price_items",
        promise:     /\b(total (is|comes to|would be|will be)|(that'?ll|that will|it'?ll) (be|come to|run) (about |around )?\$)/i,
        description: `Add up the prices of ${spec.itemNounPlural} the customer wants (with quantities). Call it before saying any total; repeat its numbers exactly.`,
        followUp:    true,
        parameters:  z.object({
            items: z.array(z.object({
                item:     z.string().describe(`The ${spec.itemNoun}, by name as listed`),
                quantity: z.number().int().min(1).max(99).default(1),
            })).min(1).max(30),
        }),
        run: args => {
            if (spec.priceField === null) {
                return ToolResult.error(`This business doesn't list prices for its ${spec.itemNounPlural}.`);
            }
            const field = spec.priceField;
            const lines: string[] = [];
            const notFound: string[] = [];
            const unpriced: string[] = [];
            let low = 0;
            let high = 0;
            let minimum = false;
            for (const want of args.items) {
                const item = Catalog.match(ctx.catalog, want.item, spec, 1)[0];
                if (item === undefined) {
                    notFound.push(want.item);
                    continue;
                }
                const title = Catalog.title(item, spec);
                const price = PriceRange.parse(String(item.fields[field] ?? ""));
                if (price === null) {
                    unpriced.push(`${title} (${String(item.fields[field] ?? "") || "no price listed"})`);
                    continue;
                }
                const line = { low: price.low * want.quantity, high: price.high * want.quantity, minimum: price.minimum };
                low += line.low;
                high += line.high;
                minimum = minimum || price.minimum;
                lines.push(`${want.quantity} x ${title} at ${PriceRange.text(price)} = ${PriceRange.text(line)}`);
            }
            return ToolResult.output({
                lines,
                subtotal:  lines.length > 0 ? PriceRange.text({ low, high, minimum }) : null,
                not_found: notFound,
                unpriced,
                note:      "Prices are as listed, before tax and fees. Say the subtotal exactly; mention anything not found or unpriced.",
            });
        },
    });
}
