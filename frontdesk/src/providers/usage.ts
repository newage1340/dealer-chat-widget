/**
 * Where usage records go, and how their cost is estimated.
 */

import { z } from "zod";

import { Result, UsageUnit, type UsageRecord } from "../core/index.ts";

export interface UsageSink {
    record(usage: UsageRecord): Promise<void>;
}

/**
 * List prices in USD per unit, keyed by provider then model ("" matches any model of that provider).
 */
export type PriceTable = Readonly<Record<string, Readonly<Record<string, Readonly<Partial<Record<UsageUnit, number>>>>>>>;

/**
 * prices.json: provider -> model -> unit -> USD per unit. Keys starting with "_" are comments.
 */
const PriceFile = z.record(z.string(), z.unknown()).transform(raw =>
    Object.fromEntries(Object.entries(raw).filter(([key]) => !key.startsWith("_")))
).pipe(z.record(z.string(), z.record(z.string(), z.partialRecord(z.enum(UsageUnit), z.number()))));

export const PriceTable = {
    /**
     * A price table from prices.json's contents.
     */
    parse(data: unknown): Result<PriceTable, string> {
        const parsed = PriceFile.safeParse(data);
        return parsed.success
            ? Result.ok(parsed.data)
            : Result.err(parsed.error.message);
    },

    /**
     * Prices for a model: an exact entry, else the longest entry the name starts with
     * (so a pinned snapshot like "gpt-4o-mini-2024-07-18" uses the "gpt-4o-mini" prices), else the provider's "" entry.
     */
    lookup(table: PriceTable, provider: string, model: string): Readonly<Partial<Record<UsageUnit, number>>> | undefined {
        const models = table[provider];
        if (models === undefined) {
            return undefined;
        }
        if (models[model] !== undefined) {
            return models[model];
        }
        const prefix = Object.keys(models)
            .filter(name => name !== "" && model.startsWith(name))
            .sort((a, b) => b.length - a.length)[0];
        return prefix !== undefined
            ? models[prefix]
            : models[""];
    },

    /**
     * Estimated cost of a usage record, or null if any of its units has no price.
     */
    estimate(table: PriceTable, usage: UsageRecord): number | null {
        const prices = PriceTable.lookup(table, usage.provider, usage.model);
        if (prices === undefined) {
            return null;
        }
        let total = 0;
        for (const [unit, amount] of Object.entries(usage.units) as [UsageUnit, number][]) {
            const price = prices[unit];
            if (price === undefined) {
                return null;
            }
            total += price * amount;
        }
        return total;
    },
} as const;
