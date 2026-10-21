/**
 * Where usage records go, and how their cost is estimated.
 */

import type { UsageRecord, UsageUnit } from "../core/index.ts";

export interface UsageSink {
    record(usage: UsageRecord): Promise<void>;
}

/**
 * List prices in USD per unit, keyed by provider then model.
 */
export type PriceTable = Readonly<Record<string, Readonly<Record<string, Readonly<Partial<Record<UsageUnit, number>>>>>>>;

export const PriceTable = {
    /**
     * Estimated cost of a usage record, or null if any of its units has no price.
     */
    estimate(table: PriceTable, usage: UsageRecord): number | null {
        const prices = table[usage.provider]?.[usage.model];
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
