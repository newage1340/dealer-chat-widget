/**
 * Prices as catalogs write them ("$15.95", "$129-$249", "from $45", "$450+", "Free"),
 * parsed so totals are computed, never estimated.
 */

/**
 * `minimum` marks prices that are a starting point ("from $45", "$450+").
 */
export type PriceRange = {
    readonly low:     number;
    readonly high:    number;
    readonly minimum: boolean;
};

export const PriceRange = {
    /**
     * The dollar amounts in a price field, or null for text like "Quoted on site".
     */
    parse(text: string): PriceRange | null {
        const amounts = (text.match(/\d[\d,]*(?:\.\d+)?/g) ?? []).map(n => Number(n.replaceAll(",", "")));
        if (amounts.length === 0 && /\b(free|gratis|no charge|complimentary|included)\b/i.test(text)) {
            return { low: 0, high: 0, minimum: false };
        }
        const [low, high] = amounts;
        if (low === undefined || amounts.length > 2) {
            return null;
        }
        const minimum = /\b(from|starting|desde|at least|and up)\b|\+\s*$/i.test(text.trim());
        return { low, high: high ?? low, minimum };
    },

    /**
     * "$15", "$129-$249", "from $45"
     */
    text(range: PriceRange): string {
        const base = range.low === range.high
            ? dollars(range.low)
            : `${dollars(range.low)}-${dollars(range.high)}`;
        return range.minimum
            ? `from ${base}`
            : base;
    },
} as const;

function dollars(amount: number): string {
    return Number.isInteger(amount)
        ? `$${amount.toLocaleString("en-US")}`
        : `$${amount.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}
