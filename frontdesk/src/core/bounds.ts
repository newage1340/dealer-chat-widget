/**
 * Limits the platform owner sets on what a tenant may choose, so no business can configure itself
 * into calls that cost more than its plan pays for. The settings page and the tenant schema both enforce them.
 * Change them here; every tenant's saved value is clamped into the new range when it is read.
 */

export type Bound = {
    readonly min:     number;
    readonly max:     number;
    readonly default: number;
};

export const PLATFORM_BOUNDS = {
    /**
     * Longest conversation before the assistant wraps up politely.
     */
    maxSessionMinutes: { min: 5, max: 45, default: 30 },
    /**
     * How long a call waits for the caller to start speaking before it counts as silence.
     */
    silenceSeconds:    { min: 3, max: 8, default: 5 },
    /**
     * "Are you still there?" prompts before a silent call is ended.
     */
    stillTherePrompts: { min: 0, max: 2, default: 1 },
} as const satisfies Readonly<Record<string, Bound>>;

export const Bound = {
    clamp(bound: Bound, value: number): number {
        return Number.isFinite(value)
            ? Math.min(bound.max, Math.max(bound.min, Math.round(value)))
            : bound.default;
    },
} as const;
