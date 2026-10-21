/**
 * What a tenant pays for, and the most we will spend serving them.
 *
 * The rule that keeps the business out of the red: a tenant's service costs
 * (models, speech, telephony, texts) may never exceed a fixed share of what they paid this month.
 * Everything they buy (extra lines, add-ons, top-ups) raises the allowance in proportion,
 * so more service is always paid for before it is delivered.
 * There is no limit on hours: the assistant answers around the clock until the allowance runs out,
 * and after that callers can still leave a voicemail.
 */

/**
 * Our price list. Comes from configuration, not from the tenant.
 * `costShare` is the largest fraction of a tenant's monthly payment their service costs may use;
 * the rest covers payment fees, hosting and profit.
 * `voicemailReserveShare` of that allowance is held back from the assistant,
 * so callers can still leave a voicemail after the assistant's share is used up.
 */
export type PlanPricing = {
    readonly basePriceUsd:          number;
    readonly extraLineUsd:          number;
    readonly smsNotificationsUsd:   number;
    readonly costShare:             number;
    readonly voicemailReserveShare: number;
};

/**
 * What one tenant has bought for the current billing month.
 * `extraLines` allow simultaneous AI calls beyond the first.
 * `topUpsUsd` are one-off purchases this month that extend the allowance.
 */
export type Subscription = {
    readonly extraLines:       number;
    readonly smsNotifications: boolean;
    readonly topUpsUsd:        number;
};

export type Plan = {
    readonly pricing:      PlanPricing;
    readonly subscription: Subscription;
};

export const DEFAULT_PRICING: PlanPricing = {
    basePriceUsd:          99,
    extraLineUsd:          25,
    smsNotificationsUsd:   10,
    costShare:             0.4,
    voicemailReserveShare: 0.05,
};

export const Plan = {
    monthlyRevenueUsd(plan: Plan): number {
        const p = plan.pricing;
        const s = plan.subscription;
        return p.basePriceUsd
            + s.extraLines * p.extraLineUsd
            + (s.smsNotifications ? p.smsNotificationsUsd : 0)
            + s.topUpsUsd;
    },

    /**
     * The most this tenant's service may cost us this month.
     */
    costCapUsd(plan: Plan): number {
        return Plan.monthlyRevenueUsd(plan) * plan.pricing.costShare;
    },

    /**
     * The part of the cost cap the assistant itself may use; the rest is the voicemail reserve.
     */
    assistantCapUsd(plan: Plan): number {
        return Plan.costCapUsd(plan) * (1 - plan.pricing.voicemailReserveShare);
    },

    maxConcurrentCalls(plan: Plan): number {
        return 1 + plan.subscription.extraLines;
    },
} as const;
