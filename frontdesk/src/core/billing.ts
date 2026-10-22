/**
 * A tenant's subscription as the payment provider (Stripe) reports it.
 * Tenants that never subscribed ("none": demos, businesses set up by hand) are served normally;
 * only a subscription that lapsed pauses the assistant.
 */

import type { ValueOf } from "./lang.ts";

export const BillingStatus = {
    None:              "none",
    Active:            "active",
    Trialing:          "trialing",
    PastDue:           "past_due",
    Incomplete:        "incomplete",
    IncompleteExpired: "incomplete_expired",
    Unpaid:            "unpaid",
    Canceled:          "canceled",
    Paused:            "paused",
} as const;
export type BillingStatus = ValueOf<typeof BillingStatus>;

/**
 * `currentPeriodEnd` is an ISO date string (tenants are stored as JSON).
 */
export type BillingState = {
    readonly status:           BillingStatus;
    readonly customerId:       string | null;
    readonly subscriptionId:   string | null;
    readonly currentPeriodEnd: string | null;
};

export const NO_BILLING: BillingState = {
    status:           "none",
    customerId:       null,
    subscriptionId:   null,
    currentPeriodEnd: null,
};

export const Billing = {
    /**
     * Whether the assistant answers. Past-due keeps working while the provider retries the card.
     */
    serves(state: BillingState): boolean {
        return !(["unpaid", "canceled", "incomplete_expired", "paused"] as BillingStatus[]).includes(state.status);
    },
} as const;
