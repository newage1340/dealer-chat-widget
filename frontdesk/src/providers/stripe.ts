/**
 * Stripe over its HTTP API (no SDK): hosted Checkout for subscriptions and top-ups, the customer portal,
 * and webhook signature checks. Prices live in Stripe; we only refer to their ids.
 */

import { createHmac, timingSafeEqual } from "node:crypto";

import { checkStatus, providerFetch } from "./http.ts";

type Params = Readonly<Record<string, unknown>>;

/**
 * Stripe's form encoding for nested objects and arrays: a[b][0][c]=x.
 */
export function stripeForm(params: Params, prefix = ""): URLSearchParams {
    const out = new URLSearchParams();
    const add = (key: string, value: unknown) => {
        if (value === undefined || value === null) {
            return;
        }
        if (Array.isArray(value)) {
            value.forEach((v, i) => add(`${key}[${i}]`, v));
        } else if (typeof value === "object") {
            for (const [k, v] of Object.entries(value as Params)) {
                add(`${key}[${k}]`, v);
            }
        } else {
            out.append(key, String(value));
        }
    };
    for (const [k, v] of Object.entries(params)) {
        add(prefix ? `${prefix}[${k}]` : k, v);
    }
    return out;
}

export type CheckoutLine = {
    readonly price:    string;
    readonly quantity: number;
};

export class StripeClient {
    readonly name = "stripe";
    readonly #secretKey: string;
    readonly #fetch: typeof fetch;

    constructor(secretKey: string, fetchImpl: typeof fetch = globalThis.fetch.bind(globalThis)) {
        this.#secretKey = secretKey;
        this.#fetch = fetchImpl;
    }

    async #post<T>(path: string, params: Params): Promise<T> {
        const response = await providerFetch(this.name, `https://api.stripe.com/v1/${path}`, {
            method:  "POST",
            headers: { Authorization: `Bearer ${this.#secretKey}`, "Content-Type": "application/x-www-form-urlencoded" },
            body:    stripeForm(params).toString(),
        }, { timeoutMs: 20_000 }, this.#fetch);
        const json = await response.json().catch(() => ({})) as T & { error?: { message?: string } };
        checkStatus(this.name, response, json.error?.message);
        return json;
    }

    /**
     * A hosted checkout page for the monthly plan and its add-ons. The tenant id rides along as metadata
     * on both the session and the subscription, so webhooks can find the tenant.
     */
    async subscriptionCheckout(options: {
        readonly tenantId:   string;
        readonly email:      string | null;
        readonly customerId: string | null;
        readonly lines:      readonly CheckoutLine[];
        readonly successUrl: string;
        readonly cancelUrl:  string;
    }): Promise<string>
    {
        const session = await this.#post<{ url: string }>("checkout/sessions", {
            mode:                "subscription",
            line_items:          options.lines.filter(l => l.quantity > 0),
            client_reference_id: options.tenantId,
            metadata:            { tenant_id: options.tenantId, kind: "subscription" },
            subscription_data:   { metadata: { tenant_id: options.tenantId } },
            success_url:         options.successUrl,
            cancel_url:          options.cancelUrl,
            ...(options.customerId !== null ? { customer: options.customerId } : options.email ? { customer_email: options.email } : {}),
        });
        return session.url;
    }

    /**
     * A one-time payment that adds `amountUsd` to this month's allowance.
     */
    async topUpCheckout(options: {
        readonly tenantId:   string;
        readonly customerId: string | null;
        readonly email:      string | null;
        readonly amountUsd:  number;
        readonly successUrl: string;
        readonly cancelUrl:  string;
    }): Promise<string>
    {
        const session = await this.#post<{ url: string }>("checkout/sessions", {
            mode:                "payment",
            line_items:          [{
                quantity:   1,
                price_data: { currency: "usd", unit_amount: Math.round(options.amountUsd * 100), product_data: { name: "Assistant usage top-up" } },
            }],
            client_reference_id: options.tenantId,
            metadata:            { tenant_id: options.tenantId, kind: "topup", amount_usd: String(options.amountUsd) },
            success_url:         options.successUrl,
            cancel_url:          options.cancelUrl,
            ...(options.customerId !== null ? { customer: options.customerId } : options.email ? { customer_email: options.email } : {}),
        });
        return session.url;
    }

    /**
     * Stripe's own page for cards, invoices, plan changes and cancelling.
     */
    async portal(customerId: string, returnUrl: string): Promise<string> {
        const session = await this.#post<{ url: string }>("billing_portal/sessions", { customer: customerId, return_url: returnUrl });
        return session.url;
    }
}

export const StripeWebhook = {
    /**
     * Whether a webhook body really came from Stripe: the Stripe-Signature header carries a timestamp and
     * an HMAC-SHA256 of "<timestamp>.<body>" with the endpoint's signing secret. Old timestamps are refused.
     */
    verify(body: string, header: string, secret: string, now: Date, toleranceSeconds = 300): boolean {
        const parts = Object.fromEntries(header.split(",").map(p => p.split("=") as [string, string]).filter(p => p.length === 2));
        const timestamp = Number(parts["t"]);
        const signatures = header.split(",").filter(p => p.startsWith("v1=")).map(p => p.slice(3));
        if (!secret || !Number.isFinite(timestamp) || signatures.length === 0) {
            return false;
        }
        if (Math.abs(now.getTime() / 1000 - timestamp) > toleranceSeconds) {
            return false;
        }
        const expected = Buffer.from(createHmac("sha256", secret).update(`${timestamp}.${body}`).digest("hex"));
        return signatures.some(s => {
            const given = Buffer.from(s);
            return given.length === expected.length && timingSafeEqual(given, expected);
        });
    },

    sign(body: string, secret: string, now: Date): string {
        const t = Math.floor(now.getTime() / 1000);
        return `t=${t},v1=${createHmac("sha256", secret).update(`${t}.${body}`).digest("hex")}`;
    },
} as const;
