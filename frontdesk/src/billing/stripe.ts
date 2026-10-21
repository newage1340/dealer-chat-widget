/**
 * What Stripe's webhook events mean for a tenant: subscription status and add-ons, and top-ups.
 * Each event is applied once (Stripe retries deliveries), and nothing here trusts the request
 * without a valid signature (see http/billing.ts).
 */

import { BillingStatus, LocalTime, TenantId, type BillingState, type Tenant } from "../core/index.ts";
import type { StripeConfig } from "../config.ts";
import type { Services } from "../pipeline/services.ts";

type StripeObject = Readonly<Record<string, unknown>>;

export type StripeEvent = {
    readonly id:   string;
    readonly type: string;
    readonly data: { readonly object: StripeObject };
};

const STATUSES = new Set<string>(Object.values(BillingStatus));

function str(value: unknown): string | null {
    return typeof value === "string" && value !== "" ? value : null;
}

function metadata(object: StripeObject): Readonly<Record<string, string>> {
    return (object["metadata"] ?? {}) as Record<string, string>;
}

/**
 * The subscription's items as add-ons, by matching their price ids to ours.
 */
function addOns(subscription: StripeObject, prices: StripeConfig): { extraLines: number; smsNotifications: boolean } {
    const items = ((subscription["items"] as { data?: StripeObject[] } | undefined)?.data ?? []);
    let extraLines = 0;
    let smsNotifications = false;
    for (const item of items) {
        const price = str((item["price"] as StripeObject | undefined)?.["id"]);
        const quantity = Number(item["quantity"] ?? 1);
        if (price !== null && price === prices.priceExtraLine) {
            extraLines += quantity;
        } else if (price !== null && price === prices.priceSmsAlerts) {
            smsNotifications = true;
        }
    }
    return { extraLines, smsNotifications };
}

function periodEnd(subscription: StripeObject): string | null {
    // Newer API versions put the period on each item; older ones on the subscription.
    const items = ((subscription["items"] as { data?: StripeObject[] } | undefined)?.data ?? []);
    const seconds = Number(subscription["current_period_end"] ?? items[0]?.["current_period_end"] ?? NaN);
    return Number.isFinite(seconds) ? new Date(seconds * 1000).toISOString() : null;
}

async function tenantFor(services: Services, object: StripeObject, subscriptionId: string | null): Promise<Tenant | null> {
    const id = metadata(object)["tenant_id"] ?? str(object["client_reference_id"]);
    if (id !== undefined && id !== null) {
        return services.store.tenants.byId(TenantId(id));
    }
    if (subscriptionId === null) {
        return null;
    }
    return (await services.store.tenants.all()).find(t => t.billing.subscriptionId === subscriptionId) ?? null;
}

/**
 * Apply one verified event. Returns what changed, for the log; "ignored" for events we don't use.
 */
export async function applyStripeEvent(services: Services, prices: StripeConfig, event: StripeEvent): Promise<string> {
    const { store } = services;
    if (!await store.webhooks.claim("stripe", event.id, services.clock())) {
        return "already applied";
    }
    const object = event.data.object;
    switch (event.type) {
        case "checkout.session.completed": {
            const tenant = await tenantFor(services, object, null);
            if (tenant === null) {
                return "no tenant";
            }
            const kind = metadata(object)["kind"];
            if (kind === "topup") {
                if (object["payment_status"] !== "paid") {
                    return "top-up not paid";
                }
                const amount = Number(metadata(object)["amount_usd"] ?? 0);
                const local = LocalTime.of(services.clock(), tenant.timezone);
                const month = `${local.year}-${String(local.month).padStart(2, "0")}`;
                const sub = tenant.plan.subscription;
                const topUpsUsd = (sub.topUpsMonth === month ? sub.topUpsUsd : 0) + amount;
                await store.tenants.save({ ...tenant, plan: { ...tenant.plan, subscription: { ...sub, topUpsUsd, topUpsMonth: month } } });
                return `top-up of $${amount} for ${tenant.slug}`;
            }
            const billing: BillingState = {
                ...tenant.billing,
                status:         "active",
                customerId:     str(object["customer"]) ?? tenant.billing.customerId,
                subscriptionId: str(object["subscription"]) ?? tenant.billing.subscriptionId,
            };
            await store.tenants.save({ ...tenant, billing });
            return `subscribed ${tenant.slug}`;
        }
        case "customer.subscription.created":
        case "customer.subscription.updated":
        case "customer.subscription.deleted": {
            const subscriptionId = str(object["id"]);
            const tenant = await tenantFor(services, object, subscriptionId);
            if (tenant === null) {
                return "no tenant";
            }
            const raw = event.type === "customer.subscription.deleted" ? "canceled" : String(object["status"] ?? "");
            const status = (STATUSES.has(raw) ? raw : "incomplete") as BillingStatus;
            const extras = addOns(object, prices);
            await store.tenants.save({
                ...tenant,
                billing: {
                    status,
                    customerId:       str(object["customer"]) ?? tenant.billing.customerId,
                    subscriptionId:   subscriptionId ?? tenant.billing.subscriptionId,
                    currentPeriodEnd: periodEnd(object),
                },
                plan:    { ...tenant.plan, subscription: { ...tenant.plan.subscription, ...extras } },
            });
            return `${tenant.slug} is ${status}`;
        }
        default:
            return "ignored";
    }
}
