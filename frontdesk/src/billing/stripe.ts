/**
 * What Stripe's webhook events mean for a tenant: subscription status and add-ons, and top-ups.
 * Each event is applied once (Stripe retries deliveries), and nothing here trusts the request
 * without a valid signature (see http/billing.ts).
 */

import { z } from "zod";

import { BillingStatus, LocalTime, TenantId, type BillingState, type Tenant } from "../core/index.ts";
import type { StripeConfig } from "../config.ts";
import type { Services } from "../pipeline/services.ts";

/**
 * An optional Stripe id; Stripe sends null (or occasionally "") when there is none.
 */
const stripeId = z.string().nullish().transform(v => v || null);
const metadata = z.record(z.string(), z.string()).nullish().transform(m => m ?? {});

/**
 * The parts of a Checkout Session we use. Our own metadata says what was bought.
 */
const CheckoutSession = z.object({
    metadata:            metadata,
    client_reference_id: stripeId,
    customer:            stripeId,
    subscription:        stripeId,
    payment_status:      z.string().nullish(),
});

/**
 * The parts of a Subscription we use. Newer API versions put the billing period on each item,
 * older ones on the subscription. A status we don't know reads as "incomplete".
 */
const Subscription = z.object({
    id:                 stripeId,
    metadata:           metadata,
    customer:           stripeId,
    status:             z.enum(BillingStatus).catch(BillingStatus.Incomplete),
    current_period_end: z.number().nullish(),
    items:              z.object({
        data: z.array(z.object({
            price:              z.object({ id: stripeId }).nullish(),
            quantity:           z.number().nullish(),
            current_period_end: z.number().nullish(),
        })),
    }).nullish(),
});
type Subscription = z.output<typeof Subscription>;

/**
 * A webhook event; `data.object` is parsed by type once we know which kind it is.
 */
export const StripeEvent = z.object({
    id:   z.string(),
    type: z.string(),
    data: z.object({ object: z.unknown() }),
});
export type StripeEvent = z.output<typeof StripeEvent>;

/**
 * The subscription's items as add-ons, by matching their price ids to ours.
 */
function addOns(subscription: Subscription, prices: StripeConfig): { extraLines: number; smsNotifications: boolean } {
    let extraLines = 0;
    let smsNotifications = false;
    for (const item of subscription.items?.data ?? []) {
        const price = item.price?.id ?? null;
        if (price !== null && price === prices.priceExtraLine) {
            extraLines += item.quantity ?? 1;
        } else if (price !== null && price === prices.priceSmsAlerts) {
            smsNotifications = true;
        }
    }
    return { extraLines, smsNotifications };
}

function periodEnd(subscription: Subscription): string | null {
    const seconds = subscription.current_period_end ?? subscription.items?.data[0]?.current_period_end ?? null;
    return seconds !== null
		? new Date(seconds * 1000).toISOString()
		: null;
}

/**
 * The tenant an object is about: by the tenant id we put in its metadata (or client reference),
 * else by the subscription it belongs to.
 */
async function tenantFor(
    services: Services,
    object: { readonly metadata: Readonly<Record<string, string>>; readonly client_reference_id?: string | null },
    subscriptionId: string | null
): Promise<Tenant | null>
{
    const id = object.metadata["tenant_id"] ?? object.client_reference_id ?? null;
    if (id !== null) {
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
export async function applyStripeEvent(
    services: Services,
    prices: StripeConfig,
    event: StripeEvent
): Promise<string> {
    const { store } = services;
    if (!await store.webhooks.claim("stripe", event.id, services.clock())) {
        return "already applied";
    }
    switch (event.type) {
        case "checkout.session.completed": {
            const session = CheckoutSession.safeParse(event.data.object);
            if (!session.success) {
                return `unexpected checkout session: ${session.error.message}`;
            }
            const object = session.data;
            const tenant = await tenantFor(services, object, null);
            if (tenant === null) {
                return "no tenant";
            }
            if (object.metadata["kind"] === "topup") {
                if (object.payment_status !== "paid") {
                    return "top-up not paid";
                }
                const amount = Number(object.metadata["amount_usd"] ?? 0);
                const local = LocalTime.of(services.clock(), tenant.timezone);
                const month = LocalTime.isoMonth(local);
                const sub = tenant.plan.subscription;
                const topUpsUsd = (sub.topUpsMonth === month ? sub.topUpsUsd : 0) + amount;
                await store.tenants.save({ ...tenant, plan: { ...tenant.plan, subscription: { ...sub, topUpsUsd, topUpsMonth: month } } });
                return `top-up of $${amount} for ${tenant.slug}`;
            }
            const billing: BillingState = {
                ...tenant.billing,
                status:         "active",
                customerId:     object.customer ?? tenant.billing.customerId,
                subscriptionId: object.subscription ?? tenant.billing.subscriptionId,
            };
            await store.tenants.save({ ...tenant, billing });
            return `subscribed ${tenant.slug}`;
        }
        case "customer.subscription.created":
        case "customer.subscription.updated":
        case "customer.subscription.deleted": {
            const parsed = Subscription.safeParse(event.data.object);
            if (!parsed.success) {
                return `unexpected subscription: ${parsed.error.message}`;
            }
            const object = parsed.data;
            const tenant = await tenantFor(services, object, object.id);
            if (tenant === null) {
                return "no tenant";
            }
            const status = event.type === "customer.subscription.deleted"
				? BillingStatus.Canceled
				: object.status;
            await store.tenants.save({
                ...tenant,
                billing: {
                    status,
                    customerId:       object.customer ?? tenant.billing.customerId,
                    subscriptionId:   object.id ?? tenant.billing.subscriptionId,
                    currentPeriodEnd: periodEnd(object),
                },
                plan:    { ...tenant.plan, subscription: { ...tenant.plan.subscription, ...addOns(object, prices) } },
            });
            return `${tenant.slug} is ${status}`;
        }
        default:
            return "ignored";
    }
}
