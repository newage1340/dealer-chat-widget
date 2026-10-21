/**
 * Stripe's webhook endpoint. The signature is checked against the raw body before anything is parsed.
 */

import { Hono } from "hono";

import { applyStripeEvent, StripeEvent } from "../billing/stripe.ts";
import { StripeWebhook } from "../providers/stripe.ts";
import type { ChannelDeps } from "./context.ts";

function parseJson(text: string): unknown {
    try {
        return JSON.parse(text);
    } catch {
        return undefined;
    }
}

export function billingRoutes(deps: ChannelDeps): Hono {
    const app = new Hono();
    const { config, services } = deps;

    app.post("/stripe/webhook", async c => {
        if (!config.stripe.webhookSecret) {
            return c.json({ error: "billing is not set up" }, 404);
        }
        const body = await c.req.text();
        if (!StripeWebhook.verify(body, c.req.header("Stripe-Signature") ?? "", config.stripe.webhookSecret, services.clock())) {
            return c.json({ error: "invalid signature" }, 400);
        }
        const parsed = StripeEvent.safeParse(parseJson(body));
        if (!parsed.success) {
            return c.json({ error: "invalid body" }, 400);
        }
        const event = parsed.data;
        const result = await applyStripeEvent(services, config.stripe, event);
        console.info(`stripe ${event.type} (${event.id}): ${result}`);
        return c.json({ received: true });
    });

    return app;
}
