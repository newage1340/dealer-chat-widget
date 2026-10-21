import { beforeEach, describe, expect, it } from "vitest";

import { FakeLlm, LoggingEmail, LoggingMessaging } from "../../src/providers/index.ts";
import { StripeWebhook, stripeForm } from "../../src/providers/stripe.ts";
import { buildApp, type App } from "../../src/server.ts";
import { openStore } from "../../src/storage/index.ts";
import { TEST_CONFIG } from "../support/config.ts";
import { CUSTOMER, PLUMBER_NUMBER, plumber } from "../support/fixtures.ts";

const STRIPE = {
    secretKey: "sk_test_x", webhookSecret: "whsec_test", priceBase: "price_base",
    priceExtraLine: "price_line", priceSmsAlerts: "price_sms", topUpAmounts: [25, 50],
};

let app: App;
let cookie: string;
let stripeCalls: { url: string; body: URLSearchParams }[];

async function webhook(event: object, secret = STRIPE.webhookSecret) {
    const body = JSON.stringify(event);
    return await app.http.request("/stripe/webhook", {
        method:  "POST",
        body,
        headers: { "Stripe-Signature": StripeWebhook.sign(body, secret, new Date()), "Content-Type": "application/json" },
    });
}

beforeEach(async () => {
    const store = await openStore("", null);
    await store.tenants.save(plumber({ plan: { extraLines: 1, smsNotifications: false, topUpsUsd: 0 } }));
    stripeCalls = [];
    const fakeFetch = (async (url: string, init?: RequestInit) => {
        stripeCalls.push({ url, body: new URLSearchParams(String(init?.body ?? "")) });
        return new Response(JSON.stringify({ url: "https://checkout.stripe.com/c/pay/cs_test_1" }));
    }) as typeof fetch;
    app = await buildApp({ ...TEST_CONFIG, stripe: STRIPE }, {
        store, llm: new FakeLlm(() => "Sure, how can I help?"), messaging: new LoggingMessaging(), email: new LoggingEmail(),
        transcriber: null, fetchRecording: null, fetch: fakeFetch,
    });
    const login = await app.http.request("/settings/login", { method: "POST", body: new URLSearchParams({ password: "admin" }) });
    cookie = (login.headers.get("set-cookie") ?? "").split(";")[0]!;
});

describe("Stripe", () => {
    it("encodes nested parameters the way Stripe expects", () => {
        const form = stripeForm({ line_items: [{ price: "p", quantity: 2 }], metadata: { tenant_id: "t" } });
        expect(form.toString()).toBe("line_items%5B0%5D%5Bprice%5D=p&line_items%5B0%5D%5Bquantity%5D=2&metadata%5Btenant_id%5D=t");
    });

    it("checks webhook signatures and refuses stale or forged ones", () => {
        const now = new Date();
        const header = StripeWebhook.sign("{}", "whsec_a", now);
        expect(StripeWebhook.verify("{}", header, "whsec_a", now)).toBe(true);
        expect(StripeWebhook.verify("{\"x\":1}", header, "whsec_a", now)).toBe(false);
        expect(StripeWebhook.verify("{}", header, "whsec_b", now)).toBe(false);
        expect(StripeWebhook.verify("{}", header, "whsec_a", new Date(now.getTime() + 600_000))).toBe(false);
    });

    it("sends the plan and add-ons to Checkout", async () => {
        const res = await app.http.request("/settings/demo-plumber/billing/subscribe", { method: "POST", headers: { Cookie: cookie } });
        expect(res.status).toBe(303);
        expect(res.headers.get("location")).toBe("https://checkout.stripe.com/c/pay/cs_test_1");
        const body = stripeCalls[0]!.body;
        expect(stripeCalls[0]!.url).toBe("https://api.stripe.com/v1/checkout/sessions");
        expect(body.get("mode")).toBe("subscription");
        expect(body.get("line_items[0][price]")).toBe("price_base");
        expect(body.get("line_items[1][price]")).toBe("price_line");
        expect(body.get("line_items[1][quantity]")).toBe("1");
        expect(body.get("line_items[2]")).toBeNull();
        expect(body.get("metadata[tenant_id]")).toBe("plumber");
        expect(body.get("subscription_data[metadata][tenant_id]")).toBe("plumber");
    });

    it("records subscriptions from webhooks, once, and pauses the assistant when one lapses", async () => {
        expect((await webhook({ id: "evt_1", type: "checkout.session.completed", data: { object: {
            metadata: { tenant_id: "plumber", kind: "subscription" }, customer: "cus_1", subscription: "sub_1",
        } } })).status).toBe(200);
        expect((await app.store.tenants.byId("plumber" as never))?.billing).toMatchObject({ status: "active", customerId: "cus_1", subscriptionId: "sub_1" });

        await webhook({ id: "evt_2", type: "customer.subscription.updated", data: { object: {
            id: "sub_1", customer: "cus_1", status: "active", metadata: {},
            items: { data: [{ price: { id: "price_base" }, quantity: 1, current_period_end: 1798000000 }, { price: { id: "price_line" }, quantity: 2 }, { price: { id: "price_sms" }, quantity: 1 }] },
        } } });
        let t = (await app.store.tenants.byId("plumber" as never))!;
        expect(t.plan.subscription).toMatchObject({ extraLines: 2, smsNotifications: true });
        expect(t.billing.currentPeriodEnd).toBe(new Date(1798000000 * 1000).toISOString());

        await webhook({ id: "evt_3", type: "customer.subscription.deleted", data: { object: { id: "sub_1", status: "canceled", metadata: {}, items: { data: [] } } } });
        t = (await app.store.tenants.byId("plumber" as never))!;
        expect(t.billing.status).toBe("canceled");
        const reply = await (await app.http.request("/sms", { method: "POST", body: new URLSearchParams({ From: CUSTOMER, To: PLUMBER_NUMBER, Body: "hi", MessageSid: "SM9" }) })).text();
        expect(reply).toContain("can't reply here right now");
    });

    it("adds paid top-ups to this month's allowance, ignoring repeats and bad signatures", async () => {
        const topUp = { id: "evt_t", type: "checkout.session.completed", data: { object: {
            metadata: { tenant_id: "plumber", kind: "topup", amount_usd: "25" }, payment_status: "paid",
        } } };
        await webhook(topUp);
        await webhook(topUp);
        expect((await webhook({ ...topUp, id: "evt_forged" }, "whsec_wrong")).status).toBe(400);
        expect((await app.store.tenants.byId("plumber" as never))?.plan.subscription.topUpsUsd).toBe(25);
    });
});
