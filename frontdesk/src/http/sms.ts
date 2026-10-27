/**
 * Twilio SMS: each inbound text is one turn; the reply goes back in the webhook response (TwiML).
 */

import { Hono } from "hono";

import { Address, type Inbound } from "../core/index.ts";
import { collect } from "../pipeline/engine.ts";
import { STRINGS, say } from "../prompts/strings.ts";
import { plainSms, smsSegments } from "../providers/twilio.ts";
import { form, twilioSignature, xml, type ChannelDeps } from "./context.ts";
import { messageResponse } from "./twiml.ts";

export function smsRoutes(deps: ChannelDeps): Hono {
    const app = new Hono();
    const { services, engine, config } = deps;
    const { store } = services;

    app.post("/sms", twilioSignature(config), async c => {
        const f = await form(c);
        const sender = Address.phone(f["From"] ?? "");
        const recipient = Address.phone(f["To"] ?? "");
        if (sender === null || recipient === null) {
            return xml(c, messageResponse(null));
        }
        const tenant = await store.tenants.byEndpoint("sms", recipient);
        if (tenant === null) {
            console.warn(`SMS to unconfigured number ${recipient.value}`);
            return xml(c, messageResponse(null));
        }
        const existing = await store.conversations.contactByAddress(tenant.id, sender);
        const firstContact = existing === null || existing.termsAcceptedAt === null;

        const inbound: Inbound = {
            channel:           "sms",
            sender,
            text:              (f["Body"] ?? "").trim(),
            receivedAt:        services.clock(),
            dedupeKey:         f["MessageSid"] || `sms:${sender.value}:${Date.now()}`,
            externalSessionId: null,
            confidence:        null,
            language:          null,
            forwardedFrom:     null,
        };
        let text: string;
        try {
            const result = await collect(engine.run(inbound, tenant));
            if (result.silent || !result.outcome.reply) {
                return xml(c, messageResponse(null));
            }
            text = result.outcome.reply;
            if (firstContact && !result.replayed) {
                text += `\n\n${say(STRINGS.smsPrimer, result.session.language)}`;
                await store.conversations.updateContact(result.contact.id, { termsAcceptedAt: services.clock() });
            }
        } catch (e) {
            console.error("SMS turn failed:", e);
            text = say(STRINGS.fallbackText, tenant.defaultLanguage);
        }
        text = plainSms(text);
        // The reply rides on the webhook response, so meter it here (Twilio bills it like any text).
        await store.usage.record({
            provider:   "twilio",
            model:      "",
            service:    "messaging",
            purpose:    "turn",
            occurredAt: services.clock(),
            units:      { sms_segments: smsSegments(text) },
            costUsd:    null,
            latencyMs:  null,
            tenantId:   tenant.id,
            channel:    "sms",
        });
        return xml(c, messageResponse(text));
    });

    return app;
}
