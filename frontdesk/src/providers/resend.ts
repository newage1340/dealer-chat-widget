/**
 * Email through Resend's HTTP API. Any other vendor only needs another EmailProvider.
 */

import { z } from "zod";

import { UsageRecord } from "../core/index.ts";
import type { EmailProvider, EmailReceipt, OutboundEmail } from "./email.ts";
import { checkStatus, providerFetch, readJson } from "./http.ts";

/**
 * The parts of Resend's reply we use: the message id, or the error message.
 */
const ResendReply = z.object({
    id:      z.string().nullish(),
    message: z.string().nullish(),
});

export class ResendEmail implements EmailProvider {
    readonly name = "resend";
    readonly #apiKey: string;

    constructor(apiKey: string) {
        this.#apiKey = apiKey;
    }

    async send(email: OutboundEmail, signal?: AbortSignal): Promise<EmailReceipt> {
        const started = performance.now();
        const response = await providerFetch(this.name, "https://api.resend.com/emails", {
            method:  "POST",
            headers: { Authorization: `Bearer ${this.#apiKey}`, "Content-Type": "application/json" },
            body:    JSON.stringify({
                from:     email.from,
                to:       email.to,
                subject:  email.subject,
                text:     email.text,
                ...(email.html !== null ? { html: email.html } : {}),
                ...(email.replyTo !== null ? { reply_to: email.replyTo } : {}),
            }),
        }, { timeoutMs: 15_000, signal });
        const json = await readJson(this.name, response, ResendReply);
        checkStatus(this.name, response, json.message);
        const usage = UsageRecord.measured({
            provider:  this.name,
            model:     "",
            service:   "email",
            purpose:   email.purpose,
            units:     { requests: 1 },
            tenantId:  email.tenantId,
            channel:   null,
            startedAt: started,
        });
        return { providerMessageId: json.id ?? "", usage };
    }
}
