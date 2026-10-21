/**
 * Email through Resend's HTTP API. Any other vendor only needs another EmailProvider.
 */

import type { UsageRecord } from "../core/index.ts";
import type { EmailProvider, EmailReceipt, OutboundEmail } from "./email.ts";
import { ProviderError } from "./errors.ts";

export class ResendEmail implements EmailProvider {
    readonly name = "resend";
    readonly #apiKey: string;

    constructor(apiKey: string) {
        this.#apiKey = apiKey;
    }

    async send(email: OutboundEmail, signal?: AbortSignal): Promise<EmailReceipt> {
        const started = performance.now();
        let response: Response;
        try {
            response = await fetch("https://api.resend.com/emails", {
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
                signal:  signal ?? AbortSignal.timeout(15_000),
            });
        } catch (e) {
            throw new ProviderError(this.name, "unavailable", String(e), { cause: e });
        }
        const json = await response.json().catch(() => ({})) as { id?: string; message?: string };
        if (!response.ok) {
            const failure = response.status === 429
                ? "rate_limited"
                : response.status >= 500
                    ? "unavailable"
                    : response.status === 401 || response.status === 403
                        ? "auth"
                        : "bad_request";
            throw new ProviderError(this.name, failure, json.message ?? `HTTP ${response.status}`);
        }
        const usage: UsageRecord = {
            provider:   this.name,
            model:      "",
            service:    "email",
            purpose:    email.purpose,
            occurredAt: new Date(),
            units:      { requests: 1 },
            costUsd:    null,
            latencyMs:  performance.now() - started,
            tenantId:   email.tenantId,
            channel:    null,
        };
        return { providerMessageId: json.id ?? "", usage };
    }
}
