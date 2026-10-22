/**
 * Email over SMTP, e.g. a Gmail account with an App Password (what the original Python app used:
 * GMAIL_USER / GMAIL_APP_PASSWORD), or any mail host's SMTP server.
 */

import nodemailer, { type Transporter } from "nodemailer";

import { errorMessage, UsageRecord } from "../core/index.ts";
import type { EmailProvider, EmailReceipt, OutboundEmail } from "./email.ts";
import { ProviderError } from "./errors.ts";

export type SmtpOptions = {
    readonly host:     string;
    readonly port:     number;
    readonly user:     string;
    readonly password: string;
};

export class SmtpEmail implements EmailProvider {
    readonly name = "smtp";
    readonly #transport: Transporter;

    constructor(options: SmtpOptions) {
        this.#transport = nodemailer.createTransport({
            host:   options.host,
            port:   options.port,
            secure: options.port === 465,
            auth:   { user: options.user, pass: options.password },
            connectionTimeout: 15_000,
        });
    }

    async send(email: OutboundEmail): Promise<EmailReceipt> {
        const started = performance.now();
        let info: { messageId?: string };
        try {
            info = await this.#transport.sendMail({
                from:    email.from,
                to:      [...email.to],
                subject: email.subject,
                text:    email.text,
                ...(email.html !== null ? { html: email.html } : {}),
                ...(email.replyTo !== null ? { replyTo: email.replyTo } : {}),
            });
        } catch (e) {
            const message = errorMessage(e);
            throw new ProviderError(this.name, /auth|535|534/i.test(message) ? "auth" : "unavailable", message, { cause: e });
        }
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
        return { providerMessageId: info.messageId ?? "", usage };
    }
}
