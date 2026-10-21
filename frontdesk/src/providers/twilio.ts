/**
 * Twilio: outbound SMS over its REST API, recording downloads, and webhook signature checks.
 * Plain fetch and node:crypto; no SDK.
 */

import { createHmac, timingSafeEqual } from "node:crypto";

import type { UsageRecord } from "../core/index.ts";
import { ProviderError } from "./errors.ts";
import type { MessagingProvider, OutboundSms, SendReceipt } from "./messaging.ts";

export type TwilioCredentials = {
    readonly accountSid:          string;
    readonly authToken:           string;
    readonly messagingServiceSid: string;
};

const API = "https://api.twilio.com/2010-04-01";

/**
 * GSM-7 messages are 160 characters per segment (153 when split); anything else is UCS-2 at 70 (67).
 */
export function smsSegments(text: string): number {
    const gsm = /^[\x20-\x7E\n\r£¥èéùìòÇØøÅåΔ_ΦΓΛΩΠΨΣΘΞÆæßÉ¤¡ÄÖÑÜ§¿äöñüà€]*$/.test(text);
    const [single, multi] = gsm
        ? [160, 153]
        : [70, 67];
    const length = [...text].length;
    return length <= single
        ? 1
        : Math.ceil(length / multi);
}

function authHeader(c: TwilioCredentials): string {
    return "Basic " + Buffer.from(`${c.accountSid}:${c.authToken}`).toString("base64");
}

export class TwilioSms implements MessagingProvider {
    readonly name = "twilio";
    readonly #credentials: TwilioCredentials;

    constructor(credentials: TwilioCredentials) {
        this.#credentials = credentials;
    }

    async send(message: OutboundSms, signal?: AbortSignal): Promise<SendReceipt> {
        const c = this.#credentials;
        const body = new URLSearchParams({ To: message.to, Body: message.text });
        if (c.messagingServiceSid) {
            body.set("MessagingServiceSid", c.messagingServiceSid);
        } else {
            body.set("From", message.from);
        }
        const started = performance.now();
        let response: Response;
        try {
            response = await fetch(`${API}/Accounts/${c.accountSid}/Messages.json`, {
                method:  "POST",
                headers: { Authorization: authHeader(c), "Content-Type": "application/x-www-form-urlencoded" },
                body,
                signal:  signal ?? AbortSignal.timeout(15_000),
            });
        } catch (e) {
            throw new ProviderError(this.name, "unavailable", String(e), { cause: e });
        }
        const json = await response.json().catch(() => ({})) as { sid?: string; message?: string };
        if (!response.ok) {
            const failure = response.status === 429
                ? "rate_limited"
                : response.status === 401
                    ? "auth"
                    : response.status >= 500
                        ? "unavailable"
                        : "bad_request";
            throw new ProviderError(this.name, failure, json.message ?? `HTTP ${response.status}`);
        }
        const usage: UsageRecord = {
            provider:   this.name,
            model:      "",
            service:    "messaging",
            purpose:    message.purpose,
            occurredAt: new Date(),
            units:      { sms_segments: smsSegments(message.text) },
            costUsd:    null,
            latencyMs:  performance.now() - started,
            tenantId:   message.tenantId,
            channel:    "sms",
        };
        return { providerMessageId: json.sid ?? "", usage };
    }

    /**
     * Download a call recording (Twilio media URLs require the account's credentials).
     */
    async recording(url: string, signal?: AbortSignal): Promise<Uint8Array> {
        const target = /\.(mp3|wav)$/.test(url)
            ? url
            : `${url}.mp3`;
        const response = await fetch(target, {
            headers: { Authorization: authHeader(this.#credentials) },
            signal:  signal ?? AbortSignal.timeout(30_000),
        });
        if (!response.ok) {
            throw new ProviderError(this.name, response.status >= 500 ? "unavailable" : "bad_request", `recording HTTP ${response.status}`);
        }
        return new Uint8Array(await response.arrayBuffer());
    }
}

export const TwilioSignature = {
    /**
     * Twilio's X-Twilio-Signature: base64 HMAC-SHA1 of the full URL followed by
     * every POST parameter's name and value, sorted by name.
     */
    expected(authToken: string, url: string, params: Readonly<Record<string, string>>): string {
        const data = Object.keys(params)
            .sort()
            .reduce((acc, key) => acc + key + params[key], url);
        return createHmac("sha1", authToken).update(data).digest("base64");
    },

    valid(
        authToken: string,
        url: string,
        params: Readonly<Record<string, string>>,
        signature: string
    ): boolean
    {
        const expected = Buffer.from(TwilioSignature.expected(authToken, url, params));
        const given = Buffer.from(signature);
        return expected.length === given.length
            && timingSafeEqual(expected, given);
    },
} as const;
