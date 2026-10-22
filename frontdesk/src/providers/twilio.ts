/**
 * Twilio: outbound SMS over its REST API, recording downloads, and webhook signature checks.
 * Plain fetch and node:crypto; no SDK.
 */

import { createHmac, timingSafeEqual } from "node:crypto";

import { z } from "zod";

import { UsageRecord } from "../core/index.ts";
import { checkStatus, providerFetch, readJson } from "./http.ts";
import type { MessagingProvider, OutboundSms, SendReceipt } from "./messaging.ts";

export type TwilioCredentials = {
    readonly accountSid:          string;
    readonly authToken:           string;
    readonly messagingServiceSid: string;
};

const API = "https://api.twilio.com/2010-04-01";

/**
 * Typographic characters models like to write, and the plain ones that keep a text in GSM-7.
 * One curly apostrophe turns a whole message into UCS-2, which fits 70 characters per segment instead of 160.
 */
const PLAIN: readonly [RegExp, string][] = [
    [/[\u2018\u2019\u201A\u201B\u2032]/g, "'"],
    [/[\u201C\u201D\u201E\u201F\u2033]/g, "\""],
    [/[\u2013\u2014\u2015\u2212]/g, "-"],
    [/\u2026/g, "..."],
    [/[\u00A0\u2007\u202F\u2009\u200A]/g, " "],
    [/[\u200B\u200C\u200D\uFEFF]/g, ""],
    [/[\u2022\u00B7]/g, "-"],
];

/**
 * A text with typographic punctuation replaced by plain characters, so it bills as GSM-7 when it can.
 * Letters in other alphabets (Spanish accents in GSM-7 stay, Chinese or Vietnamese don't) are left alone.
 */
export function plainSms(text: string): string {
    return PLAIN.reduce((out, [pattern, plain]) => out.replace(pattern, plain), text);
}

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

/**
 * The parts of Twilio's reply to a sent message we use: its id, or the error message.
 */
const TwilioReply = z.object({
    sid:     z.string().nullish(),
    message: z.string().nullish(),
});

export class TwilioSms implements MessagingProvider {
    readonly name = "twilio";
    readonly #credentials: TwilioCredentials;

    constructor(credentials: TwilioCredentials) {
        this.#credentials = credentials;
    }

    async send(message: OutboundSms, signal?: AbortSignal): Promise<SendReceipt> {
        const c = this.#credentials;
        const text = plainSms(message.text);
        const body = new URLSearchParams({ To: message.to, Body: text });
        if (c.messagingServiceSid) {
            body.set("MessagingServiceSid", c.messagingServiceSid);
        } else {
            body.set("From", message.from);
        }
        const started = performance.now();
        const response = await providerFetch(this.name, `${API}/Accounts/${c.accountSid}/Messages.json`, {
            method:  "POST",
            headers: { Authorization: authHeader(c), "Content-Type": "application/x-www-form-urlencoded" },
            body,
        }, { timeoutMs: 15_000, signal });
        const json = await readJson(this.name, response, TwilioReply);
        checkStatus(this.name, response, json.message);
        const usage = UsageRecord.measured({
            provider:  this.name,
            model:     "",
            service:   "messaging",
            purpose:   message.purpose,
            units:     { sms_segments: smsSegments(text) },
            tenantId:  message.tenantId,
            channel:   "sms",
            startedAt: started,
        });
        return { providerMessageId: json.sid ?? "", usage };
    }

    /**
     * Download a call recording (Twilio media URLs require the account's credentials).
     */
    async recording(url: string, signal?: AbortSignal): Promise<Uint8Array> {
        const target = /\.(mp3|wav)$/.test(url)
            ? url
            : `${url}.mp3`;
        const response = await providerFetch(this.name, target, {
            headers: { Authorization: authHeader(this.#credentials) },
        }, { timeoutMs: 30_000, signal });
        checkStatus(this.name, response, `recording HTTP ${response.status}`);
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
