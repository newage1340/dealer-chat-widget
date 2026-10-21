/**
 * What every channel module needs, plus small helpers they share.
 */

import type { Context, MiddlewareHandler } from "hono";

import type { Config } from "../config.ts";
import type { TurnEngine } from "../pipeline/engine.ts";
import type { Services } from "../pipeline/services.ts";
import { TwilioSignature } from "../providers/twilio.ts";
import type { VoicePipeline } from "../voice/pipeline.ts";

export type ChannelDeps = {
    readonly config:   Config;
    readonly services: Services;
    readonly engine:   TurnEngine;
    readonly voice?:   VoicePipeline;
    /**
     * For outbound HTTP to vendors called from routes (Stripe); tests pass a fake.
     */
    readonly fetch?:   typeof fetch;
};

/**
 * Form fields of a Twilio webhook, as plain strings.
 */
export async function form(c: Context): Promise<Record<string, string>> {
    const body = await c.req.parseBody();
    return Object.fromEntries(Object.entries(body).map(([k, v]) => [k, typeof v === "string" ? v : ""]));
}

/**
 * Absolute URL for a path, for Twilio callbacks (relative when no public base URL is configured).
 */
export function absolute(config: Config, path: string): string {
    return config.publicBaseUrl
        ? `${config.publicBaseUrl}${path}`
        : path;
}

export function xml(c: Context, body: string): Response {
    return c.body(body, 200, { "Content-Type": "application/xml" });
}

/**
 * Reject webhooks that were not signed by Twilio with our auth token.
 * The signed URL is the public one Twilio called, so PUBLIC_BASE_URL must match the webhook configuration.
 */
export function twilioSignature(config: Config): MiddlewareHandler {
    return async (c, next) => {
        if (!config.validateTwilio) {
            return next();
        }
        const url = new URL(c.req.url);
        const signedUrl = config.publicBaseUrl
            ? `${config.publicBaseUrl}${url.pathname}${url.search}`
            : c.req.url;
        const params = await form(c);
        const signature = c.req.header("X-Twilio-Signature") ?? "";
        if (!TwilioSignature.valid(config.twilioAuthToken, signedUrl, params, signature)) {
            return c.text("invalid signature", 403);
        }
        return next();
    };
}

/**
 * Admin endpoints need `Authorization: Bearer <ADMIN_TOKEN>` (or `?token=`). Disabled without a token.
 */
export function adminOnly(config: Config): MiddlewareHandler {
    return async (c, next) => {
        const header = c.req.header("Authorization")?.replace(/^Bearer\s+/i, "") ?? "";
        const token = header || c.req.query("token") || c.req.header("X-Admin-Token") || "";
        if (!config.adminToken || token !== config.adminToken) {
            return c.json({ error: "unauthorized" }, 401);
        }
        return next();
    };
}
