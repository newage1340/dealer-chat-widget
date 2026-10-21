/**
 * What every channel module needs, plus small helpers they share.
 */

import { createHash, timingSafeEqual } from "node:crypto";

import type { Context, MiddlewareHandler } from "hono";
import type { z } from "zod";

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
 * Compare a secret someone sent with the real one in constant time (for tokens, codes and signatures).
 * Both are hashed first, so neither the content nor the length leaks through timing.
 */
export function sameSecret(sent: string, expected: string): boolean {
    const digest = (s: string) => createHash("sha256").update(s).digest();
    return timingSafeEqual(digest(sent), digest(expected));
}

/**
 * A JSON request body checked against `schema`, or null when it has the wrong shape.
 * A missing or unparseable body is read as `{}`, so schemas with defaults still apply.
 */
export async function jsonBody<Schema extends z.ZodType>(c: Context, schema: Schema): Promise<z.output<Schema> | null> {
    const raw: unknown = await c.req.json().catch(() => ({}));
    const parsed = schema.safeParse(raw);
    return parsed.success
        ? parsed.data
        : null;
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
        if (!config.adminToken || !sameSecret(token, config.adminToken)) {
            return c.json({ error: "unauthorized" }, 401);
        }
        return next();
    };
}
