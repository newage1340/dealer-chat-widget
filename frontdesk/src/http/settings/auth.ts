/**
 * Who is looking at the settings pages: the platform admin (signed in with the admin token) or a
 * business owner (signed in with email and password, limited to their own business).
 *
 * Owner sessions are a signed cookie: account id, expiry, and an HMAC over both plus a slice of the
 * password hash, so changing the password signs out every other session.
 */

import { createHash, createHmac } from "node:crypto";

import type { Account, Tenant } from "../../core/index.ts";
import { sameSecret } from "../context.ts";

export type Viewer =
    | { readonly kind: "admin" }
    | { readonly kind: "owner";
        readonly account: Account
    };

const SESSION_DAYS = 14;

function mac(secret: string, text: string): string {
    return createHmac("sha256", secret).update(text).digest("base64url");
}

export const Session = {
    /**
     * The cookie value for an admin session: a hash of the token, never the token itself.
     */
    admin(adminToken: string): string {
        return `a.${createHash("sha256").update(`settings:${adminToken}`).digest("hex")}`;
    },

    owner(account: Account, secret: string, now: Date): string {
        const expires = Math.floor(now.getTime() / 1000) + SESSION_DAYS * 86_400;
        const body = `${account.id}.${expires}`;
        return `o.${body}.${mac(secret, `${body}.${account.passwordHash.slice(-16)}`)}`;
    },

    /**
     * The account id an owner cookie claims, before it is checked against the stored account.
     */
    ownerId(cookie: string): string | null {
        const parts = cookie.split(".");
        return parts.length === 4 && parts[0] === "o" ? parts[1]! : null;
    },

    validOwner(cookie: string, account: Account, secret: string, now: Date): boolean {
        const [, id, expires, signature] = cookie.split(".");
        if (id !== account.id || !expires || !signature || Number(expires) * 1000 < now.getTime()) {
            return false;
        }
        return sameSecret(signature, mac(secret, `${id}.${expires}.${account.passwordHash.slice(-16)}`));
    },

    validAdmin(cookie: string, adminToken: string): boolean {
        return adminToken !== "" && sameSecret(cookie, Session.admin(adminToken));
    },

    maxAgeSeconds: SESSION_DAYS * 86_400,
} as const;

export const Viewer = {
    canSee(viewer: Viewer, tenant: Tenant): boolean {
        return viewer.kind === "admin" || viewer.account.tenantId === tenant.id;
    },
} as const;

/**
 * Slows down password guessing: after `limit` failures for an email (or from an address) within the window,
 * further attempts are refused until it passes. Kept in memory; a restart clears it.
 */
export class LoginThrottle {
    readonly #failures = new Map<string, number[]>();
    readonly #limit: number;
    readonly #windowMs: number;

    constructor(limit = 8, windowMs = 15 * 60_000) {
        this.#limit = limit;
        this.#windowMs = windowMs;
    }

    blocked(keys: readonly string[], now: number): boolean {
        return keys.some(k => this.#recent(k, now).length >= this.#limit);
    }

    fail(keys: readonly string[], now: number): void {
        for (const k of keys) {
            this.#failures.set(k, [...this.#recent(k, now), now]);
        }
    }

    clear(keys: readonly string[]): void {
        keys.forEach(k => this.#failures.delete(k));
    }

    #recent(key: string, now: number): number[] {
        return (this.#failures.get(key) ?? []).filter(t => now - t < this.#windowMs);
    }
}
