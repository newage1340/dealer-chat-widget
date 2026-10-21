/**
 * Logins for business owners: each account belongs to one tenant and can only see and edit that tenant.
 * The platform admin signs in with the admin token instead and sees everything.
 */

import { randomBytes, scrypt as scryptCallback, timingSafeEqual } from "node:crypto";
import { promisify } from "node:util";

import type { TenantId } from "./ids.ts";

const scrypt = promisify(scryptCallback) as (password: string, salt: Buffer, keylen: number, options: { N: number; r: number; p: number }) => Promise<Buffer>;

export type Account = {
    readonly id:           string;
    readonly tenantId:     TenantId;
    readonly email:        string;
    readonly name:         string;
    readonly passwordHash: string;
    readonly createdAt:    Date;
    readonly lastLoginAt:  Date | null;
};

const COST = { N: 16_384, r: 8, p: 1 } as const;
export const MIN_PASSWORD_LENGTH = 10;

export const Password = {
    /**
     * A salted scrypt hash, stored as "scrypt$<N>$<salt>$<hash>" (base64).
     */
    async hash(password: string): Promise<string> {
        const salt = randomBytes(16);
        const key = await scrypt(password, salt, 32, COST);
        return `scrypt$${COST.N}$${salt.toString("base64")}$${key.toString("base64")}`;
    },

    async verify(password: string, stored: string): Promise<boolean> {
        const [scheme, n, salt, hash] = stored.split("$");
        if (scheme !== "scrypt" || !n || !salt || !hash) {
            return false;
        }
        const expected = Buffer.from(hash, "base64");
        const key = await scrypt(password, Buffer.from(salt, "base64"), expected.length, { ...COST, N: Number(n) });
        return key.length === expected.length && timingSafeEqual(key, expected);
    },

    /**
     * Why a new password isn't acceptable, or null.
     */
    problem(password: string): string | null {
        if (password.length < MIN_PASSWORD_LENGTH) {
            return `Passwords need at least ${MIN_PASSWORD_LENGTH} characters.`;
        }
        if (password.length > 200) {
            return "That password is too long.";
        }
        return null;
    },
} as const;
