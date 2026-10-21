/**
 * Channels a conversation can happen on, and the addresses that reach people.
 */

import type { Brand, ValueOf } from "./lang.ts";

export const Channel = {
    Web:   "web",
    Sms:   "sms",
    Voice: "voice",
} as const;
export type Channel = ValueOf<typeof Channel>;

/**
 * An E.164 phone number ("+13175550100").
 * Only `PhoneNumber.parse` produces one, so holding a PhoneNumber means it was normalized.
 */
export type PhoneNumber = Brand<string, "PhoneNumber">;

export const PhoneNumber = {
    /**
     * Normalize user or vendor input to E.164.
     * Bare 10-digit and 1-prefixed 11-digit numbers are treated as US numbers;
     * anything else must already carry a leading "+".
     */
    parse(raw: string): PhoneNumber | null {
        const trimmed = raw.trim();
        const digits  = trimmed.replace(/\D/g, "");
        if (digits.length === 10) {
            return `+1${digits}` as PhoneNumber;
        }
        if (digits.length === 11 && digits.startsWith("1")) {
            return `+${digits}` as PhoneNumber;
        }
        if (
			trimmed.startsWith("+")
			&& digits.length >= 8
			&& digits.length <= 15
		) {
            return `+${digits}` as PhoneNumber;
        }
        return null;
    },
} as const;

/**
 * A way to reach a person, or a tenant endpoint.
 * Values are normalized on construction (E.164 phones, lowercased email),
 * so two Addresses for the same destination compare equal by `Address.key`.
 */
export type Address =
    | { readonly kind: "phone";
		readonly value: PhoneNumber
	}
    | { readonly kind: "email";
		readonly value: string
	}
    | { readonly kind: "web_session";
		readonly value: string
	};

export const Address = {
    phone(raw: string): Address | null {
        const value = PhoneNumber.parse(raw);
        return value === null
			? null
			: { kind: "phone", value };
    },

    email(raw: string): Address | null {
        const value = raw.trim().toLowerCase();
        return value.includes("@")
			? { kind: "email", value }
			: null;
    },

    webSession(id: string): Address {
        return { kind: "web_session", value: id };
    },

    /**
     * A stable string form, for map keys and storage ("phone:+13175550100").
     */
    key(address: Address): string {
        return `${address.kind}:${address.value}`;
    },

    equals(a: Address, b: Address): boolean {
        return a.kind === b.kind
			&& a.value === b.value;
    },
} as const;
