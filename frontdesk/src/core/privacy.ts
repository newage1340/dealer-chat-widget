/**
 * Personal information the system keeps out of the model, the database and staff messages,
 * and the discretion a business asks for.
 */

import type { ValueOf } from "./lang.ts";

/**
 * Kinds of personal data scrubbed from text.
 * `card`: payment card numbers (13-19 digits that pass the Luhn check).
 * `ssn`: social security numbers written as 123-45-6789, or 9 digits right after "social" / "SSN".
 * `bank_account`: 6-17 digits right after "account" or "routing".
 * `birth_date`: a date right after "born", "birthday", "date of birth" or "DOB".
 */
export const PiiKind = {
    Card:        "card",
    Ssn:         "ssn",
    BankAccount: "bank_account",
    BirthDate:   "birth_date",
} as const;
export type PiiKind = ValueOf<typeof PiiKind>;

/**
 * `redact` lists what is scrubbed from what customers write (before the model sees it or it is stored)
 * and from what the model writes back.
 * `sensitive` is for businesses that owe their clients discretion (therapy, medical, legal):
 * the assistant never confirms who is a client and never repeats personal details,
 * crisis messages get safety resources, and staff texts and emails carry no conversation content.
 */
export type PrivacySettings = {
    readonly redact:    readonly PiiKind[];
    readonly sensitive: boolean;
};

export const DEFAULT_PRIVACY: PrivacySettings = {
    redact:    ["card", "ssn", "bank_account"],
    sensitive: false,
};

const REPLACEMENT: Readonly<Record<PiiKind, string>> = {
    card:         "[card number removed]",
    ssn:          "[SSN removed]",
    bank_account: "[account number removed]",
    birth_date:   "[birth date removed]",
};

const DATE = String.raw`(?:\d{1,2}[\/.-]\d{1,2}[\/.-]\d{2,4}|\d{4}-\d{1,2}-\d{1,2}|(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\.?\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4})`;

const PATTERNS: Readonly<Record<Exclude<PiiKind, "card">, RegExp>> = {
    ssn:          /\b\d{3}[- ]\d{2}[- ]\d{4}\b|\b(?:ssn|social(?: security)?(?: number)?)\b[^\d]{0,15}\d{9}\b/gi,
    bank_account: /\b(?:account|acct|routing)\b(?: (?:number|no\.?|#))?[^\d]{0,10}\d{6,17}\b/gi,
    birth_date:   new RegExp(String.raw`\b(?:born(?: on)?|birthday|date of birth|d\.?o\.?b\.?)\b[^\d\w]{0,10}(?:is\s+)?${DATE}`, "gi"),
};

function luhn(digits: string): boolean {
    let sum = 0;
    for (let i = 0; i < digits.length; i++) {
        let d = Number(digits[digits.length - 1 - i]);
        if (i % 2 === 1) {
            d *= 2;
            if (d > 9) {
                d -= 9;
            }
        }
        sum += d;
    }
    return sum % 10 === 0;
}

/**
 * Text with personal data replaced, and which kinds were found.
 */
export type Redacted = {
    readonly text:  string;
    readonly found: readonly PiiKind[];
};

export const Pii = {
    redact(text: string, kinds: readonly PiiKind[]): Redacted {
        const found = new Set<PiiKind>();
        let out = text;
        if (kinds.includes("card")) {
            out = out.replace(/\b\d(?:[ -]?\d){12,18}\b/g, match => {
                const digits = match.replace(/\D/g, "");
                if (digits.length < 13 || digits.length > 19 || !luhn(digits)) {
                    return match;
                }
                found.add("card");
                return REPLACEMENT.card;
            });
        }
        for (const kind of ["ssn", "bank_account", "birth_date"] as const) {
            if (!kinds.includes(kind)) {
                continue;
            }
            out = out.replace(PATTERNS[kind], () => {
                found.add(kind);
                return REPLACEMENT[kind];
            });
        }
        return { text: out, found: [...found] };
    },
} as const;

/**
 * Words that suggest someone may be in danger, for businesses marked sensitive.
 * A match adds safety instructions to the turn and alerts staff; it is a prompt, not a diagnosis.
 */
const CRISIS_RE = /\b(kill(ing)? my ?self|suicid(e|al)|end (it all|my life)|don'?t want to (live|be alive)|hurt(ing)? my ?self|self[- ]harm|overdos(e|ing)|no reason to live|matarme|suicidarme|quitarme la vida)\b/i;

export const Crisis = {
    suspected(text: string): boolean {
        return CRISIS_RE.test(text);
    },
} as const;
