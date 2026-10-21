/**
 * Turning reply text into something a text-to-speech engine reads naturally.
 *
 * Domain packs supply pronunciations (e.g. "F-250" -> "F two-fifty") and whether years are spoken
 * ("2015" -> "twenty fifteen"); engine-specific fixes live here.
 */

import type { DomainPack } from "../domains/index.ts";

const ONES = [
    "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten",
    "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen", "eighteen", "nineteen",
];
const TENS = ["", "", "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety"];
const YEAR_RE = /(?<![\d$,.])\b(19[5-9]\d|20[0-3]\d)\b(?!\d)/g;
const SSML_TAG_RE = /<(break|emphasis|prosody|say-as|sub|p|s)\b/i;
const BIG_NUMBER_RE = /\$\s?\d{1,3}(?:,\d{3})+(?:\.\d+)?|\$\s?\d+(?:\.\d+)?|\b\d{1,3}(?:,\d{3})+\b/g;

export const Speech = {
    intToWords(n: number): string {
        if (n < 20) {
            return ONES[n]!;
        }
        if (n < 100) {
            return TENS[Math.floor(n / 10)]! + (n % 10 ? ` ${ONES[n % 10]}` : "");
        }
        if (n < 1000) {
            return `${ONES[Math.floor(n / 100)]} hundred` + (n % 100 ? ` ${Speech.intToWords(n % 100)}` : "");
        }
        if (n < 1_000_000) {
            return `${Speech.intToWords(Math.floor(n / 1000))} thousand` + (n % 1000 ? ` ${Speech.intToWords(n % 1000)}` : "");
        }
        return String(n);
    },

    spellYear(year: number): string {
        if (year >= 2000 && year <= 2009) {
            return "two thousand" + (year % 10 ? ` ${ONES[year % 10]}` : "");
        }
        const hi = Math.floor(year / 100);
        const lo = year % 100;
        const tail = lo === 0
            ? "hundred"
            : lo < 10
                ? `oh ${ONES[lo]}`
                : Speech.intToWords(lo);
        return `${Speech.intToWords(hi)} ${tail}`;
    },

    applyDomain(text: string, pack: DomainPack): string {
        let out = text;
        for (const p of pack.voice.pronunciations) {
            out = out.replace(new RegExp(p.pattern, "gi"), p.say);
        }
        if (pack.voice.speakYears) {
            out = out.replace(YEAR_RE, (_, y: string) => Speech.spellYear(Number(y)));
        }
        return out;
    },

    /**
     * "$7,500" -> "seven thousand five hundred dollars" (some engines misread grouped digits).
     */
    speakBigNumbers(text: string): string {
        return text.replace(BIG_NUMBER_RE, token => {
            const digits = (token.split(".")[0] ?? "").replace(/\D/g, "");
            if (!digits) {
                return token;
            }
            return Speech.intToWords(Number(digits)) + (token.trimStart().startsWith("$") ? " dollars" : "");
        });
    },

    /**
     * Text for Twilio <Say> with Amazon Polly: SSML is kept and wrapped in <speak> when present.
     */
    forPolly(text: string, pack: DomainPack): string {
        const out = Speech.applyDomain(text.trim(), pack).replaceAll("&", " and ");
        return SSML_TAG_RE.test(out) && !out.toLowerCase().startsWith("<speak>")
            ? `<speak>${out}</speak>`
            : out;
    },

    /**
     * Text for streaming engines (Vapi/ElevenLabs, our own pipeline): SSML stripped, numbers spelled out.
     */
    forStreaming(text: string, pack: DomainPack): string {
        const out = Speech.speakBigNumbers(Speech.applyDomain(text, pack).replace(/<[^>]+>/g, ""));
        return out.replace(/[ \t]+/g, " ");
    },

    /**
     * Digits read one at a time, grouped like a phone number: "+13175550100" -> "3 1 7, 5 5 5, 0 1 0 0".
     */
    spokenDigits(phone: string): string {
        const d = phone.replace(/\D/g, "").replace(/^1(?=\d{10}$)/, "");
        if (d.length !== 10) {
            return d.split("").join(" ");
        }
        return [d.slice(0, 3), d.slice(3, 6), d.slice(6)].map(g => g.split("").join(" ")).join(", ");
    },
} as const;
