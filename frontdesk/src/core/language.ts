/**
 * Languages the assistant can converse in, and text that exists in several of them.
 *
 * The model replies in the customer's language directly; nothing is machine-translated per message.
 * Fixed strings (greetings, reprompts, opt-out confirmations, follow-up texts)
 * are written per language ahead of time, so they cost nothing at runtime.
 */

import type { ValueOf } from "./lang.ts";

/**
 * BCP-47 primary tags for the languages most spoken by limited-English households in the US.
 * `zh` means Mandarin written in simplified characters; Cantonese would be added as `yue`.
 */
export const Language = {
    English:       "en",
    Spanish:       "es",
    Chinese:       "zh",
    Vietnamese:    "vi",
    Tagalog:       "tl",
    Arabic:        "ar",
    Korean:        "ko",
    Russian:       "ru",
    French:        "fr",
    HaitianCreole: "ht",
    Portuguese:    "pt",
} as const;
export type Language = ValueOf<typeof Language>;

const TAGS: ReadonlySet<string> = new Set(Object.values(Language));

/**
 * English names, for prompts and staff-facing text.
 */
export const LANGUAGE_NAMES: Readonly<Record<Language, string>> = {
    en: "English",
    es: "Spanish",
    zh: "Mandarin Chinese",
    vi: "Vietnamese",
    tl: "Tagalog",
    ar: "Arabic",
    ko: "Korean",
    ru: "Russian",
    fr: "French",
    ht: "Haitian Creole",
    pt: "Portuguese",
};

/**
 * Text with an English original and optional translations.
 */
export type LocalizedText = { readonly en: string } & Readonly<Partial<Record<Language, string>>>;

export const LocalizedText = {
    /**
     * The text in `language`, falling back to English when no translation exists.
     */
    in(text: LocalizedText, language: Language): string {
        return text[language] ?? text.en;
    },
} as const;

export const LanguageTag = {
    /**
     * A supported Language from a tag like "es-MX" or "zh-Hans", or null if unsupported.
     */
    parse(tag: string): Language | null {
        const primary = tag.trim().toLowerCase().split(/[-_]/)[0] ?? "";
        return TAGS.has(primary)
            ? primary as Language
            : null;
    },
} as const;
