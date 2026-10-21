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


/**
 * Common short words that mark a language written in the Latin alphabet.
 * Only words that are rare in the other listed languages are included, so a hit means something.
 */
const MARKERS: Readonly<Partial<Record<Language, ReadonlySet<string>>>> = {
    en: new Set(("the and you your are is to we it that for have can what with this will be of in on do "
        + "don't i'm it's yes please thank thanks hello okay sure would like our us my me how when "
        + "here there just about get let's sounds good great").split(" ")),
    es: new Set(("el la los las que y en un una es por para con sí su lo se usted puede tenemos gracias hola "
        + "buenos buenas días quiero está como cómo qué cuándo muy mi le nos del al pero más hablar "
        + "español claro bueno hay puedo ayudar cita").split(" ")),
    fr: new Set(("le les des et une est pour avec vous nous je pas qui dans sur oui merci bonjour "
        + "votre c'est au du très ce il elle peux pouvez parler français d'accord bien rendez-vous").split(" ")),
    pt: new Set(("os um uma é com não sim você obrigado obrigada olá nós do da na mais "
        + "posso falar português tudo bom ajudar consulta").split(" ")),
    tl: new Set(("ang ng mga sa ay at po opo ko mo ka ako ikaw siya kami namin natin salamat kumusta "
        + "hindi oo may ba lang din rin naman ito iyan gusto pwede puwede magandang tagalog").split(" ")),
    ht: new Set(("mwen ou li nou yo se pa ak nan pou kounye mèsi bonjou wi kijan ki sa gen vle kreyòl "
        + "tanpri kapab").split(" ")),
};

const SCRIPTS: readonly [RegExp, Language][] = [
    [/\p{Script=Hangul}/u, "ko"],
    [/\p{Script=Han}/u, "zh"],
    [/\p{Script=Arabic}/u, "ar"],
    [/\p{Script=Cyrillic}/u, "ru"],
    [/[ơưăđƠƯĂĐẠ-ỹ]/u, "vi"],
];

export const LanguageGuess = {
    /**
     * The language `text` is written in, or null when it is too short or too mixed to tell.
     * Cheap and local: the writing system decides for non-Latin scripts and Vietnamese,
     * and marker words decide between the Latin-alphabet languages.
     */
    of(text: string): Language | null {
        const letters = text.replace(/<[^>]+>/g, " ");
        for (const [pattern, language] of SCRIPTS) {
            const all = letters.match(new RegExp(pattern.source, "gu")) ?? [];
            if (all.length >= 2) {
                return language;
            }
        }
        const words = letters.toLowerCase().match(/[\p{L}']+/gu) ?? [];
        if (words.length < 3) {
            return null;
        }
        const scores = Object.entries(MARKERS)
            .map(([language, markers]) => ({
                language: language as Language,
                hits:     words.filter(w => markers.has(w)).length,
            }))
            .sort((a, b) => b.hits - a.hits);
        const [best, second] = [scores[0]!, scores[1]!];
        if (best.hits < 2 || best.hits < second.hits * 2) {
            return null;
        }
        return best.language;
    },
} as const;
