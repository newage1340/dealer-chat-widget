import { describe, expect, it } from "vitest";

import { LanguageGuess, LanguageTag, LocalizedText } from "../../src/core/index.ts";

describe("languages", () => {
    it.each([
        ["es-MX", "es"],
        ["zh-Hans", "zh"],
        ["EN_us", "en"],
        ["de", null],
    ])("LanguageTag.parse(%s) -> %s", (tag, expected) => {
        expect(LanguageTag.parse(tag)).toBe(expected);
    });

    it("falls back to English when a translation is missing", () => {
        const text: LocalizedText = { en: "Thanks for calling", es: "Gracias por llamar" };
        expect(LocalizedText.in(text, "es")).toBe("Gracias por llamar");
        expect(LocalizedText.in(text, "vi")).toBe("Thanks for calling");
    });
});

describe("LanguageGuess", () => {
    it.each([
        ["Sure, I can help you with that. What day works for you?", "en"],
        ["Claro, puedo ayudarle con su cita. ¿Qué día le queda bien?", "es"],
        ["Bonjour, je peux vous aider avec votre rendez-vous.", "fr"],
        ["Opo, pwede po kayong pumunta bukas. Salamat po!", "tl"],
        ["我们明天上午九点开门。", "zh"],
        ["안녕하세요, 무엇을 도와드릴까요?", "ko"],
        ["Chúng tôi mở cửa lúc chín giờ sáng.", "vi"],
        ["Okay!", null],
        ["Bye", null],
    ])("%s -> %s", (text, expected) => {
        expect(LanguageGuess.of(text)).toBe(expected);
    });

    it("hears English even after a French conversation", () => {
        expect(LanguageGuess.of("Of course, we can continue in English. How can I help you today?")).toBe("en");
    });
});

describe("fixed lines", () => {
    it("exist in every language, with the same placeholders as the English", async () => {
        const { STRINGS } = await import("../../src/prompts/strings.ts");
        const { Language } = await import("../../src/core/index.ts");
        const texts: [string, Record<string, string>][] = [];
        for (const [key, value] of Object.entries(STRINGS)) {
            if (Array.isArray(value)) {
                value.forEach((v, i) => texts.push([`${key}.${i}`, v as Record<string, string>]));
            } else if ("en" in value) {
                texts.push([key, value as Record<string, string>]);
            } else {
                Object.entries(value).forEach(([k, v]) => texts.push([`${key}.${k}`, v as Record<string, string>]));
            }
        }
        const placeholders = (t: string) => [...t.matchAll(/\{[a-z_]+\}/g)].map(m => m[0]).sort();
        const problems: string[] = [];
        for (const [key, text] of texts) {
            for (const language of Object.values(Language)) {
                if (text[language] === undefined) {
                    problems.push(`${key} has no ${language}`);
                } else if (placeholders(text[language]!).join() !== placeholders(text.en!).join()) {
                    problems.push(`${key}.${language} placeholders differ from English`);
                }
            }
        }
        expect(problems).toEqual([]);
    });
});
