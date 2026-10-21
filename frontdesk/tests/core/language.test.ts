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
