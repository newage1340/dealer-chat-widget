import { describe, expect, it } from "vitest";

import { LanguageTag, LocalizedText } from "../../src/core/index.ts";

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
