import { describe, expect, it } from "vitest";

import { Crisis, Pii } from "../../src/core/index.ts";

describe("Pii.redact", () => {
    const all = ["card", "ssn", "bank_account", "birth_date"] as const;

    it("removes card numbers that pass the Luhn check, and leaves phone numbers alone", () => {
        const { text, found } = Pii.redact("my card is 4111 1111 1111 1111 and my cell is 317-555-0123", all);
        expect(text).toBe("my card is [card number removed] and my cell is 317-555-0123");
        expect(found).toEqual(["card"]);
    });

    it("keeps digit runs that are not card numbers", () => {
        expect(Pii.redact("order 1234567890123 shipped", all).text).toBe("order 1234567890123 shipped");
    });

    it("removes social security, account numbers and birth dates", () => {
        expect(Pii.redact("SSN 123-45-6789", all).text).toBe("SSN [SSN removed]");
        expect(Pii.redact("my social is 123456789", all).text).toBe("my [SSN removed]");
        expect(Pii.redact("routing number 021000021", all).text).toBe("[account number removed]");
        expect(Pii.redact("I was born on 4/12/1990", all).text).toBe("I was [birth date removed]");
        expect(Pii.redact("my date of birth is March 3, 1985", all).text).toBe("my [birth date removed]");
    });

    it("only removes the kinds asked for", () => {
        expect(Pii.redact("born on 4/12/1990", ["card"]).text).toBe("born on 4/12/1990");
    });
});

describe("Crisis.suspected", () => {
    it.each([
        ["I don't want to live anymore", true],
        ["I don't really want to live anymore", true],
        ["sometimes I want to die", true],
        ["ya no quiero vivir", true],
        ["sometimes I think about killing myself", true],
        ["my sink is killing me lol", false],
        ["I need to book a session", false],
    ])("%s -> %s", (text, expected) => {
        expect(Crisis.suspected(text)).toBe(expected);
    });
});
