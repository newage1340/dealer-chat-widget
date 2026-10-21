import { describe, expect, it } from "vitest";

import { FOLLOWUP_FIELDS, Template } from "../../src/domains/index.ts";

describe("Template", () => {
    it("renders fields and literal braces", () => {
        expect(Template.render("Hi {first_name}, {{not a field}}", { first_name: "Alex" })).toBe("Hi Alex, {not a field}");
        expect(Template.render("{year} {make}", { make: "Honda" })).toBe(" Honda");
    });

    it("finds unknown placeholders and bad braces", () => {
        expect(Template.problems("{business_name} {setting_fee_phrase} {profile_hours}")).toEqual([]);
        expect(Template.problems("{first_name}")).toEqual(["unknown placeholder {first_name}"]);
        expect(Template.problems("{first_name}", FOLLOWUP_FIELDS)).toEqual([]);
        expect(Template.problems("price is {")).toEqual(["unclosed '{' (write a literal brace as '{{')"]);
        expect(Template.problems("a } b")).toEqual(["single '}' (write a literal brace as '}}')"]);
    });
});
