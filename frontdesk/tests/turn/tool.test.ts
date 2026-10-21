import { describe, expect, it } from "vitest";
import { z } from "zod";

import { Tool, ToolResult } from "../../src/turn/index.ts";

describe("Tool.define", () => {
    it("infers argument types from the schema", async () => {
        const echo = Tool.define({
            name:        "echo",
            description: "Repeat a phrase.",
            parameters:  z.object({ phrase: z.string(), times: z.number().int() }),
            followUp:    false,
            run:         args => ToolResult.output(args.phrase.repeat(args.times)),
        });
        const args = echo.parameters.parse({ phrase: "ab", times: 2 });
        const result = await echo.run(args, undefined as never);
        expect(result.output).toBe("abab");
        expect(result.directive).toBeNull();
    });
});

describe("parsePrice", async () => {
    const { parsePrice } = await import("../../src/tools/index.ts");
    it.each([
        ["15.95", { low: 15.95, high: 15.95, minimum: false }],
        ["$129-$249", { low: 129, high: 249, minimum: false }],
        ["from $45", { low: 45, high: 45, minimum: true }],
        ["$1,450+", { low: 1450, high: 1450, minimum: true }],
        ["Quoted on site", null],
        ["Free", { low: 0, high: 0, minimum: false }],
    ])("%s", (text, expected) => {
        expect(parsePrice(text)).toEqual(expected);
    });
});
