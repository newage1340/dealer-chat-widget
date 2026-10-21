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
