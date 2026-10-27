import { describe, expect, it } from "vitest";

import { loadScenarios, runScenario } from "../src/evals/run.ts";
import { FakeLlm } from "../src/providers/index.ts";
import { openStore } from "../src/storage/index.ts";
import { plumber } from "./support/fixtures.ts";

describe("evals", () => {
    it("load, run against a model, and report failures per turn", async () => {
        const source = await openStore("", null);
        await source.tenants.save(plumber());
        const [troll] = (await loadScenarios(new URL("../evals/plumbing-troll.json", import.meta.url).pathname))
            .map(s => ({ ...s, tenant: "demo-plumber" }));
        const flags = new FakeLlm(() => ({ text: "Ha - anything plumbing I can help with?", tools: [{ name: "flag_conversation", arguments: { kind: "off_topic", note: "x" } }] }));
        expect(await runScenario(troll!, source, flags, {})).toMatchObject({ passed: true, failures: [] });

        const ignores = new FakeLlm(() => "Sure, bananas are great.");
        const result = await runScenario(troll!, source, ignores, {});
        expect(result.passed).toBe(false);
        expect(result.failures[0]).toContain("expected turn_flagged");
    });
});
