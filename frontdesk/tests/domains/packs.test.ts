import { readFile } from "node:fs/promises";

import { parse } from "smol-toml";
import { describe, expect, it } from "vitest";

import { DomainId } from "../../src/core/index.ts";
import { DomainPack, DomainRegistry, loadPack, PACKS_DIR, parsePack } from "../../src/domains/index.ts";

const CAR_WORDS = /\b(vehicle|car|cars|dealer|dealership|test drive|trade-in|carfax|mileage|financing)\b/i;

async function packData(name: string): Promise<Record<string, unknown>> {
    return parse(await readFile(`${PACKS_DIR}${name}`, "utf8")) as Record<string, unknown>;
}

function problemsOf(data: unknown): string[] {
    const result = parsePack(data);
    return result.ok
        ? []
        : result.error.map(p => `${p.path}: ${p.message}`);
}

describe("bundled packs", () => {
    it("load into the registry, skipping templates", async () => {
        const registry = await DomainRegistry.fromDirectory();
        expect(registry.ids().sort()).toEqual([
            "auto_repair", "automotive", "boxing_gym", "chinese_restaurant", "computer_repair", "esl_tutor",
            "hair_salon", "nail_salon", "plumbing", "real_estate", "talent_agency", "therapy_practice",
        ]);
    });

    it("include the template, which must also validate", async () => {
        await expect(loadPack(`${PACKS_DIR}_template.toml`)).resolves.toMatchObject({ id: "example" });
    });

    it("keep plumbing free of car vocabulary", async () => {
        const registry = await DomainRegistry.fromDirectory();
        const plumbing = registry.get(DomainId("plumbing"));
        expect(JSON.stringify(plumbing)).not.toMatch(CAR_WORDS);
    });

    it("convert to camelCase with defaults applied", async () => {
        const plumbing = (await DomainRegistry.fromDirectory()).get(DomainId("plumbing"));
        expect(plumbing.booking.location).toBe("customer");
        expect(plumbing.catalog.idField).toBe("code");
        expect(DomainPack.field(plumbing, "price_range")?.emptyText).toBe("Quoted on site");
        expect(DomainPack.intakeFor(plumbing, "chat").map(f => f.key)).toEqual(["problem", "urgency", "property"]);
        expect(DomainPack.topics(plumbing).at(-1)?.id).toBe("other");
        expect(plumbing.voice.speechHints).toContain("water heater");
        expect(plumbing.followups.confirm?.en).toContain("{business_name}");
    });
});

describe("validation", () => {
    it("reports every problem at once", async () => {
        const data = await packData("plumbing.toml");
        const catalog = data["catalog"] as Record<string, unknown>;
        const broken = {
            ...data,
            catalog:  { ...catalog, price_feild: "price_range", title_template: "{name} {colour}" },
            prompts:  { ...(data["prompts"] as object), chat: "Hi from {buisness_name} {{literal}}" },
            followups: { confirm: { es: "Hola" } },
        };
        const problems = problemsOf(broken);
        expect(problems).toEqual(expect.arrayContaining([
            expect.stringMatching(/^catalog: .*price_feild/),
            expect.stringMatching(/^followups\.confirm: translations must include English/),
        ]));
    });

    it("checks cross-references once the shape is right", async () => {
        const data = await packData("plumbing.toml");
        const broken = {
            ...data,
            catalog:  { ...(data["catalog"] as object), title_template: "{name} {colour}" },
            prompts:  { ...(data["prompts"] as object), chat: "Hi from {buisness_name} {{literal}}" },
            topics:   [...(data["topics"] as object[]), { id: "other", label: "Other", description: "x" }],
        };
        expect(problemsOf(broken)).toEqual([
            "topics: 'other' is added automatically; remove it",
            "catalog.title_template: refers to unknown catalog field 'colour'",
            "prompts.chat: unknown placeholder {buisness_name}",
        ]);
    });

    it("accepts translated follow-ups", async () => {
        const data = await packData("plumbing.toml");
        const result = parsePack({
            ...data,
            followups: { confirm: { en: "Booked for {visit_time}.", es: "Reservado para {visit_time}." } },
        });
        expect(result.ok && result.value.followups.confirm).toEqual({ en: "Booked for {visit_time}.", es: "Reservado para {visit_time}." });
    });
});

describe("lint", () => {
    it("finds nothing to warn about in the bundled packs", async () => {
        const { lintPack } = await import("../../src/domains/index.ts");
        const { TOOL_NAMES } = await import("../../src/tools/index.ts");
        const registry = await DomainRegistry.fromDirectory();
        const warnings = registry.ids().flatMap(id => lintPack(registry.get(id), TOOL_NAMES).map(w => `${id}: ${w}`));
        expect(warnings).toEqual([]);
    });

    it("knows every tool the model can be given", async () => {
        const { TOOL_NAMES, toolsFor } = await import("../../src/tools/index.ts");
        const { readdir, readFile } = await import("node:fs/promises");
        const dir = new URL("../../src/tools/", import.meta.url);
        const files = (await readdir(dir)).filter(f => f.endsWith(".ts"));
        const sources = await Promise.all(files.map(f => readFile(new URL(f, dir), "utf8")));
        const defined = sources.flatMap(source => [...source.matchAll(/name:\s+"([a-z_]+)",\n\s+(?:promise|description)/g)].map(m => m[1]));
        expect(new Set(defined)).toEqual(TOOL_NAMES);
        expect(typeof toolsFor).toBe("function");
    });

    it("warns about a tenant setup that won't work well", async () => {
        const { lintTenant } = await import("../../src/tenants/lint.ts");
        const { tenant } = await import("../support/fixtures.ts");
        const registry = await DomainRegistry.fromDirectory();
        const t = tenant({ id: "x", slug: "x", domain: "plumbing", hoursText: "Mon-Fri 8am-5pm" });
        const warnings = lintTenant(t, registry.get(t.domain), []);
        expect(warnings.join("\n")).toMatch(/only cover 5 of 7 days/);
        expect(warnings.join("\n")).toMatch(/"Service area" is not answered/);
        expect(warnings.join("\n")).toMatch(/No staff email/);
    });
});
