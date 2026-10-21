/**
 * Command line entry point.
 *
 *   node src/main.ts                                   run the server (default)
 *   node src/main.ts migrate                           bring the database schema up to date
 *   node src/main.ts import-tenants [dir]              load tenant JSON files (legacy or new format)
 *   node src/main.ts prompt <slug> [channel] [message] print the exact prompt a turn would get
 *   node src/main.ts author-prompt "<business type>"   print the pack-authoring prompt leader
 *   node src/main.ts author-domain "<business type>" [--context "..."] [--overwrite]
 *   node src/main.ts eval [file-or-dir] [--packs | --all] [--repeat N] [--variant id] [--verbose]
 *                                                      run scripted conversations against the configured model
 *   node src/main.ts seed-demo [dir]                   load the demo businesses (tenants/demo) and their catalogs
 */

import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";

import { serve } from "@hono/node-server";

import { loadConfig, ROOT } from "./config.ts";
import { syncCatalog } from "./catalogs/index.ts";
import { Catalog } from "./domains/index.ts";
import { authoringPrompt, generatePack, savePack } from "./domains/authoring.ts";
import { generatedScenarios } from "./evals/generate.ts";
import { loadScenarios, runScenario } from "./evals/run.ts";
import { previewPrompt, SAMPLE_MESSAGE } from "./http/admin.ts";
import { buildApp, buildLlm, loadPrices } from "./server.ts";
import { fromLegacy } from "./tenants/legacy.ts";
import { parseTenant } from "./tenants/schema.ts";

async function runServer(): Promise<void> {
    const config = loadConfig();
    const app = await buildApp(config);
    if (config.runWorker) {
        app.worker.start();
    }
    const server = serve({ fetch: app.http.fetch, port: config.port }, info => {
        console.info(`front desk listening on http://localhost:${info.port}`);
    });
    app.injectWebSocket(server);
    const stop = async () => {
        app.worker.stop();
        server.close();
        await app.store.close();
        process.exit(0);
    };
    process.on("SIGINT", stop);
    process.on("SIGTERM", stop);
}

async function importTenants(dir: string): Promise<void> {
    const app = await buildApp(loadConfig());
    const files = (await readdir(dir)).filter(f => f.endsWith(".json")).sort();
    for (const file of files) {
        const raw = JSON.parse(await readFile(join(dir, file), "utf8")) as Record<string, unknown>;
        const legacy = "hours" in raw || "transfer_to" in raw || "_source" in raw;
        const { tenant: input, catalog } = legacy
            ? fromLegacy(raw)
            : { tenant: raw, catalog: (raw["catalog"] as Record<string, unknown>[] | undefined) ?? [] };
        const { catalog: _, ...withoutCatalog } = input as Record<string, unknown>;
        const result = parseTenant(withoutCatalog);
        if (!result.ok) {
            console.error(`skip ${file}:\n${result.problems.map(p => `  - ${p.path}: ${p.message}`).join("\n")}`);
            continue;
        }
        const tenant = result.tenant;
        const spec = app.services.registry.get(tenant.domain).catalog;
        await app.store.tenants.save(tenant);
        const now = new Date();
        await app.store.tenants.replaceCatalog(
			tenant.id,
			catalog.map((row, i) => Catalog.normalize(row, spec, tenant.id, i, now))
		);
        console.info(`imported ${tenant.slug} (${tenant.domain}, ${catalog.length} catalog items)`);
    }
    await app.store.close();
}

/**
 * Load the demo tenants and sync their catalogs. Phone numbers and staff settings already in the
 * database are kept, so re-seeding never undoes what was set on the settings page.
 */
async function seedDemo(dir: string): Promise<void> {
    const app = await buildApp(loadConfig());
    const files = (await readdir(dir)).filter(f => f.endsWith(".json")).sort();
    for (const file of files) {
        const result = parseTenant(JSON.parse(await readFile(join(dir, file), "utf8")));
        if (!result.ok) {
            console.error(`skip ${file}:\n${result.problems.map(p => `  - ${p.path}: ${p.message}`).join("\n")}`);
            continue;
        }
        const existing = await app.store.tenants.byId(result.tenant.id);
        const tenant = existing === null
            ? result.tenant
            : { ...result.tenant, endpoints: existing.endpoints, staffContacts: existing.staffContacts };
        await app.store.tenants.save(tenant);
        const synced = await syncCatalog(app.services, tenant);
        console.info(`seeded ${tenant.slug} (${tenant.domain}): ${synced === null ? "manual catalog" : synced.ok ? `${synced.value} items` : `catalog failed: ${synced.error}`}`);
    }
    await app.store.close();
}

async function printPrompt(
    slug: string,
    channel: string,
    message: string
): Promise<void> {
    const config = loadConfig();
    const app = await buildApp(config);
    const tenant = await app.store.tenants.bySlug(slug);
    if (tenant === null) {
        console.error(`no tenant '${slug}'`);
        process.exit(1);
    }
    const ch = channel === "voice" || channel === "web"
		? channel
		: "sms";
    console.info(await previewPrompt({
		config,
		services: app.services,
		engine: app.engine
	}, tenant, ch, message
	));
    await app.store.close();
}

async function authorDomain(
    businessType: string,
    context: string,
    overwrite: boolean
): Promise<void> {
    const config = loadConfig();
    const prices = await loadPrices(config.pricesFile);
    const llm = buildLlm(config.llm, { record: async () => {} }, prices);
    if (!llm.hasRoutes()) {
        console.error("Set OPENAI_API_KEY or LOCAL_LLM_BASE_URL to generate a pack.");
        process.exit(1);
    }
    console.info(`asking the model for a "${businessType}" pack...`);
    const { text, pack } = await generatePack(llm, businessType, context);
    const path = await savePack(text, pack, overwrite);
    console.info(`saved ${path} (${pack.id}: ${pack.catalog.fields.length} catalog fields, ${pack.topics.length} topics)`);
    console.info("Review it, then restart the server to load it.");
}

/**
 * Run every scenario `repeat` times, print pass rates, and keep the results in the quality log
 * (so pass rates can be compared per model and prompt variant over time, at /settings/quality).
 */
async function runEvals(
    path: string,
    verbose: boolean,
    repeat: number,
    variant: string | undefined,
    which: "files" | "packs" | "all"
): Promise<void>
{
    const config = loadConfig();
    const app = await buildApp(config);
    const prices = await loadPrices(config.pricesFile);
    let spent = 0;
    const llm = buildLlm(config.llm, { record: async u => { spent += u.costUsd ?? 0; } }, prices);
    if (!llm.hasRoutes()) {
        console.error("Set OPENAI_API_KEY or LOCAL_LLM_BASE_URL to run evals.");
        process.exit(1);
    }
    if (variant !== undefined && app.services.composer.variant(variant) === null) {
        console.error(`no prompt variant '${variant}' in src/prompts/variants/variants.json`);
        process.exit(1);
    }
    const scenarios = [
        ...(which !== "packs"
				? await loadScenarios(path)
				: []
		   ),
        ...(which !== "files"
				? await generatedScenarios(app.store, app.services.registry)
				: []
		   ),
    ];
    if (which !== "files" && !scenarios.some(s => s.name.includes("(generated)"))) {
        console.warn("no demo tenants in the database to generate pack checks from; run `pnpm seed-demo` first");
    }
    const runId = new Date().toISOString();
    const model = config.llm.primary === "local"
		? config.llm.localModels.capable
		: config.llm.openaiModels.capable;
    let flawless = 0;
    let cost = 0;
    for (const scenario of scenarios) {
        let passes = 0;
        for (let i = 0; i < repeat; i++) {
            const before = spent;
            const result = await runScenario(scenario, app.store, llm, prices, variant);
            const scenarioCost = spent - before + result.costUsd;
            cost += scenarioCost;
            passes += result.passed ? 1 : 0;
            await app.store.quality.recordEval({
                runId, scenario: scenario.name, variant: variant ?? "control", model,
                passed: result.passed, failures: result.failures, costUsd: scenarioCost, ranAt: new Date(),
            });
            if (repeat === 1) {
                console.info(`${result.passed ? "PASS" : "FAIL"}  ${result.name}  ($${scenarioCost.toFixed(4)})`);
            }
            for (const failure of result.failures) {
                console.info(`      - ${failure}`);
            }
            if (!result.passed || verbose) {
                console.info(result.log.map(l => `        ${l}`).join("\n"));
            }
        }
        if (repeat > 1) {
            console.info(`${passes === repeat ? "PASS" : "FLAKY"}  ${scenario.name}  ${passes}/${repeat}`);
        }
        flawless += passes === repeat ? 1 : 0;
    }
	let runDetails = repeat > 1 ? ` (${repeat} runs each)` : "";
    console.info(`\n${flawless}/${scenarios.length} passed every run${runDetails}, $${cost.toFixed(4)} total`);
    await app.store.close();
    process.exit(flawless === scenarios.length ? 0 : 1);
}

function option(args: string[], name: string): string {
    const i = args.indexOf(name);
    return i >= 0 ? args[i + 1] ?? "" : "";
}

const [command = "serve", ...args] = process.argv.slice(2);
switch (command) {
    case "serve":
        await runServer();
        break;
    case "migrate": {
        const app = await buildApp(loadConfig());
        console.info("schema up to date");
        await app.store.close();
        break;
    }
    case "import-tenants":
        await importTenants(args[0] ?? join(ROOT, "..", "data", "tenants"));
        break;
    case "seed-demo":
        await seedDemo(args[0] ?? join(ROOT, "tenants", "demo"));
        break;
    case "prompt":
        await printPrompt(args[0] ?? "", args[1] ?? "sms", args.slice(2).join(" ") || SAMPLE_MESSAGE);
        break;
    case "author-prompt":
        console.info(await authoringPrompt(args[0] ?? "", option(args, "--context")));
        break;
    case "author-domain":
        await authorDomain(args[0] ?? "", option(args, "--context"), args.includes("--overwrite"));
        break;
    case "eval":
        await runEvals(
            args.find((a, i) => !a.startsWith("--") && !["--repeat", "--variant"].includes(args[i - 1] ?? "")) ?? join(ROOT, "evals"),
            args.includes("--verbose"),
            Math.max(1, Number(option(args, "--repeat")) || 1),
            option(args, "--variant") || undefined,
            args.includes("--all") ? "all" : args.includes("--packs") ? "packs" : "files",
        );
        break;
    default:
        console.error(`unknown command '${command}'`);
        process.exit(1);
}
