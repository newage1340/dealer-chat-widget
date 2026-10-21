/**
 * Command line entry point.
 *
 *   node src/main.ts                                   run the server (default)
 *   node src/main.ts migrate                           bring the database schema up to date
 *   node src/main.ts import-tenants [dir]              load tenant JSON files (legacy or new format)
 *   node src/main.ts prompt <slug> [channel] [message] print the exact prompt a turn would get
 *   node src/main.ts author-prompt "<business type>"   print the pack-authoring prompt leader
 *   node src/main.ts author-domain "<business type>" [--context "..."] [--overwrite]
 *   node src/main.ts eval [file-or-dir]               run scripted conversations against the configured model
 */

import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";

import { serve } from "@hono/node-server";

import { loadConfig, ROOT } from "./config.ts";
import { Catalog } from "./domains/index.ts";
import { authoringPrompt, generatePack, savePack } from "./domains/authoring.ts";
import { loadScenarios, runScenario } from "./evals/run.ts";
import { previewPrompt } from "./http/admin.ts";
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
        await app.store.tenants.replaceCatalog(tenant.id, catalog.map((row, i) => Catalog.normalize(row, spec, tenant.id, i, now)));
        console.info(`imported ${tenant.slug} (${tenant.domain}, ${catalog.length} catalog items)`);
    }
    await app.store.close();
}

async function printPrompt(slug: string, channel: string, message: string): Promise<void> {
    const config = loadConfig();
    const app = await buildApp(config);
    const tenant = await app.store.tenants.bySlug(slug);
    if (tenant === null) {
        console.error(`no tenant '${slug}'`);
        process.exit(1);
    }
    const ch = channel === "voice" || channel === "web" ? channel : "sms";
    console.info(await previewPrompt({ config, services: app.services, engine: app.engine }, tenant, ch, message));
    await app.store.close();
}

async function authorDomain(businessType: string, context: string, overwrite: boolean): Promise<void> {
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

async function runEvals(path: string): Promise<void> {
    const config = loadConfig();
    const app = await buildApp(config);
    const prices = await loadPrices(config.pricesFile);
    const llm = buildLlm(config.llm, { record: async () => {} }, prices);
    if (!llm.hasRoutes()) {
        console.error("Set OPENAI_API_KEY or LOCAL_LLM_BASE_URL to run evals.");
        process.exit(1);
    }
    const scenarios = await loadScenarios(path);
    let passed = 0;
    let cost = 0;
    for (const scenario of scenarios) {
        const result = await runScenario(scenario, app.store, llm, prices);
        cost += result.costUsd;
        passed += result.passed ? 1 : 0;
        console.info(`${result.passed ? "PASS" : "FAIL"}  ${result.name}  ($${result.costUsd.toFixed(4)})`);
        for (const failure of result.failures) {
            console.info(`      - ${failure}`);
        }
    }
    console.info(`\n${passed}/${scenarios.length} passed, $${cost.toFixed(4)} total`);
    await app.store.close();
    process.exit(passed === scenarios.length ? 0 : 1);
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
    case "prompt":
        await printPrompt(args[0] ?? "", args[1] ?? "sms", args.slice(2).join(" ") || "Hi, what do you have?");
        break;
    case "author-prompt":
        console.info(await authoringPrompt(args[0] ?? "", option(args, "--context")));
        break;
    case "author-domain":
        await authorDomain(args[0] ?? "", option(args, "--context"), args.includes("--overwrite"));
        break;
    case "eval":
        await runEvals(args[0] ?? join(ROOT, "evals"));
        break;
    default:
        console.error(`unknown command '${command}'`);
        process.exit(1);
}
