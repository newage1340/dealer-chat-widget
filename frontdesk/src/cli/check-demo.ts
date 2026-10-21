/**
 * Check demo tenant files end to end: the tenant parses, its domain pack loads,
 * and its catalog source yields items with every required field.
 * Usage: node src/cli/check-demo.ts [tenants/demo/x.json ...]   (no arguments: every demo tenant)
 */

import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";

import { fetchRows, mapColumns } from "../catalogs/index.ts";
import { ROOT } from "../config.ts";
import { Catalog, DomainRegistry } from "../domains/index.ts";
import { Hours } from "../prompts/hours.ts";
import { parseTenant } from "../tenants/schema.ts";

const DEMO_DIR = join(ROOT, "tenants", "demo");
const args = process.argv.slice(2);
const paths = args.length > 0
    ? args
    : (await readdir(DEMO_DIR)).filter(n => n.endsWith(".json")).sort().map(n => join(DEMO_DIR, n));

const registry = await DomainRegistry.fromDirectory();
let failed = false;
const fail = (path: string, message: string) => {
    failed = true;
    console.log(`FAIL ${path}: ${message}`);
};

for (const path of paths) {
    const parsed = parseTenant(JSON.parse(await readFile(path, "utf8")));
    if (!parsed.ok) {
        fail(path, parsed.problems.map(p => `${p.path}: ${p.message}`).join("; "));
        continue;
    }
    const tenant = parsed.tenant;
    let pack;
    try {
        pack = registry.get(tenant.domain);
    } catch (e) {
        fail(path, String(e));
        continue;
    }
    const days = Object.keys(Hours.parse(tenant.hoursText));
    if (days.length !== 7) {
        fail(path, `hoursText covers ${days.length} of 7 days (${days.join(", ") || "none"}) - write it like "Monday-Friday: 9am to 6pm, Saturday: 10am to 4pm, Sunday: closed"`);
        continue;
    }
    const rows = await fetchRows(tenant.settings.catalog.source);
    if (rows === null) {
        console.log(`ok   ${path} (${tenant.slug}: manual catalog)`);
        continue;
    }
    if (!rows.ok) {
        fail(path, rows.error);
        continue;
    }
    const spec = pack.catalog;
    const mapped = mapColumns(rows.value, tenant.settings.catalog.fieldMap);
    const known = new Set([...spec.fields.map(f => f.name), ...Object.keys(spec.aliases)]);
    const unknown = new Set(mapped.flatMap(r => Object.keys(r)).filter(k => !known.has(k.trim().toLowerCase())));
    const items = mapped.map((r, i) => Catalog.normalize(r, spec, tenant.id, i, new Date()));
    const missing = items.flatMap(item => spec.fields
        .filter(f => f.required && (item.fields[f.name] === undefined || item.fields[f.name] === ""))
        .map(f => `${item.id} lacks ${f.name}`));
    if (items.length === 0) {
        fail(path, "the dataset has no rows");
        continue;
    }
    if (missing.length > 0) {
        fail(path, missing.slice(0, 5).join("; "));
        continue;
    }
    if (unknown.size > 0) {
        console.log(`warn ${path}: columns the ${pack.id} pack ignores: ${[...unknown].join(", ")}`);
    }
    console.log(`ok   ${path} (${tenant.slug}: ${pack.id}, ${items.length} ${spec.itemNounPlural})`);
    for (const item of items.slice(0, 3)) {
        console.log(`       ${Catalog.summary(item, spec)}`);
    }
}
process.exit(failed ? 1 : 0);
