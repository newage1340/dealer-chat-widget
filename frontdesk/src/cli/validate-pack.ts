/**
 * Check domain pack files and print every problem.
 * Usage: pnpm validate-pack packs/plumbing.toml [more.toml ...]   (no arguments: every bundled pack)
 */

import { readdir } from "node:fs/promises";
import { join } from "node:path";

import { DomainPackError, loadPack, PACKS_DIR } from "../domains/index.ts";

const args = process.argv.slice(2);
const paths = args.length > 0
    ? args
    : (await readdir(PACKS_DIR)).filter(n => n.endsWith(".toml")).sort().map(n => join(PACKS_DIR, n));

let failed = false;
for (const path of paths) {
    try {
        const pack = await loadPack(path);
        console.log(`ok   ${path} (${pack.id}: ${pack.catalog.fields.length} fields, ${pack.topics.length} topics)`);
    } catch (e) {
        failed = true;
        console.log(e instanceof DomainPackError ? `FAIL ${e.message}` : `FAIL ${path}: ${String(e)}`);
    }
}
process.exit(failed ? 1 : 0);
