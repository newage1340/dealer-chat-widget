/**
 * Reading domain packs from TOML, and the registry of loaded packs.
 */

import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";

import { parse as parseToml } from "smol-toml";

import { Result, type DomainId } from "../core/index.ts";
import type { DomainPack } from "./pack.ts";
import { crossCheck, PackFile, toPack, type PackProblem } from "./schema.ts";

export const PACKS_DIR = new URL("../../packs/", import.meta.url).pathname;

/**
 * A pack that failed to load, with every problem found (not just the first).
 */
export class DomainPackError extends Error {
    readonly source:   string;
    readonly problems: readonly PackProblem[];

    constructor(source: string, problems: readonly PackProblem[]) {
        super(`${source}:\n` + problems.map(p => `  - ${p.path || "(file)"}: ${p.message}`).join("\n"));
        this.name     = "DomainPackError";
        this.source   = source;
        this.problems = problems;
    }
}

/**
 * Validate already-parsed TOML data (also used to check model-written packs before saving).
 */
export function parsePack(data: unknown): Result<DomainPack, PackProblem[]> {
    const shape = PackFile.safeParse(data);
    if (!shape.success) {
        return Result.err(shape.error.issues.map(issue => ({
            path:    issue.path.join("."),
            message: issue.message,
        })));
    }
    const problems = crossCheck(shape.data);
    return problems.length > 0
        ? Result.err(problems)
        : Result.ok(toPack(shape.data));
}

export function parsePackToml(source: string): Result<DomainPack, PackProblem[]> {
    let data: unknown;
    try {
        data = parseToml(source);
    } catch (e) {
        return Result.err([{ path: "", message: e instanceof Error ? e.message : String(e) }]);
    }
    return parsePack(data);
}

/**
 * Throws DomainPackError when the file is missing or invalid.
 */
export async function loadPack(path: string): Promise<DomainPack> {
    let source: string;
    try {
        source = await readFile(path, "utf8");
    } catch (e) {
        throw new DomainPackError(path, [{ path: "", message: e instanceof Error ? e.message : String(e) }]);
    }
    const result = parsePackToml(source);
    if (!result.ok) {
        throw new DomainPackError(path, result.error);
    }
    return result.value;
}

export class DomainRegistry {
    readonly #packs: ReadonlyMap<DomainId, DomainPack>;

    constructor(packs: readonly DomainPack[]) {
        this.#packs = new Map(packs.map(p => [p.id, p]));
    }

    /**
     * Every `*.toml` in a directory except files starting with "_" (templates).
     */
    static async fromDirectory(dir: string = PACKS_DIR): Promise<DomainRegistry> {
        const names = (await readdir(dir))
            .filter(n => n.endsWith(".toml") && !n.startsWith("_"))
            .sort();
        return new DomainRegistry(await Promise.all(names.map(n => loadPack(join(dir, n)))));
    }

    get(id: DomainId): DomainPack {
        const pack = this.#packs.get(id);
        if (pack === undefined) {
            throw new Error(`unknown domain '${id}' (loaded: ${this.ids().join(", ")})`);
        }
        return pack;
    }

    ids(): DomainId[] {
        return [...this.#packs.keys()];
    }
}
