/**
 * Reading domain packs from TOML, and the registry of loaded packs.
 */

import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";

import { parse as parseToml } from "smol-toml";

import { DomainId, errorMessage, Problem, Result } from "../core/index.ts";
import type { DomainPack } from "./pack.ts";
import { crossCheck, PackFile, toPack } from "./schema.ts";

export const PACKS_DIR = new URL("../../packs/", import.meta.url).pathname;

/**
 * A pack that failed to load, with every problem found (not just the first).
 */
export class DomainPackError extends Error {
    readonly source:   string;
    readonly problems: readonly Problem[];

    constructor(source: string, problems: readonly Problem[]) {
        super(`${source}:\n` + problems.map(p => `  - ${Problem.format(p)}`).join("\n"));
        this.name     = "DomainPackError";
        this.source   = source;
        this.problems = problems;
    }
}

/**
 * Validate already-parsed TOML data (also used to check model-written packs before saving).
 */
export function parsePack(data: unknown): Result<DomainPack, Problem[]> {
    const shape = PackFile.safeParse(data);
    if (!shape.success) {
        return Result.err(Problem.fromIssues(shape.error.issues));
    }
    const problems = crossCheck(shape.data);
    return problems.length > 0
        ? Result.err(problems)
        : Result.ok(toPack(shape.data));
}

export function parsePackToml(source: string): Result<DomainPack, Problem[]> {
    let data: unknown;
    try {
        data = parseToml(source);
    } catch (e) {
        return Result.err([{ path: "", message: errorMessage(e) }]);
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
        throw new DomainPackError(path, [{ path: "", message: errorMessage(e) }]);
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

    /**
     * The pack with this id, or null; for ids from outside (a form, an uploaded tenant file).
     */
    find(id: string): DomainPack | null {
        return this.#packs.get(DomainId(id)) ?? null;
    }

    ids(): DomainId[] {
        return [...this.#packs.keys()];
    }

    /**
     * Every pack as `[id, name]`, sorted by name, for pickers.
     */
    choices(): [DomainId, string][] {
        return [...this.#packs.values()]
            .map((p): [DomainId, string] => [p.id, p.name])
            .sort((a, b) => a[1].localeCompare(b[1]));
    }
}
