/**
 * Catalog connectors: each reads one kind of CatalogSource into raw rows.
 *
 * Rows are plain key/value records in the source's own column names; sync.ts maps them onto the
 * domain's catalog fields. A new kind of source (Square, Shopify, an MLS feed) is one more connector
 * here and one more CatalogSource variant; nothing downstream changes.
 */

import { readFile } from "node:fs/promises";
import { join } from "node:path";

import { ROOT } from "../config.ts";
import { Result, type CatalogSource } from "../core/index.ts";
import { parseCsv } from "./csv.ts";

export type RawRow = Readonly<Record<string, unknown>>;

export const DATASETS_DIR = join(ROOT, "datasets");

/**
 * `fetch` is injectable so tests (and the settings page preview) never touch the network.
 */
export type ConnectorDeps = {
    readonly fetch:       typeof globalThis.fetch;
    readonly datasetsDir: string;
};

export const DEFAULT_CONNECTOR_DEPS: ConnectorDeps = {
    fetch:       globalThis.fetch.bind(globalThis),
    datasetsDir: DATASETS_DIR,
};

const TIMEOUT_MS = 15_000;
const MAX_BYTES = 5_000_000;

function rowsFromJson(data: unknown, itemsPath: string): Result<RawRow[], string> {
    let node: unknown = data;
    for (const key of itemsPath.split(".").filter(Boolean)) {
        node = typeof node === "object" && node !== null
            ? (node as Record<string, unknown>)[key]
            : undefined;
    }
    if (itemsPath === "" && !Array.isArray(node) && typeof node === "object" && node !== null) {
        // A body like {"items": [...]}: take its only array.
        const arrays = Object.values(node).filter(Array.isArray);
        node = arrays.length === 1 ? arrays[0] : node;
    }
    if (!Array.isArray(node)) {
        return Result.err(itemsPath ? `nothing is a list at "${itemsPath}"` : "the response is not a list of rows");
    }
    return Result.ok(node.filter((r): r is RawRow => typeof r === "object" && r !== null && !Array.isArray(r)));
}

async function download(url: string, deps: ConnectorDeps): Promise<Result<string, string>> {
    try {
        const response = await deps.fetch(url, { signal: AbortSignal.timeout(TIMEOUT_MS), redirect: "follow" });
        if (!response.ok) {
            return Result.err(`${url} answered ${response.status}`);
        }
        const text = await response.text();
        return text.length > MAX_BYTES
            ? Result.err(`${url} is larger than ${MAX_BYTES / 1_000_000} MB`)
            : Result.ok(text);
    } catch (e) {
        return Result.err(`could not fetch ${url}: ${e instanceof Error ? e.message : String(e)}`);
    }
}

function parse(text: string, csv: boolean, itemsPath: string): Result<RawRow[], string> {
    if (csv) {
        return Result.ok(parseCsv(text));
    }
    try {
        return rowsFromJson(JSON.parse(text), itemsPath);
    } catch {
        return Result.err("not valid JSON");
    }
}

/**
 * Raw rows from a source, or why they could not be read. `manual` sources have nothing to fetch.
 */
export async function fetchRows(source: CatalogSource, deps: ConnectorDeps = DEFAULT_CONNECTOR_DEPS): Promise<Result<RawRow[], string> | null> {
    switch (source.kind) {
        case "manual":
            return null;
        case "dataset": {
            if (!/^[a-z0-9][a-z0-9_.-]*$/.test(source.name)) {
                return Result.err(`bad dataset name '${source.name}'`);
            }
            try {
                const text = await readFile(join(deps.datasetsDir, source.name), "utf8");
                return parse(text, source.name.endsWith(".csv"), "");
            } catch {
                return Result.err(`no dataset '${source.name}' in datasets/`);
            }
        }
        case "csv_url":
        case "json_url": {
            const text = await download(source.url, deps);
            if (!text.ok) {
                return text;
            }
            return parse(text.value, source.kind === "csv_url", source.kind === "json_url" ? source.itemsPath : "");
        }
    }
}
