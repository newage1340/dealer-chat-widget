/**
 * A small CSV reader (RFC 4180: quoted fields, doubled quotes, newlines inside quotes).
 * Enough for spreadsheet exports and published Google Sheets; not a streaming parser.
 */

export type Row = Readonly<Record<string, string>>;

function records(text: string): string[][] {
    const out: string[][] = [];
    let row: string[] = [];
    let field = "";
    let quoted = false;
    for (let i = 0; i < text.length; i++) {
        const ch = text[i]!;
        if (quoted) {
            if (ch === "\"" && text[i + 1] === "\"") {
                field += "\"";
                i++;
            } else if (ch === "\"") {
                quoted = false;
            } else {
                field += ch;
            }
            continue;
        }
        if (ch === "\"" && field === "") {
            quoted = true;
        } else if (ch === ",") {
            row.push(field);
            field = "";
        } else if (ch === "\n" || ch === "\r") {
            if (ch === "\r" && text[i + 1] === "\n") {
                i++;
            }
            row.push(field);
            out.push(row);
            row = [];
            field = "";
        } else {
            field += ch;
        }
    }
    if (field !== "" || row.length > 0) {
        row.push(field);
        out.push(row);
    }
    return out.filter(r => r.some(f => f.trim() !== ""));
}

/**
 * Rows keyed by the header line's column names. Blank lines are skipped.
 */
export function parseCsv(text: string): Row[] {
    const [header, ...body] = records(text.replace(/^﻿/, ""));
    if (header === undefined) {
        return [];
    }
    const names = header.map(h => h.trim());
    return body.map(cells => Object.fromEntries(
        names.map((name, i) => [name, (cells[i] ?? "").trim()]).filter(([name]) => name !== "")
    ));
}

/**
 * Rows from pasted text: a JSON list (or an object holding one), otherwise CSV with a header row.
 */
export function parsePastedRows(text: string): Readonly<Record<string, unknown>>[] | string {
    const trimmed = text.trim();
    if (trimmed.startsWith("[") || trimmed.startsWith("{")) {
        try {
            const data: unknown = JSON.parse(trimmed);
            const list = Array.isArray(data)
                ? data
                : Object.values(data as Record<string, unknown>).find(Array.isArray);
            return Array.isArray(list)
                ? list.filter((r): r is Readonly<Record<string, unknown>> => typeof r === "object" && r !== null)
                : "the JSON has no list of rows";
        } catch {
            return "that isn't valid JSON";
        }
    }
    return parseCsv(trimmed);
}
