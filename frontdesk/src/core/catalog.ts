/**
 * Things a tenant offers. Core knows the shape; domain packs define the fields.
 */

import type { CatalogItemId, TenantId } from "./ids.ts";
import type { ValueOf } from "./lang.ts";

export const FieldType = {
    Text:    "text",
    Integer: "integer",
    Number:  "number",
    Money:   "money",
    Url:     "url",
} as const;
export type FieldType = ValueOf<typeof FieldType>;

/**
 * `searchable` fields are considered when matching what a customer asked for.
 * `emptyText` is shown instead of dropping the field when it has no value (e.g. "Call for price").
 * `inDetails` includes the field in the per-item details given to the model.
 */
export type FieldSpec = {
    readonly name:       string;
    readonly type:       FieldType;
    readonly label:      string;
    readonly required:   boolean;
    readonly searchable: boolean;
    readonly emptyText:  string;
    readonly inDetails:  boolean;
};

export type FieldValue = string | number | null;

export type CatalogItem = {
    readonly id:        CatalogItemId;
    readonly tenantId:  TenantId;
    readonly fields:    Readonly<Record<string, FieldValue>>;
    readonly updatedAt: Date;
};

export const CatalogItem = {
    get(item: CatalogItem, name: string): FieldValue {
        return item.fields[name] ?? null;
    },

    text(item: CatalogItem, name: string): string {
        const value = item.fields[name];
        return value === null || value === undefined
			? ""
			: String(value);
    },
} as const;


/**
 * Where a tenant's catalog comes from. Every source ends up as the same CatalogItem rows,
 * so the assistant reads a menu, a price list, listings or inventory through one interface.
 * `manual`: rows uploaded through the API or typed into the settings page.
 * `dataset`: a file shipped in datasets/ (the demo businesses).
 * `csv_url`: a CSV feed, e.g. a Google Sheet published as CSV, or a POS export.
 * `json_url`: a JSON feed; `itemsPath` is the dotted path to the array of rows ("" when the body is the array).
 * Remote sources are fetched again every `refreshMinutes`.
 */
export type CatalogSource =
    | { readonly kind: "manual" }
    | { readonly kind: "dataset";
        readonly name: string
    }
    | { readonly kind: "csv_url";
        readonly url:            string;
        readonly refreshMinutes: number
    }
    | { readonly kind: "json_url";
        readonly url:            string;
        readonly itemsPath:      string;
        readonly refreshMinutes: number
    };

/**
 * `fieldMap` renames a source's columns to the domain's catalog fields ("Dish Name" -> "name"),
 * on top of the aliases the domain pack already knows.
 */
export type CatalogSettings = {
    readonly source:   CatalogSource;
    readonly fieldMap: Readonly<Record<string, string>>;
};

export const DEFAULT_CATALOG_SETTINGS: CatalogSettings = {
    source:   { kind: "manual" },
    fieldMap: {},
};

/**
 * The result of the latest sync from a tenant's catalog source.
 */
export type CatalogSyncStatus = {
    readonly syncedAt: Date;
    readonly items:    number;
    readonly error:    string | null;
};
