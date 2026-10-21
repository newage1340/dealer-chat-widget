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
