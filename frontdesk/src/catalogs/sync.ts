/**
 * Bringing a tenant's catalog in from its source: fetch rows, rename columns, normalize them onto the
 * domain's fields, and replace the stored catalog. Turns always read the stored copy, so a slow or
 * broken feed never delays a reply; a failed or empty fetch keeps the last good catalog.
 */

import {
    Result,
    type CatalogItem,
    type CatalogSyncStatus,
    type Tenant,
} from "../core/index.ts";
import { Catalog } from "../domains/index.ts";
import type { Services } from "../pipeline/services.ts";
import { DEFAULT_CONNECTOR_DEPS, fetchRows, type ConnectorDeps, type RawRow } from "./connectors.ts";

/**
 * Rows with the tenant's column renames applied (keys compared without case or surrounding spaces).
 */
export function mapColumns(rows: readonly RawRow[], fieldMap: Readonly<Record<string, string>>): RawRow[] {
    const renames = new Map(Object.entries(fieldMap).map(([from, to]) => [from.trim().toLowerCase(), to.trim()]));
    if (renames.size === 0) {
        return [...rows];
    }
    return rows.map(row => Object.fromEntries(
        Object.entries(row).map(([key, value]) => [renames.get(key.trim().toLowerCase()) ?? key, value])
    ));
}

/**
 * Catalog items for a tenant from raw rows, using its field map and its domain's catalog shape.
 * Rows that end up with none of the domain's fields are dropped.
 */
export function toItems(services: Services, tenant: Tenant, rows: readonly RawRow[]): CatalogItem[] {
    const spec = services.registry.get(tenant.domain).catalog;
    const now = services.clock();
    return mapColumns(rows, tenant.settings.catalog.fieldMap)
        .map((row, i) => Catalog.normalize(row, spec, tenant.id, i, now))
        .filter(item => Object.keys(item.fields).length > 0);
}

/**
 * Replace a tenant's catalog with `rows`. An empty result is refused so a bad upload never wipes a live catalog.
 */
export async function importRows(services: Services, tenant: Tenant, rows: readonly RawRow[]): Promise<Result<number, string>> {
    const items = toItems(services, tenant, rows);
    if (items.length === 0) {
        return Result.err(rows.length === 0
            ? "no rows - keeping the existing catalog"
            : "none of the columns match this business type's fields - check the column names or the field map");
    }
    await services.store.tenants.replaceCatalog(tenant.id, items);
    return Result.ok(items.length);
}

/**
 * Fetch a tenant's catalog from its source and store it, recording how it went.
 * Returns null for manual catalogs, which have nothing to fetch.
 */
export async function syncCatalog(
    services: Services,
    tenant: Tenant,
    deps: ConnectorDeps = DEFAULT_CONNECTOR_DEPS
): Promise<Result<number, string> | null>
{
    const rows = await fetchRows(tenant.settings.catalog.source, deps);
    if (rows === null) {
        return null;
    }
    const result = rows.ok
        ? await importRows(services, tenant, rows.value)
        : rows;
    const status: CatalogSyncStatus = {
        syncedAt: services.clock(),
        items:    result.ok ? result.value : (await services.store.tenants.catalog(tenant.id)).length,
        error:    result.ok ? null : result.error,
    };
    await services.store.tenants.recordSync(tenant.id, status);
    if (!result.ok) {
        console.warn(`catalog sync for ${tenant.slug} failed: ${result.error}`);
    }
    return result;
}

/**
 * Whether a tenant's source should be fetched now: remote feeds every `refreshMinutes`
 * (retrying failures after five minutes), datasets once.
 */
export function syncDue(
    tenant: Tenant,
    last: CatalogSyncStatus | null,
    now: Date
): boolean {
    const source = tenant.settings.catalog.source;
    switch (source.kind) {
        case "manual":
            return false;
        case "dataset":
            return last === null;
        case "csv_url":
        case "json_url": {
            if (last === null) {
                return true;
            }
            const minutes = last.error !== null
                ? Math.min(5, source.refreshMinutes)
                : source.refreshMinutes;
            return now.getTime() - last.syncedAt.getTime() >= minutes * 60_000;
        }
    }
}

/**
 * Sync every tenant whose source is due (run by the worker once a minute).
 */
export async function syncDueCatalogs(
    services: Services,
    deps: ConnectorDeps = DEFAULT_CONNECTOR_DEPS
): Promise<number> {
    let synced = 0;
    const now = services.clock();
    for (const tenant of await services.store.tenants.all()) {
        if (syncDue(tenant, await services.store.tenants.syncStatus(tenant.id), now)) {
            await syncCatalog(services, tenant, deps);
            synced++;
        }
    }
    return synced;
}
