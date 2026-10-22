/**
 * Tenants (stored in their file form), their phone endpoints, and their catalogs.
 */

import { Address, CatalogItemId, TenantId, type CatalogItem, type Tenant } from "../../core/index.ts";
import { StoredTenant } from "../../tenants/schema.ts";
import type { Db, Row } from "../db.ts";
import type { TenantDirectory } from "../ports.ts";
import { CatalogFieldsJson, date, firstOrNull, json, jsonColumn, RowJson, str } from "./columns.ts";

function toTenant(r: Row): Tenant {
    return StoredTenant.read(jsonColumn(RowJson, r["data"], "tenant"));
}

function toCatalogItem(r: Row): CatalogItem {
    return {
        id:        CatalogItemId(str(r["id"])),
        tenantId:  TenantId(str(r["tenant_id"])),
        fields:    jsonColumn(CatalogFieldsJson, r["fields"], "catalog item fields"),
        updatedAt: date(r["updated_at"]),
    };
}

export function tenantDirectory(db: Db): TenantDirectory {
    const one = async (sql: string, params: unknown[]) => firstOrNull(await db.query(sql, params), toTenant);
    return {
        byId:       id => one("select data from tenants where id = $1", [id]),
        bySlug:     slug => one("select data from tenants where slug = $1", [slug]),
        byEndpoint: (channel, address) => one(
            `select t.data from tenant_endpoints e join tenants t on t.id = e.tenant_id
             where e.channel = $1 and e.address = $2`,
            [channel, Address.key(address)],
        ),
        all:        async () => (await db.query("select data from tenants order by slug")).map(toTenant),

        save: tenant => db.transaction(async tx => {
            await tx.query(
                `insert into tenants (id, slug, data, updated_at) values ($1, $2, $3::jsonb, now())
                 on conflict (id) do update set slug = excluded.slug, data = excluded.data, updated_at = now()`,
                [tenant.id, tenant.slug, json(StoredTenant.write(tenant))],
            );
            await tx.query("delete from tenant_endpoints where tenant_id = $1", [tenant.id]);
            for (const [channel, addresses] of Object.entries(tenant.endpoints)) {
                for (const address of addresses ?? []) {
                    await tx.query(
                        `insert into tenant_endpoints (channel, address, tenant_id) values ($1, $2, $3)
                         on conflict (channel, address) do update set tenant_id = excluded.tenant_id`,
                        [channel, Address.key(address), tenant.id],
                    );
                }
            }
        }),

        catalog: async tenantId => (await db.query(
            "select * from catalog_items where tenant_id = $1 order by position", [tenantId],
        )).map(toCatalogItem),

        replaceCatalog: (tenantId, items) => db.transaction(async tx => {
            await tx.query("delete from catalog_items where tenant_id = $1", [tenantId]);
            const seen = new Set<string>();
            let position = 0;
            for (const item of items) {
                // Ids from a feed can repeat; the position keeps every row.
                const id = seen.has(item.id)
                    ? `${item.id}-${position}`
                    : item.id;
                seen.add(id);
                await tx.query(
                    "insert into catalog_items (tenant_id, id, position, fields, updated_at) values ($1, $2, $3, $4::jsonb, $5)",
                    [tenantId, id, position++, json(item.fields), item.updatedAt],
                );
            }
        }),

        syncStatus: async tenantId => firstOrNull(
            await db.query("select * from catalog_syncs where tenant_id = $1", [tenantId]),
            r => ({
                syncedAt: date(r["synced_at"]),
                items:    Number(r["items"]),
                error:    r["error"] === null ? null : str(r["error"]),
            }),
        ),

        recordSync: async (tenantId, status) => {
            await db.query(
                `insert into catalog_syncs (tenant_id, synced_at, items, error) values ($1, $2, $3, $4)
                 on conflict (tenant_id) do update set synced_at = excluded.synced_at, items = excluded.items, error = excluded.error`,
                [tenantId, status.syncedAt, status.items, status.error],
            );
        },
    };
}
