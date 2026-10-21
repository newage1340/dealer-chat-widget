/**
 * Operational and staff endpoints: health, tenants, catalog upload, usage, prompt preview,
 * transcripts and bans. These are the API the staff web app will use.
 *
 * Everything except /health and catalog upload needs the admin token.
 * Catalog upload accepts the upload token too (for the scraper job).
 */

import { readFile } from "node:fs/promises";
import { join } from "node:path";

import { Hono, type Context } from "hono";
import { z } from "zod";

import {
    Address,
    Channel,
    ContactId,
    enumValue,
    SessionId,
    SessionOutcome,
    TenantId,
    type Contact,
} from "../core/index.ts";
import { DATASETS_DIR, importRows, syncCatalog } from "../catalogs/index.ts";
import { previewPrompt, SAMPLE_MESSAGE } from "../prompts/preview.ts";
import { parseTenant } from "../tenants/schema.ts";
import { adminOnly, jsonBody, sameSecret, type ChannelDeps } from "./context.ts";

/**
 * A catalog upload from the scraper job: the tenant by slug or number, and the rows ("vehicles" is the legacy name).
 */
const CatalogUpload = z.object({
    token:         z.string().optional(),
    slug:          z.string().optional(),
    twilio_number: z.string().optional(),
    items:         z.array(z.unknown()).optional(),
    vehicles:      z.array(z.unknown()).optional(),
});

const ResetLanguages = z.object({
    phone: z.string().optional(),
});

/**
 * Block a caller by contact id or phone; `hours` omitted or null blocks indefinitely.
 */
const BlockRequest = z.object({
    tenant:    z.string().min(1),
    phone:     z.string().optional(),
    contactId: z.string().optional(),
    hours:     z.number().nullish(),
    reason:    z.string().optional(),
});

export function adminRoutes(deps: ChannelDeps): Hono {
    const app = new Hono();
    const { services, config } = deps;
    const { store, registry, composer } = services;
    const admin = adminOnly(config);

    app.get("/health", async c => c.json({ ok: true, tenants: (await store.tenants.all()).length, domains: registry.ids() }));

    // --- tenants ----------------------------------------------------------------------

    app.get("/admin/tenants", admin, async c => c.json(await store.tenants.all()));

    app.put("/admin/tenants", admin, async c => {
        const result = parseTenant(await c.req.json().catch(() => null));
        if (!result.ok) {
            return c.json({ error: "invalid tenant", problems: result.error }, 400);
        }
        if (registry.find(result.value.domain) === null) {
            return c.json({ error: `unknown business type '${result.value.domain}'` }, 400);
        }
        await store.tenants.save(result.value);
        return c.json({ ok: true, tenant: result.value.id });
    });

    /**
     * Replace a tenant's catalog (e.g. from the scraper job). Identify the tenant by "slug"
     * or by "twilio_number"; send rows as "items" (or the legacy "vehicles").
     * An empty list is refused so a failed scrape can never wipe a live catalog.
     */
    const upload = async (c: Context) => {
        const data = await jsonBody(c, CatalogUpload);
        if (data === null) {
            return c.json({ error: "invalid request body" }, 400);
        }
        const token = c.req.header("X-Upload-Token") ?? data.token ?? "";
        const accepted = [config.uploadToken, config.adminToken].filter(Boolean);
        if (!token || !accepted.some(secret => sameSecret(token, secret))) {
            return c.json({ error: "unauthorized" }, 401);
        }
        let tenant = data.slug === undefined
            ? null
            : await store.tenants.bySlug(data.slug);
        const number = Address.phone(data.twilio_number ?? "");
        if (tenant === null && number !== null) {
            tenant = await store.tenants.byEndpoint("sms", number) ?? await store.tenants.byEndpoint("voice", number);
        }
        if (tenant === null) {
            return c.json({ error: "unknown tenant" }, 404);
        }
        const rows = data.items ?? data.vehicles ?? [];
        if (rows.length === 0) {
            return c.json({ error: "no items - refusing to wipe the existing catalog" }, 400);
        }
        const result = await importRows(services, tenant, rows.filter((r): r is Record<string, unknown> => typeof r === "object" && r !== null));
        return result.ok
            ? c.json({ ok: true, tenant: tenant.id, items: result.value })
            : c.json({ error: result.error }, 400);
    };
    app.post("/admin/catalog-upload", c => upload(c));
    app.post("/admin/inventory-upload", c => upload(c));

    /**
     * A tenant's catalog as the assistant sees it, and how the latest sync from its source went.
     */
    app.get("/admin/tenants/:slug/catalog", admin, async c => {
        const tenant = await store.tenants.bySlug(c.req.param("slug"));
        if (tenant === null) {
            return c.notFound();
        }
        return c.json({
            source: tenant.settings.catalog.source,
            status: await store.tenants.syncStatus(tenant.id),
            items:  await store.tenants.catalog(tenant.id),
        });
    });

    /**
     * Fetch the tenant's catalog from its source now (instead of waiting for the refresh interval).
     */
    app.post("/admin/tenants/:slug/catalog/sync", admin, async c => {
        const tenant = await store.tenants.bySlug(c.req.param("slug"));
        if (tenant === null) {
            return c.notFound();
        }
        const result = await syncCatalog(services, tenant);
        if (result === null) {
            return c.json({ error: "this catalog is managed by hand; upload rows instead" }, 400);
        }
        return result.ok
            ? c.json({ ok: true, items: result.value })
            : c.json({ error: result.error }, 502);
    });

    /**
     * The bundled sample datasets, public so a demo tenant can point a CSV or JSON link source at them.
     */
    app.get("/datasets/:name", async c => {
        const name = c.req.param("name");
        if (!/^[a-z0-9][a-z0-9_.-]*\.(json|csv)$/.test(name)) {
            return c.notFound();
        }
        const body = await readFile(join(DATASETS_DIR, name), "utf8").catch(() => null);
        return body === null
            ? c.notFound()
            : c.body(body, 200, { "Content-Type": name.endsWith(".csv") ? "text/csv; charset=utf-8" : "application/json" });
    });

    /**
     * Forget customers' preferred languages: { phone } for one customer, nothing for all of them.
     */
    app.post("/admin/tenants/:slug/languages/reset", admin, async c => {
        const tenant = await store.tenants.bySlug(c.req.param("slug"));
        if (tenant === null) {
            return c.notFound();
        }
        const body = await jsonBody(c, ResetLanguages);
        if (body === null) {
            return c.json({ error: "invalid request body" }, 400);
        }
        if (!body.phone) {
            return c.json({ ok: true, reset: await store.conversations.resetLanguages(tenant.id, null, tenant.defaultLanguage) });
        }
        const address = Address.phone(body.phone);
        const contact = address === null ? null : await store.conversations.contactByAddress(tenant.id, address);
        if (contact === null) {
            return c.json({ error: "unknown contact" }, 404);
        }
        return c.json({ ok: true, reset: await store.conversations.resetLanguages(tenant.id, contact.id, tenant.defaultLanguage) });
    });

    // --- usage and prompts ------------------------------------------------------------

    app.get("/admin/usage", admin, async c => {
        const hours = Number(c.req.query("hours") ?? "24") || 24;
        const tenant = c.req.query("tenant") ?? null;
        const since = new Date(services.clock().getTime() - hours * 3600_000);
        const rows = await store.spend.summary(tenant === null ? null : TenantId(tenant), since);
        const t = tenant === null ? null : await store.tenants.byId(TenantId(tenant));
        return c.json({
            since:     since.toISOString(),
            totals:    rows,
            costUsd:   rows.reduce((sum, r) => sum + r.costUsd, 0),
            ...(t !== null ? { monthToDateUsd: await store.spend.monthToDate(t, services.clock()) } : {}),
        });
    });

    /**
     * The exact prompt a turn would get, e.g. /admin/prompt/demo-plumber?channel=voice&message=my+water+heater+is+leaking
     */
    app.get("/admin/prompt/:slug", admin, async c => {
        const tenant = await store.tenants.bySlug(c.req.param("slug"));
        if (tenant === null) {
            return c.notFound();
        }
        const channel = enumValue(Channel, c.req.query("channel")) ?? Channel.Sms;
        return c.text(await previewPrompt(services, tenant, channel, c.req.query("message") || SAMPLE_MESSAGE));
    });

    /**
     * Turn quality per prompt variant, model and channel, plus eval pass rates: /admin/quality?days=7&tenant=&domain=
     */
    app.get("/admin/quality", admin, async c => {
        const days = Number(c.req.query("days") ?? "7") || 7;
        const since = new Date(services.clock().getTime() - days * 86_400_000);
        return c.json({
            since,
            experiment: composer.experiment,
            variants:   composer.variants,
            turns:      await store.quality.report({ since, tenantId: c.req.query("tenant") ? TenantId(c.req.query("tenant")!) : null, domain: c.req.query("domain") ?? null }),
            evals:      await store.quality.evals(since),
        });
    });

    // --- transcripts ------------------------------------------------------------------

    app.get("/admin/transcripts", admin, async c => {
        const tenant = c.req.query("tenant");
        if (!tenant) {
            return c.json({ error: "tenant is required" }, 400);
        }
        const outcomes = (c.req.query("outcome") ?? "").split(",").flatMap(o => enumValue(SessionOutcome, o) ?? []);
        const since = c.req.query("since");
        const transcripts = await store.transcripts.list({
            tenantId: TenantId(tenant),
            since:    since ? new Date(since) : null,
            until:    null,
            outcomes,
            limit:    Math.min(Number(c.req.query("limit") ?? "50") || 50, 200),
        });
        return c.json(transcripts.map(t => ({
            sessionId:      t.session.id,
            channel:        t.session.channel,
            startedAt:      t.session.startedAt,
            endedAt:        t.session.endedAt,
            classification: t.classification,
            messages:       t.messages.length,
        })));
    });

    app.get("/admin/transcripts/:sessionId", admin, async c => {
        const transcript = await store.transcripts.transcript(SessionId(c.req.param("sessionId")));
        if (transcript === null) {
            return c.notFound();
        }
        const voicemail = await store.voicemails.bySession(transcript.session.id);
        return c.json({ ...transcript, voicemail });
    });

    // --- bans -------------------------------------------------------------------------

    app.get("/admin/blocks", admin, async c => {
        const tenant = c.req.query("tenant");
        if (!tenant) {
            return c.json({ error: "tenant is required" }, 400);
        }
        return c.json(await store.blocks.blocked(TenantId(tenant), services.clock()));
    });

    /**
     * Block a caller: { tenant, phone | contactId, hours (omit for indefinitely), reason }.
     * Posting again for a blocked contact replaces (extends or shortens) the block.
     */
    app.post("/admin/blocks", admin, async c => {
        const body = await jsonBody(c, BlockRequest);
        if (body === null) {
            return c.json({ error: "tenant is required, with a phone or contactId" }, 400);
        }
        let contact: Contact | null = null;
        if (body.contactId) {
            contact = await store.conversations.contact(ContactId(body.contactId));
        } else if (body.phone) {
            const address = Address.phone(body.phone);
            contact = address === null
                ? null
                : await store.conversations.contactByAddress(TenantId(body.tenant), address)
                    ?? (await store.conversations.resolve(TenantId(body.tenant), address, services.clock())).contact;
        }
        if (contact === null) {
            return c.json({ error: "unknown contact" }, 404);
        }
        const now = services.clock();
        await store.blocks.block(contact.id, {
            until:     typeof body.hours === "number" ? new Date(now.getTime() + body.hours * 3600_000) : null,
            source:    "staff",
            reason:    body.reason || "blocked by staff",
            createdAt: now,
        });
        return c.json({ ok: true, contactId: contact.id });
    });

    app.delete("/admin/blocks/:contactId", admin, async c => {
        await store.blocks.unblock(ContactId(c.req.param("contactId")));
        return c.json({ ok: true });
    });

    return app;
}
