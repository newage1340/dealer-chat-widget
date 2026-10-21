/**
 * Operational and staff endpoints: health, tenants, catalog upload, usage, prompt preview,
 * transcripts and bans. These are the API the staff web app will use.
 *
 * Everything except /health and catalog upload needs the admin token.
 * Catalog upload accepts the upload token too (for the scraper job).
 */

import { Hono, type Context } from "hono";

import {
    Address,
    ContactId,
    LocalTime,
    SessionId,
    SessionMeter,
    TenantId,
    type Contact,
    type Conversation,
    type Inbound,
    type SessionOutcome,
    type Tenant,
} from "../core/index.ts";
import { Catalog } from "../domains/index.ts";
import { parseTenant } from "../tenants/schema.ts";
import type { TurnContext } from "../turn/index.ts";
import { adminOnly, type ChannelDeps } from "./context.ts";

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
            return c.json({ error: "invalid tenant", problems: result.problems }, 400);
        }
        try {
            registry.get(result.tenant.domain);
        } catch (e) {
            return c.json({ error: String(e) }, 400);
        }
        await store.tenants.save(result.tenant);
        return c.json({ ok: true, tenant: result.tenant.id });
    });

    /**
     * Replace a tenant's catalog (e.g. from the scraper job). Identify the tenant by "slug"
     * or by "twilio_number"; send rows as "items" (or the legacy "vehicles").
     * An empty list is refused so a failed scrape can never wipe a live catalog.
     */
    const upload = async (c: Context) => {
        const data = await c.req.json<Record<string, unknown>>().catch(() => ({} as Record<string, unknown>));
        const token = c.req.header("X-Upload-Token") ?? String(data["token"] ?? "");
        const accepted = [config.uploadToken, config.adminToken].filter(Boolean);
        if (!token || !accepted.includes(token)) {
            return c.json({ error: "unauthorized" }, 401);
        }
        let tenant: Tenant | null = typeof data["slug"] === "string" ? await store.tenants.bySlug(data["slug"]) : null;
        const number = Address.phone(String(data["twilio_number"] ?? ""));
        if (tenant === null && number !== null) {
            tenant = await store.tenants.byEndpoint("sms", number) ?? await store.tenants.byEndpoint("voice", number);
        }
        if (tenant === null) {
            return c.json({ error: "unknown tenant" }, 404);
        }
        const rows = data["items"] ?? data["vehicles"];
        if (!Array.isArray(rows) || rows.length === 0) {
            return c.json({ error: "no items - refusing to wipe the existing catalog" }, 400);
        }
        const spec = registry.get(tenant.domain).catalog;
        const now = services.clock();
        const items = rows
            .filter((r): r is Record<string, unknown> => typeof r === "object" && r !== null)
            .map((r, i) => Catalog.normalize(r, spec, tenant.id, i, now));
        await store.tenants.replaceCatalog(tenant.id, items);
        return c.json({ ok: true, tenant: tenant.id, items: items.length });
    };
    app.post("/admin/catalog-upload", c => upload(c));
    app.post("/admin/inventory-upload", c => upload(c));

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
        const channel = (c.req.query("channel") ?? "sms") as Inbound["channel"];
        return c.text(await previewPrompt(deps, tenant, channel, c.req.query("message") ?? "Hi, what do you have?"));
    });

    // --- transcripts ------------------------------------------------------------------

    app.get("/admin/transcripts", admin, async c => {
        const tenant = c.req.query("tenant");
        if (!tenant) {
            return c.json({ error: "tenant is required" }, 400);
        }
        const outcomes = (c.req.query("outcome") ?? "").split(",").filter(Boolean) as SessionOutcome[];
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
        const body = await c.req.json<{ tenant?: string; phone?: string; contactId?: string; hours?: number | null; reason?: string }>()
            .catch(() => ({} as { tenant?: string }));
        if (!body.tenant) {
            return c.json({ error: "tenant is required" }, 400);
        }
        let contact: Contact | null = null;
        if ("contactId" in body && body.contactId) {
            contact = await store.conversations.contact(ContactId(body.contactId));
        } else if ("phone" in body && body.phone) {
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
        const hours = "hours" in body ? body.hours : null;
        await store.blocks.block(contact.id, {
            until:     typeof hours === "number" ? new Date(now.getTime() + hours * 3600_000) : null,
            source:    "staff",
            reason:    ("reason" in body ? body.reason : undefined) ?? "blocked by staff",
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

/**
 * The prompt a turn would get for a sample customer, as text.
 */
export async function previewPrompt(deps: ChannelDeps, tenant: Tenant, channel: Inbound["channel"], message: string): Promise<string> {
    const { services } = deps;
    const now = services.clock();
    const sender = channel === "web" ? Address.webSession("preview") : Address.phone("+13175550199")!;
    const contact: Contact = {
        id: ContactId("preview"), tenantId: tenant.id, firstName: "Alex", lastName: "", email: "", phone: Address.phone("+13175550199")!.value as Contact["phone"],
        addresses: [sender], optedOut: false, termsAcceptedAt: null, preferredLanguage: null, block: null, abuseStrikes: [], attributes: {},
    };
    const conversation: Conversation = { id: "preview" as Conversation["id"], tenantId: tenant.id, contactId: contact.id, createdAt: now, lastMessageAt: null };
    const base = { id: SessionId("preview"), conversationId: conversation.id, tenantId: tenant.id, startedAt: now, endedAt: null, sentUrls: [], language: tenant.defaultLanguage, meter: SessionMeter.start(now) };
    const ctx: TurnContext = {
        inbound:       {
            channel, tenant: { kind: "slug", slug: tenant.slug }, sender, text: message, receivedAt: now, dedupeKey: "preview",
            externalSessionId: null, confidence: null, language: null, forwardedFrom: null,
        },
        tenant,
        pack:          services.registry.get(tenant.domain),
        catalog:       await services.store.tenants.catalog(tenant.id),
        contact,
        conversation,
        session:       channel === "voice"
            ? { ...base, channel: "voice", externalId: "preview", vendor: "twilio", handling: "assistant", call: { status: "listening", turns: 0, silentTurns: 0 } }
            : channel === "web"
                ? { ...base, channel: "web", externalId: "preview" }
                : { ...base, channel: "sms", externalId: null },
        history:       [],
        appointment:   null,
        costThisMonth: 0,
        now,
        local:         LocalTime.of(now, tenant.timezone),
    };
    const messages = services.composer.turn(ctx, { kind: "ok" });
    return messages.map(m => `##### ${m.role}\n${"text" in m ? m.text : m.output}`).join("\n\n");
}
