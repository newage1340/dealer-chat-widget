/**
 * Web chat widget: the page, its JSON API, and the embeddable loader.
 *
 * Endpoints and payloads match the existing public/static/script.js and embed.js.
 * Each browser session is its own contact (a web_session address); the visitor's real phone
 * is stored on the contact once they give it.
 */

import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";

import { Hono } from "hono";

import { Address, PhoneNumber, type Inbound, type Tenant } from "../core/index.ts";
import { Catalog, type WidgetTopic } from "../domains/index.ts";
import { collect, UnknownTenant } from "../pipeline/engine.ts";
import type { ChannelDeps } from "./context.ts";

const PUBLIC = new URL("../../public/", import.meta.url);

function escape(text: string): string {
    return text
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll("\"", "&quot;")
        .replaceAll("'", "&#39;");
}

function sessionAddress(sessionId: string): Address {
    return Address.webSession(sessionId.replace(/[^a-zA-Z0-9_-]/g, "").slice(0, 64));
}

/**
 * A US phone number from what a visitor typed, or null.
 */
function usPhone(raw: unknown): PhoneNumber | null {
    const digits = String(raw ?? "").replace(/\D/g, "").replace(/^1(?=\d{10}$)/, "");
    return digits.length === 10
        ? PhoneNumber.parse(digits)
        : null;
}

type Body = {
    readonly session_id?: string;
    readonly slug?:       string;
    readonly message?:    string;
    readonly name?:       string;
    readonly phone?:      string;
    readonly real_phone?: string;
};

function page(tenant: Tenant, sections: [string, WidgetTopic[]][], widget: { icon: string; tagline: string; headerTitle: string; inputPlaceholder: string }, terms: string, privacy: string): string {
    const e = escape;
    const color = /^#[0-9a-f]{3,8}$/i.test(tenant.branding.color) ? tenant.branding.color : "#4a90e2";
    const topics = sections.map(([section, items], i) =>
        `<div class="topic-label${i > 0 ? " section-spacer" : ""}">${e(section)}</div>`
        + items.map(t => `<button class="topic-btn" data-message="${e(t.message)}" onclick="askTopic(this.dataset.message)">${e(t.label)}</button>`).join("")
    ).join("\n");
    const logo = tenant.branding.logoUrl
        ? `<img class="logo-image" src="${e(tenant.branding.logoUrl)}" alt="${e(tenant.name)} logo" />`
        : `<div class="logo-icon">${e(widget.icon)}</div>`;
    return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>${e(tenant.name)} - Chat</title>
  <link rel="stylesheet" href="/static/style.css" />
  <style>:root { --accent: ${color}; --accent-hover: color-mix(in srgb, ${color} 80%, white); }</style>
</head>
<body data-dealer-slug="${e(tenant.slug)}">
  <div class="phone-gate" id="phoneGate" hidden>
    <div class="phone-gate-card">
      <div class="phone-gate-title">Welcome to ${e(tenant.name)}</div>
      <p class="phone-gate-sub">Please accept our terms and privacy policy to start the chat.</p>
      <div id="phoneGateForm">
        <label class="phone-gate-terms" for="phoneGateTerms">
          <input type="checkbox" id="phoneGateTerms" name="terms_accept" autocomplete="off">
          <span>Yes, I'd like to chat with ${e(tenant.name)} about my inquiry. If I share my phone number, I may also receive text follow-ups — message frequency varies, msg &amp; data rates may apply, reply STOP anytime to opt out. <a href="${e(terms)}" target="_blank" rel="noopener" onclick="event.stopPropagation();">View Terms of Service</a>.</span>
        </label>
        <label class="phone-gate-terms" for="phoneGatePrivacy">
          <input type="checkbox" id="phoneGatePrivacy" name="privacy_accept" autocomplete="off">
          <span>I have read and agree to the <a href="${e(privacy)}" target="_blank" rel="noopener" onclick="event.stopPropagation();">Privacy Policy</a>.</span>
        </label>
        <button type="button" id="phoneGateSubmit">Enter chat</button>
      </div>
      <div class="phone-gate-error" id="phoneGateError"></div>
    </div>
  </div>
  <div class="app-container">
    <button class="mobile-menu-toggle" id="menuToggle" aria-label="Open menu">&#9776;</button>
    <div class="sidebar" id="sidebar">
      <div class="sidebar-close" id="sidebarClose" aria-label="Close menu">&times;</div>
      <div class="logo">${logo}<div class="logo-text">${e(tenant.name)}</div></div>
      <div class="sidebar-tagline">${e(widget.tagline)}</div>
      <div class="topic-list">
${topics}
      </div>
      <div class="sidebar-footer">
        <button class="clear-btn" onclick="clearChat()">&#128465; Clear Chat</button>
        <a class="terms-link" href="${e(terms)}" target="_blank" rel="noopener">Terms of Service</a>
        <a class="terms-link" href="${e(privacy)}" target="_blank" rel="noopener">Privacy Policy</a>
      </div>
    </div>
    <div class="main">
      <div class="main-header">
        <div class="main-header-title">${e(widget.headerTitle)}</div>
        <div class="main-header-status"><span class="status-dot"></span>Online</div>
      </div>
      <div class="chat-window" id="chatWindow"></div>
      <div class="input-area">
        <textarea id="userInput" placeholder="${e(widget.inputPlaceholder)}" rows="1" onkeydown="handleKey(event)"></textarea>
        <button id="sendBtn" onclick="sendMessage()" aria-label="Send">&#10148;</button>
      </div>
    </div>
  </div>
  <script src="/static/script.js"></script>
</body>
</html>`;
}

const STATIC_TYPES: Record<string, string> = {
    ".js":  "application/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".png": "image/png",
    ".svg": "image/svg+xml",
};

export function webRoutes(deps: ChannelDeps): Hono {
    const app = new Hono();
    const { services, config, engine } = deps;
    const { store, registry } = services;

    const tenantFor = async (slug: string | undefined): Promise<Tenant | null> => {
        const s = slug?.trim() || config.defaultSlug;
        return s
            ? await store.tenants.bySlug(s)
            : null;
    };
    const visitor = async (tenant: Tenant, sessionId: string) =>
        await store.conversations.resolve(tenant.id, sessionAddress(sessionId), services.clock());

    app.get("/", async c => {
        if (config.defaultSlug) {
            return c.redirect(`/widget/${config.defaultSlug}`);
        }
        const tenants = await store.tenants.all();
        const links = tenants.map(t => `<li><a href="/widget/${escape(t.slug)}">${escape(t.name)}</a> <small>(${escape(t.domain)})</small></li>`).join("");
        return c.html(`<h1>Front Desk</h1><p>Widgets:</p><ul>${links}</ul>`);
    });

    app.get("/static/:file", async c => {
        const name = c.req.param("file");
        const ext = name.slice(name.lastIndexOf("."));
        if (!/^[\w.-]+$/.test(name) || STATIC_TYPES[ext] === undefined) {
            return c.notFound();
        }
        try {
            const body = await readFile(new URL(`static/${name}`, PUBLIC));
            return c.body(body, 200, { "Content-Type": STATIC_TYPES[ext], "Cache-Control": "public, max-age=3600" });
        } catch {
            return c.notFound();
        }
    });

    app.get("/embed.js", async c => {
        const body = await readFile(new URL("static/embed.js", PUBLIC));
        return c.body(body, 200, { "Content-Type": STATIC_TYPES[".js"]!, "Cache-Control": "public, max-age=3600" });
    });

    app.get("/widget/:slug", async c => {
        const tenant = await store.tenants.bySlug(c.req.param("slug"));
        if (tenant === null) {
            return c.text(`No business with slug '${c.req.param("slug")}'.`, 404);
        }
        const pack = registry.get(tenant.domain);
        const catalog = await store.tenants.catalog(tenant.id);
        const sections = new Map<string, WidgetTopic[]>();
        for (const topic of pack.widget.topics) {
            if (Catalog.topicVisible(topic, catalog)) {
                sections.set(topic.section, [...(sections.get(topic.section) ?? []), topic]);
            }
        }
        return c.html(page(tenant, [...sections.entries()], pack.widget, config.termsUrl, config.privacyUrl));
    });

    app.get("/widget-config", async c => {
        c.header("Access-Control-Allow-Origin", "*");
        const tenant = await tenantFor(c.req.query("dealer") ?? c.req.query("business"));
        if (tenant === null) {
            return c.json({ error: "business not found" }, 404);
        }
        return c.json({
            dealer_name:   tenant.name,
            business_name: tenant.name,
            brand_color:   tenant.branding.color,
            logo_url:      tenant.branding.logoUrl,
            slug:          tenant.slug,
        });
    });

    app.post("/chat", async c => {
        const body = await c.req.json<Body>().catch((): Body => ({}));
        const message = body.message?.trim() ?? "";
        const tenant = await tenantFor(body.slug);
        if (tenant === null) {
            return c.json({ error: "no business resolved (missing or unknown slug)" }, 400);
        }
        if (!message) {
            return c.json({ error: "empty message" }, 400);
        }
        const sessionId = body.session_id?.trim() || randomUUID().replaceAll("-", "");

        // Self-heal: the widget echoes the phone it cached, in case the server lost it.
        const cached = usPhone(body.real_phone);
        const { contact } = await visitor(tenant, sessionId);
        if (cached !== null && contact.phone === null) {
            await store.conversations.updateContact(contact.id, { phone: cached });
        }

        const inbound: Inbound = {
            channel:           "web",
            tenant:            { kind: "slug", slug: tenant.slug },
            sender:            sessionAddress(sessionId),
            text:              message,
            receivedAt:        services.clock(),
            dedupeKey:         `web:${sessionId}:${randomUUID()}`,
            externalSessionId: sessionId,
            confidence:        null,
            language:          null,
            forwardedFrom:     null,
        };
        try {
            const result = await collect(engine.run(inbound, { tenant }));
            if (result.data["needs_profile"]) {
                return c.json({
                    reply:           result.outcome.reply,
                    needs_profile:   true,
                    missing:         result.data["missing"],
                    pending_message: result.data["pending_message"],
                    session_id:      sessionId,
                });
            }
            return c.json({ reply: result.outcome.reply, primer: null, silent: result.silent, session_id: sessionId });
        } catch (e) {
            if (e instanceof UnknownTenant) {
                return c.json({ error: e.message }, 400);
            }
            console.error("chat turn failed:", e);
            return c.json({ error: "processing error" }, 500);
        }
    });

    app.post("/widget/welcome", async c => {
        const body = await c.req.json<Body>().catch((): Body => ({}));
        const tenant = await tenantFor(body.slug);
        if (!body.session_id || tenant === null) {
            return c.json({ error: "missing session_id or unknown slug" }, 400);
        }
        const { contact } = await visitor(tenant, body.session_id);
        if (contact.termsAcceptedAt !== null) {
            return c.json({ welcome: "" });
        }
        const pack = registry.get(tenant.domain);
        const agent = tenant.agentName || pack.defaultAgentName;
        const welcome = `Hi, my name is ${agent} with ${tenant.name}. I can help with ${pack.widget.capabilities}.\n\n`
            + `By communicating with our assistant, you agree to our [terms](${config.termsUrl}). `
            + "If you receive and respond to SMS follow-ups, msg frequency varies and msg & data rates may apply. "
            + "Reply HELP for help, STOP to opt out anytime.";
        await store.conversations.updateContact(contact.id, { termsAcceptedAt: services.clock() });
        return c.json({ welcome, welcome_followup: "How can I help you?" });
    });

    app.post("/widget/history", async c => {
        const body = await c.req.json<Body>().catch((): Body => ({}));
        const tenant = await tenantFor(body.slug);
        if (!body.session_id || tenant === null) {
            return c.json({ messages: [] });
        }
        const contact = await store.conversations.contactByAddress(tenant.id, sessionAddress(body.session_id));
        if (contact === null) {
            return c.json({ messages: [] });
        }
        const { conversation } = await visitor(tenant, body.session_id);
        const history = await store.conversations.history(conversation.id, 40);
        return c.json({ messages: history.map(m => ({ role: m.role, content: m.text })) });
    });

    app.post("/widget/profile", async c => {
        const body = await c.req.json<Body>().catch((): Body => ({}));
        const tenant = await tenantFor(body.slug);
        const name = body.name?.trim() ?? "";
        const phone = usPhone(body.phone);
        if (!body.session_id || tenant === null) {
            return c.json({ error: "missing session_id or unknown slug" }, 400);
        }
        if (!name) {
            return c.json({ error: "missing name" }, 400);
        }
        if (phone === null) {
            return c.json({ error: "phone must be 10 digits" }, 400);
        }
        const { contact } = await visitor(tenant, body.session_id);
        const [first, ...rest] = name.split(/\s+/);
        await store.conversations.updateContact(contact.id, { firstName: first ?? "", lastName: rest.join(" "), phone });
        return c.json({ ok: true });
    });

    app.post("/widget/register-phone", async c => {
        const body = await c.req.json<Body>().catch((): Body => ({}));
        const tenant = await tenantFor(body.slug);
        const phone = usPhone(body.phone);
        if (!body.session_id || tenant === null || phone === null) {
            return c.json({ error: "missing session_id, slug, or valid phone" }, 400);
        }
        const { contact } = await visitor(tenant, body.session_id);
        await store.conversations.updateContact(contact.id, { phone });
        return c.json({ ok: true, phone });
    });

    app.post("/widget/clear-session", async c => {
        const body = await c.req.json<Body>().catch((): Body => ({}));
        const tenant = await tenantFor(body.slug);
        if (!body.session_id || tenant === null) {
            return c.json({ error: "missing session_id or unknown slug" }, 400);
        }
        const contact = await store.conversations.contactByAddress(tenant.id, sessionAddress(body.session_id));
        if (contact !== null) {
            await store.conversations.deleteContact(contact.id);
        }
        return c.json({ ok: true });
    });

    return app;
}
