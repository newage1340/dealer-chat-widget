/**
 * The settings website: plain HTML forms over the same tenant file the admin API takes.
 *
 * Every section posts its own form; the handler edits the tenant's file form, validates it with
 * `parseTenant` (so the page can never save something the API would refuse), saves, and redirects back.
 * Sign-in is the admin token, kept in an HTTP-only, same-site cookie.
 */

import { randomUUID } from "node:crypto";
import { readdir, readFile } from "node:fs/promises";

import { Hono, type Context, type MiddlewareHandler } from "hono";
import { deleteCookie, getCookie, setCookie } from "hono/cookie";

import { DATASETS_DIR, importRows, parsePastedRows, syncCatalog } from "../../catalogs/index.ts";
import {
    Address,
    ContactId,
    DomainId,
    Password,
    PhoneNumber,
    SessionId,
    type CatalogSource,
    type Language,
    type PiiKind,
    type Professionalism,
    type Tenant,
    type Weekday,
} from "../../core/index.ts";
import { lintTenant } from "../../tenants/lint.ts";
import { StripeClient } from "../../providers/stripe.ts";
import { LoginThrottle, Session, Viewer } from "./auth.ts";
import { fromTenant, parseTenant, type TenantDraft } from "../../tenants/schema.ts";
import { previewPrompt, SAMPLE_MESSAGE } from "../admin.ts";
import type { ChannelDeps } from "../context.ts";
import { indexPage, loginPage, promptPage, qualityPage, tenantPage, transcriptPage } from "./pages.ts";

const COOKIE = "frontdesk_settings";
const STATIC = new URL("../../../public/settings/", import.meta.url);

type Body = Record<string, string | File | (string | File)[]>;

function one(body: Body, name: string): string {
    const value = body[name];
    const first = Array.isArray(value) ? value[0] : value;
    return typeof first === "string" ? first.trim() : "";
}

function many(body: Body, name: string): string[] {
    const value = body[name];
    const all = Array.isArray(value) ? value : value === undefined ? [] : [value];
    return all.filter((v): v is string => typeof v === "string" && v.trim() !== "").map(v => v.trim());
}

function on(body: Body, name: string): boolean {
    return one(body, name) !== "";
}

/**
 * A number field, or undefined when it was left blank (so the current value stays).
 */
function num(body: Body, name: string): number | undefined {
    const raw = one(body, name);
    return raw === "" ? undefined : Number(raw);
}

function lines(text: string): string[] {
    return text.split(/[\n,]+/).map(l => l.trim()).filter(Boolean);
}

/**
 * "name = value" lines as pairs; lines without "=" are ignored.
 */
function pairs(text: string): [string, string][] {
    return text.split("\n").flatMap(line => {
        const i = line.indexOf("=");
        return i > 0 ? [[line.slice(0, i).trim(), line.slice(i + 1).trim()] as [string, string]] : [];
    }).filter(([k]) => k !== "");
}

function minutes(clock: string): number {
    const [h, m] = clock.split(":").map(Number);
    return (h ?? 0) * 60 + (m ?? 0);
}

function optionValue(raw: string): string | number | boolean {
    if (/^(true|yes|on)$/i.test(raw)) {
        return true;
    }
    if (/^(false|no|off)$/i.test(raw)) {
        return false;
    }
    return /^-?\d+(\.\d+)?$/.test(raw) ? Number(raw) : raw;
}

/**
 * A section's edit: change the draft from the form, or return problems to show instead of saving.
 */
type Edit = (draft: TenantDraft, body: Body, tenant: Tenant, viewer: Viewer) => string[] | void | Promise<string[] | void>;

type Env = { Variables: { viewer: Viewer } };

export function settingsRoutes(deps: ChannelDeps): Hono<Env> {
    const app = new Hono<Env>();
    const { services, config } = deps;
    const { store, registry } = services;
    const throttle = new LoginThrottle();
    const stripe = config.stripe.secretKey ? new StripeClient(config.stripe.secretKey, deps.fetch) : null;
    const origin = (c: Context) => config.publicBaseUrl || new URL(c.req.url).origin;

    /**
     * Who the session cookie belongs to, or null when nobody valid is signed in.
     */
    const viewerOf = async (c: Context): Promise<Viewer | null> => {
        const cookie = getCookie(c, COOKIE) ?? "";
        if (Session.validAdmin(cookie, config.adminToken)) {
            return { kind: "admin" };
        }
        const id = Session.ownerId(cookie);
        const account = id === null ? null : await store.accounts.byId(id);
        return account !== null && Session.validOwner(cookie, account, config.sessionSecret, services.clock())
            ? { kind: "owner", account }
            : null;
    };
    const auth: MiddlewareHandler<Env> = async (c, next) => {
        const viewer = await viewerOf(c);
        if (viewer === null) {
            return c.redirect("/settings/login");
        }
        c.set("viewer", viewer);
        return next();
    };
    const adminOnly: MiddlewareHandler<Env> = async (c, next) => c.get("viewer").kind === "admin"
        ? next()
        : c.notFound();
    const startSession = (c: Context, value: string) => {
        // Secure only when this request came over https (directly or through a tunnel/proxy): a Secure cookie
        // set on http://localhost is dropped by the browser, which made sign-in loop back to the login page.
        const https = new URL(c.req.url).protocol === "https:" || c.req.header("x-forwarded-proto") === "https";
        setCookie(c, COOKIE, value, { httpOnly: true, sameSite: "Lax", path: "/settings", secure: https, maxAge: Session.maxAgeSeconds });
    };

    const domains = () => registry.ids().map(id => [id, registry.get(id).name] as const).sort((a, b) => a[1].localeCompare(b[1]));
    const datasets = async () => (await readdir(DATASETS_DIR).catch(() => [] as string[]))
        .filter(n => n.endsWith(".json") || n.endsWith(".csv"))
        .sort();

    const render = async (c: Context<Env>, tenant: Tenant, notice: string, problems: readonly string[] = []) => {
        const now = services.clock();
        const pack = registry.get(tenant.domain);
        const items = await store.tenants.catalog(tenant.id);
        const html = tenantPage({
            tenant,
            pack,
            domains:     domains(),
            items,
            sync:        await store.tenants.syncStatus(tenant.id),
            datasets:    await datasets(),
            blocked:     await store.blocks.blocked(tenant.id, now),
            transcripts: await store.transcripts.list({ tenantId: tenant.id, since: null, until: null, outcomes: [], limit: 25 }),
            monthToDate: await store.spend.monthToDate(tenant, now),
            notice,
            problems,
            warnings:    lintTenant(tenant, pack, items),
            viewer:      c.get("viewer"),
            accounts:    await store.accounts.forTenant(tenant.id),
            billing:     stripe === null ? null : { topUpAmounts: config.stripe.topUpAmounts },
        });
        return c.html(html, problems.length > 0 ? 400 : 200);
    };
    const back = (c: Context, slug: string, section: string, notice = "Saved.") =>
        c.redirect(`/settings/${slug}?notice=${encodeURIComponent(notice)}#${section}`, 303);
    // Every per-business route goes through this: an owner asking for another business gets a 404.
    const tenantOr404 = async (c: Context<Env>) => {
        const tenant = await store.tenants.bySlug(c.req.param("slug") ?? "");
        return tenant !== null && Viewer.canSee(c.get("viewer"), tenant)
            ? tenant
            : null;
    };

    // --- sign in -------------------------------------------------------------------------

    app.get("/settings/static/settings.css", async c => c.body(await readFile(new URL("settings.css", STATIC), "utf8"), 200, { "Content-Type": "text/css" }));

    app.get("/settings/login", c => c.html(loginPage("")));

    /**
     * Owners sign in with email and password; the platform admin leaves email blank and gives the admin token.
     */
    app.post("/settings/login", async c => {
        const body = await c.req.parseBody();
        const email = String(body["email"] ?? "").trim().toLowerCase();
        const password = String(body["password"] ?? body["token"] ?? "");
        const now = Date.now();
        const keys = [`ip:${c.req.header("x-forwarded-for")?.split(",")[0]?.trim() ?? "local"}`, ...(email ? [`email:${email}`] : [])];
        if (throttle.blocked(keys, now)) {
            return c.html(loginPage("Too many tries. Wait 15 minutes and try again."), 429);
        }
        if (email === "") {
            if (config.adminToken && Session.validAdmin(Session.admin(password), config.adminToken)) {
                throttle.clear(keys);
                startSession(c, Session.admin(config.adminToken));
                return c.redirect("/settings", 303);
            }
        } else {
            const account = await store.accounts.byEmail(email);
            if (account !== null && await Password.verify(password, account.passwordHash)) {
                throttle.clear(keys);
                await store.accounts.touchLogin(account.id, services.clock());
                startSession(c, Session.owner(account, config.sessionSecret, services.clock()));
                return c.redirect("/settings", 303);
            }
        }
        throttle.fail(keys, now);
        return c.html(loginPage(email === "" ? "That admin token isn't right." : "That email and password don't match."), 401);
    });

    app.post("/settings/logout", c => {
        deleteCookie(c, COOKIE, { path: "/settings" });
        return c.redirect("/settings/login", 303);
    });

    // --- businesses ----------------------------------------------------------------------

    app.get("/settings", auth, async c => {
        const viewer = c.get("viewer");
        if (viewer.kind === "owner") {
            const own = await store.tenants.byId(viewer.account.tenantId);
            return own === null ? c.notFound() : c.redirect(`/settings/${own.slug}`);
        }
        return c.html(indexPage(await store.tenants.all(), domains(), [], await store.accounts.unreviewed()));
    });

    app.post("/settings/:slug/reviewed", auth, adminOnly, async c => {
        const tenant = await tenantOr404(c);
        if (tenant === null) {
            return c.notFound();
        }
        await store.accounts.markReviewed(tenant.id, services.clock());
        return c.redirect("/settings", 303);
    });

    app.get("/settings/quality", auth, adminOnly, async c => {
        const days = [1, 7, 30, 90].find(n => n === Number(c.req.query("days"))) ?? 7;
        const since = new Date(services.clock().getTime() - days * 86_400_000);
        return c.html(qualityPage({
            days,
            experiment: services.composer.experiment,
            variants:   services.composer.variants,
            rows:       await store.quality.report({ since, tenantId: null, domain: null }),
            evals:      await store.quality.evals(since),
        }));
    });

    app.post("/settings/new", auth, adminOnly, async c => {
        const body = await c.req.parseBody();
        const slug = String(body["slug"] ?? "").trim().toLowerCase();
        const result = parseTenant({
            id:       slug,
            slug,
            name:     String(body["name"] ?? "").trim(),
            domain:   String(body["domain"] ?? ""),
            timezone: "America/Indiana/Indianapolis",
        });
        const problems = result.ok ? [] : result.problems.map(p => `${p.path}: ${p.message}`);
        if (result.ok && await store.tenants.bySlug(slug) !== null) {
            problems.push(`'${slug}' is already taken`);
        }
        if (!result.ok || problems.length > 0) {
            return c.html(indexPage(await store.tenants.all(), domains(), problems, await store.accounts.unreviewed()), 400);
        }
        await store.tenants.save(result.tenant);
        return c.redirect(`/settings/${slug}`, 303);
    });

    app.get("/settings/:slug", auth, async c => {
        const tenant = await tenantOr404(c);
        return tenant === null
            ? c.notFound()
            : render(c, tenant, c.req.query("notice") ?? "");
    });

    app.get("/settings/:slug/prompt", auth, async c => {
        const tenant = await tenantOr404(c);
        if (tenant === null) {
            return c.notFound();
        }
        const channel = (["sms", "web", "voice"] as const).find(ch => ch === c.req.query("channel")) ?? "sms";
        const message = (c.req.query("message") ?? "").trim().slice(0, 500) || SAMPLE_MESSAGE;
        return c.html(promptPage(tenant, channel, message, await previewPrompt(deps, tenant, channel, message)));
    });

    app.get("/settings/:slug/conversations/:sessionId", auth, async c => {
        const tenant = await tenantOr404(c);
        const transcript = await store.transcripts.transcript(SessionId(c.req.param("sessionId")));
        if (tenant === null || transcript === null || transcript.session.tenantId !== tenant.id) {
            return c.notFound();
        }
        return c.html(transcriptPage(tenant, registry.get(tenant.domain), transcript));
    });

    // --- section edits -------------------------------------------------------------------

    const edit = (section: string, apply: Edit, after?: (tenant: Tenant, before: Tenant) => Promise<string>, admin = false) => {
        app.post(`/settings/:slug/${section}`, auth, ...(admin ? [adminOnly] : []), async c => {
            const tenant = await tenantOr404(c);
            if (tenant === null) {
                return c.notFound();
            }
            const body = await c.req.parseBody({ all: true });
            const draft = fromTenant(tenant);
            const problems = await apply(draft, body, tenant, c.get("viewer")) ?? [];
            if (problems.length > 0) {
                return render(c, tenant, "", problems);
            }
            const parsed = parseTenant(draft);
            if (!parsed.ok) {
                return render(c, tenant, "", parsed.problems.map(p => `${p.path || "settings"}: ${p.message}`));
            }
            await store.tenants.save(parsed.tenant);
            const notice = after === undefined ? "Saved." : await after(parsed.tenant, tenant);
            return back(c, tenant.slug, section, notice);
        });
    };

    edit("business", (draft, body, _tenant, viewer) => {
        draft.name = one(body, "name");
        // The business type decides the whole assistant; only the platform admin changes it.
        if (viewer.kind === "admin") {
            draft.domain = DomainId(one(body, "domain"));
        }
        draft.phone = one(body, "phone");
        draft.address = one(body, "address");
        draft.website = one(body, "website");
        draft.timezone = one(body, "timezone");
        draft.hoursText = one(body, "hoursText");
        draft.agentName = one(body, "agentName");
        draft.voiceAgentName = one(body, "voiceAgentName");
        return registry.ids().includes(draft.domain)
            ? []
            : [`unknown business type '${draft.domain}'`];
    });

    edit("numbers", (draft, body) => {
        draft.endpoints.voice = lines(one(body, "voice"));
        draft.endpoints.sms = lines(one(body, "sms"));
        draft.settings.transferTo = one(body, "transferTo");
    }, async (tenant) => {
        // A number belongs to one business: take it away from any other that had it.
        const mine = new Set(Object.values(tenant.endpoints).flatMap(list => (list ?? []).map(a => Address.key(a))));
        const moved: string[] = [];
        for (const other of await store.tenants.all()) {
            if (other.id === tenant.id) {
                continue;
            }
            const endpoints = Object.fromEntries(Object.entries(other.endpoints).map(([channel, list]) =>
                [channel, (list ?? []).filter(a => !mine.has(Address.key(a)))]));
            const changed = Object.entries(other.endpoints).some(([channel, list]) => (list ?? []).length !== endpoints[channel]?.length);
            if (changed) {
                await store.tenants.save({ ...other, endpoints });
                moved.push(other.name);
            }
        }
        return moved.length > 0 ? `Saved. Moved the number from ${moved.join(", ")}.` : "Saved.";
    }, true);

    // Owners can't move phone numbers, but they choose where transfers go.
    edit("transfer", (draft, body) => {
        draft.settings.transferTo = one(body, "transferTo");
    });

    edit("assistant", (draft, body) => {
        const honest = one(body, "honestAboutAi");
        const cold = one(body, "coldFollowupMinutes");
        draft.settings.style = {
            professionalism: (one(body, "professionalism") || null) as Professionalism | null,
            honestAboutAi:   honest === "" ? null : honest === "yes",
        };
        draft.settings.maxSessionMinutes = num(body, "maxSessionMinutes") ?? draft.settings.maxSessionMinutes;
        draft.settings.calls = {
            silenceSeconds:    num(body, "silenceSeconds") ?? draft.settings.calls.silenceSeconds,
            stillTherePrompts: num(body, "stillTherePrompts") ?? draft.settings.calls.stillTherePrompts,
            hiddenCallerId:    (["assistant", "voicemail", "reject"] as const).find(v => v === one(body, "hiddenCallerId")) ?? "assistant",
        };
        draft.settings.coldFollowupMinutes = cold === "" ? null : Number(cold);
        draft.settings.recordingDisclosure = on(body, "recordingDisclosure");
        draft.settings.recordingNotice = one(body, "recordingNotice");
        draft.settings.skipProfileGate = on(body, "skipProfileGate");
    });

    edit("privacy", (draft, body) => {
        draft.settings.privacy = {
            redact:    many(body, "redact") as PiiKind[],
            sensitive:     on(body, "sensitive"),
            crisisSupport: on(body, "crisisSupport"),
        };
    });

    edit("languages", (draft, body) => {
        draft.defaultLanguage = one(body, "defaultLanguage") as Language;
        draft.staffLanguage = one(body, "staffLanguage") as Language;
        draft.settings.languages = [...new Set([draft.defaultLanguage, ...many(body, "languages") as Language[]])];
    }, async (tenant, before) => {
        // Customers whose language is no longer offered start over in the main language.
        const dropped = before.settings.languages.filter(l => !tenant.settings.languages.includes(l));
        return dropped.length > 0
            ? "Saved. Customers who used a language you removed will be answered in the main language."
            : "Saved.";
    });

    app.post("/settings/:slug/languages/reset", auth, async c => {
        const tenant = await tenantOr404(c);
        if (tenant === null) {
            return c.notFound();
        }
        const body = await c.req.parseBody();
        const raw = String(body["phone"] ?? "").trim();
        if (raw === "") {
            const n = await store.conversations.resetLanguages(tenant.id, null, tenant.defaultLanguage);
            return back(c, tenant.slug, "languages", `Reset ${n} customer${n === 1 ? "" : "s"} to the main language.`);
        }
        const address = Address.phone(raw);
        const contact = address === null ? null : await store.conversations.contactByAddress(tenant.id, address);
        if (contact === null) {
            return render(c, tenant, "", [`No customer with the number ${raw}.`]);
        }
        await store.conversations.resetLanguages(tenant.id, contact.id, tenant.defaultLanguage);
        return back(c, tenant.slug, "languages", `Reset ${PhoneNumber.display(contact.phone ?? address!.value as PhoneNumber)} to the main language.`);
    });

    edit("schedule", (draft, body) => {
        const windows = [0, 1, 2, 3, 4, 5, 6]
            .filter(day => on(body, `day${day}`))
            .map(day => ({ day: day as Weekday, startMinute: minutes(one(body, `start${day}`)), endMinute: minutes(one(body, `end${day}`)) }));
        if (one(body, "mode") === "windows") {
            if (windows.length === 0) {
                return ["Pick at least one day, or choose Always."];
            }
            const bad = windows.find(w => w.endMinute <= w.startMinute);
            if (bad !== undefined) {
                return ["Each day's end time must be after its start time."];
            }
            draft.schedule = windows;
        } else {
            draft.schedule = "always";
        }
        draft.settings.textAlwaysOn = on(body, "textAlwaysOn");
        draft.settings.callFallbacks = {
            offSchedule: one(body, "offSchedule") as "voicemail",
            lineBusy:    one(body, "lineBusy") as "voicemail",
        };
        return [];
    });

    edit("catalog", (draft, body) => {
        const kind = one(body, "kind");
        const refreshMinutes = Number(one(body, "refreshMinutes")) || 60;
        const source: CatalogSource | null = kind === "manual"
            ? { kind: "manual" }
            : kind === "dataset"
                ? { kind: "dataset", name: one(body, "dataset") }
                : kind === "csv_url"
                    ? { kind: "csv_url", url: one(body, "url"), refreshMinutes }
                    : kind === "json_url"
                        ? { kind: "json_url", url: one(body, "url"), itemsPath: one(body, "itemsPath"), refreshMinutes }
                        : null;
        if (source === null) {
            return ["Pick where the list comes from."];
        }
        draft.settings.catalog = { source, fieldMap: Object.fromEntries(pairs(one(body, "fieldMap"))) };
        return [];
    }, async (tenant) => {
        const result = await syncCatalog(services, tenant);
        if (result === null) {
            return "Saved.";
        }
        return result.ok
            ? `Saved and synced ${result.value} items.`
            : `Saved, but the sync failed: ${result.error}`;
    });

    app.post("/settings/:slug/catalog/sync", auth, async c => {
        const tenant = await tenantOr404(c);
        if (tenant === null) {
            return c.notFound();
        }
        const result = await syncCatalog(services, tenant);
        const notice = result === null
            ? "This list is entered by hand; there is nothing to sync."
            : result.ok
                ? `Synced ${result.value} items.`
                : `Sync failed: ${result.error}`;
        return back(c, tenant.slug, "catalog", notice);
    });

    app.post("/settings/:slug/catalog/upload", auth, async c => {
        const tenant = await tenantOr404(c);
        if (tenant === null) {
            return c.notFound();
        }
        const rows = parsePastedRows(String((await c.req.parseBody())["rows"] ?? ""));
        if (typeof rows === "string") {
            return render(c, tenant, "", [rows]);
        }
        const result = await importRows(services, tenant, rows);
        return result.ok
            ? back(c, tenant.slug, "catalog", `Replaced the list with ${result.value} items.`)
            : render(c, tenant, "", [result.error]);
    });

    edit("details", (draft, body, tenant) => {
        const pack = registry.get(tenant.domain);
        draft.profile = Object.fromEntries(pack.profile.map(q => [q.key, one(body, `profile.${q.key}`)]).filter(([, v]) => v !== ""));
        draft.options = Object.fromEntries(pairs(one(body, "options")).map(([k, v]) => [k, optionValue(v)]));
    });

    edit("notifications", (draft, body, _tenant, viewer) => {
        draft.staff = { emails: lines(one(body, "emails")), phones: lines(one(body, "phones")) as PhoneNumber[] };
        const vias = (name: string) => {
            const chosen = many(body, name).filter((v): v is "email" | "sms" => v === "email" || v === "sms");
            return chosen.length > 0 ? chosen : ["email" as const];
        };
        draft.settings.notifications = {
            transcript:   vias("transcript"),
            booking:      vias("booking"),
            handoff:      vias("handoff"),
            budget_alert: vias("budget_alert"),
        };
        // Add-ons are part of the plan: the admin (or billing) sets them, not the owner.
        if (viewer.kind === "admin") {
            draft.plan = { ...draft.plan, smsNotifications: on(body, "smsNotifications"), extraLines: Number(one(body, "extraLines")) || 0 };
        }
    });

    app.post("/settings/:slug/blocks", auth, async c => {
        const tenant = await tenantOr404(c);
        if (tenant === null) {
            return c.notFound();
        }
        const body = await c.req.parseBody();
        const address = Address.phone(String(body["phone"] ?? ""));
        if (address === null) {
            return render(c, tenant, "", ["Enter a phone number to block."]);
        }
        const now = services.clock();
        const contact = await store.conversations.contactByAddress(tenant.id, address)
            ?? (await store.conversations.resolve(tenant.id, address, now)).contact;
        const hours = Number(String(body["hours"] ?? "").trim());
        await store.blocks.block(contact.id, {
            until:     hours > 0 ? new Date(now.getTime() + hours * 3600_000) : null,
            source:    "staff",
            reason:    String(body["reason"] ?? "").trim() || "blocked by staff",
            createdAt: now,
        });
        return back(c, tenant.slug, "blocks", "Blocked.");
    });

    app.post("/settings/:slug/blocks/:contactId/remove", auth, async c => {
        const tenant = await tenantOr404(c);
        if (tenant === null) {
            return c.notFound();
        }
        const contact = await store.conversations.contact(ContactId(c.req.param("contactId")));
        if (contact === null || contact.tenantId !== tenant.id) {
            return c.notFound();
        }
        await store.blocks.unblock(contact.id);
        return back(c, tenant.slug, "blocks", "Unblocked.");
    });

    // --- billing ---------------------------------------------------------------------------

    const billing = (name: string, start: (tenant: Tenant, c: Context<Env>, body: Body) => Promise<string>) => {
        app.post(`/settings/:slug/billing/${name}`, auth, async c => {
            const tenant = await tenantOr404(c);
            if (tenant === null || stripe === null) {
                return c.notFound();
            }
            try {
                return c.redirect(await start(tenant, c, await c.req.parseBody({ all: true })), 303);
            } catch (e) {
                console.error(`stripe ${name} failed for ${tenant.slug}:`, e);
                return render(c, tenant, "", ["Billing isn't available right now. Please try again in a few minutes."]);
            }
        });
    };
    const email = (tenant: Tenant) => tenant.staffContacts.find(a => a.kind === "email")?.value ?? null;
    const returnTo = (c: Context, tenant: Tenant, notice: string) => `${origin(c)}/settings/${tenant.slug}?notice=${encodeURIComponent(notice)}#billing`;

    billing("subscribe", async (tenant, c) => {
        const p = config.stripe;
        const sub = tenant.plan.subscription;
        return stripe!.subscriptionCheckout({
            tenantId:   tenant.id,
            email:      email(tenant),
            customerId: tenant.billing.customerId,
            lines:      [
                { price: p.priceBase, quantity: 1 },
                ...(p.priceExtraLine ? [{ price: p.priceExtraLine, quantity: sub.extraLines }] : []),
                ...(p.priceSmsAlerts ? [{ price: p.priceSmsAlerts, quantity: sub.smsNotifications ? 1 : 0 }] : []),
            ],
            successUrl: returnTo(c, tenant, "Thanks! Your subscription is being set up."),
            cancelUrl:  returnTo(c, tenant, "Checkout canceled."),
        });
    });

    billing("topup", async (tenant, c, body) => {
        const amount = Number(one(body, "amount"));
        if (!config.stripe.topUpAmounts.includes(amount)) {
            throw new Error(`top-up amount ${amount} not offered`);
        }
        return stripe!.topUpCheckout({
            tenantId:   tenant.id,
            customerId: tenant.billing.customerId,
            email:      email(tenant),
            amountUsd:  amount,
            successUrl: returnTo(c, tenant, `Thanks! $${amount} will be added to this month's allowance.`),
            cancelUrl:  returnTo(c, tenant, "Top-up canceled."),
        });
    });

    billing("portal", async (tenant, c) => {
        if (tenant.billing.customerId === null) {
            throw new Error("no Stripe customer yet");
        }
        return stripe!.portal(tenant.billing.customerId, `${origin(c)}/settings/${tenant.slug}#billing`);
    });

    // --- logins ----------------------------------------------------------------------------

    app.post("/settings/:slug/accounts", auth, adminOnly, async c => {
        const tenant = await tenantOr404(c);
        if (tenant === null) {
            return c.notFound();
        }
        const body = await c.req.parseBody();
        const email = String(body["email"] ?? "").trim().toLowerCase();
        const password = String(body["password"] ?? "");
        const problem = !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) ? "Enter a valid email address." : Password.problem(password);
        if (problem !== null) {
            return render(c, tenant, "", [problem]);
        }
        const created = await store.accounts.create({
            id: randomUUID(), tenantId: tenant.id, email, name: String(body["name"] ?? "").trim(),
            passwordHash: await Password.hash(password), createdAt: services.clock(), lastLoginAt: null,
        });
        return created
            ? back(c, tenant.slug, "logins", `Added a login for ${email}.`)
            : render(c, tenant, "", [`${email} already has a login.`]);
    });

    app.post("/settings/:slug/accounts/:id/remove", auth, adminOnly, async c => {
        const tenant = await tenantOr404(c);
        const account = await store.accounts.byId(c.req.param("id"));
        if (tenant === null || account === null || account.tenantId !== tenant.id) {
            return c.notFound();
        }
        await store.accounts.remove(account.id);
        return back(c, tenant.slug, "logins", `Removed the login for ${account.email}.`);
    });

    /**
     * The admin sets a new password; an owner changes their own, giving the current one.
     */
    app.post("/settings/:slug/accounts/:id/password", auth, async c => {
        const tenant = await tenantOr404(c);
        const account = await store.accounts.byId(c.req.param("id"));
        const viewer = c.get("viewer");
        if (tenant === null || account === null || account.tenantId !== tenant.id
            || (viewer.kind === "owner" && viewer.account.id !== account.id)) {
            return c.notFound();
        }
        const body = await c.req.parseBody();
        const password = String(body["password"] ?? "");
        if (viewer.kind === "owner" && !await Password.verify(String(body["current"] ?? ""), account.passwordHash)) {
            return render(c, tenant, "", ["Your current password isn't right."]);
        }
        const problem = Password.problem(password);
        if (problem !== null) {
            return render(c, tenant, "", [problem]);
        }
        const hash = await Password.hash(password);
        await store.accounts.setPassword(account.id, hash);
        if (viewer.kind === "owner") {
            // The old session was signed with the old password; start a new one so they stay signed in.
            startSession(c, Session.owner({ ...account, passwordHash: hash }, config.sessionSecret, services.clock()));
        }
        return back(c, tenant.slug, "logins", "Password changed.");
    });

    return app;
}
