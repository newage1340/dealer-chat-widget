/**
 * The settings website: plain HTML forms over the same tenant file the admin API takes.
 *
 * Every section posts its own form; the handler edits the tenant's file form, validates it with
 * `parseTenant` (so the page can never save something the API would refuse), saves, and redirects back.
 * Sign-in is the admin token, kept in an HTTP-only, same-site cookie.
 */

import { createHash, timingSafeEqual } from "node:crypto";
import { readdir, readFile } from "node:fs/promises";

import { Hono, type Context, type MiddlewareHandler } from "hono";
import { deleteCookie, getCookie, setCookie } from "hono/cookie";

import { DATASETS_DIR, importRows, parsePastedRows, syncCatalog } from "../../catalogs/index.ts";
import {
    Address,
    ContactId,
    DomainId,
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
import { fromTenant, parseTenant, type TenantDraft } from "../../tenants/schema.ts";
import { previewPrompt } from "../admin.ts";
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
type Edit = (draft: TenantDraft, body: Body, tenant: Tenant) => string[] | void | Promise<string[] | void>;

export function settingsRoutes(deps: ChannelDeps): Hono {
    const app = new Hono();
    const { services, config } = deps;
    const { store, registry } = services;
    const token = createHash("sha256").update(`settings:${config.adminToken}`).digest("hex");

    const signedIn = (c: Context): boolean => {
        const cookie = getCookie(c, COOKIE) ?? "";
        return config.adminToken !== ""
            && cookie.length === token.length
            && timingSafeEqual(Buffer.from(cookie), Buffer.from(token));
    };
    const auth: MiddlewareHandler = async (c, next) => signedIn(c)
        ? next()
        : c.redirect("/settings/login");

    const domains = () => registry.ids().map(id => [id, registry.get(id).name] as const).sort((a, b) => a[1].localeCompare(b[1]));
    const datasets = async () => (await readdir(DATASETS_DIR).catch(() => [] as string[]))
        .filter(n => n.endsWith(".json") || n.endsWith(".csv"))
        .sort();

    const render = async (c: Context, tenant: Tenant, notice: string, problems: readonly string[] = []) => {
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
        });
        return c.html(html, problems.length > 0 ? 400 : 200);
    };
    const back = (c: Context, slug: string, section: string, notice = "Saved.") =>
        c.redirect(`/settings/${slug}?notice=${encodeURIComponent(notice)}#${section}`, 303);
    const tenantOr404 = async (c: Context) => store.tenants.bySlug(c.req.param("slug") ?? "");

    // --- sign in -------------------------------------------------------------------------

    app.get("/settings/static/settings.css", async c => c.body(await readFile(new URL("settings.css", STATIC), "utf8"), 200, { "Content-Type": "text/css" }));

    app.get("/settings/login", c => c.html(loginPage(config.adminToken ? "" : "Set ADMIN_TOKEN in .env to use the settings pages.")));

    app.post("/settings/login", async c => {
        const body = await c.req.parseBody();
        const given = createHash("sha256").update(`settings:${String(body["token"] ?? "")}`).digest("hex");
        if (!config.adminToken || given !== token) {
            return c.html(loginPage("That token isn't right."), 401);
        }
        setCookie(c, COOKIE, token, { httpOnly: true, sameSite: "Lax", path: "/settings", secure: config.publicBaseUrl.startsWith("https://"), maxAge: 7 * 24 * 3600 });
        return c.redirect("/settings", 303);
    });

    app.post("/settings/logout", c => {
        deleteCookie(c, COOKIE, { path: "/settings" });
        return c.redirect("/settings/login", 303);
    });

    // --- businesses ----------------------------------------------------------------------

    app.get("/settings", auth, async c => c.html(indexPage(await store.tenants.all(), domains(), [])));

    app.get("/settings/quality", auth, async c => {
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

    app.post("/settings/new", auth, async c => {
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
            return c.html(indexPage(await store.tenants.all(), domains(), problems), 400);
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
        return c.html(promptPage(tenant, channel, await previewPrompt(deps, tenant, channel, "Hi, what do you have?")));
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

    const edit = (section: string, apply: Edit, after?: (tenant: Tenant, before: Tenant) => Promise<string>) => {
        app.post(`/settings/:slug/${section}`, auth, async c => {
            const tenant = await tenantOr404(c);
            if (tenant === null) {
                return c.notFound();
            }
            const body = await c.req.parseBody({ all: true });
            const draft = fromTenant(tenant);
            const problems = await apply(draft, body, tenant) ?? [];
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

    edit("business", (draft, body) => {
        draft.name = one(body, "name");
        draft.domain = DomainId(one(body, "domain"));
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

    edit("notifications", (draft, body) => {
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
        draft.plan = { ...draft.plan, smsNotifications: on(body, "smsNotifications"), extraLines: Number(one(body, "extraLines")) || 0 };
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

    return app;
}
