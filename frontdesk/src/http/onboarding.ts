/**
 * The public onboarding pages: a new client picks a business type, describes the business,
 * and the answers become a tenant (no phone numbers yet; staff assign those in /settings).
 *
 * The form only reaches what a client may set: business details, hours, the pack's questions,
 * tone, languages and the catalog. Endpoints, plan, privacy and admin settings keep their defaults.
 * Everything is validated through `parseTenant`, like the settings pages.
 */

import { createHash, timingSafeEqual } from "node:crypto";

import { Hono, type Context } from "hono";

import { importRows, parsePastedRows, syncCatalog } from "../catalogs/index.ts";
import {
    LANGUAGE_NAMES,
    PhoneNumber,
    Professionalism,
    type Language,
    type Tenant,
    type WeekdayName,
} from "../core/index.ts";
import type { DomainPack } from "../domains/index.ts";
import { Hours } from "../prompts/hours.ts";
import { Style } from "../prompts/style.ts";
import { parseTenant, type TenantFile } from "../tenants/schema.ts";
import type { ChannelDeps } from "./context.ts";
import { check, checkbox, field, form, html, input, page, raw, select, textarea, type Html } from "./settings/html.ts";

const MAX_FIELD   = 5_000;
const MAX_CATALOG = 200_000;
const NOTES_KEY   = "policies";

const DAYS: readonly WeekdayName[] = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];

const TIME_ZONES: readonly (readonly [string, string])[] = [
    ["America/New_York",             "Eastern"],
    ["America/Indiana/Indianapolis", "Eastern (Indiana)"],
    ["America/Chicago",              "Central"],
    ["America/Denver",               "Mountain"],
    ["America/Phoenix",              "Mountain (Arizona)"],
    ["America/Los_Angeles",          "Pacific"],
    ["America/Anchorage",            "Alaska"],
    ["Pacific/Honolulu",             "Hawaii"],
    ["America/Puerto_Rico",          "Atlantic (Puerto Rico)"],
];
const DEFAULT_TIME_ZONE = "America/Indiana/Indianapolis";

const LANGUAGE_OPTIONS = Object.entries(LANGUAGE_NAMES) as [Language, string][];

/**
 * Friendlier names for the tenant fields a problem can point at.
 */
const PROBLEM_LABELS: Readonly<Record<string, string>> = {
    "name":                         "Business name",
    "phone":                        "Main phone",
    "timezone":                     "Time zone",
    "staff.emails.0":               "Email for summaries and alerts",
    "settings.catalog.source.url":  "Catalog link",
};

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

function same(a: string, b: string): boolean {
    const digest = (s: string) => createHash("sha256").update(`onboarding:${s}`).digest();
    return timingSafeEqual(digest(a), digest(b));
}

/**
 * Minutes after midnight from "13:30"; null when it isn't a clock time.
 */
function minuteOf(clock: string): number | null {
    const m = /^(\d{1,2}):(\d{2})$/.exec(clock);
    if (m === null || Number(m[1]) > 23 || Number(m[2]) > 59) {
        return null;
    }
    return Number(m[1]) * 60 + Number(m[2]);
}

/**
 * Minutes after midnight as "1:30pm".
 */
function spoken(minuteOfDay: number): string {
    const hour = Math.floor(minuteOfDay / 60);
    const minute = minuteOfDay % 60;
    const suffix = hour < 12 ? "am" : "pm";
    const h12 = hour % 12 === 0 ? 12 : hour % 12;
    return minute === 0
        ? `${h12}${suffix}`
        : `${h12}:${String(minute).padStart(2, "0")}${suffix}`;
}

/**
 * The hours table as hoursText ("Monday: 9am to 6pm, ..., Sunday: closed"), or problems.
 */
export function hoursFromForm(body: Body): { readonly text: string; readonly problems: readonly string[] } {
    const problems: string[] = [];
    const parts = DAYS.map(day => {
        if (on(body, `closed.${day}`)) {
            return `${day}: closed`;
        }
        if (on(body, `appointment.${day}`)) {
            return `${day}: by appointment only`;
        }
        const opens = minuteOf(one(body, `opens.${day}`));
        const closes = minuteOf(one(body, `closes.${day}`));
        if (opens === null || closes === null) {
            problems.push(`${day}: enter when you open and close, or mark it closed.`);
            return "";
        }
        if (closes <= opens) {
            problems.push(`${day}: the closing time must be after the opening time.`);
            return "";
        }
        return `${day}: ${spoken(opens)} to ${spoken(closes)}`;
    });
    if (problems.length > 0) {
        return { text: "", problems };
    }
    const text = parts.join(", ");
    const parsed = Hours.parse(text);
    const missing = DAYS.filter(day => parsed[day] === undefined);
    return missing.length > 0
        ? { text: "", problems: [`Couldn't read the hours for ${missing.join(", ")}.`] }
        : { text, problems: [] };
}

/**
 * Lowercase letters, digits and dashes from a business name.
 */
export function slugify(name: string): string {
    const slug = name
        .normalize("NFKD")
        .replace(/[̀-ͯ]/g, "")
        .toLowerCase()
        .replace(/&/g, " and ")
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/^-+|-+$/g, "")
        .slice(0, 48)
        .replace(/-+$/, "");
    return slug || "business";
}

function capitalized(text: string): string {
    return text.charAt(0).toUpperCase() + text.slice(1);
}

/**
 * The pack question whose answer holds "anything else", if the pack has one.
 */
function notesQuestion(pack: DomainPack) {
    return pack.profile.find(q => q.key === NOTES_KEY) ?? null;
}

// --- pages -------------------------------------------------------------------------------

function problemList(problems: readonly string[]): Html {
    return problems.length > 0
        ? html`<div role="alert"><p><strong>Please fix these:</strong></p><ul>${problems.map(p => html`<li>${p}</li>`)}</ul></div>\n`
        : raw("");
}

function pickerPage(types: readonly (readonly [string, string])[], problems: readonly string[]): string {
    return page("Get started", html`
<header><h1>Get started</h1></header>
<main>
<p>Tell us about your business and we'll set up an assistant that answers your customers' calls, texts and web chats.
It takes about ten minutes. Start by choosing the kind of business you run.</p>
${problemList(problems)}
<form method="get" action="/onboarding/form">
${field("Business type", select("type", types, types[0]?.[0] ?? ""))}
<p><button type="submit">Continue</button></p>
</form>
</main>`);
}

type FormData = {
    readonly pack:     DomainPack;
    readonly body:     Body | null;
    readonly problems: readonly string[];
    readonly needCode: boolean;
};

function formPage(d: FormData): string {
    const { pack, body } = d;
    const value = (name: string, fallback = "") => body === null ? fallback : one(body, name);
    const ticked = (name: string, fallback = false) => body === null ? fallback : on(body, name);
    const required = (label: string, yes: boolean) => yes ? `${label} (required)` : label;
    const spec = pack.catalog;
    const notes = notesQuestion(pack);
    const questions = pack.profile.filter(q => q.key !== NOTES_KEY);
    const levels: [string, string][] = [
        ["", `Business type default (${Style.describe(pack.defaults.professionalism)})`],
        ...Object.values(Professionalism).map((l): [string, string] => [l, l]),
    ];
    const offered = body === null ? [] : many(body, "languages");
    const link = value("catalogLink", "csv_url");
    return page("Tell us about your business", html`
<header>
<p><a href="/onboarding">Choose a different business type</a></p>
<h1>Tell us about your business</h1>
<p>Business type: ${pack.name}</p>
</header>
<main>
${problemList(d.problems)}
${form("/onboarding", html`
<input type="hidden" name="type" value="${pack.id}">
${d.needCode ? field(required("Invite code", true), input("inviteCode", value("inviteCode"))) : ""}
<fieldset>
<legend><strong>About your business</strong></legend>
${field(required("Business name", true), input("name", value("name")))}
${field(required("Main phone", true), input("phone", value("phone"), "tel"))}
${field("Address", input("address", value("address")))}
${field("Website", input("website", value("website"), "url"))}
${field("Time zone", select("timezone", TIME_ZONES, value("timezone", DEFAULT_TIME_ZONE)))}
${field(required("Email for summaries and alerts", true), input("email", value("email"), "email"), "where we send conversation summaries and alerts")}
</fieldset>
<fieldset>
<legend><strong>Hours</strong></legend>
<table>
<thead><tr><th>Day</th><th>Closed</th><th>Opens</th><th>Closes</th><th>By appointment only</th></tr></thead>
<tbody>
${DAYS.map(day => {
        const weekend = day === "Saturday" || day === "Sunday";
        return html`<tr><td>${day}</td><td>${checkbox(`closed.${day}`, ticked(`closed.${day}`, weekend))}</td><td>${input(`opens.${day}`, value(`opens.${day}`, "09:00"), "time")}</td><td>${input(`closes.${day}`, value(`closes.${day}`, "17:00"), "time")}</td><td>${checkbox(`appointment.${day}`, ticked(`appointment.${day}`))}</td></tr>\n`;
    })}
</tbody>
</table>
</fieldset>
${questions.length > 0 ? html`<fieldset>
<legend><strong>About your ${pack.businessNoun}</strong></legend>
${questions.map(q => field(required(q.label, q.required), textarea(`profile.${q.key}`, value(`profile.${q.key}`), 3), q.question))}
</fieldset>` : ""}
<fieldset>
<legend><strong>How the assistant should sound</strong></legend>
${field("How formal", select("professionalism", levels, value("professionalism")))}
${field("Assistant's name", input("agentName", value("agentName")), `optional; blank: ${pack.defaultAgentName}`)}
</fieldset>
<fieldset>
<legend><strong>Languages offered</strong></legend>
<p>English is always offered.</p>
${LANGUAGE_OPTIONS.filter(([code]) => code !== "en").map(([code, name]) => html`<p>${check(name, "languages", offered.includes(code), code)}</p>`)}
${field("Main language", select("defaultLanguage", LANGUAGE_OPTIONS, value("defaultLanguage", "en")), "used with new customers")}
</fieldset>
<fieldset>
<legend><strong>Your ${spec.itemNounPlural}</strong></legend>
<p>Optional. Paste a list, or give us a link we can check for updates. You can also send it later.</p>
<p><small>Columns we understand: ${spec.fields.map(f => `${f.name} (${f.label || f.name})`).join(", ")}</small></p>
${field(`Paste your ${spec.itemNounPlural}`, textarea("catalogRows", value("catalogRows"), 8), "CSV with a header row, or a JSON list")}
${field("Or a link", input("catalogUrl", value("catalogUrl"), "url"))}
<p><label><input type="radio" name="catalogLink" value="csv_url"${raw(link !== "json_url" ? " checked" : "")}> CSV link (e.g. a spreadsheet published as CSV)</label>
<label><input type="radio" name="catalogLink" value="json_url"${raw(link === "json_url" ? " checked" : "")}> JSON link</label></p>
</fieldset>
<fieldset>
<legend><strong>Anything else the assistant should know</strong></legend>
${field(required(notes?.label ?? "Notes", notes?.required ?? false), textarea("notes", value("notes"), 5), notes?.question ?? "payment methods, things you don't do, anything customers often ask")}
</fieldset>
`, "Send")}
</main>`);
}

function donePage(tenant: Tenant, pack: DomainPack, items: number, error: string | null): string {
    const source = tenant.settings.catalog.source;
    const where = source.kind === "csv_url" || source.kind === "json_url"
        ? html` (from ${source.url})`
        : "";
    return page("Thank you", html`
<header><h1>Thank you</h1></header>
<main>
<p>We have your information. Our team will set up a phone number for ${tenant.name} and contact you when the assistant is ready.
Summaries and alerts will go to the email you gave us.</p>
<section>
<h2>What we saved</h2>
<dl>
<dt>Business name</dt><dd>${tenant.name}</dd>
<dt>Business type</dt><dd>${pack.name}</dd>
<dt>Main phone</dt><dd>${tenant.phone === null ? "" : PhoneNumber.display(tenant.phone)}</dd>
<dt>Address</dt><dd>${tenant.address || "(none)"}</dd>
<dt>Website</dt><dd>${tenant.website || "(none)"}</dd>
<dt>Time zone</dt><dd>${tenant.timezone}</dd>
<dt>Hours</dt><dd>${tenant.hoursText}</dd>
<dt>Languages</dt><dd>${tenant.settings.languages.map(l => LANGUAGE_NAMES[l]).join(", ")} (main: ${LANGUAGE_NAMES[tenant.defaultLanguage]})</dd>
<dt>Assistant's name</dt><dd>${tenant.agentName || pack.defaultAgentName}</dd>
<dt>${capitalized(pack.catalog.itemNounPlural)}</dt><dd>${items}${where}${error !== null ? html` - we couldn't read your list (${error}); we'll follow up about it` : ""}</dd>
</dl>
</section>
</main>`);
}

// --- routes ------------------------------------------------------------------------------

export function onboardingRoutes(deps: ChannelDeps): Hono {
    const app = new Hono();
    const { services, config } = deps;
    const { store, registry } = services;
    const needCode = () => config.onboardingCode !== "";

    const types = () => registry.ids()
        .map(id => [id, registry.get(id).name] as const)
        .sort((a, b) => a[1].localeCompare(b[1]));
    const packFor = (id: string): DomainPack | null => registry.ids().includes(id as DomainPack["id"])
        ? registry.get(id as DomainPack["id"])
        : null;
    const showForm = (c: Context, pack: DomainPack, body: Body | null, problems: readonly string[]) =>
        c.html(formPage({ pack, body, problems, needCode: needCode() }), problems.length > 0 ? 400 : 200);

    const freeSlug = async (name: string): Promise<string> => {
        const base = slugify(name);
        for (let n = 1; ; n++) {
            const slug = n === 1 ? base : `${base}-${n}`;
            if (await store.tenants.bySlug(slug) === null) {
                return slug;
            }
        }
    };

    app.get("/onboarding", c => c.html(pickerPage(types(), [])));

    app.get("/onboarding/form", c => {
        const pack = packFor(c.req.query("type") ?? "");
        return pack === null
            ? c.html(pickerPage(types(), ["Choose a business type."]), 400)
            : showForm(c, pack, null, []);
    });

    app.post("/onboarding", async c => {
        const body = await c.req.parseBody({ all: true });
        const pack = packFor(one(body, "type"));
        if (pack === null) {
            return c.html(pickerPage(types(), ["Choose a business type."]), 400);
        }
        const problems: string[] = [];

        if (needCode() && !same(one(body, "inviteCode"), config.onboardingCode)) {
            problems.push("The invite code isn't right.");
        }
        for (const [name, v] of Object.entries(body)) {
            const texts = (Array.isArray(v) ? v : [v]).filter((x): x is string => typeof x === "string");
            const limit = name === "catalogRows" ? MAX_CATALOG : MAX_FIELD;
            if (texts.some(t => t.length > limit)) {
                problems.push(name === "catalogRows"
                    ? "The pasted list is too long (200 KB at most); send a link instead."
                    : `One of the answers is too long (${MAX_FIELD.toLocaleString("en-US")} characters at most).`);
            }
        }

        const name = one(body, "name");
        const email = one(body, "email");
        if (name === "") {
            problems.push("Business name is required.");
        }
        if (one(body, "phone") === "") {
            problems.push("Main phone is required.");
        }
        if (email === "") {
            problems.push("Email for summaries and alerts is required.");
        }
        const timezone = one(body, "timezone") || DEFAULT_TIME_ZONE;
        if (!TIME_ZONES.some(([zone]) => zone === timezone)) {
            problems.push("Choose a time zone from the list.");
        }

        const hours = hoursFromForm(body);
        problems.push(...hours.problems);

        const profile: Record<string, string> = {};
        for (const q of pack.profile.filter(q => q.key !== NOTES_KEY)) {
            const answer = one(body, `profile.${q.key}`);
            if (answer !== "") {
                profile[q.key] = answer;
            } else if (q.required) {
                problems.push(`${q.label} is required.`);
            }
        }
        const notes = one(body, "notes");
        const notesQ = notesQuestion(pack);
        if (notes !== "") {
            profile[notesQ?.key ?? "notes"] = notes;
        } else if (notesQ?.required) {
            problems.push(`${notesQ.label} is required.`);
        }

        const level = one(body, "professionalism");
        if (level !== "" && !Object.values(Professionalism).includes(level as Professionalism)) {
            problems.push("Choose how formal the assistant should be from the list.");
        }

        const rowsText = one(body, "catalogRows");
        const url = one(body, "catalogUrl");
        if (rowsText !== "" && url !== "") {
            problems.push("Paste a list or give a link, not both.");
        }
        const source = url === ""
            ? { kind: "manual" as const }
            : one(body, "catalogLink") === "json_url"
                ? { kind: "json_url" as const, url }
                : { kind: "csv_url" as const, url };

        if (problems.length > 0) {
            return showForm(c, pack, body, problems);
        }

        const defaultLanguage = one(body, "defaultLanguage") || "en";
        const slug = await freeSlug(name);
        const agentName = one(body, "agentName");
        const draft = {
            id:              slug,
            slug,
            name,
            domain:          pack.id,
            timezone,
            phone:           one(body, "phone"),
            address:         one(body, "address"),
            website:         one(body, "website"),
            agentName,
            voiceAgentName:  agentName,
            staff:           { emails: [email] },
            hoursText:       hours.text,
            defaultLanguage: defaultLanguage as Language,
            profile,
            options:         {},
            settings:        {
                languages: [...new Set(["en", defaultLanguage, ...many(body, "languages")])] as Language[],
                style:     { professionalism: (level || null) as Professionalism | null, honestAboutAi: null },
                catalog:   { source, fieldMap: {} },
            },
        } satisfies TenantFile;
        const parsed = parseTenant(draft);
        if (!parsed.ok) {
            return showForm(c, pack, body, parsed.problems.map(p => `${PROBLEM_LABELS[p.path] ?? (p.path || "Form")}: ${p.message}`));
        }
        const tenant = parsed.tenant;
        await store.tenants.save(tenant);

        // The catalog never fails onboarding: how it went is recorded and shown on the next page.
        if (source.kind !== "manual") {
            await syncCatalog(services, tenant);
        } else if (rowsText !== "") {
            const rows = parsePastedRows(rowsText);
            const result = typeof rows === "string"
                ? { ok: false as const, error: rows }
                : await importRows(services, tenant, rows);
            await store.tenants.recordSync(tenant.id, {
                syncedAt: services.clock(),
                items:    result.ok ? result.value : 0,
                error:    result.ok ? null : result.error,
            });
        }
        return c.redirect(`/onboarding/done?slug=${encodeURIComponent(slug)}`, 303);
    });

    app.get("/onboarding/done", async c => {
        const tenant = await store.tenants.bySlug(c.req.query("slug") ?? "");
        if (tenant === null) {
            return c.notFound();
        }
        const pack = registry.get(tenant.domain);
        const items = (await store.tenants.catalog(tenant.id)).length;
        const sync = await store.tenants.syncStatus(tenant.id);
        return c.html(donePage(tenant, pack, items, sync?.error ?? null));
    });

    return app;
}
