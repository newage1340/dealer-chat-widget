/**
 * The settings pages, rendered from plain data. Each section is its own form posting to
 * /settings/<slug>/<section>, so saving one never touches another.
 */

import {
    Budget,
    LANGUAGE_NAMES,
    LocalTime,
    MIN_PASSWORD_LENGTH,
    PhoneNumber,
    PiiKind,
    Plan,
    PLATFORM_BOUNDS,
    Professionalism,
    Tenant,
    Text,
    WEEKDAY_NAMES,
    type Account,
    type Bound,
    type CatalogItem,
    type CatalogSyncStatus,
    type Contact,
    type EvalSummary,
    type Language,
    type PromptVariant,
    type QualityRow,
    type Transcript,
} from "../../core/index.ts";
import { Catalog, DomainPack } from "../../domains/index.ts";
import { Style } from "../../prompts/style.ts";
import type { Viewer } from "./auth.ts";
import { check, field, form, html, input, page, raw, select, textarea, type Html } from "../html.ts";

const LANGUAGE_OPTIONS = Object.entries(LANGUAGE_NAMES) as [Language, string][];

const PII_LABELS: Readonly<Record<PiiKind, string>> = {
    card:         "Payment card numbers",
    ssn:          "Social security numbers",
    bank_account: "Bank account and routing numbers",
    birth_date:   "Dates of birth",
};

const NOTICE_LABELS = {
    transcript:   "Conversation transcripts",
    booking:      "New bookings",
    handoff:      "Callback requests and transfers",
    budget_alert: "Monthly allowance alerts",
} as const;

const FALLBACKS: readonly (readonly [string, string])[] = [
    ["voicemail", "Our voicemail (records and emails you the message)"],
    ["forward", "Forward to the transfer number"],
    ["reject", "Don't answer"],
];

export type TenantPageData = {
    readonly tenant:      Tenant;
    readonly pack:        DomainPack;
    readonly domains:     readonly (readonly [string, string])[];
    readonly items:       readonly CatalogItem[];
    readonly sync:        CatalogSyncStatus | null;
    readonly datasets:    readonly string[];
    readonly blocked:     readonly Contact[];
    readonly transcripts: readonly Transcript[];
    readonly monthToDate: number;
    readonly notice:      string;
    readonly problems:    readonly string[];
    readonly warnings:    readonly string[];
    readonly viewer:      Viewer;
    readonly accounts:    readonly Account[];
    readonly billing:     { readonly topUpAmounts: readonly number[] } | null;
};

function clock(minute: number): string {
    const h = Math.floor(minute / 60) % 24;
    const m = minute % 60;
    return `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}`;
}

function phones(values: readonly string[]): string {
    return values.map(v => PhoneNumber.parse(v) !== null ? PhoneNumber.display(PhoneNumber.parse(v)!) : v).join("\n");
}

function when(date: Date | null, tenant: Tenant): string {
    return date === null
        ? ""
        : LocalTime.long(LocalTime.of(new Date(date), tenant.timezone));
}

// --- pages -------------------------------------------------------------------------------

export function loginPage(error: string): string {
    return page("Sign in", html`
<header><h1>Front desk settings</h1></header>
<main>
${error ? html`<p><strong>${error}</strong></p>` : ""}
${form("/settings/login", html`
${field("Email", input("email", "", "email", "autocomplete=\"username\""))}
${field("Password", input("password", "", "password", "autocomplete=\"current-password\""))}
<p><small>Platform admin: leave the email blank and enter the admin token as the password.</small></p>
`, "Sign in")}
</main>`);
}

export function indexPage(
    tenants: readonly Tenant[],
    domains: readonly (readonly [string, string])[],
    problems: readonly string[],
    unreviewed: readonly string[] = []
): string
{
    const names = Object.fromEntries(domains);
    return page("Businesses", html`
<header>
<h1>Businesses</h1>
<nav><ul><li><a href="/settings/quality">Quality</a></li><li><a href="/onboarding">Onboarding page</a></li></ul></nav>
${form("/settings/logout", raw(""), "Sign out")}
</header>
<main>
<section id="businesses">
<h2>All businesses</h2>
<table>
<thead><tr><th>Name</th><th>Type</th><th>Phone numbers</th><th>Status</th></tr></thead>
<tbody>
${tenants.map(t => html`<tr><td><a href="/settings/${t.slug}">${t.name}</a></td><td>${names[t.domain] ?? t.domain}</td><td>${[...(t.endpoints.voice ?? [])].map(a => a.value).join(", ")}</td><td>${unreviewed.includes(t.id)
        ? html`<strong>New from onboarding, not reviewed</strong> ${form(`/settings/${t.slug}/reviewed`, raw(""), "Mark reviewed")}`
        : ""}</td></tr>\n`)}
</tbody>
</table>
</section>
<section id="new">
<h2>Add a business</h2>
${problems.length > 0 ? html`<ul>${problems.map(p => html`<li>${p}</li>`)}</ul>` : ""}
${form("/settings/new", html`
${field("Name", input("name", ""))}
${field("Web address name", input("slug", ""), "lowercase letters, digits and dashes, e.g. joes-pizza")}
${field("Business type", select("domain", domains, domains[0]?.[0] ?? ""))}
`, "Create")}
</section>
</main>`);
}

export function tenantPage(d: TenantPageData): string {
    const { tenant, pack } = d;
    const sections: [string, string][] = [
        ["business", "Business"],
        ["numbers", "Phone numbers"],
        ["assistant", "Assistant"],
        ["privacy", "Privacy"],
        ["languages", "Languages"],
        ["schedule", "Schedule"],
        ["catalog", Text.capitalize(pack.catalog.itemNounPlural)],
        ["details", "Business details"],
        ["notifications", "Notifications"],
        ["blocks", "Blocked callers"],
        ["logins", "Logins"],
        ["usage", "Usage"],
        ...(d.billing !== null ? [["billing", "Billing"] as [string, string]] : []),
        ["conversations", "Conversations"],
    ];
    return page(`${tenant.name} settings`, html`
<header>
${d.viewer.kind === "admin"
        ? html`<p><a href="/settings">All businesses</a></p>`
        : html`<p>Signed in as ${d.viewer.account.email}</p>`}
${form("/settings/logout", raw(""), "Sign out")}
<h1>${tenant.name}</h1>
<p>${pack.name} &middot; <a href="/widget/${tenant.slug}">Open web chat</a> &middot; <a href="/settings/${tenant.slug}/prompt">See the assistant's instructions</a></p>
<nav><ul>${sections.map(([id, label]) => html`<li><a href="#${id}">${label}</a></li>`)}</ul></nav>
</header>
<main>
${d.notice ? html`<p role="status"><strong>${d.notice}</strong></p>` : ""}
${d.problems.length > 0 ? html`<div role="alert"><p><strong>Not saved:</strong></p><ul>${d.problems.map(p => html`<li>${p}</li>`)}</ul></div>` : ""}
${d.warnings.length > 0 ? html`<section id="check"><h2>Things to check</h2><ul>${d.warnings.map(w => html`<li>${w}</li>`)}</ul></section>` : ""}
${business(d)}
${numbers(d)}
${assistant(d)}
${privacy(d)}
${languages(d)}
${schedule(d)}
${catalog(d)}
${details(d)}
${notifications(d)}
${blocks(d)}
${logins(d)}
${usage(d)}
${billingSection(d)}
${conversations(d)}
</main>`);
}

export function transcriptPage(tenant: Tenant, pack: DomainPack, transcript: Transcript): string {
    const c = transcript.classification;
    const topic = c === null ? "" : DomainPack.topics(pack).find(t => t.id === c.topic)?.label ?? c.topic;
    return page(`Conversation - ${tenant.name}`, html`
<header>
<p><a href="/settings/${tenant.slug}#conversations">Back to ${tenant.name}</a></p>
<h1>Conversation</h1>
<p>${transcript.session.channel} &middot; ${when(transcript.session.startedAt, tenant)}</p>
</header>
<main>
${c !== null ? html`<dl>
<dt>Summary</dt><dd>${c.gist}</dd>
<dt>Topic</dt><dd>${topic}</dd>
<dt>Outcome</dt><dd>${c.outcome}${c.followupNeeded ? " (needs follow-up)" : ""}</dd>
<dt>Urgency</dt><dd>${c.urgency}</dd>
<dt>Language</dt><dd>${LANGUAGE_NAMES[c.language]}</dd>
</dl>` : html`<p>Not summarized yet.</p>`}
<ol>
${transcript.messages.map(m => html`<li><strong>${m.role === "user" ? "Customer" : "Assistant"}:</strong> ${m.text}</li>\n`)}
</ol>
</main>`);
}

export type QualityPageData = {
    readonly days:       number;
    readonly experiment: string;
    readonly variants:   readonly PromptVariant[];
    readonly rows:       readonly QualityRow[];
    readonly evals:      readonly EvalSummary[];
};

function pct(rate: number): string {
    return `${(rate * 100).toFixed(1)}%`;
}

export function qualityPage(d: QualityPageData): string {
    return page("Quality", html`
<header>
<p><a href="/settings">All businesses</a></p>
<h1>Quality</h1>
<p>${[1, 7, 30, 90].map(n => n === d.days ? html`<strong>${n} days</strong> ` : html`<a href="/settings/quality?days=${n}">${n} days</a> `)}</p>
</header>
<main>
<section id="variants">
<h2>Prompt variants</h2>
<p>Experiment: ${d.experiment}. Edit src/prompts/variants/variants.json to change who gets which.</p>
<table>
<thead><tr><th>Variant</th><th>Share</th><th>Description</th></tr></thead>
<tbody>
${d.variants.map(v => html`<tr><td>${v.id}</td><td>${v.weight}</td><td>${v.description}</td></tr>\n`)}
</tbody>
</table>
</section>
<section id="turns">
<h2>Conversations</h2>
<table>
<thead><tr><th>Variant</th><th>Model</th><th>Channel</th><th>Turns</th><th>Conversations</th><th>Avg reply time</th><th>First words</th><th>Needed correcting</th><th>Tool errors</th><th>Fallbacks</th><th>Cached input</th><th>Cost per turn</th><th>Booked</th><th>Booked or lead</th></tr></thead>
<tbody>
${d.rows.map(r => html`<tr><td>${r.variant}</td><td>${r.model}</td><td>${r.channel}</td><td>${r.turns}</td><td>${r.sessions}</td><td>${(r.avgLatencyMs / 1000).toFixed(2)}s</td><td>${r.avgFirstTextMs === null ? "" : `${(r.avgFirstTextMs / 1000).toFixed(2)}s`}</td><td>${pct(r.correctedRate)}</td><td>${pct(r.failureRate)}</td><td>${pct(r.fallbackRate)}</td><td>${pct(r.cachedShare)}</td><td>$${r.costPerTurn.toFixed(5)}</td><td>${pct(r.bookedRate)}</td><td>${pct(r.leadRate)}</td></tr>\n`)}
</tbody>
</table>
${d.rows.length === 0 ? html`<p>No conversations in this period.</p>` : ""}
</section>
<section id="evals">
<h2>Eval pass rates</h2>
<p>From <code>pnpm eval --repeat 5</code> runs. Lowest first.</p>
<table>
<thead><tr><th>Scenario</th><th>Variant</th><th>Model</th><th>Runs</th><th>Passed</th><th>Last run</th></tr></thead>
<tbody>
${d.evals.map(e => html`<tr><td>${e.scenario}</td><td>${e.variant}</td><td>${e.model}</td><td>${e.runs}</td><td>${pct(e.passRate)}</td><td>${e.lastRun.toISOString().slice(0, 16).replace("T", " ")}</td></tr>\n`)}
</tbody>
</table>
${d.evals.length === 0 ? html`<p>No eval runs in this period.</p>` : ""}
</section>
</main>`);
}

export function promptPage(tenant: Tenant, channel: string, message: string, text: string): string {
    return page(`Instructions - ${tenant.name}`, html`
<header>
<p><a href="/settings/${tenant.slug}">Back to ${tenant.name}</a></p>
<h1>What the assistant is told</h1>
<p>Everything the assistant sees on one turn, for a sample customer message. The matching details near the end change with what the customer says.</p>
<form method="get" action="/settings/${tenant.slug}/prompt">
${field("Customer says", input("message", message))}
${field("Channel", select("channel", [["sms", "Text"], ["web", "Web chat"], ["voice", "Phone call"]], channel))}
<p><button type="submit">Show</button></p>
</form>
</header>
<main><pre>${text}</pre></main>`);
}

// --- sections ----------------------------------------------------------------------------

function section(id: string, title: string, body: Html): Html {
    return html`<section id="${id}">\n<h2>${title}</h2>\n${body}</section>\n`;
}

/**
 * A number input limited to a platform bound, with the range shown.
 */
function bounded(name: string, value: number, bound: Bound): Html {
    return html`${input(name, value, "number", `min="${bound.min}" max="${bound.max}"`)} <small>${bound.min}-${bound.max}</small>`;
}

function action(d: TenantPageData, name: string): string {
    return `/settings/${d.tenant.slug}/${name}`;
}

function business(d: TenantPageData): Html {
    const t = d.tenant;
    return section("business", "Business", form(action(d, "business"), html`
${field("Name", input("name", t.name))}
${d.viewer.kind === "admin"
        ? field("Business type", select("domain", d.domains, t.domain))
        : html`<p><strong>Business type</strong> ${d.pack.name}</p>`}
${field("Main phone", input("phone", t.phone === null ? "" : PhoneNumber.display(t.phone), "tel"))}
${field("Address", input("address", t.address))}
${field("Website", input("website", t.website, "url"))}
${field("Time zone", input("timezone", t.timezone))}
${field("Hours", input("hoursText", t.hoursText), "e.g. Monday-Friday: 9am to 6pm, Saturday: 10am to 2pm, Sunday: closed")}
${field("Assistant's name in chat", input("agentName", t.agentName), `blank: ${d.pack.defaultAgentName}`)}
${field("Assistant's name on calls", input("voiceAgentName", t.voiceAgentName), `blank: ${d.pack.defaultVoiceAgentName}`)}
`));
}

function numbers(d: TenantPageData): Html {
    const t = d.tenant;
    if (d.viewer.kind !== "admin") {
        const list = (channel: "voice" | "sms") => (t.endpoints[channel] ?? []).map(a => PhoneNumber.display(a.value)).join(", ") || "none yet";
        return section("numbers", "Phone numbers", html`
<p><strong>Calls to</strong> ${list("voice")}</p>
<p><strong>Texts to</strong> ${list("sms")}</p>
<p>To add or change a number, contact us.</p>
${form(action(d, "transfer"), field("Transfer calls to", input("transferTo", t.settings.transferTo === null ? "" : PhoneNumber.display(t.settings.transferTo), "tel"), "where callers go when they need a person"))}
`);
    }
    return section("numbers", "Phone numbers", form(action(d, "numbers"), html`
<p>Numbers the assistant answers. A number can belong to one business at a time; adding it here takes it from any other business.</p>
${field("Calls to", textarea("voice", phones((t.endpoints.voice ?? []).map(a => a.value)), 2), "one per line")}
${field("Texts to", textarea("sms", phones((t.endpoints.sms ?? []).map(a => a.value)), 2), "one per line")}
${field("Transfer calls to", input("transferTo", t.settings.transferTo === null ? "" : PhoneNumber.display(t.settings.transferTo), "tel"), "where callers go when they need a person")}
`));
}

function assistant(d: TenantPageData): Html {
    const s = d.tenant.settings;
    const defaults = d.pack.defaults;
    const levels: [string, string][] = [
        ["", `Business type default (${Style.describe(defaults.professionalism)})`],
        ...Object.values(Professionalism).map((l): [string, string] => [l, l]),
    ];
    const honesty: [string, string][] = [
        ["", `Business type default (${defaults.honestAboutAi ? "says so" : "stays in character"})`],
        ["yes", "Says it is a virtual assistant when asked"],
        ["no", "Stays in character as a team member"],
    ];
    return section("assistant", "Assistant", form(action(d, "assistant"), html`
${field("How formal", select("professionalism", levels, s.style.professionalism ?? ""))}
${field("If asked whether it's an AI", select("honestAboutAi", honesty, s.style.honestAboutAi === null ? "" : s.style.honestAboutAi ? "yes" : "no"))}
${field("Longest conversation (minutes)", bounded("maxSessionMinutes", Math.round(s.limits.maxSessionSeconds / 60), PLATFORM_BOUNDS.maxSessionMinutes), "the assistant starts wrapping up politely three replies before this")}
<fieldset>
<legend><strong>Calls</strong></legend>
${field("Wait for the caller to speak (seconds)", bounded("silenceSeconds", s.calls.silenceSeconds, PLATFORM_BOUNDS.silenceSeconds))}
${field("\"Are you still there?\" prompts before hanging up", bounded("stillTherePrompts", s.calls.stillTherePrompts, PLATFORM_BOUNDS.stillTherePrompts))}
${field("Calls with hidden caller ID", select("hiddenCallerId", [["assistant", "Answer normally"], ["voicemail", "Send to voicemail"], ["reject", "Don't answer"]], s.calls.hiddenCallerId), "robocalls usually hide their number")}
</fieldset>
${field("Follow-up text after a quiet chat (minutes)", input("coldFollowupMinutes", s.coldFollowupMinutes, "number", "min=\"1\""), "blank: no follow-up texts")}
<p>${check("Announce that calls are recorded", "recordingDisclosure", s.recordingDisclosure.enabled)}</p>
${field("Recording notice wording", input("recordingNotice", s.recordingDisclosure.text.en, "text", "maxlength=\"300\" size=\"60\""), "said at the start of each call when the notice is on; leave the standard wording for the Spanish version")}
<p>${check("Let web chat visitors skip the name and phone form", "skipProfileGate", s.skipProfileGate)}</p>
`));
}

function privacy(d: TenantPageData): Html {
    const s = d.tenant.settings.privacy;
    const packSensitive = d.pack.defaults.sensitive;
    return section("privacy", "Privacy", form(action(d, "privacy"), html`
<fieldset>
<legend><strong>Remove from what customers write (before the assistant sees it or anything is saved)</strong></legend>
${Object.values(PiiKind).map(k => html`<p>${check(PII_LABELS[k], "redact", s.redact.includes(k), k)}</p>`)}
</fieldset>
<p>${check("Confidential business (therapy, medical, legal)", "sensitive", s.sensitive || packSensitive)}${packSensitive ? html` <small>always on for ${d.pack.name}</small>` : ""}</p>
<p><small>Confidential: the assistant never confirms who is a client, asks only what scheduling needs,
and staff texts and emails leave out what was said.</small></p>
<p>${check("Crisis support", "crisisSupport", s.crisisSupport)} <small>off by default. When on, messages that suggest self-harm get the 988 crisis line and 911, and staff get an urgent alert. Turn on only if the business wants this.</small></p>
`));
}

function languages(d: TenantPageData): Html {
    const t = d.tenant;
    return section("languages", "Languages", html`
${form(action(d, "languages"), html`
${field("Main language", select("defaultLanguage", LANGUAGE_OPTIONS, t.defaultLanguage), "used with new customers")}
${field("Language for your summaries and alerts", select("staffLanguage", LANGUAGE_OPTIONS, t.staffLanguage))}
<fieldset>
<legend><strong>Languages the assistant offers</strong></legend>
${LANGUAGE_OPTIONS.map(([code, name]) => html`<p>${check(name, "languages", t.settings.languages.includes(code) || code === t.defaultLanguage, code)}</p>`)}
</fieldset>
`)}
<h3>Reset languages</h3>
<p>Customers keep the language they last used. If one got stuck in the wrong language, reset it here; their next conversation starts in the main language.</p>
${form(action(d, "languages/reset"), field("Customer's phone number", input("phone", "", "tel")), "Reset this customer")}
${form(action(d, "languages/reset"), raw(""), "Reset every customer")}
`);
}

function schedule(d: TenantPageData): Html {
    const s = d.tenant.settings;
    const sched = d.tenant.schedule;
    const windowFor = (day: number) => sched.kind === "windows"
        ? sched.windows.find(w => w.day === day) ?? null
        : null;
    return section("schedule", "Schedule", form(action(d, "schedule"), html`
<p>When the assistant answers calls. Outside these times calls get the choice below.</p>
<p><label><input type="radio" name="mode" value="always"${raw(sched.kind === "always" ? " checked" : "")}> Always</label>
<label><input type="radio" name="mode" value="windows"${raw(sched.kind === "windows" ? " checked" : "")}> Only these hours</label></p>
<table>
<thead><tr><th>Day</th><th>Answer</th><th>From</th><th>To</th></tr></thead>
<tbody>
${WEEKDAY_NAMES.map((name, day) => {
        const w = windowFor(day);
        return html`<tr><td>${name}</td><td>${check("", `day${day}`, w !== null)}</td><td>${input(`start${day}`, clock(w?.startMinute ?? 9 * 60), "time")}</td><td>${input(`end${day}`, clock(w?.endMinute ?? 17 * 60), "time")}</td></tr>\n`;
    })}
</tbody>
</table>
<p>${check("Keep answering texts and web chat all the time", "textAlwaysOn", s.textAlwaysOn)}</p>
${field("Calls outside these hours", select("offSchedule", FALLBACKS, s.callFallbacks.offSchedule))}
${field("Calls when every line is busy", select("lineBusy", FALLBACKS, s.callFallbacks.lineBusy))}
`));
}

function catalog(d: TenantPageData): Html {
    const { tenant, pack } = d;
    const spec = pack.catalog;
    const source = tenant.settings.catalog.source;
    const kinds: [string, string][] = [
        ["manual", "Entered here (paste below)"],
        ["dataset", "Sample data file"],
        ["csv_url", "CSV link (e.g. a Google Sheet published as CSV)"],
        ["json_url", "JSON link"],
    ];
    const fieldMap = Object.entries(tenant.settings.catalog.fieldMap).map(([from, to]) => `${from} = ${to}`).join("\n");
    const status = d.sync === null
        ? "Never synced."
        : d.sync.error !== null
            ? `Last sync ${when(d.sync.syncedAt, tenant)} failed: ${d.sync.error}. Still using the previous ${d.sync.items} ${spec.itemNounPlural}.`
            : `Last synced ${when(d.sync.syncedAt, tenant)}: ${d.sync.items} ${spec.itemNounPlural}.`;
    const title = Text.capitalize(spec.itemNounPlural);
    const shown = spec.fields.filter(f => f.name !== spec.idField);
    return section("catalog", title, html`
<p>Everything the assistant knows about your ${spec.itemNounPlural}. However they come in, the assistant reads them the same way.</p>
<h3>Where they come from</h3>
${form(action(d, "catalog"), html`
${field("Source", select("kind", kinds, source.kind))}
${field("Sample data file", select("dataset", d.datasets.map(n => [n, n] as const), source.kind === "dataset" ? source.name : ""))}
${field("Link", input("url", source.kind === "csv_url" || source.kind === "json_url" ? source.url : "", "url"))}
${field("Where the list is in the JSON", input("itemsPath", source.kind === "json_url" ? source.itemsPath : ""), "dotted path like data.items; blank if the response is the list")}
${field("Check the link every (minutes)", input("refreshMinutes", source.kind === "csv_url" || source.kind === "json_url" ? source.refreshMinutes : 60, "number", "min=\"5\""))}
${field("Column names", textarea("fieldMap", fieldMap, 4), "one per line: Your Column = our field")}
<p><small>Our fields: ${spec.fields.map(f => `${f.name} (${f.label || f.name})`).join(", ")}</small></p>
`)}
<p>${status}</p>
${source.kind !== "manual" ? form(action(d, "catalog/sync"), raw(""), "Sync now") : ""}
<h3>Paste a list</h3>
${form(action(d, "catalog/upload"), html`
${field("CSV with a header row, or a JSON list", textarea("rows", "", 8))}
<p><small>Replaces the current list. If the source above is a link, the next sync replaces it again.</small></p>
`, "Replace list")}
<h3>Current list (${d.items.length})</h3>
<table>
<thead><tr>${shown.map(f => html`<th>${f.label || f.name}</th>`)}</tr></thead>
<tbody>
${d.items.slice(0, 200).map(item => html`<tr>${shown.map(f => html`<td>${Catalog.formatValue(f, item.fields[f.name])}</td>`)}</tr>\n`)}
</tbody>
</table>
${d.items.length > 200 ? html`<p>Showing the first 200.</p>` : ""}
`);
}

function details(d: TenantPageData): Html {
    const { tenant, pack } = d;
    const options = Object.entries(tenant.options).map(([k, v]) => `${k} = ${v}`).join("\n");
    const toggles = pack.prompts.toggles.map(t => t.setting);
    return section("details", "Business details", form(action(d, "details"), html`
<p>What the assistant tells customers about the business.</p>
${pack.profile.map(q => field(q.label, textarea(`profile.${q.key}`, tenant.profile[q.key] ?? "", 3), q.question))}
${field("Options", textarea("options", options, 3), toggles.length > 0 ? `one per line: name = value. This business type understands: ${toggles.join(", ")}` : "one per line: name = value")}
`));
}

function notifications(d: TenantPageData): Html {
    const t = d.tenant;
    const s = t.settings;
    const emails = t.staffContacts.flatMap(a => a.kind === "email" ? [a.value] : []);
    return section("notifications", "Notifications", form(action(d, "notifications"), html`
${field("Staff emails", textarea("emails", emails.join("\n"), 2), "one per line")}
${field("Staff phones", textarea("phones", Tenant.staffPhones(t).map(PhoneNumber.display).join("\n"), 2), "one per line")}
<table>
<thead><tr><th>Tell staff about</th><th>Email</th><th>Text</th></tr></thead>
<tbody>
${(Object.keys(NOTICE_LABELS) as (keyof typeof NOTICE_LABELS)[]).map(n => html`<tr><td>${NOTICE_LABELS[n]}</td><td>${check("", `${n}`, s.notifications[n].includes("email"), "email")}</td><td>${check("", `${n}`, s.notifications[n].includes("sms"), "sms")}</td></tr>\n`)}
</tbody>
</table>
${d.viewer.kind === "admin"
        ? html`<p>${check("Staff text alerts add-on", "smsNotifications", t.plan.subscription.smsNotifications)} <small>without it, text alerts go by email</small></p>
${field("Extra phone lines", input("extraLines", t.plan.subscription.extraLines, "number", "min=\"0\""))}`
        : html`<p><strong>Staff text alerts add-on</strong> ${t.plan.subscription.smsNotifications ? "on" : "off (text alerts go by email)"}</p>`}
`));
}

function blocks(d: TenantPageData): Html {
    const t = d.tenant;
    return section("blocks", "Blocked callers", html`
<p>Callers are blocked for 24 hours automatically after two abusive conversations in 30 days. You can block, extend or unblock here.</p>
<table>
<thead><tr><th>Caller</th><th>Until</th><th>Why</th><th>By</th><th></th></tr></thead>
<tbody>
${d.blocked.map(c => html`<tr>
<td>${[c.firstName, c.phone === null ? "" : PhoneNumber.display(c.phone)].filter(Boolean).join(" - ") || c.id}</td>
<td>${c.block?.until ? when(c.block.until, t) : "indefinitely"}</td>
<td>${c.block?.reason ?? ""}</td>
<td>${c.block?.source === "staff" ? "staff" : "automatic"}</td>
<td>${form(action(d, `blocks/${c.id}/remove`), raw(""), "Unblock")}</td>
</tr>\n`)}
</tbody>
</table>
${d.blocked.length === 0 ? html`<p>Nobody is blocked.</p>` : ""}
<h3>Block a caller</h3>
${form(action(d, "blocks"), html`
${field("Phone number", input("phone", "", "tel"))}
${field("For how many hours", input("hours", "24", "number", "min=\"1\""), "blank: until you unblock them")}
${field("Reason", input("reason", ""))}
`, "Block")}
`);
}

function logins(d: TenantPageData): Html {
    const t = d.tenant;
    if (d.viewer.kind === "owner") {
        const me = d.viewer.account;
        return section("logins", "Your login", html`
<p><strong>Email</strong> ${me.email}</p>
${form(action(d, `accounts/${me.id}/password`), html`
${field("Current password", input("current", "", "password", "autocomplete=\"current-password\""))}
${field("New password", input("password", "", "password", "autocomplete=\"new-password\""), `at least ${MIN_PASSWORD_LENGTH} characters`)}
`, "Change password")}
`);
    }
    return section("logins", "Logins", html`
<p>People who can sign in to this business's settings. They only see this business.</p>
<table>
<thead><tr><th>Email</th><th>Name</th><th>Last signed in</th><th>New password</th><th></th></tr></thead>
<tbody>
${d.accounts.map(a => html`<tr>
<td>${a.email}</td>
<td>${a.name}</td>
<td>${a.lastLoginAt === null ? "never" : when(a.lastLoginAt, t)}</td>
<td>${form(action(d, `accounts/${a.id}/password`), input("password", "", "password", "autocomplete=\"new-password\""), "Set")}</td>
<td>${form(action(d, `accounts/${a.id}/remove`), raw(""), "Remove")}</td>
</tr>\n`)}
</tbody>
</table>
${d.accounts.length === 0 ? html`<p>No logins yet.</p>` : ""}
<h3>Add a login</h3>
${form(action(d, "accounts"), html`
${field("Email", input("email", "", "email"))}
${field("Name", input("name", ""))}
${field("Password", input("password", "", "password", "autocomplete=\"new-password\""), `at least ${MIN_PASSWORD_LENGTH} characters; share it with them privately`)}
`, "Add login")}
`);
}

function billingSection(d: TenantPageData): Html {
    if (d.billing === null) {
        return html``;
    }
    const b = d.tenant.billing;
    const label = {
        none:               "Not subscribed yet.",
        active:             "Active.",
        trialing:           "In a free trial.",
        past_due:           "Payment failed; we're retrying the card. The assistant keeps working for now.",
        incomplete:         "Waiting for the first payment.",
        incomplete_expired: "The first payment didn't go through. The assistant is paused.",
        unpaid:             "Unpaid. The assistant is paused until the balance is paid.",
        canceled:           "Canceled. The assistant is paused.",
        paused:             "Paused.",
    }[b.status];
    const subscribed = b.subscriptionId !== null && b.status !== "canceled";
    return section("billing", "Billing", html`
<dl>
<dt>Subscription</dt><dd>${label}</dd>
${b.currentPeriodEnd !== null ? html`<dt>Current period ends</dt><dd>${when(new Date(b.currentPeriodEnd), d.tenant)}</dd>` : ""}
<dt>Top-ups this month</dt><dd>$${(d.tenant.plan.subscription.topUpsUsd ?? 0).toFixed(2)}</dd>
</dl>
${subscribed
        ? form(action(d, "billing/portal"), raw(""), "Manage billing (cards, invoices, cancel)")
        : form(action(d, "billing/subscribe"), html`<p>$${Plan.monthlyRevenueUsd({ ...d.tenant.plan, subscription: { ...d.tenant.plan.subscription, topUpsUsd: 0 } }).toFixed(2)} a month, including the add-ons above.</p>`, "Subscribe")}
<h3>Add to this month's allowance</h3>
${form(action(d, "billing/topup"), field("Amount", select("amount", d.billing.topUpAmounts.map(a => [String(a), `$${a}`] as const), String(d.billing.topUpAmounts[0] ?? ""))), "Buy top-up")}
`);
}

function usage(d: TenantPageData): Html {
    const plan = d.tenant.plan;
    const cap = Plan.assistantCapUsd(plan);
    const status = Budget.capStatus(d.monthToDate, plan);
    const label = {
        ok:             "Within the monthly allowance.",
        warning:        "Most of this month's allowance is used.",
        assistant_used: "This month's allowance is used; calls go to voicemail until it resets.",
        exceeded:       "Over the monthly limit; the assistant is paused until next month.",
    }[status];
    return section("usage", "Usage", html`
<dl>
<dt>Plan</dt><dd>$${Plan.monthlyRevenueUsd(plan).toFixed(2)} a month</dd>
<dt>Service cost so far this month</dt><dd>$${d.monthToDate.toFixed(2)} of $${cap.toFixed(2)}</dd>
<dt>Status</dt><dd>${label}</dd>
</dl>
`);
}

function conversations(d: TenantPageData): Html {
    const { tenant, pack } = d;
    const sensitive = Style.resolve(tenant, pack).sensitive;
    return section("conversations", "Conversations", html`
<table>
<thead><tr><th>When</th><th>Channel</th><th>Topic</th><th>Outcome</th><th>Summary</th></tr></thead>
<tbody>
${d.transcripts.map(tr => {
        const c = tr.classification;
        const topic = c === null ? "" : DomainPack.topics(pack).find(x => x.id === c.topic)?.label ?? c.topic;
        return html`<tr>
<td><a href="/settings/${tenant.slug}/conversations/${tr.session.id}">${when(tr.session.startedAt, tenant)}</a></td>
<td>${tr.session.channel}</td>
<td>${topic}</td>
<td>${c?.outcome ?? "open"}</td>
<td>${sensitive ? "" : c?.gist ?? ""}</td>
</tr>\n`;
    })}
</tbody>
</table>
${d.transcripts.length === 0 ? html`<p>No conversations yet.</p>` : ""}
`);
}
