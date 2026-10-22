/**
 * The sections of a business's settings page. Each record says everything about one section:
 * its heading, how it renders, and the forms it posts with what each one saves, so the page,
 * its navigation and its routes are all generated from SETTINGS_SECTIONS and can't drift apart.
 *
 * A form edits the tenant's file form (TenantDraft); the route validates the result with
 * `parseTenant`, so the page can never save something the admin API would refuse.
 * Actions that aren't edits to the tenant (uploads, blocks, logins, billing) are routes of their own.
 */

import {
    Address,
    Budget,
    CallFallback,
    DomainId,
    enumValue,
    HiddenCallerId,
    Language,
    LANGUAGE_NAMES,
    MIN_PASSWORD_LENGTH,
    NotifyVia,
    PhoneNumber,
    PiiKind,
    Plan,
    PLATFORM_BOUNDS,
    Professionalism,
    StaffNotice,
    Tenant,
    Text,
    WEEKDAY_NAMES,
    type Account,
    type Bound,
    type CatalogItem,
    type CatalogSource,
    type CatalogSyncStatus,
    type Contact,
    type Transcript,
    type Weekday,
} from "../../core/index.ts";
import { syncCatalog } from "../../catalogs/index.ts";
import { Catalog, DomainPack } from "../../domains/index.ts";
import type { Services } from "../../pipeline/services.ts";
import { Style } from "../../prompts/style.ts";
import type { TenantDraft } from "../../tenants/schema.ts";
import { many, minuteOf, num, on, one, type FormBody } from "../form.ts";
import { check, field, form, html, input, page, raw, select, textarea, type Html } from "../html.ts";
import type { Viewer } from "./auth.ts";
import { when } from "./pages.ts";

const LANGUAGE_OPTIONS = Object.values(Language).map(l => [l, LANGUAGE_NAMES[l]] as const);

const PII_LABELS: Readonly<Record<PiiKind, string>> = {
    card:         "Payment card numbers",
    ssn:          "Social security numbers",
    bank_account: "Bank account and routing numbers",
    birth_date:   "Dates of birth",
};

const NOTICE_LABELS: Readonly<Record<StaffNotice, string>> = {
    transcript:   "Conversation transcripts",
    booking:      "New bookings",
    handoff:      "Callback requests and transfers",
    budget_alert: "Monthly allowance alerts",
};

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

// --- sections ----------------------------------------------------------------------------

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
    return form(action(d, "business"), html`
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
`);
}

function numbers(d: TenantPageData): Html {
    const t = d.tenant;
    if (d.viewer.kind !== "admin") {
        const list = (channel: "voice" | "sms") => (t.endpoints[channel] ?? []).map(a => PhoneNumber.display(a.value)).join(", ") || "none yet";
        return html`
<p><strong>Calls to</strong> ${list("voice")}</p>
<p><strong>Texts to</strong> ${list("sms")}</p>
<p>To add or change a number, contact us.</p>
${form(action(d, "transfer"), field("Transfer calls to", input("transferTo", t.settings.transferTo === null ? "" : PhoneNumber.display(t.settings.transferTo), "tel"), "where callers go when they need a person"))}
`;
    }
    return form(action(d, "numbers"), html`
<p>Numbers the assistant answers. A number can belong to one business at a time; adding it here takes it from any other business.</p>
${field("Calls to", textarea("voice", phones((t.endpoints.voice ?? []).map(a => a.value)), 2), "one per line")}
${field("Texts to", textarea("sms", phones((t.endpoints.sms ?? []).map(a => a.value)), 2), "one per line")}
${field("Transfer calls to", input("transferTo", t.settings.transferTo === null ? "" : PhoneNumber.display(t.settings.transferTo), "tel"), "where callers go when they need a person")}
`);
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
    return form(action(d, "assistant"), html`
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
`);
}

function privacy(d: TenantPageData): Html {
    const s = d.tenant.settings.privacy;
    const packSensitive = d.pack.defaults.sensitive;
    return form(action(d, "privacy"), html`
<fieldset>
<legend><strong>Remove from what customers write (before the assistant sees it or anything is saved)</strong></legend>
${Object.values(PiiKind).map(k => html`<p>${check(PII_LABELS[k], "redact", s.redact.includes(k), k)}</p>`)}
</fieldset>
<p>${check("Confidential business (therapy, medical, legal)", "sensitive", s.sensitive || packSensitive)}${packSensitive ? html` <small>always on for ${d.pack.name}</small>` : ""}</p>
<p><small>Confidential: the assistant never confirms who is a client, asks only what scheduling needs,
and staff texts and emails leave out what was said.</small></p>
<p>${check("Crisis support", "crisisSupport", s.crisisSupport)} <small>off by default. When on, messages that suggest self-harm get the 988 crisis line and 911, and staff get an urgent alert. Turn on only if the business wants this.</small></p>
`);
}

function languages(d: TenantPageData): Html {
    const t = d.tenant;
    return html`
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
`;
}

function schedule(d: TenantPageData): Html {
    const s = d.tenant.settings;
    const sched = d.tenant.schedule;
    const windowFor = (day: number) => sched.kind === "windows"
        ? sched.windows.find(w => w.day === day) ?? null
        : null;
    return form(action(d, "schedule"), html`
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
`);
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
    const shown = spec.fields.filter(f => f.name !== spec.idField);
    return html`
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
`;
}

function details(d: TenantPageData): Html {
    const { tenant, pack } = d;
    const options = Object.entries(tenant.options).map(([k, v]) => `${k} = ${v}`).join("\n");
    const toggles = pack.prompts.toggles.map(t => t.setting);
    return form(action(d, "details"), html`
<p>What the assistant tells customers about the business.</p>
${pack.profile.map(q => field(q.label, textarea(`profile.${q.key}`, tenant.profile[q.key] ?? "", 3), q.question))}
${field("Options", textarea("options", options, 3), toggles.length > 0 ? `one per line: name = value. This business type understands: ${toggles.join(", ")}` : "one per line: name = value")}
`);
}

function notifications(d: TenantPageData): Html {
    const t = d.tenant;
    const s = t.settings;
    const emails = t.staffContacts.flatMap(a => a.kind === "email" ? [a.value] : []);
    return form(action(d, "notifications"), html`
${field("Staff emails", textarea("emails", emails.join("\n"), 2), "one per line")}
${field("Staff phones", textarea("phones", Tenant.staffPhones(t).map(PhoneNumber.display).join("\n"), 2), "one per line")}
<table>
<thead><tr><th>Tell staff about</th><th>Email</th><th>Text</th></tr></thead>
<tbody>
${Object.values(StaffNotice).map(n => html`<tr><td>${NOTICE_LABELS[n]}</td><td>${check("", `${n}`, s.notifications[n].includes("email"), "email")}</td><td>${check("", `${n}`, s.notifications[n].includes("sms"), "sms")}</td></tr>\n`)}
</tbody>
</table>
${d.viewer.kind === "admin"
        ? html`<p>${check("Staff text alerts add-on", "smsNotifications", t.plan.subscription.smsNotifications)} <small>without it, text alerts go by email</small></p>
${field("Extra phone lines", input("extraLines", t.plan.subscription.extraLines, "number", "min=\"0\""))}`
        : html`<p><strong>Staff text alerts add-on</strong> ${t.plan.subscription.smsNotifications ? "on" : "off (text alerts go by email)"}</p>`}
`);
}

function blocks(d: TenantPageData): Html {
    const t = d.tenant;
    return html`
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
`;
}

function logins(d: TenantPageData): Html {
    const t = d.tenant;
    if (d.viewer.kind === "owner") {
        const me = d.viewer.account;
        return html`
<p><strong>Email</strong> ${me.email}</p>
${form(action(d, `accounts/${me.id}/password`), html`
${field("Current password", input("current", "", "password", "autocomplete=\"current-password\""))}
${field("New password", input("password", "", "password", "autocomplete=\"new-password\""), `at least ${MIN_PASSWORD_LENGTH} characters`)}
`, "Change password")}
`;
    }
    return html`
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
`;
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
    return html`
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
`;
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
    return html`
<dl>
<dt>Plan</dt><dd>$${Plan.monthlyRevenueUsd(plan).toFixed(2)} a month</dd>
<dt>Service cost so far this month</dt><dd>$${d.monthToDate.toFixed(2)} of $${cap.toFixed(2)}</dd>
<dt>Status</dt><dd>${label}</dd>
</dl>
`;
}

function conversations(d: TenantPageData): Html {
    const { tenant, pack } = d;
    const sensitive = Style.resolve(tenant, pack).sensitive;
    return html`
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
`;
}

// --- forms -------------------------------------------------------------------------------

/**
 * What a form's save can use besides the posted fields.
 */
export type EditContext = {
    readonly tenant:   Tenant;
    readonly viewer:   Viewer;
    readonly services: Services;
};

/**
 * A form posting to /settings/<slug>/<action>. `apply` changes the draft from the posted fields,
 * or returns problems to show instead of saving. `after` runs once the tenant is saved and
 * returns the notice to show (default "Saved.").
 */
export type SectionForm = {
    readonly action:     string;
    readonly adminOnly?: boolean;
    apply(draft: TenantDraft, body: FormBody, ctx: EditContext): readonly string[];
    after?(saved: Tenant, before: Tenant, services: Services): Promise<string>;
};

function lines(text: string): string[] {
    return text.split(/[\n,]+/).map(l => l.trim()).filter(Boolean);
}

/**
 * "name = value" lines as pairs; lines without "=" are ignored.
 */
function pairs(text: string): [string, string][] {
    return text.split("\n").flatMap((line): [string, string][] => {
        const i = line.indexOf("=");
        return i > 0
            ? [[line.slice(0, i).trim(), line.slice(i + 1).trim()]]
            : [];
    }).filter(([k]) => k !== "");
}

/**
 * An option typed the way a pack's toggles read it: true/false words as booleans, numbers as numbers.
 */
function optionValue(raw: string): string | number | boolean {
    if (/^(true|yes|on)$/i.test(raw)) {
        return true;
    }
    if (/^(false|no|off)$/i.test(raw)) {
        return false;
    }
    return /^-?\d+(\.\d+)?$/.test(raw)
        ? Number(raw)
        : raw;
}

const businessForm: SectionForm = {
    action: "business",
    apply(draft, body, { viewer, services }) {
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
        return services.registry.find(draft.domain) !== null
            ? []
            : [`unknown business type '${draft.domain}'`];
    },
};

const numbersForm: SectionForm = {
    action:    "numbers",
    adminOnly: true,
    apply(draft, body) {
        draft.endpoints.voice = lines(one(body, "voice"));
        draft.endpoints.sms = lines(one(body, "sms"));
        draft.settings.transferTo = one(body, "transferTo");
        return [];
    },
    async after(tenant, _before, { store }) {
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
        return moved.length > 0
            ? `Saved. Moved the number from ${moved.join(", ")}.`
            : "Saved.";
    },
};

/**
 * Owners can't move phone numbers, but they choose where transfers go.
 */
const transferForm: SectionForm = {
    action: "transfer",
    apply(draft, body) {
        draft.settings.transferTo = one(body, "transferTo");
        return [];
    },
};

const assistantForm: SectionForm = {
    action: "assistant",
    apply(draft, body) {
        const honest = one(body, "honestAboutAi");
        const cold = one(body, "coldFollowupMinutes");
        draft.settings.style = {
            professionalism: enumValue(Professionalism, one(body, "professionalism")),
            honestAboutAi:   honest === "" ? null : honest === "yes",
        };
        draft.settings.maxSessionMinutes = num(body, "maxSessionMinutes") ?? draft.settings.maxSessionMinutes;
        draft.settings.calls = {
            silenceSeconds:    num(body, "silenceSeconds") ?? draft.settings.calls.silenceSeconds,
            stillTherePrompts: num(body, "stillTherePrompts") ?? draft.settings.calls.stillTherePrompts,
            hiddenCallerId:    enumValue(HiddenCallerId, one(body, "hiddenCallerId")) ?? HiddenCallerId.Assistant,
        };
        draft.settings.coldFollowupMinutes = cold === "" ? null : Number(cold);
        draft.settings.recordingDisclosure = on(body, "recordingDisclosure");
        draft.settings.recordingNotice = one(body, "recordingNotice");
        draft.settings.skipProfileGate = on(body, "skipProfileGate");
        return [];
    },
};

const privacyForm: SectionForm = {
    action: "privacy",
    apply(draft, body) {
        draft.settings.privacy = {
            redact:        many(body, "redact").flatMap(kind => enumValue(PiiKind, kind) ?? []),
            sensitive:     on(body, "sensitive"),
            crisisSupport: on(body, "crisisSupport"),
        };
        return [];
    },
};

const languagesForm: SectionForm = {
    action: "languages",
    apply(draft, body) {
        draft.defaultLanguage = enumValue(Language, one(body, "defaultLanguage")) ?? draft.defaultLanguage;
        draft.staffLanguage = enumValue(Language, one(body, "staffLanguage")) ?? draft.staffLanguage;
        const offered = many(body, "languages").flatMap(language => enumValue(Language, language) ?? []);
        draft.settings.languages = [...new Set([draft.defaultLanguage, ...offered])];
        return [];
    },
    async after(tenant, before) {
        // Customers whose language is no longer offered start over in the main language.
        const dropped = before.settings.languages.filter(l => !tenant.settings.languages.includes(l));
        return dropped.length > 0
            ? "Saved. Customers who used a language you removed will be answered in the main language."
            : "Saved.";
    },
};

const scheduleForm: SectionForm = {
    action: "schedule",
    apply(draft, body) {
        const windows = ([0, 1, 2, 3, 4, 5, 6] as const satisfies readonly Weekday[])
            .filter(day => on(body, `day${day}`))
            .map(day => ({ day, startMinute: minuteOf(one(body, `start${day}`)) ?? 0, endMinute: minuteOf(one(body, `end${day}`)) ?? 0 }));
        if (one(body, "mode") === "windows") {
            if (windows.length === 0) {
                return ["Pick at least one day, or choose Always."];
            }
            if (windows.some(w => w.endMinute <= w.startMinute)) {
                return ["Each day's end time must be after its start time."];
            }
            draft.schedule = windows;
        } else {
            draft.schedule = "always";
        }
        draft.settings.textAlwaysOn = on(body, "textAlwaysOn");
        draft.settings.callFallbacks = {
            offSchedule: enumValue(CallFallback, one(body, "offSchedule")) ?? draft.settings.callFallbacks.offSchedule,
            lineBusy:    enumValue(CallFallback, one(body, "lineBusy")) ?? draft.settings.callFallbacks.lineBusy,
        };
        return [];
    },
};

/**
 * Where the catalog comes from, from the posted fields; null for a kind we don't know.
 */
function catalogSource(body: FormBody): CatalogSource | null {
    const refreshMinutes = Number(one(body, "refreshMinutes")) || 60;
    switch (one(body, "kind")) {
        case "manual":
            return { kind: "manual" };
        case "dataset":
            return { kind: "dataset", name: one(body, "dataset") };
        case "csv_url":
            return { kind: "csv_url", url: one(body, "url"), refreshMinutes };
        case "json_url":
            return { kind: "json_url", url: one(body, "url"), itemsPath: one(body, "itemsPath"), refreshMinutes };
        default:
            return null;
    }
}

const catalogForm: SectionForm = {
    action: "catalog",
    apply(draft, body) {
        const source = catalogSource(body);
        if (source === null) {
            return ["Pick where the list comes from."];
        }
        draft.settings.catalog = { source, fieldMap: Object.fromEntries(pairs(one(body, "fieldMap"))) };
        return [];
    },
    async after(tenant, _before, services) {
        const result = await syncCatalog(services, tenant);
        if (result === null) {
            return "Saved.";
        }
        return result.ok
            ? `Saved and synced ${result.value} items.`
            : `Saved, but the sync failed: ${result.error}`;
    },
};

const detailsForm: SectionForm = {
    action: "details",
    apply(draft, body, { tenant, services }) {
        const pack = services.registry.get(tenant.domain);
        draft.profile = Object.fromEntries(pack.profile.map(q => [q.key, one(body, `profile.${q.key}`)]).filter(([, v]) => v !== ""));
        draft.options = Object.fromEntries(pairs(one(body, "options")).map(([k, v]) => [k, optionValue(v)]));
        return [];
    },
};

const notificationsForm: SectionForm = {
    action: "notifications",
    apply(draft, body, { viewer }) {
        draft.staff = { emails: lines(one(body, "emails")), phones: lines(one(body, "phones")) };
        const vias = (name: string) => {
            const chosen = many(body, name).flatMap(via => enumValue(NotifyVia, via) ?? []);
            return chosen.length > 0
                ? chosen
                : [NotifyVia.Email];
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
        return [];
    },
};

// --- the page ----------------------------------------------------------------------------

/**
 * `label` is the heading and navigation link; `shown` hides a section that doesn't apply (e.g. billing when it is off).
 */
export type SettingsSection = {
    readonly id:     string;
    readonly label:  string | ((d: TenantPageData) => string);
    readonly forms:  readonly SectionForm[];
    readonly shown?: (d: TenantPageData) => boolean;
    render(d: TenantPageData): Html;
};

/**
 * In page order.
 */
export const SETTINGS_SECTIONS: readonly SettingsSection[] = [
    { id: "business",      label: "Business",          render: business,       forms: [businessForm] },
    { id: "numbers",       label: "Phone numbers",     render: numbers,        forms: [numbersForm, transferForm] },
    { id: "assistant",     label: "Assistant",         render: assistant,      forms: [assistantForm] },
    { id: "privacy",       label: "Privacy",           render: privacy,        forms: [privacyForm] },
    { id: "languages",     label: "Languages",         render: languages,      forms: [languagesForm] },
    { id: "schedule",      label: "Schedule",          render: schedule,       forms: [scheduleForm] },
    { id: "catalog",       label: d => Text.capitalize(d.pack.catalog.itemNounPlural), render: catalog, forms: [catalogForm] },
    { id: "details",       label: "Business details",  render: details,        forms: [detailsForm] },
    { id: "notifications", label: "Notifications",     render: notifications,  forms: [notificationsForm] },
    { id: "blocks",        label: "Blocked callers",   render: blocks,         forms: [] },
    { id: "logins",        label: "Logins",            render: logins,         forms: [] },
    { id: "usage",         label: "Usage",             render: usage,          forms: [] },
    { id: "billing",       label: "Billing",           render: billingSection, forms: [], shown: d => d.billing !== null },
    { id: "conversations", label: "Conversations",     render: conversations,  forms: [] },
];

function labelOf(section: SettingsSection, d: TenantPageData): string {
    return typeof section.label === "string"
        ? section.label
        : section.label(d);
}

export function tenantPage(d: TenantPageData): string {
    const { tenant, pack } = d;
    const sections = SETTINGS_SECTIONS.filter(s => s.shown?.(d) ?? true);
    return page(`${tenant.name} settings`, html`
<header>
${d.viewer.kind === "admin"
        ? html`<p><a href="/settings">All businesses</a></p>`
        : html`<p>Signed in as ${d.viewer.account.email}</p>`}
${form("/settings/logout", raw(""), "Sign out")}
<h1>${tenant.name}</h1>
<p>${pack.name} &middot; <a href="/widget/${tenant.slug}">Open web chat</a> &middot; <a href="/settings/${tenant.slug}/prompt">See the assistant's instructions</a></p>
<nav><ul>${sections.map(s => html`<li><a href="#${s.id}">${labelOf(s, d)}</a></li>`)}</ul></nav>
</header>
<main>
${d.notice ? html`<p role="status"><strong>${d.notice}</strong></p>` : ""}
${d.problems.length > 0 ? html`<div role="alert"><p><strong>Not saved:</strong></p><ul>${d.problems.map(p => html`<li>${p}</li>`)}</ul></div>` : ""}
${d.warnings.length > 0 ? html`<section id="check"><h2>Things to check</h2><ul>${d.warnings.map(w => html`<li>${w}</li>`)}</ul></section>` : ""}
${sections.map(s => html`<section id="${s.id}">\n<h2>${labelOf(s, d)}</h2>\n${s.render(d)}</section>\n`)}
</main>`);
}
