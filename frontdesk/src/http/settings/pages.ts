/**
 * The settings pages other than a business's own (which is built from its sections, in sections.ts):
 * sign-in, the list of businesses, a conversation, the quality report and the prompt preview.
 */

import {
    LANGUAGE_NAMES,
    LocalTime,
    Tenant,
    type EvalSummary,
    type PromptVariant,
    type QualityRow,
    type Transcript,
} from "../../core/index.ts";
import { DomainPack } from "../../domains/index.ts";
import { field, form, html, input, page, raw, select } from "../html.ts";

export function when(date: Date | null, tenant: Tenant): string {
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

