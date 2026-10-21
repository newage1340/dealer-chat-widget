/**
 * Evals: scripted conversations run against the real configured model, with checks on each turn.
 *
 * Use them before changing a prompt, and to compare models (OpenAI tiers, a local model on the GPU box)
 * on the behavior that matters: booking, emergencies, languages, abuse handling, never inventing links.
 * Each scenario runs on a throwaway in-memory database seeded with the tenant from the configured one.
 */

import { readdir, readFile, stat } from "node:fs/promises";
import { join } from "node:path";

import { z } from "zod";

import { Address, Language, type EventKind, type Inbound } from "../core/index.ts";
import { DomainRegistry } from "../domains/index.ts";
import { collect, TurnEngine } from "../pipeline/engine.ts";
import { DEFAULT_POLICIES } from "../pipeline/policies.ts";
import type { Services } from "../pipeline/services.ts";
import { DEFAULT_SUBSCRIBERS } from "../pipeline/subscribers.ts";
import { PromptComposer } from "../prompts/composer.ts";
import type { LlmProvider, PriceTable } from "../providers/index.ts";
import { openStore, type Store } from "../storage/index.ts";

const LANGUAGES = Object.values(Language) as [Language, ...Language[]];

const Expectation = z.strictObject({
    events:        z.array(z.string()).default([]),
    noEvents:      z.array(z.string()).default([]),
    replyIncludes: z.array(z.string()).default([]),
    replyExcludes: z.array(z.string()).default([]),
    replyMatches:  z.string().optional(),
    language:      z.enum(LANGUAGES).optional(),
    directive:     z.enum(["continue", "transfer", "end"]).optional(),
});

const Scenario = z.strictObject({
    name:    z.string(),
    tenant:  z.string().describe("slug of a tenant in the configured database"),
    channel: z.enum(["web", "sms", "voice"]).default("sms"),
    profile: z.strictObject({ firstName: z.string().optional(), phone: z.string().optional() }).default({}),
    turns:   z.array(z.strictObject({ say: z.string(), expect: Expectation.prefault({}) })).min(1),
});
type Scenario = z.output<typeof Scenario>;

export type EvalResult = {
    readonly name:     string;
    readonly passed:   boolean;
    readonly failures: readonly string[];
    readonly costUsd:  number;
};

export async function loadScenarios(path: string): Promise<Scenario[]> {
    const files = (await stat(path)).isDirectory()
        ? (await readdir(path)).filter(f => f.endsWith(".json")).sort().map(f => join(path, f))
        : [path];
    const scenarios: Scenario[] = [];
    for (const file of files) {
        const parsed = Scenario.safeParse(JSON.parse(await readFile(file, "utf8")));
        if (!parsed.success) {
            throw new Error(`${file}: ${parsed.error.message}`);
        }
        scenarios.push(parsed.data);
    }
    return scenarios;
}

export async function runScenario(scenario: Scenario, source: Store, llm: LlmProvider, prices: PriceTable): Promise<EvalResult> {
    const tenant = await source.tenants.bySlug(scenario.tenant);
    if (tenant === null) {
        return { name: scenario.name, passed: false, failures: [`no tenant '${scenario.tenant}' in the configured database`], costUsd: 0 };
    }
    const store = await openStore("", null, prices);
    await store.tenants.save(tenant);
    await store.tenants.replaceCatalog(tenant.id, await source.tenants.catalog(tenant.id));
    const services: Services = {
        store,
        registry:    await DomainRegistry.fromDirectory(),
        composer:    await PromptComposer.load(),
        llm,
        policies:    DEFAULT_POLICIES,
        subscribers: DEFAULT_SUBSCRIBERS,
        clock:       () => new Date(),
        smsIdleMs:   30 * 60_000,
    };
    const engine = new TurnEngine(services);
    const sender = scenario.channel === "web"
        ? Address.webSession("eval")
        : Address.phone("+13175550123")!;
    const { contact } = await store.conversations.resolve(tenant.id, sender, new Date());
    await store.conversations.updateContact(contact.id, {
        firstName: scenario.profile.firstName ?? (scenario.channel === "web" ? "Alex" : ""),
        ...(scenario.channel === "web" ? { phone: Address.phone(scenario.profile.phone ?? "+13175550123")!.value as never } : {}),
    });

    const failures: string[] = [];
    for (const [i, turn] of scenario.turns.entries()) {
        const inbound: Inbound = {
            channel:           scenario.channel,
            tenant:            { kind: "slug", slug: tenant.slug },
            sender,
            text:              turn.say,
            receivedAt:        new Date(),
            dedupeKey:         `eval:${i}`,
            externalSessionId: scenario.channel === "sms" ? null : "eval",
            confidence:        null,
            language:          null,
            forwardedFrom:     null,
        };
        const result = await collect(engine.run(inbound, { tenant, vendor: "twilio" }));
        const reply = result.outcome.reply;
        const kinds = new Set<string>(result.outcome.events.map(e => e.kind));
        const at = `turn ${i + 1} ("${turn.say.slice(0, 40)}")`;
        const e = turn.expect;
        for (const kind of e.events) {
            if (!kinds.has(kind as EventKind) && !(kind === "link_requested" && /https?:\/\//.test(reply))) {
                failures.push(`${at}: expected ${kind}`);
            }
        }
        for (const kind of e.noEvents) {
            if (kinds.has(kind as EventKind)) {
                failures.push(`${at}: did not expect ${kind}`);
            }
        }
        for (const text of e.replyIncludes) {
            if (!reply.toLowerCase().includes(text.toLowerCase())) {
                failures.push(`${at}: reply should include "${text}" - got: ${reply}`);
            }
        }
        for (const text of e.replyExcludes) {
            if (reply.toLowerCase().includes(text.toLowerCase())) {
                failures.push(`${at}: reply should not include "${text}" - got: ${reply}`);
            }
        }
        if (e.replyMatches !== undefined && !new RegExp(e.replyMatches, "i").test(reply)) {
            failures.push(`${at}: reply should match /${e.replyMatches}/ - got: ${reply}`);
        }
        if (e.language !== undefined && result.session.language !== e.language) {
            failures.push(`${at}: expected language ${e.language}, got ${result.session.language}`);
        }
        if (e.directive !== undefined && result.outcome.directive.kind !== e.directive) {
            failures.push(`${at}: expected call directive ${e.directive}, got ${result.outcome.directive.kind}`);
        }
    }
    const costUsd = (await store.spend.summary(null, new Date(0))).reduce((sum, r) => sum + r.costUsd, 0);
    await store.close();
    return { name: scenario.name, passed: failures.length === 0, failures, costUsd };
}
