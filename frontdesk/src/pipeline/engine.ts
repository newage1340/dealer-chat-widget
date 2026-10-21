/**
 * Runs one inbound message through a turn: resolve who and where, check policies and the budget,
 * let the model reply (streaming, with tools), and commit everything the turn established at once.
 *
 * The reply streams out as it is generated; texts, alerts and summaries the turn caused run later
 * as jobs, so nothing slow sits between the customer and the answer.
 */

import {
    Budget,
    CallState,
    LocalTime,
    PhoneNumber,
    SessionMeter,
    TurnAssessment,
    type BudgetInput,
    type BudgetVerdict,
    type CallDirective,
    type CallVendor,
    type Contact,
    type ContactChanges,
    type EventBody,
    type Inbound,
    type Job,
    type Language,
    type Session,
    type Tenant,
} from "../core/index.ts";
import { STRINGS, say } from "../prompts/strings.ts";
import {
    ProviderError,
    type ChatMessage,
    type ToolCall,
} from "../providers/index.ts";
import type { NewMessage, SessionKey } from "../storage/index.ts";
import { definitions, toolsFor } from "../tools/index.ts";
import {
    Subscriber,
    TurnOutcome,
    type Tool,
    type ToolResult,
    type TurnContext,
} from "../turn/index.ts";
import { closeSession } from "./sessions.ts";
import { stamp, type Services } from "./services.ts";

const MAX_TOOL_ROUNDS = 4;
const HISTORY_LIMIT = 40;
const URL_RE = /\b(?:https?:\/\/|www\.)\S+/gi;

export class UnknownTenant extends Error {
    override readonly name = "UnknownTenant";
}

/**
 * The finished turn, for the channel to render.
 * `data` carries channel extras (the web widget's profile form); `replayed` marks a retried webhook.
 */
export type TurnResult = {
    readonly outcome:  TurnOutcome;
    readonly tenant:   Tenant;
    readonly contact:  Contact;
    readonly session:  Session;
    readonly data:     Readonly<Record<string, unknown>>;
    readonly silent:   boolean;
    readonly replayed: boolean;
};

export type EnginePart =
    | { readonly kind: "text";
        readonly delta: string
    }
    | { readonly kind: "done";
        readonly result: TurnResult
    };

/**
 * `vendor` names who placed a call (required for voice); `tenant` skips the lookup when the channel already has it.
 */
export type RunOptions = {
    readonly tenant?: Tenant;
    readonly vendor?: CallVendor;
};

function sessionKey(inbound: Inbound, vendor: CallVendor | undefined): SessionKey {
    switch (inbound.channel) {
        case "web":
            return { channel: "web", externalId: inbound.externalSessionId ?? inbound.sender.value };
        case "sms":
            return { channel: "sms" };
        case "voice":
            return {
                channel:    "voice",
                externalId: inbound.externalSessionId ?? inbound.dedupeKey,
                vendor:     vendor ?? "twilio",
                handling:   "assistant",
            };
    }
}

function worse(a: TurnAssessment, b: TurnAssessment | null): TurnAssessment {
    const rank = { productive: 0, off_topic: 1, abusive: 2 } as const;
    return b !== null && rank[b] > rank[a]
        ? b
        : a;
}

/**
 * Collect a run into its result (for channels that do not stream).
 */
export async function collect(parts: AsyncIterable<EnginePart>): Promise<TurnResult> {
    for await (const part of parts) {
        if (part.kind === "done") {
            return part.result;
        }
    }
    throw new Error("turn ended without a result");
}

export class TurnEngine {
    readonly #s: Services;

    constructor(services: Services) {
        this.#s = services;
    }

    async *run(inbound: Inbound, options: RunOptions = {}): AsyncGenerator<EnginePart> {
        const { store } = this.#s;
        const now = this.#s.clock();

        const tenant = options.tenant ?? await this.#tenant(inbound);
        const pack = this.#s.registry.get(tenant.domain);
        const { contact, conversation } = await store.conversations.resolve(tenant.id, inbound.sender, now);

        const previous = await store.turns.find(inbound.dedupeKey);
        if (previous !== null) {
            const session = await store.conversations.sessionById(previous.sessionId);
            if (session !== null) {
                yield { kind: "text", delta: previous.reply };
                yield {
                    kind:   "done",
                    result: {
                        outcome:  TurnOutcome.reply(previous.reply, session.language),
                        tenant,
                        contact,
                        session,
                        data:     {},
                        silent:   previous.reply === "",
                        replayed: true,
                    },
                };
                return;
            }
        }

        const session = await store.conversations.session(
            conversation,
            sessionKey(inbound, options.vendor),
            { language: contact.preferredLanguage ?? inbound.language ?? tenant.defaultLanguage, smsIdleMs: this.#s.smsIdleMs },
            now,
        );
        const ctx: TurnContext = {
            inbound,
            tenant,
            pack,
            catalog:       await store.tenants.catalog(tenant.id),
            contact,
            conversation,
            session,
            history:       await store.conversations.history(conversation.id, HISTORY_LIMIT),
            appointment:   await store.appointments.latest(conversation.id),
            costThisMonth: await store.spend.monthToDate(tenant, now),
            now,
            local:         LocalTime.of(now, tenant.timezone),
        };

        // Policies: compliance, blocks, the web profile gate.
        for (const policy of this.#s.policies) {
            const decision = await policy.check(ctx);
            if (decision.kind === "continue") {
                continue;
            }
            if (decision.kind === "silence") {
                await this.#commit(ctx, TurnOutcome.reply("", session.language), session, false);
                yield { kind: "done", result: this.#result(ctx, TurnOutcome.reply("", session.language), session, {}, true) };
                return;
            }
            if (decision.outcome.reply) {
                yield { kind: "text", delta: decision.outcome.reply };
            }
            const saved = decision.ephemeral
                ? session
                : await this.#commit(ctx, decision.outcome, session, false);
            yield { kind: "done", result: this.#result(ctx, decision.outcome, saved, decision.data, false) };
            return;
        }

        // Budget: whether this session may use the model, and whether it is winding down.
        const input: BudgetInput = {
            channel:       inbound.channel,
            plan:          tenant.plan,
            schedule:      tenant.schedule,
            timeZone:      tenant.timezone,
            textAlwaysOn:  tenant.settings.textAlwaysOn,
            costThisMonth: ctx.costThisMonth,
            activeCalls:   0,
            now,
        };
        if (inbound.channel !== "voice" && session.meter.turns === 0) {
            const admission = Budget.admit(contact.block, input);
            if (admission.kind === "refuse") {
                const outcome = this.#refusal(ctx, admission.reason);
                if (outcome.reply) {
                    yield { kind: "text", delta: outcome.reply };
                }
                const saved = await this.#commit(ctx, outcome, session, false);
                yield { kind: "done", result: this.#result(ctx, outcome, saved, {}, outcome.reply === "") };
                return;
            }
        }
        const verdict = Budget.check(session.meter, tenant.settings.limits, input);
        if (verdict.kind === "end") {
            const outcome = { ...this.#goodbye(ctx), directive: { kind: "end" } as const };
            yield { kind: "text", delta: outcome.reply };
            const saved = await this.#commit(ctx, outcome, session, true);
            if (inbound.channel !== "voice") {
                await closeSession(this.#s, session.id, "budget");
            }
            yield { kind: "done", result: this.#result(ctx, outcome, saved, {}, false) };
            return;
        }

        // The model.
        let outcome: TurnOutcome;
        let costUsd = 0;
        const generated = this.#generate(ctx, verdict);
        for (;;) {
            const step = await generated.next();
            if (step.done) {
                ({ outcome, costUsd } = step.value);
                break;
            }
            yield step.value;
        }

        // Links in text channels go into the reply itself; on calls they're texted (link_requested -> job).
        if (inbound.channel !== "voice") {
            const links = outcome.events.filter(e => e.kind === "link_requested");
            if (links.length > 0) {
                const lines = links.map(l => `${l.label.charAt(0).toUpperCase()}${l.label.slice(1)}: ${l.url}`).join("\n");
                yield { kind: "text", delta: `\n\n${lines}` };
                outcome = {
                    ...outcome,
                    reply:  `${outcome.reply}\n\n${lines}`,
                    events: outcome.events.filter(e => e.kind !== "link_requested"),
                };
            }
        }

        // A wind-down's last turn ends the session, whatever the model did.
        const lastClosingTurn = verdict.kind === "closing" && verdict.turnsLeft <= 1;
        if (lastClosingTurn && inbound.channel === "voice" && outcome.directive.kind === "continue") {
            outcome = { ...outcome, directive: { kind: "end" } };
        }
        const extra: EventBody[] = verdict.kind === "closing" && session.meter.closing === null
            ? [{ kind: "session_closing", reason: verdict.reason }]
            : [];
        const capBefore = Budget.capStatus(ctx.costThisMonth, tenant.plan);
        const capAfter = Budget.capStatus(ctx.costThisMonth + costUsd, tenant.plan);
        if (capAfter !== capBefore && (capAfter === "warning" || capAfter === "assistant_used")) {
            extra.push({ kind: "tenant_cap", status: capAfter });
        }

        const meter = SessionMeter.record(session.meter, verdict, outcome.assessment, costUsd);
        const saved = await this.#commit(ctx, { ...outcome, events: [...outcome.events, ...extra] }, { ...session, meter }, true);
        if (lastClosingTurn && inbound.channel !== "voice") {
            await closeSession(this.#s, session.id, "budget");
        }
        yield { kind: "done", result: this.#result(ctx, outcome, saved, {}, false) };
    }

    // --- the model loop -------------------------------------------------------------

    async *#generate(ctx: TurnContext, verdict: BudgetVerdict): AsyncGenerator<EnginePart, { outcome: TurnOutcome; costUsd: number }> {
        const voice = ctx.inbound.channel === "voice";
        const tools = toolsFor(ctx);
        const defs = definitions(tools);
        let messages: ChatMessage[] = this.#s.composer.turn(ctx, verdict);
        let text = "";
        let costUsd = 0;
        const results: ToolResult[] = [];

        try {
            for (let round = 0; round < MAX_TOOL_ROUNDS; round++) {
                let roundText = "";
                const calls: ToolCall[] = [];
                const stream = this.#s.llm.stream({
                    messages,
                    tools:       defs,
                    format:      { kind: "text" },
                    tier:        voice ? "fast" : "capable",
                    maxTokens:   voice ? 300 : 600,
                    temperature: 0.4,
                    timeoutMs:   voice ? 15_000 : 40_000,
                    purpose:     "turn",
                    tenantId:    ctx.tenant.id,
                    channel:     ctx.inbound.channel,
                });
                for await (const part of stream) {
                    if (part.kind === "text") {
                        const delta = text === "" && roundText === "" && round > 0
                            ? part.delta.trimStart()
                            : part.delta;
                        if (delta) {
                            if (text !== "" && roundText === "") {
                                roundText += " ";
                                yield { kind: "text", delta: " " };
                            }
                            roundText += delta;
                            yield { kind: "text", delta };
                        }
                    } else if (part.kind === "tool_call") {
                        calls.push(part.call);
                    } else {
                        costUsd += part.usage.costUsd ?? 0;
                    }
                }
                text += roundText;
                if (calls.length === 0) {
                    break;
                }

                const roundResults: { call: ToolCall; result: ToolResult; tool: Tool | undefined }[] = [];
                for (const call of calls) {
                    const tool = tools.find(t => t.name === call.name);
                    const result = await this.#runTool(tool, call, ctx);
                    roundResults.push({ call, result, tool });
                    results.push(result);
                }
                const needMore = text.trim() === ""
                    || roundResults.some(r => r.result.failed || r.tool?.followUp === true);
                if (!needMore) {
                    break;
                }
                messages = [
                    ...messages,
                    { role: "assistant", text: roundText, toolCalls: calls },
                    ...roundResults.map(r => ({ role: "tool" as const, toolCallId: r.call.id, output: JSON.stringify(r.result.output) })),
                ];
            }
        } catch (e) {
            if (!(e instanceof ProviderError)) {
                throw e;
            }
            console.error(`LLM turn failed for ${ctx.tenant.id}: ${e.message}`);
        }

        const language = results.reduce<Language>((l, r) => r.language ?? l, ctx.session.language);
        let reply = text.replace(URL_RE, "").replace(/[ \t]+\n/g, "\n").trim();
        if (!reply) {
            reply = say(voice ? STRINGS.fallbackVoice : STRINGS.fallbackText, language);
            yield { kind: "text", delta: reply };
        }
        const contactChanges: ContactChanges = Object.assign({}, ...results.map(r => r.contactChanges));
        const outcome: TurnOutcome = {
            reply,
            language,
            assessment:     results.reduce<TurnAssessment>((a, r) => worse(a, r.assessment), "productive"),
            events:         results.flatMap(r => r.events),
            contactChanges: language !== ctx.contact.preferredLanguage
                ? { ...contactChanges, preferredLanguage: language }
                : contactChanges,
            sentUrls:       results.flatMap(r => r.sentUrls),
            directive:      results.reduce<CallDirective>((d, r) => r.directive ?? d, { kind: "continue" }),
        };
        return { outcome, costUsd };
    }

    async #runTool(tool: Tool | undefined, call: ToolCall, ctx: TurnContext): Promise<ToolResult> {
        const failed = (message: string): ToolResult => ({
            output:         { error: message },
            failed:         true,
            events:         [],
            contactChanges: {},
            sentUrls:       [],
            directive:      null,
            assessment:     null,
            language:       null,
        });
        if (tool === undefined) {
            return failed(`There is no tool called ${call.name}.`);
        }
        const args = tool.parameters.safeParse(call.arguments);
        if (!args.success) {
            return failed(args.error.issues.map(i => `${i.path.join(".") || "arguments"}: ${i.message}`).join("; "));
        }
        try {
            return await tool.run(args.data, ctx);
        } catch (e) {
            console.error(`tool ${call.name} failed:`, e);
            return failed("That didn't work; continue without it.");
        }
    }

    // --- canned outcomes --------------------------------------------------------------

    #values(ctx: TurnContext): Record<string, string> {
        return {
            ...this.#s.composer.values(ctx.tenant, ctx.pack, ctx.inbound.channel),
            phone_sentence: ctx.tenant.phone !== null
                ? ` at ${PhoneNumber.display(ctx.tenant.phone)}`
                : "",
            hours_sentence: ctx.tenant.hoursText
                ? say(STRINGS.hoursSentence, ctx.session.language, { hours: ctx.tenant.hoursText })
                : "",
        };
    }

    #goodbye(ctx: TurnContext): TurnOutcome {
        return TurnOutcome.reply(say(STRINGS.goodbye, ctx.session.language, this.#values(ctx)), ctx.session.language);
    }

    #refusal(ctx: TurnContext, reason: "blocked" | "tenant_cap" | "allowance_used" | "off_schedule" | "line_busy"): TurnOutcome {
        const language = ctx.session.language;
        switch (reason) {
            case "blocked":
                return TurnOutcome.reply("", language);
            case "tenant_cap":
            case "allowance_used":
                return TurnOutcome.reply(say(STRINGS.unavailableText, language, this.#values(ctx)), language);
            case "off_schedule":
            case "line_busy":
                return {
                    ...TurnOutcome.reply(say(STRINGS.awayText, language, this.#values(ctx)), language),
                    events: [{ kind: "handoff_requested", reason: "take_message", note: `Message while the assistant was off: "${ctx.inbound.text}"` }],
                };
        }
    }

    // --- persistence --------------------------------------------------------------------

    async #commit(
        ctx: TurnContext,
        outcome: TurnOutcome,
        session: Session,
        recordCall: boolean
    ): Promise<Session>
    {
        const now = ctx.now;
        let next: Session = {
            ...session,
            language: outcome.language,
            sentUrls: [...new Set([...session.sentUrls, ...outcome.sentUrls])],
        };
        if (next.channel === "voice" && recordCall) {
            const heard = CallState.next(next.call, { kind: "heard", text: ctx.inbound.text });
            const call = heard.ok ? heard.value : next.call;
            const replied = CallState.next(call, { kind: "replied", directive: outcome.directive });
            next = { ...next, call: replied.ok ? replied.value : call };
        }
        const messages: NewMessage[] = [];
        if (ctx.inbound.text) {
            messages.push({ conversationId: ctx.conversation.id, sessionId: session.id, role: "user", text: ctx.inbound.text, channel: ctx.inbound.channel });
        }
        if (outcome.reply) {
            messages.push({ conversationId: ctx.conversation.id, sessionId: session.id, role: "assistant", text: outcome.reply, channel: ctx.inbound.channel });
        }
        const events = stamp(outcome.events, {
            tenantId:       ctx.tenant.id,
            conversationId: ctx.conversation.id,
            sessionId:      session.id,
            occurredAt:     now,
            causedBy:       ctx.inbound.dedupeKey,
        });
        const contact = { ...ctx.contact, ...outcome.contactChanges };
        const jobs = [
            ...Subscriber.jobsFor(this.#s.subscribers, events, {
                tenant:  ctx.tenant,
                pack:    ctx.pack,
                contact,
                channel: ctx.inbound.channel,
                now,
            }),
            ...this.#coldFollowup(ctx, outcome),
        ];
        await this.#s.store.turns.commit({
            dedupeKey:      ctx.inbound.dedupeKey,
            reply:          outcome.reply,
            contactId:      ctx.contact.id,
            contactChanges: outcome.contactChanges,
            session:        next,
            messages,
            events,
            jobs,
            at:             now,
        });
        return next;
    }

    /**
     * A follow-up text if the customer goes quiet after this reply (text channels, when the tenant wants them).
     */
    #coldFollowup(ctx: TurnContext, outcome: TurnOutcome): Job[] {
        const minutes = ctx.tenant.settings.coldFollowupMinutes;
        if (minutes === null || ctx.inbound.channel === "voice" || !outcome.reply || ctx.contact.phone === null) {
            return [];
        }
        const day = ctx.local.year * 10_000 + ctx.local.month * 100 + ctx.local.day;
        return [{
            kind:           "cold_followup",
            conversationId: ctx.conversation.id,
            afterMessageAt: ctx.now.toISOString(),
            tenantId:       ctx.tenant.id,
            dedupeKey:      `cold:${ctx.conversation.id}:${day}:${ctx.inbound.dedupeKey}`,
            runAt:          new Date(ctx.now.getTime() + minutes * 60_000),
        }];
    }

    #result(
        ctx: TurnContext,
        outcome: TurnOutcome,
        session: Session,
        data: Readonly<Record<string, unknown>>,
        silent: boolean
    ): TurnResult
    {
        return {
            outcome,
            tenant:   ctx.tenant,
            contact:  { ...ctx.contact, ...outcome.contactChanges },
            session,
            data,
            silent,
            replayed: false,
        };
    }

    async #tenant(inbound: Inbound): Promise<Tenant> {
        const ref = inbound.tenant;
        const tenant = ref.kind === "slug"
            ? await this.#s.store.tenants.bySlug(ref.slug)
            : await this.#s.store.tenants.byEndpoint(inbound.channel, ref.address);
        if (tenant === null) {
            throw new UnknownTenant(ref.kind === "slug" ? `no tenant '${ref.slug}'` : `no tenant for ${inbound.channel} ${ref.address.value}`);
        }
        return tenant;
    }
}
