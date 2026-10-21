/**
 * Runs one inbound message through a turn: resolve who and where, check policies and the budget,
 * let the model reply (streaming, with tools), and commit everything the turn established at once.
 *
 * The reply streams out as it is generated; texts, alerts and summaries the turn caused run later
 * as jobs, so nothing slow sits between the customer and the answer.
 */

import {
    assertNever,
    Billing,
    Budget,
    CallState,
    Contact,
    Crisis,
    LanguageGuess,
    LocalTime,
    Pii,
    PromptVariant,
    SessionMeter,
    Tenant,
    Text,
    TurnAssessment,
    type BudgetInput,
    type BudgetVerdict,
    type CallDirective,
    type CallVendor,
    type ContactChanges,
    type Conversation,
    type ConversationId,
    type EventBody,
    type Inbound,
    type Job,
    type Language,
    type RefusalReason,
    type Session,
} from "../core/index.ts";
import { STRINGS, phoneSentence, say } from "../prompts/strings.ts";
import {
    ProviderError,
    type ChatMessage,
    type ToolCall,
} from "../providers/index.ts";
import type { NewMessage, SessionKey } from "../storage/index.ts";
import { definitions, toolsFor } from "../tools/index.ts";
import {
    ToolResult,
    TurnOutcome,
    type Tool,
    type TurnContext,
} from "../turn/index.ts";
import { closeSession } from "./sessions.ts";
import { eventsAndJobs, type Services } from "./services.ts";

const MAX_TOOL_ROUNDS = 4;
/**
 * Low enough that the same question gets the same kind of answer, high enough to not sound canned.
 */
const TURN_TEMPERATURE = 0.2;
const HISTORY_LIMIT = 40;
const URL_RE = /\b(?:https?:\/\/|www\.)\S+/gi;

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
 * `vendor` names who placed a call (required for voice); `variant` forces a prompt variant (evals).
 */
export type RunOptions = {
    readonly vendor?:  CallVendor;
    readonly variant?: string;
};

/**
 * What the model loop measured, for the quality log.
 */
type Measured = {
    readonly started:      number;
    readonly firstText:    number | null;
    readonly rounds:       number;
    readonly tools:        readonly string[];
    readonly toolFailures: number;
    readonly corrected:    boolean;
    readonly fallback:     boolean;
    readonly model:        string;
    readonly inputTokens:  number;
    readonly cachedTokens: number;
    readonly outputTokens: number;
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
        default:
            return assertNever(inbound.channel);
    }
}

function worse(a: TurnAssessment, b: TurnAssessment | null): TurnAssessment {
    const rank = { productive: 0, off_topic: 1, abusive: 2 } as const;
    return b !== null && rank[b] > rank[a]
        ? b
        : a;
}

/**
 * Move requested links into the reply text, for channels that can show them.
 * `appended` is the text added to the reply ("" when there were no links).
 */
function inlineLinks(outcome: TurnOutcome): { readonly outcome: TurnOutcome; readonly appended: string } {
    const links = outcome.events.filter(e => e.kind === "link_requested");
    if (links.length === 0) {
        return { outcome, appended: "" };
    }
    const appended = `\n\n${links.map(l => `${Text.capitalize(l.label)}: ${l.url}`).join("\n")}`;
    return {
        outcome:  {
            ...outcome,
            reply:  `${outcome.reply}${appended}`,
            events: outcome.events.filter(e => e.kind !== "link_requested"),
        },
        appended,
    };
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

    async *run(
        received: Inbound,
        tenant: Tenant,
        options: RunOptions = {}
    ): AsyncGenerator<EnginePart>
    {
        const now = this.#s.clock();
        // Card numbers and the like never reach the model or the database.
        const inbound: Inbound = { ...received, text: Pii.redact(received.text, tenant.settings.privacy.redact).text };
        const { contact, conversation } = await this.#s.store.conversations.resolve(tenant.id, inbound.sender, now);

        const replayed = await this.#replay(inbound, tenant, contact);
        if (replayed !== null) {
            yield { kind: "text", delta: replayed.outcome.reply };
            yield { kind: "done", result: replayed };
            return;
        }

        const ctx = await this.#context(inbound, tenant, contact, conversation, options.vendor, now);
        const { session } = ctx;

        // Policies: compliance, blocks, the web profile gate.
        for (const policy of this.#s.policies) {
            const decision = await policy.check(ctx);
            switch (decision.kind) {
                case "continue":
                    continue;
                case "silence":
                    yield* this.#answer(ctx, TurnOutcome.reply("", session.language));
                    return;
                case "answer":
                    yield* this.#answer(ctx, decision.outcome, { ephemeral: decision.ephemeral, data: decision.data });
                    return;
                default:
                    assertNever(decision);
            }
        }

        // Budget: whether this session may use the model, and whether it is winding down.
        const input = this.#budgetInput(ctx);
        if (inbound.channel !== "voice" && session.meter.turns === 0) {
            const admission = Budget.admit(contact.block, input);
            if (admission.kind === "refuse") {
                yield* this.#answer(ctx, this.#refusal(ctx, admission.reason));
                return;
            }
        }
        const verdict = Budget.check(session.meter, tenant.settings.limits, input);
        if (verdict.kind === "end") {
            const goodbye: TurnOutcome = { ...this.#goodbye(ctx), directive: { kind: "end" } };
            yield* this.#answer(ctx, goodbye, { recordCall: true, closeText: true });
            return;
        }

        // The model, with this conversation's prompt variant.
        const variant = this.#variant(conversation.id, options.variant);
        const { outcome: generated, costUsd, measured } = yield* this.#generate(ctx, verdict, variant);
        let outcome = generated;

        // Links in text channels go into the reply itself; on calls they're texted (link_requested -> job).
        if (inbound.channel !== "voice") {
            const inlined = inlineLinks(outcome);
            if (inlined.appended !== "") {
                yield { kind: "text", delta: inlined.appended };
                outcome = inlined.outcome;
            }
        }

        // A wind-down's last turn ends the session, whatever the model did.
        const lastClosingTurn = verdict.kind === "closing" && verdict.turnsLeft <= 1;
        if (lastClosingTurn && inbound.channel === "voice" && outcome.directive.kind === "continue") {
            outcome = { ...outcome, directive: { kind: "end" } };
        }

        const meter = SessionMeter.record(session.meter, verdict, outcome.assessment, costUsd);
        const committed = { ...outcome, events: [...outcome.events, ...this.#sideEvents(ctx, verdict, costUsd)] };
        const saved = await this.#commit(ctx, committed, { ...session, meter }, true);
        if (lastClosingTurn && inbound.channel !== "voice") {
            await closeSession(this.#s, session.id, "budget");
        }
        await this.#measure(ctx, variant, measured, outcome, costUsd);
        yield { kind: "done", result: this.#result(ctx, committed, saved, {}, false) };
    }

    // --- the steps of a turn --------------------------------------------------------

    /**
     * The stored answer to a message we already handled (a retried webhook), or null.
     */
    async #replay(inbound: Inbound, tenant: Tenant, contact: Contact): Promise<TurnResult | null> {
        const { store } = this.#s;
        const previous = await store.turns.find(inbound.dedupeKey);
        const session = previous === null
            ? null
            : await store.conversations.sessionById(previous.sessionId);
        if (previous === null || session === null) {
            return null;
        }
        return {
            outcome:  TurnOutcome.reply(previous.reply, session.language),
            tenant,
            contact,
            session,
            data:     {},
            silent:   previous.reply === "",
            replayed: true,
        };
    }

    async #context(
        inbound: Inbound,
        tenant: Tenant,
        contact: Contact,
        conversation: Conversation,
        vendor: CallVendor | undefined,
        now: Date
    ): Promise<TurnContext>
    {
        const { store } = this.#s;
        const session = await store.conversations.session(
            conversation,
            sessionKey(inbound, vendor),
            { language: Tenant.startLanguage(tenant, contact.preferredLanguage ?? inbound.language), smsIdleMs: this.#s.smsIdleMs },
            now,
        );
        return {
            inbound,
            tenant,
            pack:          this.#s.registry.get(tenant.domain),
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
    }

    /**
     * Finish a turn without the model: send `outcome`'s reply and store it.
     * `ephemeral` answers are not stored; `closeText` ends a text session after storing.
     */
    async *#answer(
        ctx: TurnContext,
        outcome: TurnOutcome,
        options: {
            readonly recordCall?: boolean;
            readonly ephemeral?:  boolean;
            readonly closeText?:  boolean;
            readonly data?:       Readonly<Record<string, unknown>>;
        } = {}
    ): AsyncGenerator<EnginePart>
    {
        if (outcome.reply) {
            yield { kind: "text", delta: outcome.reply };
        }
        const saved = options.ephemeral === true
            ? ctx.session
            : await this.#commit(ctx, outcome, ctx.session, options.recordCall ?? false);
        if (options.closeText === true && ctx.inbound.channel !== "voice") {
            await closeSession(this.#s, ctx.session.id, "budget");
        }
        yield { kind: "done", result: this.#result(ctx, outcome, saved, options.data ?? {}, outcome.reply === "") };
    }

    #budgetInput(ctx: TurnContext): BudgetInput {
        const { tenant } = ctx;
        return {
            channel:       ctx.inbound.channel,
            plan:          tenant.plan,
            schedule:      tenant.schedule,
            timeZone:      tenant.timezone,
            textAlwaysOn:  tenant.settings.textAlwaysOn,
            costThisMonth: ctx.costThisMonth,
            activeCalls:   0,
            billingActive: Billing.serves(tenant.billing),
            now:           ctx.now,
        };
    }

    /**
     * The variant asked for by name (evals), else the one this conversation is assigned to.
     */
    #variant(conversationId: ConversationId, requested: string | undefined): PromptVariant {
        const { composer } = this.#s;
        const named = requested === undefined
            ? null
            : composer.variant(requested);
        return named ?? PromptVariant.assign(composer.variants, composer.experiment, conversationId);
    }

    /**
     * Events the turn causes besides the model's own: the start of a wind-down,
     * a possible crisis for staff, and the tenant crossing a spending threshold.
     */
    #sideEvents(ctx: TurnContext, verdict: BudgetVerdict, costUsd: number): EventBody[] {
        const { tenant } = ctx;
        const events: EventBody[] = [];
        if (verdict.kind === "closing" && ctx.session.meter.closing === null) {
            events.push({ kind: "session_closing", reason: verdict.reason });
        }
        if (tenant.settings.privacy.crisisSupport && Crisis.suspected(ctx.inbound.text)) {
            events.push({ kind: "handoff_requested", reason: "crisis", note: "A message in this conversation may indicate a safety concern. Please reach out as soon as possible." });
        }
        const capBefore = Budget.capStatus(ctx.costThisMonth, tenant.plan);
        const capAfter = Budget.capStatus(ctx.costThisMonth + costUsd, tenant.plan);
        if (capAfter !== capBefore && (capAfter === "warning" || capAfter === "assistant_used")) {
            events.push({ kind: "tenant_cap", status: capAfter });
        }
        return events;
    }

    // --- the model loop -------------------------------------------------------------

    async *#generate(
        ctx: TurnContext,
        verdict: BudgetVerdict,
        variant: PromptVariant
    ): AsyncGenerator<EnginePart, { outcome: TurnOutcome; costUsd: number; measured: Measured }>
    {
        const voice = ctx.inbound.channel === "voice";
        const tools = toolsFor(ctx);
        const defs = definitions(tools);
        let messages: ChatMessage[] = this.#s.composer.turn(ctx, verdict, variant);
        let text = "";
        let costUsd = 0;
        const started = performance.now();
        let firstText: number | null = null;
        let rounds = 0;
        let fallback = false;
        const usage = { model: "", input: 0, cached: 0, output: 0 };
        const results: ToolResult[] = [];
        const called = new Set<string>();
        let correcting = false;
        let corrected = false;

        try {
            for (let round = 0; round < MAX_TOOL_ROUNDS; round++) {
                rounds++;
                // A corrective round only calls the tool; its text (if any) is not spoken twice.
                const quiet = correcting;
                correcting = false;
                let roundText = "";
                const calls: ToolCall[] = [];
                const stream = this.#s.llm.stream({
                    cacheKey:    `turn:${ctx.pack.id}:${ctx.inbound.channel}`,
                    messages,
                    tools:       defs,
                    format:      { kind: "text" },
                    tier:        variant.tier ?? (voice ? "fast" : "capable"),
                    maxTokens:   voice ? 300 : 600,
                    temperature: variant.temperature ?? TURN_TEMPERATURE,
                    timeoutMs:   voice ? 15_000 : 40_000,
                    purpose:     "turn",
                    tenantId:    ctx.tenant.id,
                    channel:     ctx.inbound.channel,
                });
                for await (const part of stream) {
                    if (part.kind === "text" && quiet) {
                        continue;
                    }
                    if (part.kind === "text") {
                        const delta = text === "" && roundText === "" && round > 0
                            ? part.delta.trimStart()
                            : part.delta;
                        if (delta) {
                            firstText ??= performance.now() - started;
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
                        usage.model = part.usage.model || usage.model;
                        usage.input += part.usage.units.input_tokens ?? 0;
                        usage.cached += part.usage.units.cached_input_tokens ?? 0;
                        usage.output += part.usage.units.output_tokens ?? 0;
                    }
                }
                text += roundText;
                calls.forEach(c => called.add(c.name));
                if (calls.length === 0) {
                    const broken = tools.filter(t => t.promise !== undefined && !called.has(t.name) && t.promise.test(text));
                    if (broken.length === 0 || corrected) {
                        break;
                    }
                    corrected = true;
                    correcting = true;
                    const names = broken.map(t => `\`${t.name}\``).join(" and ");
                    messages = [
                        ...messages,
                        { role: "assistant", text: roundText, toolCalls: [] },
                        {
                            role: "user",
                            text: `[system note, not from the customer] Your reply said you would do something that only ${names} can do, but you didn't call it. `
                                + `Words alone do nothing. Call ${names} now and write nothing else. If you didn't really mean that, call no tool and write nothing.`,
                        },
                    ];
                    continue;
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
            fallback = true;
        }

        let reply = Pii.redact(text, ctx.tenant.settings.privacy.redact).text
            .replace(URL_RE, "")
            .replace(/[ \t]+\n/g, "\n")
            .trim();
        // The reply's own language wins: it is what the caller hears, so the voice must match it,
        // even when the model switched languages without saying so (or said so and didn't).
        const switched = results.reduce<Language | null>((l, r) => r.language ?? l, null);
        const written = LanguageGuess.of(reply);
        const language = written !== null && Tenant.offers(ctx.tenant, written)
            ? written
            : switched !== null && Tenant.offers(ctx.tenant, switched)
                ? switched
                : ctx.session.language;
        if (!reply) {
            fallback = true;
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
        const measured: Measured = {
            started,
            firstText,
            rounds,
            tools:        [...called],
            toolFailures: results.filter(r => r.failed).length,
            corrected,
            fallback,
            model:        usage.model,
            inputTokens:  usage.input,
            cachedTokens: usage.cached,
            outputTokens: usage.output,
        };
        return { outcome, costUsd, measured };
    }

    /**
     * Record the turn in the quality log. Metering never fails a turn.
     */
    async #measure(ctx: TurnContext, variant: PromptVariant, m: Measured, outcome: TurnOutcome, costUsd: number): Promise<void> {
        try {
            await this.#s.store.quality.recordTurn({
                tenantId:     ctx.tenant.id,
                domain:       ctx.tenant.domain,
                sessionId:    ctx.session.id,
                channel:      ctx.inbound.channel,
                variant:      variant.id,
                model:        m.model || "none",
                occurredAt:   ctx.now,
                latencyMs:    performance.now() - m.started,
                firstTextMs:  m.firstText,
                rounds:       m.rounds,
                tools:        m.tools,
                toolFailures: m.toolFailures,
                corrected:    m.corrected,
                fallback:     m.fallback,
                inputTokens:  m.inputTokens,
                cachedTokens: m.cachedTokens,
                outputTokens: m.outputTokens,
                costUsd,
                replyChars:   outcome.reply.length,
                language:     outcome.language,
                switched:     outcome.language !== ctx.session.language,
            });
        } catch (e) {
            console.error("could not record turn metrics:", e);
        }
    }

    async #runTool(tool: Tool | undefined, call: ToolCall, ctx: TurnContext): Promise<ToolResult> {
        if (tool === undefined) {
            return ToolResult.error(`There is no tool called ${call.name}.`);
        }
        const args = tool.parameters.safeParse(call.arguments);
        if (!args.success) {
            return ToolResult.error(args.error.issues.map(i => `${i.path.join(".") || "arguments"}: ${i.message}`).join("; "));
        }
        try {
            return await tool.run(args.data, ctx);
        } catch (e) {
            console.error(`tool ${call.name} failed:`, e);
            return ToolResult.error("That didn't work; continue without it.");
        }
    }

    // --- canned outcomes --------------------------------------------------------------

    #values(ctx: TurnContext): Record<string, string> {
        return {
            ...this.#s.composer.values(ctx.tenant, ctx.pack, ctx.inbound.channel),
            phone_sentence: phoneSentence(ctx.tenant),
            hours_sentence: ctx.tenant.hoursText
                ? say(STRINGS.hoursSentence, ctx.session.language, { hours: ctx.tenant.hoursText })
                : "",
        };
    }

    #goodbye(ctx: TurnContext): TurnOutcome {
        return TurnOutcome.reply(say(STRINGS.goodbye, ctx.session.language, this.#values(ctx)), ctx.session.language);
    }

    #refusal(ctx: TurnContext, reason: RefusalReason): TurnOutcome {
        const language = ctx.session.language;
        switch (reason) {
            case "blocked":
                return TurnOutcome.reply("", language);
            case "tenant_cap":
            case "allowance_used":
            case "billing_inactive":
                return TurnOutcome.reply(say(STRINGS.unavailableText, language, this.#values(ctx)), language);
            case "off_schedule":
            case "line_busy":
                return {
                    ...TurnOutcome.reply(say(STRINGS.awayText, language, this.#values(ctx)), language),
                    events: [{ kind: "handoff_requested", reason: "take_message", note: `Message while the assistant was off: "${ctx.inbound.text}"` }],
                };
            default:
                return assertNever(reason);
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
        const { events, jobs } = eventsAndJobs(this.#s, {
            tenant:         ctx.tenant,
            conversationId: ctx.conversation.id,
            sessionId:      session.id,
            contact:        Contact.apply(ctx.contact, outcome.contactChanges),
            channel:        ctx.inbound.channel,
            causedBy:       ctx.inbound.dedupeKey,
        }, outcome.events, now);
        await this.#s.store.turns.commit({
            dedupeKey:      ctx.inbound.dedupeKey,
            reply:          outcome.reply,
            contactId:      ctx.contact.id,
            contactChanges: outcome.contactChanges,
            session:        next,
            messages,
            events,
            jobs:           [...jobs, ...this.#coldFollowup(ctx, outcome)],
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
            contact:  Contact.apply(ctx.contact, outcome.contactChanges),
            session,
            data,
            silent,
            replayed: false,
        };
    }
}
