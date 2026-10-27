/**
 * Builds model prompts as: universal base rules + the domain pack's sections + tenant facts + per-turn context.
 *
 * The first system message is everything that stays the same between turns
 * (base rules, domain sections, business info, the catalog), so providers with prefix caching reuse it.
 * The second carries what changes every turn (customer, appointment, time, focus, wind-down).
 * The sections themselves are in sections.ts; the placeholder values in values.ts.
 */

import { readFile } from "node:fs/promises";

import {
    CONTROL_VARIANT,
    LANGUAGE_NAMES,
    LocalizedText,
    LocalTime,
    type BudgetVerdict,
    type Contact,
    type Language,
    type Message,
    type PromptVariant,
    type Tenant,
} from "../core/index.ts";
import { DomainPack, Template, type FollowupKind } from "../domains/index.ts";
import type { ChatMessage } from "../providers/index.ts";
import type { TurnContext } from "../turn/index.ts";
import { baseValues, PER_TURN_SECTIONS, STABLE_SECTIONS } from "./sections.ts";
import { PromptValues, type Values } from "./values.ts";
import { BASE_TEMPLATE_FILES, Experiment, type BaseTemplates } from "./variants.ts";

const TEMPLATES_DIR = new URL("./templates/", import.meta.url);
const MAX_HISTORY = 30;

function history(messages: readonly Message[]): ChatMessage[] {
    return messages
        .slice(-MAX_HISTORY)
        .filter(m => m.text.trim())
        .map(m => m.role === "user"
            ? { role: "user", text: m.text }
            : { role: "assistant", text: m.text, toolCalls: [] });
}

export class PromptComposer {
    readonly #base:       BaseTemplates;
    readonly #classify:   string;
    readonly #experiment: Experiment;

    constructor(base: BaseTemplates, classify: string, experiment: Experiment = Experiment.none(base)) {
        this.#base       = base;
        this.#classify   = classify;
        this.#experiment = experiment;
    }

    static async load(): Promise<PromptComposer> {
        const read = (name: string) => readFile(new URL(name, TEMPLATES_DIR), "utf8");
        const base: BaseTemplates = {
            chat:  await read(BASE_TEMPLATE_FILES.chat),
            voice: await read(BASE_TEMPLATE_FILES.voice),
        };
        return new PromptComposer(base, await read("classify.md"), await Experiment.load(base));
    }

    get experiment(): string {
        return this.#experiment.name;
    }

    get variants(): readonly PromptVariant[] {
        return this.#experiment.variants;
    }

    variant(id: string): PromptVariant | null {
        return this.variants.find(v => v.id === id) ?? null;
    }

    /**
     * The messages for one conversation turn.
     */
    turn(ctx: TurnContext, verdict: BudgetVerdict, variant: PromptVariant = CONTROL_VARIANT): ChatMessage[] {
        const voice = ctx.inbound.channel === "voice";
        const templates = this.#experiment.templates.get(variant.id) ?? this.#base;
        const base = Template.render(voice ? templates.voice : templates.chat, baseValues(ctx));
        // Shared by every tenant of this type first, then this tenant's parts, so the cacheable prefix is as long as possible.
        const stable = [base, ...STABLE_SECTIONS.map(section => section(ctx, verdict))].join("\n\n");
        const perTurn = PER_TURN_SECTIONS.map(section => section(ctx, verdict)).filter(Boolean).join("\n\n");
        const message = voice
            ? `[NOW: ${LocalTime.weekdayName(ctx.local)} ${LocalTime.clock(ctx.local)}]\n${ctx.inbound.text}`
            : ctx.inbound.text;
        return [
            { role: "system", text: stable, cacheable: true },
            { role: "system", text: perTurn, cacheable: false },
            ...history(ctx.history),
            { role: "user", text: message },
        ];
    }

    /**
     * The messages that classify a finished conversation.
     */
    classify(tenant: Tenant, pack: DomainPack, transcript: string): ChatMessage[] {
        const values = PromptValues.tenant(tenant, pack, "web");
        const all: Values = {
            ...values,
            topics:              DomainPack.topics(pack).map(t => `-   ${t.id}: ${t.description}`).join("\n"),
            languages:           Object.entries(LANGUAGE_NAMES).map(([code, name]) => `${code} = ${name}`).join(", "),
            staff_language_name: LANGUAGE_NAMES[tenant.staffLanguage],
            summary_focus:       pack.prompts.summaryFocus
                ? `\n    What matters most for this business: ${Template.render(pack.prompts.summaryFocus, values)}`
                : "",
        };
        return [
            { role: "system", text: Template.render(this.#classify, all), cacheable: true },
            { role: "user", text: `TRANSCRIPT:\n${transcript}` },
        ];
    }

    /**
     * A pack follow-up text (confirmation, reminder, cold follow-up) in the customer's language, or null if the pack has none.
     */
    followup(
        tenant: Tenant,
        pack: DomainPack,
        kind: FollowupKind,
        language: Language,
        contact: Contact | null,
        extra: Values = {}
    ): string | null
    {
        const template = pack.followups[kind];
        if (template === undefined) {
            return null;
        }
        return Template.render(LocalizedText.in(template, language), {
            ...PromptValues.tenant(tenant, pack, "sms"),
            first_name: contact?.firstName || "there",
            ...extra,
        });
    }
}
