/**
 * Deterministic checks that run before the model.
 * Order matters: compliance first (opt-out), then blocks, then the web profile gate.
 */

import { Block } from "../core/index.ts";
import { STRINGS, phoneSentence, say } from "../prompts/strings.ts";
import { PolicyDecision, TurnOutcome, type Policy } from "../turn/index.ts";

const STOP_WORDS: ReadonlySet<string> = new Set(["stop", "stopall", "unsubscribe", "cancel", "end", "quit", "alto", "parar", "baja"]);
const START_WORDS: ReadonlySet<string> = new Set(["start", "unstop", "resume", "inicio", "empezar"]);
const HELP_WORDS: ReadonlySet<string> = new Set(["help", "info", "ayuda"]);

/**
 * SMS compliance keywords (STOP / START / HELP, and their Spanish equivalents).
 * Opted-out contacts get no replies at all.
 */
export const optOut: Policy = {
    name: "opt_out",
    check(ctx) {
        if (ctx.inbound.channel !== "sms") {
            return PolicyDecision.continue;
        }
        const word = ctx.inbound.text.toLowerCase().replace(/[^a-z]/g, "");
        const language = ["alto", "parar", "baja", "inicio", "empezar", "ayuda"].includes(word)
            ? "es"
            : ctx.session.language;
        const values = { business_name: ctx.tenant.name, phone_sentence: phoneSentence(ctx.tenant) };
        if (STOP_WORDS.has(word)) {
            return PolicyDecision.answer({
                ...TurnOutcome.reply(say(STRINGS.optedOut, language, values), language),
                contactChanges: { optedOut: true },
                events:         [{ kind: "opted_out", contactId: ctx.contact.id }],
            });
        }
        if (START_WORDS.has(word)) {
            return PolicyDecision.answer({
                ...TurnOutcome.reply(say(STRINGS.optedIn, language, values), language),
                contactChanges: { optedOut: false },
                events:         [{ kind: "opted_in", contactId: ctx.contact.id }],
            });
        }
        if (HELP_WORDS.has(word)) {
            return PolicyDecision.answer(TurnOutcome.reply(say(STRINGS.help, language, values), language));
        }
        return ctx.contact.optedOut
            ? PolicyDecision.silence
            : PolicyDecision.continue;
    },
};

/**
 * Blocked contacts (automatically after abuse, or by staff) get no replies.
 */
export const blocked: Policy = {
    name: "blocked",
    check(ctx) {
        return Block.isActive(ctx.contact.block, ctx.now)
            ? PolicyDecision.silence
            : PolicyDecision.continue;
    },
};

/**
 * Web-widget visitors give a first name and phone before the assistant answers,
 * so confirmations and follow-ups can reach a real number.
 * Tenants can turn it off (e.g. a demo site).
 */
export const webProfileGate: Policy = {
    name: "profile_gate",
    check(ctx) {
        if (ctx.inbound.channel !== "web" || ctx.tenant.settings.skipProfileGate) {
            return PolicyDecision.continue;
        }
        const missingName = !ctx.contact.firstName;
        const missingPhone = ctx.contact.phone === null;
        if (!missingName && !missingPhone) {
            return PolicyDecision.continue;
        }
        return PolicyDecision.answer(
            TurnOutcome.reply(say(STRINGS.profileGate, ctx.session.language), ctx.session.language),
            true,
            {
                needs_profile:   true,
                missing:         { name: missingName, phone: missingPhone },
                pending_message: ctx.inbound.text,
            },
        );
    },
};

export const DEFAULT_POLICIES: readonly Policy[] = [optOut, blocked, webProfileGate];
