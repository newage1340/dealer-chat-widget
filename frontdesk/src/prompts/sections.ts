/**
 * The sections of a turn prompt. Each reads only the turn's context and returns its text,
 * or "" when it doesn't apply to this turn.
 *
 * `STABLE_SECTIONS` are the same on every turn of a conversation and form the cached prefix;
 * `PER_TURN_SECTIONS` change with every message. Adding a section is one function and one list entry.
 */

import {
    Crisis,
    LANGUAGE_NAMES,
    LocalTime,
    PhoneNumber,
    Text,
    type BudgetVerdict,
} from "../core/index.ts";
import { Catalog, DomainPack, Template, type IntakeField } from "../domains/index.ts";
import type { TurnContext } from "../turn/index.ts";
import { Hours } from "./hours.ts";
import { Speech } from "./speech.ts";
import { Style } from "./style.ts";
import { PromptValues, type PackValues, type Values } from "./values.ts";

/**
 * Catalogs up to this size go into the prompt whole; larger ones are searched with `search_catalog`.
 */
export const FULL_CATALOG_LIMIT = 60;

export type Section = (ctx: TurnContext, verdict: BudgetVerdict) => string;

function isVoice(ctx: TurnContext): boolean {
    return ctx.inbound.channel === "voice";
}

function truthy(value: string | undefined): boolean {
    return value !== undefined
        && !["", "0", "false", "no", "none", "off"].includes(value.trim().toLowerCase());
}

/**
 * Names of the links a tenant's assistant may send: the pack's per-item links and the tenant's own.
 */
export function linkLabels(ctx: TurnContext): string[] {
    return [...ctx.pack.catalog.sendables.map(s => s.label), ...ctx.tenant.settings.links.map(l => l.label)];
}

// --- the base template's values ----------------------------------------------------------

/**
 * The intake steps and location wording for the base template. `packValues` fill the pack's own
 * placeholders in the intake questions.
 */
function bookingValues(pack: DomainPack, voice: boolean, packValues: PackValues): Values {
    const b = pack.booking;
    const intake = DomainPack.intakeFor(pack, voice ? "voice" : "chat");
    const customerSite = b.location === "customer";
    const ask = (f: IntakeField) => Template.render(f.ask, packValues);
    const skipWhen = Template.render(b.skipIntakeWhen, packValues);
    let steps: string;
    if (voice) {
        steps = intake.map((f, i) => `b${i + 1}. ${f.label}: ${ask(f)}`).join("\n        ");
        if (skipWhen) {
            steps += `\n        (Skip the b-steps when ${skipWhen}.)`;
        }
    } else if (intake.length > 0) {
        steps = intake.map(f => `-   ${f.label}: ${ask(f)}`).join("\n");
        if (skipWhen) {
            steps += `\n-   SKIP this step entirely when ${skipWhen}.`;
        }
    } else {
        steps = "-   Nothing extra to ask for this business - go straight to STEP 2.";
    }
    return {
        intake_steps:        steps,
        voice_intake_steps:  voice ? steps : "",
        location_step:       customerSite
            ? `\nSTEP 0.5 - Where the ${b.visitNoun} happens\n-   The team comes to the customer. Get the full address (street and city) before booking, unless it's already in the conversation.\n`
            : "",
        voice_location_step: customerSite
            ? `a2. Get the address for the ${b.visitNoun} (street and city) if you don't have it.`
            : "",
        see_phrase:          customerSite
            ? "helping you"
            : "seeing you",
    };
}

/**
 * The values for the universal base template. Nothing tenant-specific goes in here,
 * so every tenant of the same type shares this prefix (and the provider's prompt cache for it).
 */
export function baseValues(ctx: TurnContext): Values {
    const { pack } = ctx;
    const channel = ctx.inbound.channel;
    const packValues = PromptValues.pack(pack, channel);
    return {
        ...packValues,
        ...bookingValues(pack, channel === "voice", packValues),
        channel_label:       { sms: "SMS", web: "web chat", voice: "phone" }[channel],
        catalog_heading:     pack.catalog.heading,
        goal:                Template.render(pack.goal, packValues),
        upsell_clause:       pack.upsellTopics
            ? ` - ${pack.upsellTopics} -`
            : "",
        business_noun_title: Text.capitalize(pack.businessNoun),
        phone_detail:        channel === "web"
            ? ", or phone number"
            : "",
    };
}

// --- stable sections ---------------------------------------------------------------------

/**
 * Who the assistant is, its languages, tone and privacy rules: everything about the persona that the tenant sets.
 */
function identity(ctx: TurnContext): string {
    const { tenant, pack } = ctx;
    const voice = isVoice(ctx);
    const values = PromptValues.tenant(tenant, pack, ctx.inbound.channel);
    const style = Style.resolve(tenant, pack);
    const role = values.agent_role;
    const intro = voice
        ? ""
        : `You are ${values.agent_name}, ${/^[aeiou]/i.test(role) ? "an" : "a"} ${role} representing ${tenant.name}, on behalf of the ${pack.businessNoun}.\n`
            + `If a customer asks your name, you are ${values.agent_name}.\n`;
    return [
        `=== YOUR IDENTITY ===\n${intro}${Template.render(Style.persona(style, voice), values)}`,
        `=== LANGUAGE ===\n${Style.languages(tenant, voice)}`,
        `=== STYLE ===\n${Style.rules(style, voice)}`,
        Style.privacy(style).trim(),
    ].filter(Boolean).join("\n\n");
}

/**
 * The pack's own instructions, with the toggles the tenant's options turn on or off.
 */
function domainRules(ctx: TurnContext): string {
    const { tenant, pack } = ctx;
    const voice = isVoice(ctx);
    const values = PromptValues.tenant(tenant, pack, ctx.inbound.channel);
    const parts = [Template.render(pack.prompts.chat, values)];
    if (voice) {
        parts.push(Template.render(pack.prompts.voice, values));
    }
    for (const toggle of pack.prompts.toggles) {
        const option = tenant.options[toggle.setting];
        const on = option !== undefined && truthy(String(option));
        const text = on
            ? (voice && toggle.voice ? toggle.voice : toggle.chat)
            : (voice && toggle.elseVoice ? toggle.elseVoice : toggle.elseChat);
        if (text) {
            parts.push(Template.render(text, values));
        }
    }
    const body = parts.map(p => p.trim()).filter(Boolean).join("\n\n");
    return `=== ${pack.name.toUpperCase()} SPECIFICS ===\n${body}`;
}

function businessInfo(ctx: TurnContext): string {
    const { tenant, pack } = ctx;
    const lines = [
        "=== BUSINESS INFO ===",
        `Name: ${tenant.name}`,
        `Address: ${tenant.address || "(not listed)"}`,
        `Hours: ${tenant.hoursText || "(not listed)"}`,
        `Phone (use exactly, never invent): ${tenant.phone === null ? "(not listed)" : PhoneNumber.display(tenant.phone)}`,
    ];
    if (tenant.website) {
        lines.push(`Website: ${tenant.website}`);
    }
    for (const q of pack.profile) {
        lines.push(`${q.label}: ${tenant.profile[q.key] || "(not listed)"}`);
    }
    return lines.join("\n");
}

function links(ctx: TurnContext): string {
    const labels = linkLabels(ctx);
    return labels.length > 0
        ? `=== LINKS YOU CAN SEND (with \`send_link\`) ===\n${labels.map(l => `-   ${l}`).join("\n")}`
        : "=== LINKS YOU CAN SEND ===\n(None. Never offer to send a link; give the information directly or point to the phone number.)";
}

function catalog(ctx: TurnContext): string {
    const largeCatalog = ctx.catalog.length > FULL_CATALOG_LIMIT;
    const spec = ctx.pack.catalog;
    const count = ctx.catalog.length;
    const head = [`=== ${spec.heading} ===`];
    if (spec.intro) {
        head.push(spec.intro);
    }
    head.push(`THERE ARE EXACTLY ${count} ${spec.itemNounPlural.toUpperCase()} LISTED. If asked how many, say ${count} - don't count the list yourself.`);
    head.push(largeCatalog
        ? `The list is too long to show here; call \`search_catalog\` before saying we do or don't have something.`
        : `The list below is complete. Scan all of it before saying we don't have something; \`search_catalog\` gives exact details.`);
    const body = largeCatalog
        ? "(Too many to list - use `search_catalog`.)"
        : Catalog.overview(ctx.catalog, spec);
    return `${head.join("\n")}\n${body}`;
}

// --- per-turn sections -------------------------------------------------------------------

function crisis(ctx: TurnContext): string {
    if (!ctx.tenant.settings.privacy.crisisSupport || !Crisis.suspected(ctx.inbound.text)) {
        return "";
    }
    return "=== SAFETY FIRST (CRITICAL) ===\n"
        + "The customer's latest message may mean they are thinking about harming themselves or are in danger.\n"
        + "Before anything else, respond with warmth and without judgment. Tell them they can call or text 988 (Suicide & Crisis Lifeline) any time, "
        + "or call 911 if they are in immediate danger. Don't try to counsel them, don't change the subject to booking, "
        + `and call \`request_callback\` with the note "urgent: safety concern" so the ${ctx.pack.staffNoun} can reach out.`;
}

function focus(ctx: TurnContext): string {
    const spec = ctx.pack.catalog;
    const text = ctx.inbound.text;
    const own = Catalog.match(ctx.catalog, text, spec);
    const assistantTexts = [...ctx.history].reverse().filter(m => m.role === "assistant").map(m => m.text);
    const anchor = Catalog.anchor(assistantTexts.slice(0, 3), ctx.catalog, spec);
    const header = "=== TOP MATCHING DETAILS ===\n(Use ONLY these facts - do NOT guess anything not shown here)";
    if (anchor !== null && (own.length === 0 || own.some(i => i.id === anchor.id))) {
        const title = Catalog.title(anchor, spec);
        return `${header}\n${Catalog.details(anchor, spec)}\n\n`
            + `=== FOCUS (CRITICAL) ===\nThe conversation has been focused on the "${title}". `
            + `Refer to it exactly; do not drift to a different ${spec.itemNoun} or invent one.`;
    }
    const recent = ctx.history.slice(-2).map(m => m.text).join(" ");
    const matches = own.length > 0
        ? own
        : Catalog.match(ctx.catalog, `${recent} ${text}`, spec);
    const details = matches.length > 0
        ? matches.map(i => Catalog.details(i, spec)).join("\n\n---\n\n")
        : "(No close match found - what the customer asked about may not be something we have.)";
    return `${header}\n${details}`;
}

function customer(ctx: TurnContext): string {
    const c = ctx.contact;
    const voice = isVoice(ctx);
    const known = [
        c.firstName ? `- First name: ${c.firstName}` : "",
        c.lastName ? `- Last name: ${c.lastName}` : "",
        c.email ? `- Email: ${c.email}` : "",
        c.phone !== null && !voice ? `- Phone (for follow-up texts): ${PhoneNumber.display(c.phone)}` : "",
    ].filter(Boolean);
    const block = known.length > 0
        ? `Already collected:\n${known.join("\n")}`
        : "No customer details collected yet.";
    const rules: string[] = [];
    if (c.firstName) {
        rules.push(voice ? "Never ask for their name again." : "Use the first name naturally when it fits. Never ask for it again.");
    }
    if (c.phone !== null) {
        rules.push("The phone number is ALREADY ON FILE - never ask for it.");
    }
    rules.push("LAST NAME IS NEVER REQUIRED - never ask for it.");
    return `=== CUSTOMER PROFILE ===\n${block}\n${rules.join(" ")}`;
}

function appointment(ctx: TurnContext): string {
    const a = ctx.appointment;
    if (a === null) {
        return "=== CONFIRMED APPOINTMENT ===\nNo appointment confirmed yet.";
    }
    const what = `${a.timeText} for ${a.subject}`;
    if (a.startsAt !== null && a.startsAt < ctx.now) {
        return `=== CONFIRMED APPOINTMENT ===\nThis customer had an appointment ${what}, but that time has ALREADY PASSED. `
            + "Don't talk about it as upcoming; acknowledge it and gently offer to reschedule if it fits.";
    }
    return `=== CONFIRMED APPOINTMENT ===\nThis customer has a confirmed appointment ${what}. `
        + "Don't push for a booking - they're booked.\nAnswer their questions naturally.";
}

/**
 * Calls only: the time and whether we're open, stated flatly so the caller hears facts, not guesses.
 */
function realWorld(ctx: TurnContext): string {
    if (!isVoice(ctx)) {
        return "";
    }
    const status = Hours.openStatusLine(ctx.tenant.hoursText, ctx.local);
    return "=== REAL-WORLD FACTS (USE THESE - DO NOT GUESS) ===\n"
        + `CURRENT TIME: ${LocalTime.long(ctx.local)}\n`
        + `HOURS: ${ctx.tenant.hoursText || "(not listed)"}\n`
        + (status ? `${status}\n` : "")
        + "-   Being closed never limits what you answer; it only affects coming in right now.\n"
        + "-   Only bring up open/closed when the caller asks about now/today.\n"
        + "-   Never say we're open until a time that has already passed. Only suggest future times within the hours.";
}

function time(ctx: TurnContext): string {
    const now = ctx.local;
    return "=== CURRENT DATE & TIME (use to interpret \"tomorrow\", \"Friday\", etc.) ===\n"
        + `Today is ${LocalTime.long(now)}.\n\n`
        + "=== WHICH DAYS WE ARE ACTUALLY OPEN (already worked out - do NOT calculate this yourself) ===\n"
        + `${Hours.scheduleBlock(ctx.tenant.hoursText, now)}\n`
        + "NEVER offer, suggest, or confirm a time on a day marked CLOSED.\n"
        + "If the day they asked for is closed, say so and offer the next open day by name.\n"
        + "If TOMORROW is closed, don't use the word \"tomorrow\" for a booking - name the next open day instead.\n"
        + "Quote the opening time listed for THAT day.";
}

function language(ctx: TurnContext): string {
    const language = ctx.session.language;
    return `=== CONVERSATION LANGUAGE ===\nSo far this conversation has been in ${LANGUAGE_NAMES[language]}. `
        + "Reply in the offered language the customer uses in their latest message (see LANGUAGE), and only in that language.";
}

function callerNumber(ctx: TurnContext): string {
    const sender = ctx.inbound.sender;
    if (ctx.inbound.channel !== "voice" || sender.kind !== "phone") {
        return "";
    }
    return "=== CALLER'S NUMBER (from caller ID - already on file) ===\n"
        + `${Speech.spokenDigits(sender.value)}.\nNever ask for it and never read it back; use it silently.`;
}

function windDown(ctx: TurnContext, verdict: BudgetVerdict): string {
    if (verdict.kind !== "closing") {
        return "";
    }
    const voice = isVoice(ctx);
    const hours = ctx.tenant.hoursText
        ? ` (we're open ${ctx.tenant.hoursText})`
        : "";
    const last = verdict.turnsLeft <= 1;
    const lines = [
        "=== WRAP UP NOW (CRITICAL) ===",
        verdict.reason === "abusive"
            ? "The customer has been abusive. Do not engage with it or argue. Stay calm and polite and end the conversation."
            : "This conversation has to end soon. Don't start anything new; answer briefly and steer politely to a close.",
        last
            ? `This is your LAST reply: thank them, mention when they can reach the ${ctx.pack.businessNoun}${hours}, and say goodbye.`
                + (voice ? " Call `end_call`." : "")
            : `You have ${verdict.turnsLeft} replies left, including this one. Within them, mention when they can reach the ${ctx.pack.businessNoun}${hours} and thank them.`,
        "Never mention limits, budgets or rules - just close naturally.",
    ];
    return lines.join("\n");
}

/**
 * Text channels: a last nudge to answer with a single message.
 */
function writeOne(ctx: TurnContext): string {
    return isVoice(ctx)
        ? ""
        : "Write ONE reply now.";
}

/**
 * After the base template, in prompt order. The same on every turn, so providers cache them.
 */
export const STABLE_SECTIONS: readonly Section[] = [
    identity,
    domainRules,
    businessInfo,
    links,
    catalog,
];

/**
 * In prompt order; sections that return "" are left out.
 */
export const PER_TURN_SECTIONS: readonly Section[] = [
    crisis,
    focus,
    customer,
    appointment,
    realWorld,
    time,
    language,
    callerNumber,
    windDown,
    writeOne,
];
