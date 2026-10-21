/**
 * The tools the model acts through. Each records facts (events, contact changes, a call directive);
 * nothing here sends, books or writes anything itself.
 *
 * `toolsFor` picks the set for a turn: some only make sense on calls, some only when the pack or tenant
 * has something to offer (links), and the booking tool's parameters come from the pack's intake fields.
 */

import { randomUUID } from "node:crypto";

import { z } from "zod";

import {
    Address,
    AppointmentId,
    Language,
    LANGUAGE_NAMES,
    LocalTime,
    PhoneNumber,
    WEEKDAY_NAMES,
    type Appointment,
    type ContactChanges,
} from "../core/index.ts";
import { Catalog, DomainPack } from "../domains/index.ts";
import { FULL_CATALOG_LIMIT } from "../prompts/composer.ts";
import { Hours } from "../prompts/hours.ts";
import type { ToolDefinition } from "../providers/index.ts";
import { Tool, ToolResult, type TurnContext } from "../turn/index.ts";

const LANGUAGES = Object.values(Language) as [Language, ...Language[]];

// --- Contact details ---------------------------------------------------------------

function saveCustomerDetails(ctx: TurnContext): Tool {
    const web = ctx.inbound.channel === "web";
    return Tool.define({
        name:        "save_customer_details",
        description: "Record details the customer just told you about themselves. Include only what they said this turn.",
        followUp:    false,
        parameters:  z.object({
            first_name: z.string().optional().describe("First name only"),
            last_name:  z.string().optional(),
            email:      z.string().optional(),
            ...(web ? { phone: z.string().optional().describe("Their phone number, if they gave one") } : {}),
        }),
        run: (args: { first_name?: string | undefined; last_name?: string | undefined; email?: string | undefined; phone?: string | undefined }) => {
            const changes: { -readonly [K in keyof ContactChanges]: ContactChanges[K] } = {};
            const problems: string[] = [];
            if (args.first_name?.trim()) {
                changes.firstName = args.first_name.trim().split(/\s+/)[0]!;
            }
            if (args.last_name?.trim()) {
                changes.lastName = args.last_name.trim();
            }
            if (args.email?.trim()) {
                const email = Address.email(args.email);
                if (email === null) {
                    problems.push(`'${args.email}' is not a valid email address`);
                } else {
                    changes.email = email.value;
                }
            }
            if (args.phone?.trim()) {
                const phone = PhoneNumber.parse(args.phone);
                if (phone === null) {
                    problems.push(`'${args.phone}' is not a valid phone number`);
                } else {
                    changes.phone = phone;
                }
            }
            if (problems.length > 0 && Object.keys(changes).length === 0) {
                return ToolResult.error(problems.join("; "));
            }
            return ToolResult.output(
                { saved: Object.keys(changes), problems },
                {
                    contactChanges: changes,
                    events:         Object.keys(changes).length > 0
                        ? [{ kind: "contact_updated", contactId: ctx.contact.id, changes }]
                        : [],
                },
            );
        },
    });
}

// --- Booking -----------------------------------------------------------------------

/**
 * Why a proposed visit time cannot be booked, or null if it can.
 */
function timeProblem(ctx: TurnContext, startsAt: Date): string | null {
    if (startsAt.getTime() < ctx.now.getTime() - 5 * 60_000) {
        return "That time has already passed. Ask for a time in the future.";
    }
    const local = LocalTime.of(startsAt, ctx.tenant.timezone);
    const hours = Hours.parse(ctx.tenant.hoursText);
    const day = hours[LocalTime.weekdayName(local)];
    if (day === undefined) {
        return null;
    }
    if (Hours.isClosed(day)) {
        const next = Hours.nextOpenDay(hours, local);
        return `We're closed on ${LocalTime.weekdayName(local)}s.`
            + (next !== null ? ` The next open day after that is ${next.label === "tomorrow" ? WEEKDAY_NAMES[(local.weekday + 1) % 7] : next.label} (${next.hours.text}).` : "");
    }
    if (day.opens !== null && day.closes !== null && (local.minuteOfDay < day.opens || local.minuteOfDay >= day.closes)) {
        return `That's outside our hours on ${LocalTime.weekdayName(local)} (${day.text}). Offer a time within them.`;
    }
    return null;
}

function bookAppointment(ctx: TurnContext): Tool {
    const pack = ctx.pack;
    const voice = ctx.inbound.channel === "voice";
    const intake = DomainPack.intakeFor(pack, voice ? "voice" : "chat");
    const customerSite = pack.booking.location === "customer";
    const details = z.object(Object.fromEntries(intake.map(f => [
        f.key,
        f.required
            ? z.string().min(1).describe(f.label)
            : z.string().optional().describe(f.label),
    ])));

    return Tool.define({
        name:        "book_appointment",
        description: `Book (or reschedule) the customer's ${pack.booking.visitNoun}. Call it before confirming anything; the result says whether it worked.`,
        followUp:    true,
        parameters:  z.object({
            time_text:      z.string().describe("The time as you'd say it to the customer, with the day, e.g. 'Saturday at 2 PM'"),
            time_iso:       z.string().describe("The same time as local YYYY-MM-DDTHH:MM, using CURRENT DATE & TIME to resolve words like 'tomorrow'"),
            subject:        z.string().describe(`${pack.booking.subjectHint}, or '${pack.booking.generalSubject}'`),
            ...(customerSite ? { location: z.string().min(1).describe(`Street address and city for the ${pack.booking.visitNoun}`) } : {}),
            details,
            customer_name:  z.string().optional().describe("First name, if known"),
            customer_email: z.string().optional(),
            reschedule:     z.boolean().default(false).describe("True when moving an existing confirmed appointment"),
        }),
        run: (args: {
            time_text: string;
            time_iso: string;
            subject: string;
            location?: string;
            details: Record<string, string | undefined>;
            customer_name?: string | undefined;
            customer_email?: string | undefined;
            reschedule: boolean;
        }) => {
            const startsAt = LocalTime.parseLocalIso(args.time_iso, ctx.tenant.timezone);
            if (startsAt === null) {
                return ToolResult.error("time_iso must be a local date and time like 2026-10-24T14:00.");
            }
            const problem = timeProblem(ctx, startsAt);
            if (problem !== null) {
                return ToolResult.error(problem);
            }
            const existing = ctx.appointment;
            const replaces = existing !== null && (args.reschedule || (existing.startsAt !== null && existing.startsAt > ctx.now))
                ? existing.id
                : null;
            const appointment: Appointment = {
                id:             AppointmentId(randomUUID()),
                tenantId:       ctx.tenant.id,
                contactId:      ctx.contact.id,
                conversationId: ctx.conversation.id,
                timeText:       args.time_text,
                startsAt,
                subject:        args.subject.trim() || pack.booking.generalSubject,
                location:       args.location?.trim() ?? "",
                details:        Object.fromEntries(
                    Object.entries(args.details).flatMap(([k, v]) => v?.trim() ? [[k, v.trim()]] : [])
                ),
                itemIds:        [],
                status:         "confirmed",
                sentNotices:    [],
                createdAt:      ctx.now,
            };
            const changes: { -readonly [K in keyof ContactChanges]: ContactChanges[K] } = {};
            if (args.customer_name?.trim() && !ctx.contact.firstName) {
                changes.firstName = args.customer_name.trim().split(/\s+/)[0]!;
            }
            const email = args.customer_email
                ? Address.email(args.customer_email)
                : null;
            if (email !== null) {
                changes.email = email.value;
            }
            return ToolResult.output(
                { booked: true, time: args.time_text, rescheduled: replaces !== null },
                {
                    events:         [{ kind: "appointment_booked", appointment, replaces }],
                    contactChanges: changes,
                },
            );
        },
    });
}

// --- Links -------------------------------------------------------------------------

type LinkOption =
    | { readonly kind: "catalog";
		readonly label: string;
		readonly field: string
	}
    | { readonly kind: "tenant";
		readonly label: string;
		readonly url: string
	};

function linkOptions(ctx: TurnContext): LinkOption[] {
    return [
        ...ctx.pack.catalog.sendables.map((s): LinkOption => ({ kind: "catalog", label: s.label, field: s.field })),
        ...ctx.tenant.settings.links.map((l): LinkOption => ({ kind: "tenant", label: l.label, url: l.url })),
    ];
}

function sendLink(ctx: TurnContext, options: readonly LinkOption[]): Tool {
    const labels = options.map(o => o.label) as [string, ...string[]];
    const spec = ctx.pack.catalog;
    return Tool.define({
        name:        "send_link",
        description: "Send the customer a real link. Say you're sending it; never write the URL yourself.",
        followUp:    false,
        parameters:  z.object({
            link: z.enum(labels).describe("Which link"),
            item: z.string().optional().describe(`For ${spec.itemNoun} links: which ${spec.itemNoun}, by name`),
        }),
        run: (args: { link: string; item?: string | undefined }) => {
            const option = options.find(o => o.label === args.link)!;
            let url: string;
            let label: string;
            if (option.kind === "tenant") {
                url = option.url;
                label = option.label;
            } else {
                const history = [...ctx.history].reverse().map(m => m.text);
                const item = (args.item ? Catalog.match(ctx.catalog, args.item, spec, 1)[0] : undefined)
                    ?? Catalog.anchor([ctx.inbound.text, ...history.slice(0, 4)], ctx.catalog, spec)
                    ?? Catalog.match(ctx.catalog, ctx.inbound.text, spec, 1)[0];
                if (item === undefined) {
                    return ToolResult.error(`Which ${spec.itemNoun}? Ask the customer, then call send_link with the item.`);
                }
                const value = item.fields[option.field];
                if (value === null || value === undefined || value === "") {
                    return ToolResult.error(`There's no ${option.label} for the ${Catalog.title(item, spec)}.`);
                }
                url = String(value);
                label = `${option.label} for the ${Catalog.title(item, spec)}`;
            }
            if (ctx.session.sentUrls.includes(url)) {
                return ToolResult.output({ alreadySent: label });
            }
            return ToolResult.output(
                { sending: label },
                {
                    events:   [{ kind: "link_requested", to: ctx.inbound.sender, label, url }],
                    sentUrls: [url],
                },
            );
        },
    });
}

// --- Catalog search ----------------------------------------------------------------

function searchCatalog(ctx: TurnContext): Tool {
    const spec = ctx.pack.catalog;
    return Tool.define({
        name:        "search_catalog",
        description: `Find ${spec.itemNounPlural} matching what the customer described.`,
        followUp:    true,
        parameters:  z.object({ query: z.string().describe("What they're looking for, in a few words") }),
        run: (args: { query: string }) => {
            const matches = Catalog.match(ctx.catalog, args.query, spec, 5);
            return ToolResult.output(matches.length > 0
                ? { matches: matches.map(i => Catalog.details(i, spec)) }
                : { matches: [], note: `No ${spec.itemNounPlural} match that. Tell the customer we don't currently have it.` });
        },
    });
}

// --- Conversation control ----------------------------------------------------------

function switchLanguage(): Tool {
    return Tool.define({
        name:        "switch_language",
        description: "The customer wants to (or started to) talk in another language. Continue in it from this reply on.",
        followUp:    false,
        parameters:  z.object({ language: z.enum(LANGUAGES).describe(Object.entries(LANGUAGE_NAMES).map(([k, v]) => `${k}=${v}`).join(", ")) }),
        run: (args: { language: Language }) => ToolResult.output({ language: args.language }, { language: args.language }),
    });
}

function flagConversation(): Tool {
    return Tool.define({
        name:        "flag_conversation",
        description: "This turn was clearly not a real customer request (prank, nonsense, trolling) or was abusive.",
        followUp:    false,
        parameters:  z.object({
            kind: z.enum(["off_topic", "abusive"]),
            note: z.string().describe("A few words on what happened"),
        }),
        run: (args: { kind: "off_topic" | "abusive"; note: string }) => ToolResult.output(
            { flagged: args.kind },
            {
                assessment: args.kind,
                events:     [{ kind: "turn_flagged", assessment: args.kind, note: args.note }],
            },
        ),
    });
}

function requestCallback(): Tool {
    return Tool.define({
        name:        "request_callback",
        description: "The customer wants someone from the team to get back to them. The team is notified right away.",
        followUp:    false,
        parameters:  z.object({ note: z.string().describe("What they want and anything the team should know, in one or two sentences") }),
        run: (args: { note: string }) => ToolResult.output(
            { notified: true },
            { events: [{ kind: "handoff_requested", reason: "take_message", note: args.note }] },
        ),
    });
}

function transferCall(ctx: TurnContext): Tool {
    return Tool.define({
        name:        "transfer_call",
        description: "Connect the caller to a live person now (emergency, safety, or they insist on a person).",
        followUp:    false,
        parameters:  z.object({ reason: z.string().describe("Why, in a few words") }),
        run: (args: { reason: string }) => {
            const target = ctx.tenant.settings.transferTo ?? ctx.tenant.phone;
            if (target === null || ctx.inbound.forwardedFrom === target) {
                return ToolResult.error("Nobody can take a live call right now. Apologize, take a message with request_callback, and tell them the team will call back as soon as possible.");
            }
            return ToolResult.output(
                { transferring: true },
                {
                    directive: { kind: "transfer", to: target },
                    events:    [{ kind: "handoff_requested", reason: "transfer", note: args.reason }],
                },
            );
        },
    });
}

function endCall(): Tool {
    return Tool.define({
        name:        "end_call",
        description: "Hang up after this reply. Only when the conversation is over and you've said goodbye.",
        followUp:    false,
        parameters:  z.object({}),
        run: () => ToolResult.output({ ending: true }, { directive: { kind: "end" } }),
    });
}

// --- Selection ---------------------------------------------------------------------

export function toolsFor(ctx: TurnContext): Tool[] {
    const voice = ctx.inbound.channel === "voice";
    const links = linkOptions(ctx);
    return [
        saveCustomerDetails(ctx),
        bookAppointment(ctx),
        ...(links.length > 0 ? [sendLink(ctx, links)] : []),
        ...(ctx.catalog.length > FULL_CATALOG_LIMIT ? [searchCatalog(ctx)] : []),
        requestCallback(),
        switchLanguage(),
        flagConversation(),
        ...(voice ? [transferCall(ctx), endCall()] : []),
    ];
}

export function definitions(tools: readonly Tool[]): ToolDefinition[] {
    return tools.map(t => {
        const { $schema: _, ...parameters } = z.toJSONSchema(t.parameters) as Record<string, unknown>;
        return { name: t.name, description: t.description, parameters };
    });
}
