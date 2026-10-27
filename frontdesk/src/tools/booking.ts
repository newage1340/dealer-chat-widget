/**
 * Booking and rescheduling visits, checked against the business's hours.
 */

import { randomUUID } from "node:crypto";

import { z } from "zod";

import {
    Address,
    AppointmentId,
    LocalTime,
    WEEKDAY_NAMES,
    type Appointment,
    type ContactChanges,
    type Mutable,
} from "../core/index.ts";
import { DomainPack } from "../domains/index.ts";
import { Hours } from "../prompts/hours.ts";
import { Tool, ToolResult, type TurnContext } from "../turn/index.ts";
import { firstWord } from "./contact.ts";

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

export function bookAppointment(ctx: TurnContext): Tool {
    const pack = ctx.pack;
    const voice = ctx.inbound.channel === "voice";
    const intake = DomainPack.intakeFor(pack, voice ? "voice" : "chat");
    const customerSite = pack.booking.location === "customer";
    const details: z.ZodType<Record<string, string | undefined>> = z.object(Object.fromEntries(intake.map(f => [
        f.key,
        f.required
            ? z.string().min(1).describe(f.label)
            : z.string().optional().describe(f.label),
    ])));
    // `location` is only asked for when staff go to the customer.
    const parameters = z.object({
        time_text:      z.string().describe("The time as you'd say it to the customer, with the day, e.g. 'Saturday at 2 PM'"),
        time_iso:       z.string().describe("The same time as local YYYY-MM-DDTHH:MM. Copy the date from WHICH DAYS WE ARE ACTUALLY OPEN; never calculate it"),
        subject:        z.string().describe(`${pack.booking.subjectHint}, or '${pack.booking.generalSubject}'`),
        location:       z.string().min(1).describe(`Street address and city for the ${pack.booking.visitNoun}`).optional(),
        details,
        customer_name:  z.string().optional().describe("First name, if known"),
        customer_email: z.string().optional(),
        reschedule:     z.boolean().default(false).describe("True when moving an existing confirmed appointment"),
    });

    return Tool.define({
        name:        "book_appointment",
        promise:     /\b(you'?re (all set|booked)|(is|are) (now )?confirmed|booked you)\b/i,
        description: `Book (or reschedule) the customer's ${pack.booking.visitNoun}. Call it before confirming anything; the result says whether it worked.`,
        followUp:    true,
        parameters:  customerSite
            ? parameters.required({ location: true })
            : parameters.omit({ location: true }),
        run: (args: z.output<typeof parameters>) => {
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
            const changes: Mutable<ContactChanges> = {};
            if (args.customer_name?.trim() && !ctx.contact.firstName) {
                changes.firstName = firstWord(args.customer_name);
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
