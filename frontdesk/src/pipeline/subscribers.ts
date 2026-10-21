/**
 * What happens after each kind of event, as jobs. Pure functions of the event and its context;
 * the jobs run later in the worker, so replies never wait on texts, emails or summaries.
 */

import { Contact, PhoneNumber, type Job, type JobBody } from "../core/index.ts";
import { Subscriber, type SubscriberContext } from "../turn/index.ts";

/**
 * How long before a booked visit the reminder text goes out.
 */
export const REMINDER_LEAD_MS = 60 * 60_000;

function job(ctx: SubscriberContext, dedupeKey: string, body: JobBody, runAt: Date | null = null): Job {
    return { ...body, tenantId: ctx.tenant.id, dedupeKey, runAt } as Job;
}

function who(ctx: SubscriberContext): string {
    const c = ctx.contact;
    if (c === null) {
        return "(unknown)";
    }
    const name = Contact.displayName(c) || "(name not given)";
    const phone = c.phone !== null
        ? PhoneNumber.display(c.phone)
        : c.email || "(no phone)";
    return `${name} - ${phone}`;
}

export const bookingNotices = Subscriber.define({
    name:   "booking_notices",
    events: ["appointment_booked"],
    react(event, ctx) {
        const a = event.appointment;
        const b = ctx.pack.booking;
        const jobs: Job[] = [];
        // The SMS reply itself confirms on SMS; other channels get a text confirmation.
        if (ctx.contact?.phone && ctx.channel !== "sms") {
            jobs.push(job(ctx, `confirm:${a.id}`, {
                kind:          "send_followup",
                followup:      "confirm",
                appointmentId: a.id,
            }));
        }
        const lines = [
            `[${ctx.tenant.name} AI] ${b.visitNoun.charAt(0).toUpperCase() + b.visitNoun.slice(1)} ${event.replaces !== null ? "rescheduled" : "booked"}`,
            `Customer: ${who(ctx)}`,
            ...(ctx.contact?.email ? [`Email: ${ctx.contact.email}`] : []),
            `Time: ${a.timeText}`,
            `${b.subjectLabel}: ${a.subject}`,
            ...(a.location ? [`Location: ${a.location}`] : []),
            ...b.intake.flatMap(f => a.details[f.key] ? [`${f.label}: ${a.details[f.key]}`] : []),
            `Channel: ${ctx.channel ?? "unknown"}`,
        ];
        jobs.push(job(ctx, `staff-booking:${a.id}`, {
            kind:    "notify_staff",
            notice:  "booking",
            subject: `${b.visitNoun} ${event.replaces !== null ? "rescheduled" : "booked"}: ${a.timeText} - ${a.subject}`,
            text:    lines.join("\n"),
        }));
        if (a.startsAt !== null && a.startsAt.getTime() - REMINDER_LEAD_MS > ctx.now.getTime()) {
            jobs.push(job(ctx, `reminder:${a.id}`, {
                kind:          "send_followup",
                followup:      "reminder",
                appointmentId: a.id,
            }, new Date(a.startsAt.getTime() - REMINDER_LEAD_MS)));
        }
        return jobs;
    },
});

export const handoffAlerts = Subscriber.define({
    name:   "handoff_alerts",
    events: ["handoff_requested"],
    react(event, ctx) {
        const headline = event.reason === "transfer"
            ? "Call transferred"
            : event.reason === "missed_call"
                ? "Missed call"
                : "Callback requested";
        return [job(ctx, `handoff:${event.id}`, {
            kind:    "notify_staff",
            notice:  "handoff",
            subject: `${headline}: ${who(ctx)}`,
            text:    `[${ctx.tenant.name} AI] ${headline}\nCustomer: ${who(ctx)}\n\n${event.note}`,
        })];
    },
});

export const linkTexts = Subscriber.define({
    name:   "link_texts",
    events: ["link_requested"],
    react(event, ctx) {
        if (event.to.kind !== "phone") {
            return [];
        }
        return [job(ctx, `link:${event.id}`, {
            kind:    "send_sms",
            to:      event.to.value,
            text:    `Here's the ${event.label} from ${ctx.tenant.name}: ${event.url}`,
            purpose: "link",
        })];
    },
});

export const classifyOnClose = Subscriber.define({
    name:   "classify_on_close",
    events: ["session_closed"],
    react(event, ctx) {
        return event.sessionId === null
            ? []
            : [job(ctx, `classify:${event.sessionId}`, { kind: "classify_session", sessionId: event.sessionId })];
    },
});

export const voicemailTranscription = Subscriber.define({
    name:   "voicemail_transcription",
    events: ["voicemail_left"],
    react(event, ctx) {
        return event.sessionId === null
            ? []
            : [job(ctx, `transcribe:${event.sessionId}`, {
                kind:         "transcribe_voicemail",
                sessionId:    event.sessionId,
                recordingUrl: event.recordingUrl,
            })];
    },
});

export const capAlerts = Subscriber.define({
    name:   "cap_alerts",
    events: ["tenant_cap"],
    react(event, ctx) {
        const month = `${ctx.now.getUTCFullYear()}-${ctx.now.getUTCMonth() + 1}`;
        const text = event.status === "warning"
            ? "Your assistant has used 80% of this month's allowance. Add a top-up to keep it answering for the rest of the month."
            : "Your assistant has used this month's allowance. Callers can still leave voicemails; add a top-up to turn it back on.";
        return [job(ctx, `cap:${event.status}:${month}`, {
            kind:    "notify_staff",
            notice:  "budget_alert",
            subject: `${ctx.tenant.name}: assistant allowance ${event.status === "warning" ? "80% used" : "used up"}`,
            text,
        })];
    },
});

export const blockAlerts = Subscriber.define({
    name:   "block_alerts",
    events: ["contact_blocked"],
    react(event, ctx) {
        return [job(ctx, `block:${event.id}`, {
            kind:    "notify_staff",
            notice:  "handoff",
            subject: `Caller blocked: ${who(ctx)}`,
            text:    `[${ctx.tenant.name} AI] ${who(ctx)} was blocked until ${event.block.until?.toISOString() ?? "further notice"} (${event.block.reason}). You can lift the block from the dashboard.`,
        })];
    },
});

export const DEFAULT_SUBSCRIBERS: readonly Subscriber[] = [
    bookingNotices,
    handoffAlerts,
    linkTexts,
    classifyOnClose,
    voicemailTranscription,
    capAlerts,
    blockAlerts,
] as readonly Subscriber[];
