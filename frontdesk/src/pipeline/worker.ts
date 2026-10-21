/**
 * Runs queued jobs: texts, emails, staff notices, follow-ups, voicemail transcription,
 * classification and transcript delivery. Also sweeps idle sessions closed.
 *
 * Failed jobs retry with backoff; a job that keeps failing is given up after a few attempts
 * and left in the table with its last error.
 */

import { z } from "zod";

import {
    Address,
    Contact,
    LANGUAGE_NAMES,
    LocalTime,
    PhoneNumber,
    Sentiment,
    SessionOutcome,
    Tenant,
    Urgency,
    type Classification,
    type Conversation,
    type JobOf,
    type Language,
    type Purpose,
    type SessionId,
    type Transcript,
} from "../core/index.ts";
import { DomainPack, type FollowupKind } from "../domains/index.ts";
import {
    LlmProvider,
    type BatchTranscriber,
    type EmailProvider,
    type MessagingProvider,
} from "../providers/index.ts";
import type { ClaimedJob } from "../storage/index.ts";
import { closeSession, sweepIdleSessions } from "./sessions.ts";
import type { Services } from "./services.ts";

/**
 * `emailFrom` is the sender for all email; `defaultSmsFrom` is used when a tenant has no number of its own.
 * `dashboardUrl` (optional) is linked from transcript emails.
 */
export type WorkerDeps = {
    readonly services:       Services;
    readonly messaging:      MessagingProvider;
    readonly email:          EmailProvider;
    readonly transcriber:    BatchTranscriber | null;
    readonly fetchRecording: ((url: string) => Promise<Uint8Array>) | null;
    readonly emailFrom:      string;
    readonly defaultSmsFrom: string;
    readonly dashboardUrl:   string;
};

const MAX_ATTEMPTS = 5;
const SMS_LIMIT = 600;

const ClassificationOutput = z.object({
    outcome:         z.enum(Object.values(SessionOutcome) as [SessionOutcome, ...SessionOutcome[]]),
    topic:           z.string(),
    sentiment:       z.enum(Object.values(Sentiment) as [Sentiment, ...Sentiment[]]),
    urgency:         z.enum(Object.values(Urgency) as [Urgency, ...Urgency[]]),
    language:        z.string(),
    followup_needed: z.boolean(),
    gist:            z.string(),
});

export class Worker {
    readonly #d: WorkerDeps;
    #timer: NodeJS.Timeout | null = null;
    #lastSweep = 0;
    #running = false;

    constructor(deps: WorkerDeps) {
        this.#d = deps;
    }

    start(intervalMs = 1000): void {
        if (this.#timer !== null) {
            return;
        }
        this.#timer = setInterval(() => {
            if (!this.#running) {
                this.#running = true;
                this.tick().catch(e => console.error("worker tick failed:", e)).finally(() => { this.#running = false; });
            }
        }, intervalMs);
    }

    stop(): void {
        if (this.#timer !== null) {
            clearInterval(this.#timer);
            this.#timer = null;
        }
    }

    /**
     * Run every job that is due (and the idle sweep, once a minute). Returns how many jobs ran.
     */
    async tick(): Promise<number> {
        const { store, clock } = this.#d.services;
        const now = clock();
        if (now.getTime() - this.#lastSweep >= 60_000) {
            this.#lastSweep = now.getTime();
            await sweepIdleSessions(this.#d.services);
            await this.#scheduleFixedCosts(now);
        }
        let ran = 0;
        for (;;) {
            const jobs = await store.jobs.claim(10, 120_000, clock());
            if (jobs.length === 0) {
                return ran;
            }
            for (const job of jobs) {
                ran++;
                try {
                    await this.run(job);
                    await store.jobs.complete(job.id);
                } catch (e) {
                    const message = e instanceof Error ? e.message : String(e);
                    const retryAt = job.attempts >= MAX_ATTEMPTS
                        ? null
                        : new Date(clock().getTime() + 30_000 * 2 ** (job.attempts - 1));
                    console.error(`job ${job.kind} (${job.dedupeKey}) failed, attempt ${job.attempts}: ${message}`);
                    await store.jobs.fail(job.id, message, retryAt);
                }
            }
        }
    }

    async run(job: ClaimedJob): Promise<void> {
        const tenant = await this.#d.services.store.tenants.byId(job.tenantId);
        if (tenant === null) {
            return;
        }
        switch (job.kind) {
            case "send_sms":             return this.#sendSms(tenant, job);
            case "send_email":           return this.#sendEmail(tenant, job);
            case "notify_staff":         return this.#notifyStaff(tenant, job);
            case "send_followup":        return this.#sendFollowup(tenant, job);
            case "cold_followup":        return this.#coldFollowup(tenant, job);
            case "transcribe_voicemail": return this.#transcribeVoicemail(tenant, job);
            case "classify_session":     return this.#classify(tenant, job);
            case "deliver_transcript":   return this.#deliverTranscript(tenant, job);
            case "record_fixed_costs":   return this.#recordFixedCosts(tenant, job);
        }
    }

    // --- fixed costs ------------------------------------------------------------------

    /**
     * Once a month per tenant, charge the costs that don't come from usage (their phone numbers)
     * against their allowance. The dedupe key makes this run once per tenant per month.
     */
    async #scheduleFixedCosts(now: Date): Promise<void> {
        const { store } = this.#d.services;
        const tenants = await store.tenants.all();
        await store.jobs.enqueue(tenants.map(t => {
            const local = LocalTime.of(now, t.timezone);
            const month = `${local.year}-${String(local.month).padStart(2, "0")}`;
            return { kind: "record_fixed_costs", month, tenantId: t.id, dedupeKey: `fixed:${t.id}:${month}`, runAt: null };
        }));
    }

    async #recordFixedCosts(tenant: Tenant, _job: JobOf<"record_fixed_costs">): Promise<void> {
        const numbers = new Set(
            Object.values(tenant.endpoints).flatMap(list => (list ?? []).map(a => a.value))
        );
        if (numbers.size === 0) {
            return;
        }
        await this.#d.services.store.usage.record({
            provider:   "twilio",
            model:      "",
            service:    "telephony",
            purpose:    "turn",
            occurredAt: this.#d.services.clock(),
            units:      { number_months: numbers.size },
            costUsd:    null,
            latencyMs:  null,
            tenantId:   tenant.id,
            channel:    null,
        });
    }

    // --- sending ----------------------------------------------------------------------

    #from(tenant: Tenant): PhoneNumber {
        const endpoint = Tenant.endpoint(tenant, "sms") ?? Tenant.endpoint(tenant, "voice");
        return endpoint?.kind === "phone"
            ? endpoint.value
            : this.#d.defaultSmsFrom as PhoneNumber;
    }

    /**
     * Text a customer, unless they opted out. Returns whether it was sent.
     */
    async #textCustomer(tenant: Tenant, to: PhoneNumber, text: string, purpose: Purpose): Promise<boolean> {
        const { store } = this.#d.services;
        const contact = await store.conversations.contactByAddress(tenant.id, { kind: "phone", value: to });
        if (contact?.optedOut) {
            return false;
        }
        await this.#text(tenant, to, text, purpose);
        return true;
    }

    async #text(tenant: Tenant, to: PhoneNumber, text: string, purpose: Purpose): Promise<void> {
        const receipt = await this.#d.messaging.send({ to, from: this.#from(tenant), text, purpose, tenantId: tenant.id });
        await this.#d.services.store.usage.record({ ...receipt.usage, tenantId: tenant.id });
    }

    async #email(tenant: Tenant, to: readonly string[], subject: string, text: string, purpose: Purpose): Promise<void> {
        if (to.length === 0) {
            return;
        }
        const receipt = await this.#d.email.send({
            to,
            from:     this.#d.emailFrom,
            replyTo:  null,
            subject,
            text,
            html:     null,
            purpose,
            tenantId: tenant.id,
        });
        await this.#d.services.store.usage.record({ ...receipt.usage, tenantId: tenant.id });
    }

    async #sendSms(tenant: Tenant, job: JobOf<"send_sms">): Promise<void> {
        await this.#textCustomer(tenant, job.to, job.text, job.purpose);
    }

    async #sendEmail(tenant: Tenant, job: JobOf<"send_email">): Promise<void> {
        await this.#email(tenant, [job.to], job.subject, job.text, job.purpose);
    }

    /**
     * Tell staff, the ways they chose (email by default; SMS only with the paid add-on).
     */
    async #staff(tenant: Tenant, notice: Parameters<typeof Tenant.notifyVia>[1], subject: string, text: string, smsText: string): Promise<void> {
        const vias = Tenant.notifyVia(tenant, notice);
        const emails = tenant.staffContacts.flatMap(a => a.kind === "email" ? [a.value] : []);
        const phones = Tenant.staffPhones(tenant);
        if (vias.includes("email")) {
            if (emails.length === 0) {
                console.warn(`${tenant.id}: no staff email for ${notice} notice "${subject}"`);
            }
            await this.#email(tenant, emails, subject, text, "staff_alert");
        }
        if (vias.includes("sms")) {
            for (const phone of phones) {
                await this.#text(tenant, phone, smsText.slice(0, SMS_LIMIT), "staff_alert");
            }
        }
    }

    async #notifyStaff(tenant: Tenant, job: JobOf<"notify_staff">): Promise<void> {
        await this.#staff(tenant, job.notice, job.subject, job.text, job.text);
    }

    // --- follow-ups -------------------------------------------------------------------

    /**
     * Record a message we sent outside a turn, so the model sees it in later turns.
     */
    async #recordOutbound(conversation: Conversation, text: string, dedupeKey: string): Promise<void> {
        const { store, clock, smsIdleMs } = this.#d.services;
        const contact = await store.conversations.contact(conversation.contactId);
        const session = await store.conversations.session(
            conversation,
            { channel: "sms" },
            { language: contact?.preferredLanguage ?? "en", smsIdleMs },
            clock(),
        );
        await store.turns.commit({
            dedupeKey,
            reply:          text,
            contactId:      conversation.contactId,
            contactChanges: {},
            session,
            messages:       [{ conversationId: conversation.id, sessionId: session.id, role: "assistant", text, channel: "sms" }],
            events:         [],
            jobs:           [],
            at:             clock(),
        });
    }

    async #followupText(tenant: Tenant, kind: FollowupKind, contact: Contact, extra: Record<string, string>): Promise<string | null> {
        const { composer, registry } = this.#d.services;
        return composer.followup(tenant, registry.get(tenant.domain), kind, contact.preferredLanguage ?? tenant.defaultLanguage, contact, extra);
    }

    async #sendFollowup(tenant: Tenant, job: JobOf<"send_followup">): Promise<void> {
        const { store } = this.#d.services;
        const appointment = await store.appointments.byId(job.appointmentId);
        const notice = job.followup === "confirm"
            ? "confirmation"
            : "reminder";
        if (appointment === null || appointment.status !== "confirmed" || appointment.sentNotices.includes(notice)) {
            return;
        }
        const contact = await store.conversations.contact(appointment.contactId);
        const conversation = await store.conversations.conversation(appointment.conversationId);
        if (contact?.phone == null || conversation === null) {
            return;
        }
        const text = await this.#followupText(tenant, job.followup, contact, {
            visit_time: appointment.timeText,
            subject:    appointment.subject,
        });
        if (text !== null && await this.#textCustomer(tenant, contact.phone, text, job.followup === "reminder" ? "reminder" : "booking_confirmation")) {
            await this.#recordOutbound(conversation, text, `job:${job.dedupeKey}`);
        }
        await store.appointments.markNotice(appointment.id, notice);
    }

    async #coldFollowup(tenant: Tenant, job: JobOf<"cold_followup">): Promise<void> {
        const { store } = this.#d.services;
        const conversation = await store.conversations.conversation(job.conversationId);
        const last = await store.conversations.lastMessage(job.conversationId);
        // Only if our reply is still the last word: the customer never answered and nothing else was sent.
        if (conversation === null || last === null || last.role !== "assistant" || last.createdAt.getTime() > Date.parse(job.afterMessageAt) + 5_000) {
            return;
        }
        if (await store.appointments.latest(job.conversationId) !== null) {
            return;
        }
        const contact = await store.conversations.contact(conversation.contactId);
        if (contact?.phone == null) {
            return;
        }
        const text = await this.#followupText(tenant, "cold", contact, {});
        if (text !== null && await this.#textCustomer(tenant, contact.phone, text, "followup")) {
            await this.#recordOutbound(conversation, text, `job:${job.dedupeKey}`);
        }
    }

    // --- voicemail --------------------------------------------------------------------

    async #transcribeVoicemail(tenant: Tenant, job: JobOf<"transcribe_voicemail">): Promise<void> {
        const { store } = this.#d.services;
        const session = await store.conversations.sessionById(job.sessionId);
        if (session === null) {
            return;
        }
        let text = "(voicemail - transcription unavailable; listen to the recording)";
        let language: Language = session.language;
        if (this.#d.transcriber !== null && this.#d.fetchRecording !== null) {
            const audio = await this.#d.fetchRecording(job.recordingUrl);
            const result = await this.#d.transcriber.transcribe(audio, "audio/mpeg", {
                tenantId:  tenant.id,
                purpose:   "voicemail",
                languages: [],
                hints:     this.#d.services.registry.get(tenant.domain).voice.speechHints,
            });
            await store.usage.record({ ...result.usage, tenantId: tenant.id });
            text = result.text || "(no words in the voicemail)";
            language = result.language ?? language;
        }
        await store.turns.commit({
            dedupeKey:      `voicemail:${session.id}`,
            reply:          "",
            contactId:      (await store.conversations.conversation(session.conversationId))!.contactId,
            contactChanges: {},
            session:        { ...session, language },
            messages:       [{ conversationId: session.conversationId, sessionId: session.id, role: "user", text, channel: "voice" }],
            events:         [],
            jobs:           [],
            at:             this.#d.services.clock(),
        });
        await closeSession(this.#d.services, session.id, "completed");
    }

    // --- classification and delivery --------------------------------------------------

    async #classify(tenant: Tenant, job: JobOf<"classify_session">): Promise<void> {
        const { store, composer, registry, llm, clock } = this.#d.services;
        const transcript = await store.transcripts.transcript(job.sessionId);
        if (transcript === null) {
            return;
        }
        const pack = registry.get(tenant.domain);
        const said = transcript.messages.filter(m => m.role === "user" && m.text.trim());
        let classification: Classification;
        if (said.length === 0) {
            classification = {
                sessionId:      job.sessionId,
                outcome:        "abandoned",
                topic:          "other",
                sentiment:      "neutral",
                urgency:        "low",
                language:       transcript.session.language,
                followupNeeded: false,
                gist:           "The customer left without saying anything.",
                model:          "rule",
                classifiedAt:   clock(),
            };
        } else {
            const response = await LlmProvider.complete(llm, {
                messages:    composer.classify(tenant, pack, transcriptText(transcript, pack)),
                tools:       [],
                format:      { kind: "json_schema", name: "classification", schema: classificationSchema(pack) },
                tier:        "small",
                maxTokens:   300,
                temperature: 0,
                timeoutMs:   30_000,
                purpose:     "classification",
                tenantId:    tenant.id,
                channel:     transcript.session.channel,
            });
            const parsed = ClassificationOutput.safeParse(JSON.parse(response.text || "{}"));
            if (!parsed.success) {
                throw new Error(`classification did not match the schema: ${parsed.error.message}`);
            }
            const out = parsed.data;
            const topics = new Set(DomainPack.topics(pack).map(t => t.id));
            classification = {
                sessionId:      job.sessionId,
                outcome:        out.outcome,
                topic:          topics.has(out.topic) ? out.topic : "other",
                sentiment:      out.sentiment,
                urgency:        out.urgency,
                language:       (Object.keys(LANGUAGE_NAMES) as Language[]).find(l => l === out.language.slice(0, 2).toLowerCase()) ?? transcript.session.language,
                followupNeeded: out.followup_needed,
                gist:           out.gist.trim(),
                model:          `${response.usage.provider}:${response.usage.model}`,
                classifiedAt:   clock(),
            };
        }
        await store.transcripts.saveClassification(classification);
        await store.jobs.enqueue([{
            kind:      "deliver_transcript",
            sessionId: job.sessionId,
            tenantId:  tenant.id,
            dedupeKey: `deliver:${job.sessionId}`,
            runAt:     null,
        }]);
    }

    async #deliverTranscript(tenant: Tenant, job: JobOf<"deliver_transcript">): Promise<void> {
        const { store, registry } = this.#d.services;
        const transcript = await store.transcripts.transcript(job.sessionId);
        const c = transcript?.classification;
        if (transcript === null || c === null || c === undefined) {
            return;
        }
        const voicemail = await store.voicemails.bySession(job.sessionId);
        if ((c.outcome === "abandoned" || c.outcome === "spam") && voicemail === null) {
            return;
        }
        const pack = registry.get(tenant.domain);
        const conversation = await store.conversations.conversation(transcript.session.conversationId);
        const contact = conversation === null ? null : await store.conversations.contact(conversation.contactId);
        const who = describeContact(contact);
        const topic = DomainPack.topics(pack).find(t => t.id === c.topic)?.label ?? c.topic;
        const kind = voicemail !== null
            ? "Voicemail"
            : { web: "Chat", sms: "Text", voice: "Call" }[transcript.session.channel];
        const started = LocalTime.of(transcript.session.startedAt, tenant.timezone);
        const urgent = c.urgency === "urgent" ? "URGENT - " : "";
        const subject = `${urgent}${kind} - ${topic} - ${who}: ${c.gist.slice(0, 80)}`;
        const body = [
            c.gist,
            "",
            `Customer: ${who}`,
            `When: ${LocalTime.long(started)}`,
            `Outcome: ${c.outcome}${c.followupNeeded ? " (follow up)" : ""} | Topic: ${topic} | Sentiment: ${c.sentiment} | Language: ${LANGUAGE_NAMES[c.language]}`,
            ...(voicemail !== null ? [`Recording: ${voicemail.recordingUrl}`] : []),
            ...(this.#d.dashboardUrl ? [`Dashboard: ${this.#d.dashboardUrl}/transcripts/${transcript.session.id}`] : []),
            "",
            "--- Transcript ---",
            transcriptText(transcript, pack),
        ].join("\n");
        await this.#staff(tenant, "transcript", subject, body, `[${tenant.name}] ${urgent}${kind}: ${who}. ${c.gist}`);
    }
}

function describeContact(contact: Contact | null): string {
    if (contact === null) {
        return "Unknown";
    }
    const name = Contact.displayName(contact);
    const reach = contact.phone !== null
        ? PhoneNumber.display(contact.phone)
        : contact.email || contact.addresses.map(a => a.kind === "web_session" ? "web visitor" : Address.key(a))[0] || "";
    return [name, reach].filter(Boolean).join(" - ") || "Unknown";
}

function transcriptText(transcript: Transcript, pack: DomainPack): string {
    return transcript.messages
        .map(m => `${m.role === "user" ? "Customer" : pack.defaultAgentName}: ${m.text}`)
        .join("\n");
}

function classificationSchema(pack: DomainPack): Record<string, unknown> {
    return {
        type:                 "object",
        additionalProperties: false,
        required:             ["outcome", "topic", "sentiment", "urgency", "language", "followup_needed", "gist"],
        properties:           {
            outcome:         { type: "string", enum: Object.values(SessionOutcome) },
            topic:           { type: "string", enum: DomainPack.topics(pack).map(t => t.id) },
            sentiment:       { type: "string", enum: Object.values(Sentiment) },
            urgency:         { type: "string", enum: Object.values(Urgency) },
            language:        { type: "string", enum: Object.keys(LANGUAGE_NAMES) },
            followup_needed: { type: "boolean" },
            gist:            { type: "string" },
        },
    };
}

