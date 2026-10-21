/**
 * Twilio voice: admission and fallbacks at the start of a call, then a speech <Gather> loop
 * driven by the same turn engine as chat and SMS.
 *
 * Configure the Twilio number with:
 *   A call comes in        -> POST /voice
 *   Call status changes    -> POST /voice/status   (closes the session, meters the call)
 */

import { Hono, type Context } from "hono";

import {
    Address,
    Billing,
    Budget,
    CallState,
    Coverage,
    LocalTime,
    MAX_VOICEMAIL_SECONDS,
    PhoneNumber,
    Tenant,
    TenantId,
    type Contact,
    type Conversation,
    type EventBody,
    type Inbound,
    type CallSettings,
    type Language,
    type Session,
} from "../core/index.ts";
import type { DomainPack } from "../domains/index.ts";
import { collect } from "../pipeline/engine.ts";
import { closeSession } from "../pipeline/sessions.ts";
import { stamp, type Services } from "../pipeline/services.ts";
import { Speech } from "../prompts/speech.ts";
import { STRINGS, say } from "../prompts/strings.ts";
import { Subscriber } from "../turn/index.ts";
import { absolute, form, twilioSignature, xml, type ChannelDeps } from "./context.ts";
import { PENDING_DIRECTIVES } from "../voice/stream.ts";
import { streamUrl } from "./stream.ts";
import { VoiceResponse } from "./twiml.ts";

const LOW_CONFIDENCE = 0.45;

/**
 * A greeting in the caller's language that asks for their name up front,
 * so even early hang-ups reach the team with a name. Includes the recording disclosure when it is on.
 */
export function greeting(tenant: Tenant, pack: DomainPack, language: Language, now: Date): string {
    const local = LocalTime.of(now, tenant.timezone);
    const part = local.hour < 12
        ? STRINGS.partOfDay.morning
        : local.hour < 17
            ? STRINGS.partOfDay.afternoon
            : STRINGS.partOfDay.evening;
    const options = STRINGS.greetings;
    const text = say(options[Math.floor(Math.random() * options.length)]!, language, {
        part_of_day:   say(part, language),
        business_name: tenant.name,
        agent_name:    tenant.voiceAgentName || pack.defaultVoiceAgentName,
    });
    const disclosure = tenant.settings.recordingDisclosure;
    return disclosure.enabled
        ? `${say(disclosure.text, language)} ${text}`
        : text;
}

/**
 * Publish events about a call (and the jobs they cause) outside a turn.
 */
async function publish(services: Services, tenant: Tenant, contact: Contact, conversation: Conversation, session: Session | null, bodies: readonly EventBody[]): Promise<void> {
    const now = services.clock();
    const events = stamp(bodies, {
        tenantId:       tenant.id,
        conversationId: conversation.id,
        sessionId:      session?.id ?? null,
        occurredAt:     now,
        causedBy:       null,
    });
    const jobs = Subscriber.jobsFor(services.subscribers, events, {
        tenant,
        pack:    services.registry.get(tenant.domain),
        contact,
        channel: "voice",
        now,
    });
    await services.store.outbox.publish(events, jobs);
}

/**
 * Record the assistant's opening line as the first message of the call.
 */
async function recordGreeting(services: Services, contact: Contact, conversation: Conversation, session: Session, text: string): Promise<void> {
    await services.store.turns.commit({
        dedupeKey:      `greeting:${session.id}`,
        reply:          text,
        contactId:      contact.id,
        contactChanges: {},
        session,
        messages:       [{ conversationId: conversation.id, sessionId: session.id, role: "assistant", text, channel: "voice" }],
        events:         [],
        jobs:           [],
        at:             services.clock(),
    });
}

/**
 * What to do with a call the assistant answers or not: shared by Twilio and Vapi.
 */
export async function startCall(services: Services, tenant: Tenant, caller: Address, callId: string, forwardedFrom: PhoneNumber | null, vendor: "twilio" | "vapi") {
    const { store } = services;
    const now = services.clock();
    const { contact, conversation } = await store.conversations.resolve(tenant.id, caller, now);
    const admission = Budget.admit(contact.block, {
        channel:       "voice",
        plan:          tenant.plan,
        schedule:      tenant.schedule,
        timeZone:      tenant.timezone,
        textAlwaysOn:  tenant.settings.textAlwaysOn,
        costThisMonth: await store.spend.monthToDate(tenant, now),
        activeCalls:   await store.conversations.activeCalls(tenant.id, null),
        billingActive: Billing.serves(tenant.billing),
        now,
    });
    const language = Tenant.startLanguage(tenant, contact.preferredLanguage);
    // Robocalls usually hide their caller ID; the tenant decides whether those reach the assistant.
    if (caller.kind !== "phone" && tenant.settings.calls.hiddenCallerId !== "assistant") {
        if (tenant.settings.calls.hiddenCallerId === "reject") {
            return { kind: "reject" as const, contact };
        }
        const session = await store.conversations.session(conversation, { channel: "voice", externalId: callId, vendor, handling: "voicemail" }, { language, smsIdleMs: services.smsIdleMs }, now);
        return { kind: "voicemail" as const, session, contact };
    }
    const defaults = { language, smsIdleMs: services.smsIdleMs };
    if (admission.kind === "admit") {
        const session = await store.conversations.session(conversation, { channel: "voice", externalId: callId, vendor, handling: "assistant" }, defaults, now);
        const text = greeting(tenant, services.registry.get(tenant.domain), session.language, now);
        await recordGreeting(services, contact, conversation, session, text);
        await publish(services, tenant, contact, conversation, session, [{ kind: "call_started", from: caller }]);
        return { kind: "assistant" as const, session, text, contact };
    }
    const forwardTo = tenant.settings.transferTo ?? tenant.phone;
    const fallback = Coverage.fallbackFor(tenant.settings.callFallbacks, admission.reason, { forwardTo, forwardedFrom });
    if (fallback === "reject") {
        return { kind: "reject" as const, contact };
    }
    const handling = fallback === "forward" ? "forwarded" : "voicemail";
    const session = await store.conversations.session(conversation, { channel: "voice", externalId: callId, vendor, handling }, defaults, now);
    if (fallback === "forward" && forwardTo !== null) {
        await publish(services, tenant, contact, conversation, session, [{ kind: "call_forwarded", to: forwardTo, reason: admission.reason }]);
        return { kind: "forward" as const, session, to: forwardTo, contact };
    }
    return { kind: "voicemail" as const, session, contact };
}

/**
 * What Twilio puts in `From` when the caller hid their number (anonymous, restricted, unavailable, blocked).
 */
const HIDDEN_CALLER_IDS: ReadonlySet<string> = new Set([
    "", "anonymous", "restricted", "unknown", "unavailable", "private",
    "+266696687", "+7378742833", "+8656696", "+2562533",
]);

export function voiceRoutes(deps: ChannelDeps): Hono {
    const app = new Hono();
    const { services, engine, config } = deps;
    const { store, registry } = services;
    const signed = twilioSignature(config);

    const reply = (c: Context, vr: VoiceResponse) => xml(c, vr.toString());
    const turnUrl = (callId: string) => absolute(config, `/voice/turn?call=${encodeURIComponent(callId)}`);
    /**
     * Say `text` and listen; on silence ask "still there?" as many times as the tenant allows, then hang up.
     * Every <Gather> is billed, so the tenant's call settings decide how much dead air we pay for.
     */
    const listen = (vr: VoiceResponse, text: string, callId: string, language: Language, pack: DomainPack, calls: CallSettings) => {
        const hints = pack.voice.speechHints;
        vr.gather({ action: turnUrl(callId), language, prompt: text, hints, timeout: calls.silenceSeconds });
        for (let i = 0; i < calls.stillTherePrompts; i++) {
            vr.gather({ action: turnUrl(callId), language, prompt: say(STRINGS.stillThere, language), hints, timeout: calls.silenceSeconds + 2 });
        }
        vr.say(say(STRINGS.silenceGoodbye, language), language).hangup();
        return vr;
    };

    const lookup = async (f: Record<string, string>) => {
        const called = Address.phone(f["To"] ?? "");
        return called === null
            ? null
            : await store.tenants.byEndpoint("voice", called);
    };
    const callerOf = (f: Record<string, string>, callId: string): Address => {
        const from = (f["From"] ?? "").trim();
        return HIDDEN_CALLER_IDS.has(from.toLowerCase())
            ? Address.webSession(`call-${callId}`)
            : Address.phone(from) ?? Address.webSession(`call-${callId}`);
    };

    app.post("/voice", signed, async c => {
        const f = await form(c);
        const callId = f["CallSid"] ?? "";
        const tenant = await lookup(f);
        if (tenant === null || !callId) {
            return reply(c, new VoiceResponse().say(say(STRINGS.unconfigured, "en"), "en").hangup());
        }
        const pack = registry.get(tenant.domain);
        const forwardedFrom = PhoneNumber.parse(f["ForwardedFrom"] ?? "");
        const start = await startCall(services, tenant, callerOf(f, callId), callId, forwardedFrom, "twilio");
        switch (start.kind) {
            case "reject":
                return reply(c, new VoiceResponse().reject());
            case "forward":
                return reply(c, new VoiceResponse().dial(start.to, { timeout: 25 }));
            case "voicemail": {
                const language = start.session.language;
                return reply(c, new VoiceResponse()
                    .say(say(STRINGS.voicemailGreeting, language, { business_name: tenant.name }), language)
                    .record({
                        action:         absolute(config, "/voice/recorded"),
                        statusCallback: absolute(config, `/voice/recording?call=${encodeURIComponent(callId)}&tenant=${encodeURIComponent(tenant.id)}`),
                        maxLength:      MAX_VOICEMAIL_SECONDS,
                    })
                    .hangup());
            }
            case "assistant":
                if (deps.voice !== undefined && deps.voice.kind !== "twilio_gather") {
                    // Our own pipeline: Twilio streams the audio to /voice/stream, which greets the caller;
                    // when we close the stream, Twilio follows the redirect to learn what comes next.
                    return reply(c, new VoiceResponse()
                        .stream(streamUrl(config.publicBaseUrl, c.req.header("host") ?? ""), {
                            call:          callId,
                            tenant:        tenant.id,
                            from:          f["From"] ?? "",
                            forwardedFrom: f["ForwardedFrom"] ?? "",
                        })
                        .redirect(absolute(config, `/voice/after?call=${encodeURIComponent(callId)}`)));
                }
                return reply(c, listen(new VoiceResponse().pause(1), Speech.forPolly(start.text, pack), callId, start.session.language, pack, tenant.settings.calls));
        }
    });

    app.post("/voice/turn", signed, async c => {
        const f = await form(c);
        const callId = c.req.query("call") || f["CallSid"] || "";
        const tenant = await lookup(f);
        if (tenant === null) {
            return reply(c, new VoiceResponse().say(say(STRINGS.unconfigured, "en"), "en").hangup());
        }
        const pack = registry.get(tenant.domain);
        const session = await callSession(tenant, callId);
        if (session === null || session.channel !== "voice") {
            return reply(c, new VoiceResponse().hangup());
        }
        const language = session.language;
        const said = (f["SpeechResult"] ?? "").trim();
        const confidence = Number(f["Confidence"] ?? "0") || 0;

        if (!said) {
            const next = CallState.next(session.call, { kind: "silence" }, tenant.settings.limits);
            const call = next.ok ? next.value : session.call;
            await store.conversations.saveSession({ ...session, call });
            if (call.status === "ended") {
                return reply(c, new VoiceResponse().say(say(STRINGS.silenceGoodbye, language), language).hangup());
            }
            return reply(c, listen(new VoiceResponse(), say(STRINGS.stillThere, language), callId, language, pack, tenant.settings.calls));
        }
        if (confidence > 0 && confidence < LOW_CONFIDENCE && said.split(/\s+/).length <= 2) {
            return reply(c, listen(new VoiceResponse(), say(STRINGS.reprompt, language), callId, language, pack, tenant.settings.calls));
        }

        const inbound: Inbound = {
            channel:           "voice",
            tenant:            { kind: "endpoint", address: Address.phone(f["To"] ?? "")! },
            sender:            callerOf(f, callId),
            text:              said,
            receivedAt:        services.clock(),
            dedupeKey:         `${callId}:${session.call.turns + 1}:${said.slice(0, 40)}`,
            externalSessionId: callId,
            confidence,
            language:          null,
            forwardedFrom:     PhoneNumber.parse(f["ForwardedFrom"] ?? ""),
        };
        try {
            const result = await collect(engine.run(inbound, { tenant, vendor: "twilio" }));
            const text = Speech.forPolly(result.outcome.reply, pack);
            const spoken = result.session.language;
            const directive = result.outcome.directive;
            if (directive.kind === "transfer") {
                return reply(c, new VoiceResponse().say(text, spoken).dial(directive.to, { timeout: 25 }));
            }
            if (directive.kind === "end") {
                return reply(c, new VoiceResponse().say(text, spoken).hangup());
            }
            return reply(c, listen(new VoiceResponse(), text, callId, spoken, pack, tenant.settings.calls));
        } catch (e) {
            console.error("voice turn failed:", e);
            return reply(c, listen(new VoiceResponse(), say(STRINGS.fallbackVoice, language), callId, language, pack, tenant.settings.calls));
        }
    });

    app.post("/voice/recorded", signed, c => reply(c, new VoiceResponse().hangup()));

    /**
     * Where a streamed call goes after its stream closes: a transfer the assistant asked for, or hang up.
     */
    app.post("/voice/after", signed, c => {
        const callId = c.req.query("call") ?? "";
        const directive = PENDING_DIRECTIVES.get(callId);
        PENDING_DIRECTIVES.delete(callId);
        return reply(c, directive?.kind === "transfer"
            ? new VoiceResponse().dial(directive.to, { timeout: 25 })
            : new VoiceResponse().hangup());
    });

    /**
     * Twilio posts here when a voicemail recording is ready.
     */
    app.post("/voice/recording", signed, async c => {
        const f = await form(c);
        const callId = c.req.query("call") || f["CallSid"] || "";
        const tenant = await store.tenants.byId(TenantId(c.req.query("tenant") ?? ""));
        if (tenant === null) {
            return c.body(null, 204);
        }
        const session = await callSession(tenant, callId);
        if (session === null) {
            return c.body(null, 204);
        }
        const duration = Number(f["RecordingDuration"] ?? "0") || 0;
        const url = f["RecordingUrl"] ?? "";
        const conversation = await store.conversations.conversation(session.conversationId);
        const contact = conversation === null ? null : await store.conversations.contact(conversation.contactId);
        if (!url || duration < 2 || conversation === null || contact === null) {
            await closeSession(services, session.id, "caller_hung_up");
            return c.body(null, 204);
        }
        await store.voicemails.save({
            sessionId:       session.id,
            tenantId:        tenant.id,
            conversationId:  conversation.id,
            from:            contact.addresses[0] ?? Address.webSession(callId),
            recordingUrl:    url,
            durationSeconds: duration,
            receivedAt:      services.clock(),
        });
        await publish(services, tenant, contact, conversation, session, [{ kind: "voicemail_left", recordingUrl: url, durationSeconds: duration }]);
        return c.body(null, 204);
    });

    /**
     * Call status changes: close the session when the call ends, and meter the call.
     */
    app.post("/voice/status", signed, async c => {
        const f = await form(c);
        const status = f["CallStatus"] ?? "";
        if (!["completed", "busy", "failed", "no-answer", "canceled"].includes(status)) {
            return c.body(null, 204);
        }
        const callId = f["CallSid"] ?? "";
        const tenant = await lookup(f);
        if (tenant === null) {
            return c.body(null, 204);
        }
        const seconds = Number(f["CallDuration"] ?? "0") || 0;
        const session = await callSession(tenant, callId);
        await store.usage.record({
            provider:   "twilio",
            model:      PhoneNumber.isTollFree(f["To"] ?? "") ? "toll_free" : "",
            service:    "telephony",
            purpose:    "turn",
            occurredAt: services.clock(),
            units:      { call_seconds: seconds },
            costUsd:    null,
            latencyMs:  null,
            tenantId:   tenant.id,
            channel:    "voice",
        });
        if (session === null || session.channel !== "voice") {
            return c.body(null, 204);
        }
        // With <Gather>, Twilio bills each listen and every character <Say> speaks with a neural voice.
        // Streamed calls meter their own speech services (see voice/stream.ts).
        const gathered = deps.voice === undefined || deps.voice.kind === "twilio_gather";
        if (session.handling === "assistant" && gathered) {
            const turns = session.call.turns;
            const spoken = (await store.conversations.sessionMessages(session.id))
                .filter(m => m.role === "assistant")
                .reduce((n, m) => n + m.text.length, 0);
            await store.usage.record({
                provider:   "twilio-tts",
                model:      "polly-neural",
                service:    "tts",
                purpose:    "turn",
                occurredAt: services.clock(),
                units:      { characters: spoken },
                costUsd:    null,
                latencyMs:  null,
                tenantId:   tenant.id,
                channel:    "voice",
            });
            await store.usage.record({
                provider:   "twilio-speech",
                model:      "",
                service:    "stt",
                purpose:    "turn",
                occurredAt: services.clock(),
                units:      { requests: turns + 1 },
                costUsd:    null,
                latencyMs:  null,
                tenantId:   tenant.id,
                channel:    "voice",
            });
        }
        if (session.handling !== "voicemail") {
            const reason = session.call.status === "ended"
                ? session.call.reason
                : session.call.status === "transferring"
                    ? "transferred"
                    : "caller_hung_up";
            await closeSession(services, session.id, reason);
        }
        return c.body(null, 204);
    });

    /**
     * The open session for a call, however it was handled (sessions are keyed by CallSid alone).
     */
    async function callSession(tenant: Tenant, callId: string): Promise<Session | null> {
        return await store.conversations.findSession(tenant.id, { channel: "voice", externalId: callId, vendor: "twilio", handling: "assistant" });
    }

    return app;
}
