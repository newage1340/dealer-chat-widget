/**
 * Vapi custom-LLM endpoint: Vapi runs the call (telephony, speech-to-text, text-to-speech, barge-in)
 * and POSTs an OpenAI-style chat-completions request each turn. We answer from the same turn engine,
 * streaming the reply sentence by sentence so speech starts before the whole reply is written.
 *
 * End-of-call and transfer go back as calls to the assistant's Vapi tools (endCall / transferCall).
 * Point the assistant's Server URL at /vapi/events so call ends close the session and meter the cost.
 */

import { randomUUID } from "node:crypto";

import { Hono, type Context } from "hono";
import { stream } from "hono/streaming";

import { Address, CallState, type Inbound, type Tenant } from "../core/index.ts";
import type { DomainPack } from "../domains/index.ts";
import { closeSession } from "../pipeline/sessions.ts";
import { Speech } from "../prompts/speech.ts";
import { STRINGS, say } from "../prompts/strings.ts";
import type { ChannelDeps } from "./context.ts";
import { startCall } from "./voice.ts";

type VapiMessage = { readonly role?: string; readonly content?: string | null };

type VapiRequest = {
    readonly stream?:      boolean;
    readonly messages?:    readonly VapiMessage[];
    readonly tools?:       readonly { readonly type?: string; readonly function?: { readonly name?: string } }[];
    readonly call?:        { readonly id?: string; readonly customer?: { readonly number?: string }; readonly phoneNumber?: { readonly number?: string } };
    readonly customer?:    { readonly number?: string };
    readonly phoneNumber?: { readonly number?: string };
};

type Action =
    | { readonly kind: "continue" }
    | { readonly kind: "hangup" }
    | { readonly kind: "transfer";
        readonly to: string
    };

function toolCall(action: Action, toolNames: readonly string[]): unknown[] | null {
    const find = (...keywords: string[]) => toolNames.find(n => keywords.every(k => n.toLowerCase().includes(k)));
    if (action.kind === "hangup") {
        return [{ index: 0, id: "call_end", type: "function", function: { name: find("end", "call") ?? find("hangup") ?? "endCall", arguments: "{}" } }];
    }
    if (action.kind === "transfer") {
        return [{ index: 0, id: "call_xfer", type: "function", function: { name: find("transfer") ?? "transferCall", arguments: JSON.stringify({ destination: action.to }) } }];
    }
    return null;
}

function chunk(id: string, delta: Record<string, unknown>, finish: string | null): string {
    const body = { id, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model: "front-desk", choices: [{ index: 0, delta, finish_reason: finish }] };
    return `data: ${JSON.stringify(body)}\n\n`;
}

/**
 * Cut streamed text into speakable pieces: whole sentences, so pronunciations and number spelling see full words.
 */
function* sentences(buffer: { text: string }, flush: boolean): Generator<string> {
    for (;;) {
        const m = /^[\s\S]*?[.!?…](\s|$)/.exec(buffer.text);
        if (m === null || (m[0].length === buffer.text.length && !flush && !/\s$/.test(m[0]))) {
            break;
        }
        buffer.text = buffer.text.slice(m[0].length);
        yield m[0];
    }
    if (flush && buffer.text) {
        yield buffer.text;
        buffer.text = "";
    }
}

export function vapiRoutes(deps: ChannelDeps): Hono {
    const app = new Hono();
    const { services, engine } = deps;
    const { store, registry } = services;

    app.post("/vapi/chat/completions", async c => handle(c));
    app.post("/vapi", async c => handle(c));

    async function handle(c: Context) {
        const payload = await c.req.json<VapiRequest>().catch((): VapiRequest => ({}));
        const call = payload.call ?? {};
        const callId = (call.id ?? "").trim() || `vapi-${randomUUID()}`;
        const callerRaw = call.customer?.number ?? payload.customer?.number ?? "";
        const calledRaw = call.phoneNumber?.number ?? payload.phoneNumber?.number ?? "";
        const caller = Address.phone(callerRaw) ?? Address.webSession(`call-${callId}`);
        const called = Address.phone(calledRaw);
        const lastUser = [...(payload.messages ?? [])].reverse().find(m => m.role === "user");
        const toolNames = (payload.tools ?? []).map(t => t.function?.name ?? t.type ?? "");
        const id = `chatcmpl-${callId}`;
        const tenant: Tenant | null = called === null ? null : await store.tenants.byEndpoint("voice", called);

        return stream(c, async out => {
            c.header("Content-Type", "text/event-stream");
            const send = async (text: string, action: Action) => {
                if (text) {
                    await out.write(chunk(id, { content: text }, null));
                }
                const tools = toolCall(action, toolNames);
                if (tools !== null) {
                    await out.write(chunk(id, { tool_calls: tools }, null));
                }
                await out.write(chunk(id, {}, tools !== null ? "tool_calls" : "stop"));
                await out.write("data: [DONE]\n\n");
            };

            if (tenant === null) {
                await send(say(STRINGS.unconfigured, "en"), { kind: "hangup" });
                return;
            }
            const pack = registry.get(tenant.domain);
            if (lastUser === undefined || !lastUser.content?.trim()) {
                const start = await startCall(services, tenant, caller, callId, null, "vapi");
                switch (start.kind) {
                    case "assistant":
                        await send(Speech.forStreaming(start.text, pack), { kind: "continue" });
                        return;
                    case "forward":
                        await send("", { kind: "transfer", to: start.to });
                        return;
                    default: {
                        // Vapi can't take our voicemail; tell the caller and end politely.
                        const language = start.kind === "voicemail" ? start.session.language : tenant.defaultLanguage;
                        await send(say(STRINGS.awayText, language, { business_name: tenant.name }), { kind: "hangup" });
                        return;
                    }
                }
            }

            const inbound: Inbound = {
                channel:           "voice",
                sender:            caller,
                text:              lastUser.content.trim(),
                receivedAt:        services.clock(),
                dedupeKey:         `${callId}:${(payload.messages ?? []).length}`,
                externalSessionId: callId,
                confidence:        null,
                language:          null,
                forwardedFrom:     null,
            };
            await streamTurn(inbound, tenant, pack, async text => {
                await out.write(chunk(id, { content: text }, null));
            }, async action => {
                const tools = toolCall(action, toolNames);
                if (tools !== null) {
                    await out.write(chunk(id, { tool_calls: tools }, null));
                }
                await out.write(chunk(id, {}, tools !== null ? "tool_calls" : "stop"));
                await out.write("data: [DONE]\n\n");
            });
        });
    }

    async function streamTurn(
        inbound: Inbound,
        tenant: Tenant,
        pack: DomainPack,
        write: (text: string) => Promise<void>,
        finish: (action: Action) => Promise<void>
    ): Promise<void>
    {
        const buffer = { text: "" };
        let action: Action = { kind: "continue" };
        try {
            for await (const part of engine.run(inbound, tenant, { vendor: "vapi" })) {
                if (part.kind === "text") {
                    buffer.text += part.delta;
                    for (const piece of sentences(buffer, false)) {
                        await write(Speech.forStreaming(piece, pack));
                    }
                } else {
                    const d = part.result.outcome.directive;
                    action = d.kind === "transfer"
                        ? { kind: "transfer", to: d.to }
                        : d.kind === "end"
                            ? { kind: "hangup" }
                            : { kind: "continue" };
                }
            }
            for (const piece of sentences(buffer, true)) {
                await write(Speech.forStreaming(piece, pack));
            }
        } catch (e) {
            console.error("vapi turn failed:", e);
            await write(say(STRINGS.fallbackVoice, tenant.defaultLanguage));
        }
        await finish(action);
    }

    /**
     * Vapi server messages: close the session at the end of the call and record what the call cost.
     */
    app.post("/vapi/events", async c => {
        const body = await c.req.json<{ message?: { type?: string; call?: VapiRequest["call"]; cost?: number; durationSeconds?: number } }>()
            .catch(() => ({ message: undefined }));
        const message = body.message;
        if (message?.type !== "end-of-call-report") {
            return c.json({ ok: true });
        }
        const called = Address.phone(message.call?.phoneNumber?.number ?? "");
        const tenant = called === null ? null : await store.tenants.byEndpoint("voice", called);
        const callId = message.call?.id ?? "";
        if (tenant === null || !callId) {
            return c.json({ ok: true });
        }
        await store.usage.record({
            provider:   "vapi",
            model:      "",
            service:    "voice_orchestration",
            purpose:    "turn",
            occurredAt: services.clock(),
            units:      { call_seconds: message.durationSeconds ?? 0 },
            costUsd:    typeof message.cost === "number" ? message.cost : null,
            latencyMs:  null,
            tenantId:   tenant.id,
            channel:    "voice",
        });
        const session = await store.conversations.findSession(tenant.id, { channel: "voice", externalId: callId, vendor: "vapi", handling: "assistant" });
        if (session !== null && session.channel === "voice") {
            await closeSession(services, session.id, CallState.endReason(session.call));
        }
        return c.json({ ok: true });
    });

    return app;
}
