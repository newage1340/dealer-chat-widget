/**
 * A full pipeline over an in-memory Postgres, with a scripted model and recording messengers.
 */

import { Address, type Inbound, type Tenant } from "../../src/core/index.ts";
import { DomainRegistry } from "../../src/domains/index.ts";
import { TurnEngine, collect, type RunOptions, type TurnResult } from "../../src/pipeline/engine.ts";
import { DEFAULT_POLICIES } from "../../src/pipeline/policies.ts";
import type { Services } from "../../src/pipeline/services.ts";
import { DEFAULT_SUBSCRIBERS } from "../../src/pipeline/subscribers.ts";
import { Worker } from "../../src/pipeline/worker.ts";
import { PromptComposer } from "../../src/prompts/composer.ts";
import {
    FakeLlm,
    FakeTranscriber,
    LoggingEmail,
    LoggingMessaging,
    type LlmRequest,
    type ScriptedReply,
} from "../../src/providers/index.ts";
import { loadPrices } from "../../src/server.ts";
import { openStore, type Store } from "../../src/storage/index.ts";

export type Harness = {
    readonly store:     Store;
    readonly services:  Services;
    readonly engine:    TurnEngine;
    readonly worker:    Worker;
    readonly llm:       FakeLlm;
    readonly messaging: LoggingMessaging;
    readonly email:     LoggingEmail;
    readonly clock:     { now: Date };
    turn(inbound: Partial<Inbound> & Pick<Inbound, "text">, options?: RunOptions): Promise<TurnResult>;
};

let counter = 0;

export async function harness(
    tenants: readonly Tenant[],
    script: (request: LlmRequest) => ScriptedReply | string
): Promise<Harness> {
    const store = await openStore("", null, await loadPrices(new URL("../../prices.json", import.meta.url).pathname));
    for (const t of tenants) {
        await store.tenants.save(t);
    }
    const clock = { now: new Date("2026-10-21T15:00:00Z") };
    const llm = new FakeLlm(script);
    const services: Services = {
        store,
        registry:    await DomainRegistry.fromDirectory(),
        composer:    await PromptComposer.load(),
        llm,
        policies:    DEFAULT_POLICIES,
        subscribers: DEFAULT_SUBSCRIBERS,
        clock:       () => clock.now,
        smsIdleMs:   30 * 60_000,
    };
    const messaging = new LoggingMessaging();
    const email = new LoggingEmail();
    const engine = new TurnEngine(services);
    const worker = new Worker({
        services,
        messaging,
        email,
        transcriber:    new FakeTranscriber("Hi, this is Sam, my basement is flooding, call me back at 317-555-0111."),
        fetchRecording: async () => new Uint8Array([1, 2, 3]),
        emailFrom:      "assistant@frontdesk.example",
        defaultSmsFrom: "+13175550000",
        dashboardUrl:   "",
    });
    const first = tenants[0]!;
    return {
        store, services, engine, worker, llm, messaging, email, clock,
        turn: (partial, options) => {
            const inbound: Inbound = {
                channel:           "sms",
                tenant:            { kind: "endpoint", address: Address.phone(first.endpoints.sms?.[0]?.value ?? "+13175550200")! },
                sender:            Address.phone("+13175550111")!,
                receivedAt:        clock.now,
                dedupeKey:         `msg-${++counter}`,
                externalSessionId: null,
                confidence:        null,
                language:          null,
                forwardedFrom:     null,
                ...partial,
            };
            return collect(engine.run(inbound, options));
        },
    };
}

/**
 * The last user message the model saw.
 */
export function lastUser(request: LlmRequest): string {
    const users = request.messages.filter(m => m.role === "user");
    const last = users[users.length - 1];
    return last !== undefined && last.role === "user"
        ? last.text.toLowerCase()
        : "";
}

/**
 * Whether this request already carries a tool result (i.e. it is a follow-up round).
 */
export function hasToolResult(request: LlmRequest, name?: string): boolean {
    return request.messages.some(m => m.role === "tool" && (name === undefined || m.output.includes(name)));
}
