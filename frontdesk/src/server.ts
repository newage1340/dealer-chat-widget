/**
 * Builds the whole application from configuration: storage, providers, the turn engine,
 * the job worker and the HTTP routes. Tests pass fakes in place of any external service.
 */

import { readFile } from "node:fs/promises";

import { Hono } from "hono";

import type { Config, LlmConfig } from "./config.ts";
import { DomainRegistry } from "./domains/index.ts";
import { adminRoutes } from "./http/admin.ts";
import type { ChannelDeps } from "./http/context.ts";
import { onboardingRoutes } from "./http/onboarding.ts";
import { settingsRoutes } from "./http/settings/routes.ts";
import { smsRoutes } from "./http/sms.ts";
import { vapiRoutes } from "./http/vapi.ts";
import { voiceRoutes } from "./http/voice.ts";
import { webRoutes } from "./http/web.ts";
import { TurnEngine } from "./pipeline/engine.ts";
import { DEFAULT_POLICIES } from "./pipeline/policies.ts";
import type { Services } from "./pipeline/services.ts";
import { DEFAULT_SUBSCRIBERS } from "./pipeline/subscribers.ts";
import { Worker } from "./pipeline/worker.ts";
import { PromptComposer } from "./prompts/composer.ts";
import {
    LlmRouter,
    LoggingEmail,
    LoggingMessaging,
    type BatchTranscriber,
    type EmailProvider,
    type LlmProvider,
    type MessagingProvider,
    type PriceTable,
    type Route,
    type UsageSink,
} from "./providers/index.ts";
import { DeepgramTranscriber } from "./providers/deepgram.ts";
import { OpenAiCompatibleLlm } from "./providers/openai.ts";
import { ResendEmail } from "./providers/resend.ts";
import { TwilioSms } from "./providers/twilio.ts";
import { openStore, type Store } from "./storage/index.ts";

export type Overrides = {
    readonly store?:          Store;
    readonly llm?:            LlmProvider;
    readonly messaging?:      MessagingProvider;
    readonly email?:          EmailProvider;
    readonly transcriber?:    BatchTranscriber | null;
    readonly fetchRecording?: ((url: string) => Promise<Uint8Array>) | null;
    readonly clock?:          () => Date;
};

export type App = {
    readonly http:     Hono;
    readonly services: Services;
    readonly engine:   TurnEngine;
    readonly worker:   Worker;
    readonly store:    Store;
};

export async function loadPrices(path: string): Promise<PriceTable> {
    try {
        const raw = JSON.parse(await readFile(path, "utf8")) as Record<string, unknown>;
        return Object.fromEntries(Object.entries(raw).filter(([k]) => !k.startsWith("_"))) as PriceTable;
    } catch (e) {
        console.warn(`no price table loaded from ${path}: ${String(e)}`);
        return {};
    }
}

/**
 * Routes per tier, in priority order. "local" is any OpenAI-compatible server (e.g. Ollama on the GPU box);
 * its concurrency cap makes overflow and failures spill to OpenAI. Classification ("small") prefers local.
 */
export function buildLlm(cfg: LlmConfig, sink: UsageSink, prices: PriceTable): LlmRouter {
    const openai: Route | null = cfg.openaiApiKey
        ? { provider: new OpenAiCompatibleLlm({ name: "openai", apiKey: cfg.openaiApiKey, baseUrl: null, models: cfg.openaiModels }), maxConcurrency: null }
        : null;
    const local: Route | null = cfg.localBaseUrl && cfg.localModels.fast
        ? { provider: new OpenAiCompatibleLlm({ name: "local", apiKey: cfg.localApiKey, baseUrl: cfg.localBaseUrl, models: cfg.localModels }), maxConcurrency: cfg.localMaxConcurrency }
        : null;
    const ordered = (cfg.primary === "local" ? [local, openai] : [openai, local]).filter((r): r is Route => r !== null);
    const smallFirst = [local, openai].filter((r): r is Route => r !== null);
    if (ordered.length === 0) {
        console.warn("No LLM configured (set OPENAI_API_KEY and/or LOCAL_LLM_BASE_URL); replies will be fallbacks.");
    } else {
        console.info(`LLM routes: ${ordered.map(r => r.provider.name).join(" -> ")}`);
    }
    return new LlmRouter({ small: smallFirst, fast: ordered, capable: ordered }, sink, prices);
}

export async function buildApp(config: Config, overrides: Overrides = {}): Promise<App> {
    const prices = await loadPrices(config.pricesFile);
    const store = overrides.store ?? await openStore(config.databaseUrl, config.embeddedDbDir, prices);
    const twilio = config.twilioAccountSid && config.twilioAuthToken
        ? new TwilioSms({ accountSid: config.twilioAccountSid, authToken: config.twilioAuthToken, messagingServiceSid: config.twilioMessagingSid })
        : null;
    if (twilio === null && overrides.messaging === undefined) {
        console.warn("Twilio credentials not set; outbound texts will be logged, not sent.");
    }
    if (!config.resendApiKey && overrides.email === undefined) {
        console.warn("RESEND_API_KEY not set; emails will be logged, not sent.");
    }

    const services: Services = {
        store,
        registry:    await DomainRegistry.fromDirectory(),
        composer:    await PromptComposer.load(),
        llm:         overrides.llm ?? buildLlm(config.llm, store.usage, prices),
        policies:    DEFAULT_POLICIES,
        subscribers: DEFAULT_SUBSCRIBERS,
        clock:       overrides.clock ?? (() => new Date()),
        smsIdleMs:   config.sessionIdleMinutes * 60_000,
    };
    const engine = new TurnEngine(services);
    const worker = new Worker({
        services,
        messaging:      overrides.messaging ?? twilio ?? new LoggingMessaging(true),
        email:          overrides.email ?? (config.resendApiKey ? new ResendEmail(config.resendApiKey) : new LoggingEmail(true)),
        transcriber:    overrides.transcriber !== undefined
            ? overrides.transcriber
            : config.deepgramApiKey ? new DeepgramTranscriber(config.deepgramApiKey) : null,
        fetchRecording: overrides.fetchRecording !== undefined
            ? overrides.fetchRecording
            : twilio !== null ? (url: string) => twilio.recording(url) : null,
        emailFrom:      config.emailFrom,
        defaultSmsFrom: config.twilioDefaultFrom,
        dashboardUrl:   config.dashboardUrl,
    });

    const deps: ChannelDeps = { config, services, engine };
    const http = new Hono();
    http.onError((e, c) => {
        console.error(`${c.req.method} ${c.req.path} failed:`, e);
        return c.json({ error: "internal error" }, 500);
    });
    http.route("/", adminRoutes(deps));
    http.route("/", settingsRoutes(deps));
    http.route("/", onboardingRoutes(deps));
    http.route("/", smsRoutes(deps));
    http.route("/", voiceRoutes(deps));
    http.route("/", vapiRoutes(deps));
    http.route("/", webRoutes(deps));
    return { http, services, engine, worker, store };
}
