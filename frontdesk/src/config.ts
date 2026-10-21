/**
 * Runtime configuration, read from environment variables once at startup.
 */

import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";

export const ROOT = fileURLToPath(new URL("../", import.meta.url));

/**
 * A dated snapshot, not the moving "gpt-4o-mini" alias, so behavior only changes when we upgrade on purpose
 * (after the evals pass on the new one).
 */
export const PINNED_OPENAI_MODEL = "gpt-4o-mini-2024-07-18";

/**
 * `primary` picks which route is tried first: "openai" or "local".
 * The local route is any OpenAI-compatible server (Ollama, llama.cpp, vLLM) at `localBaseUrl`;
 * `localMaxConcurrency` caps in-flight requests so overflow spills to OpenAI.
 */
export type LlmConfig = {
    readonly primary: "openai" | "local";
    readonly openaiApiKey: string;
    readonly openaiModels: {
        readonly small: string;
        readonly fast: string;
        readonly capable: string
    };
    readonly localBaseUrl: string;
    readonly localApiKey: string;
    readonly localModels: {
        readonly small: string;
        readonly fast: string;
        readonly capable: string
    };
    readonly localMaxConcurrency: number;
};

/**
 * Self-hosted speech servers on the GPU machine (OpenAI-compatible APIs), for the media_streams_local pipeline.
 */
export type LocalSpeechConfig = {
    readonly sttBaseUrl:        string;
    readonly sttApiKey:         string;
    readonly sttModel:          string;
    readonly sttMaxConcurrency: number;
    readonly sttTimeoutMs:      number;
    readonly ttsBaseUrl:        string;
    readonly ttsApiKey:         string;
    readonly ttsModel:          string;
    readonly ttsVoice:          string;
    readonly ttsLanguages:      readonly string[];
};

/**
 * Stripe billing. Prices are created once in the Stripe dashboard (monthly recurring) and referenced by id.
 * Billing is off while `secretKey` is empty.
 */
export type StripeConfig = {
    readonly secretKey:      string;
    readonly webhookSecret:  string;
    readonly priceBase:      string;
    readonly priceExtraLine: string;
    readonly priceSmsAlerts: string;
    readonly topUpAmounts:   readonly number[];
};

export type Config = {
    readonly port:                 number;
    readonly publicBaseUrl:        string;
    readonly databaseUrl:          string;
    readonly embeddedDbDir:        string | null;
    readonly adminToken:           string;
    readonly uploadToken:          string;
    readonly llm:                  LlmConfig;
    readonly pricesFile:           string;
    readonly twilioAccountSid:     string;
    readonly twilioAuthToken:      string;
    readonly twilioMessagingSid:   string;
    readonly twilioDefaultFrom:    string;
    readonly validateTwilio:       boolean;
    readonly resendApiKey:         string;
    readonly smtp:                 { readonly host: string; readonly port: number; readonly user: string; readonly password: string };
    readonly emailFrom:            string;
    readonly deepgramApiKey:       string;
    readonly termsUrl:             string;
    readonly privacyUrl:           string;
    readonly defaultSlug:          string;
    readonly dashboardUrl:         string;
    readonly sessionIdleMinutes:   number;
    readonly runWorker:            boolean;
    readonly onboardingCode:       string;
    readonly onboardingNotifyEmail: string;
    readonly sessionSecret:        string;
    readonly stripe:               StripeConfig;
    readonly voicePipeline:        "twilio_gather" | "media_streams" | "media_streams_local";
    readonly openaiTtsModel:       string;
    readonly openaiTtsVoice:       string;
    readonly localSpeech:          LocalSpeechConfig;
};

function env(name: string, fallback = ""): string {
    return (process.env[name] || fallback).trim();
}

function int(name: string, fallback: number): number {
    const value = Number.parseInt(env(name), 10);
    return Number.isFinite(value)
        ? value
        : fallback;
}

function flag(name: string, fallback: boolean): boolean {
    const value = env(name).toLowerCase();
    if (!value) {
        return fallback;
    }
    return ["1", "true", "yes", "on"].includes(value);
}

/**
 * Read `.env` from the project root, if there is one. Variables already set in the environment win.
 */
function loadEnvFile(): void {
    try {
        process.loadEnvFile(`${ROOT}.env`);
    } catch (e) {
        if ((e as NodeJS.ErrnoException).code !== "ENOENT") {
            throw e;
        }
    }
}

export function loadConfig(): Config {
    loadEnvFile();
    const localModel	  = env("LOCAL_LLM_MODEL");
    const twilioAuthToken = env("TWILIO_AUTH_TOKEN");
    const embedded		  = env("EMBEDDED_DB_DIR", `${ROOT}data/pglite`);
    return {
        port:           int("PORT", 5001),
        publicBaseUrl:  env("PUBLIC_BASE_URL").replace(/\/$/, ""),
        databaseUrl:    env("DATABASE_URL"),
        embeddedDbDir:  embedded === "memory" ? null : embedded,
        adminToken:     env("ADMIN_TOKEN"),
        uploadToken:    env("UPLOAD_TOKEN", env("INVENTORY_UPLOAD_TOKEN")),
        llm: {
            primary: env("LLM_PRIMARY", "openai").toLowerCase() === "local"
				? "local"
				: "openai",
            openaiApiKey: env("OPENAI_API_KEY"),
            openaiModels: {
                small:   env("OPENAI_MODEL_SMALL", PINNED_OPENAI_MODEL),
                fast:    env("OPENAI_MODEL_FAST", PINNED_OPENAI_MODEL),
                capable: env("OPENAI_MODEL_CAPABLE", PINNED_OPENAI_MODEL),
            },
            localBaseUrl: env("LOCAL_LLM_BASE_URL"),
            localApiKey:  env("LOCAL_LLM_API_KEY"),
            localModels: {
                small:   env("LOCAL_LLM_MODEL_SMALL", localModel),
                fast:    env("LOCAL_LLM_MODEL_FAST", localModel),
                capable: env("LOCAL_LLM_MODEL_CAPABLE", localModel),
            },
            localMaxConcurrency: int("LOCAL_LLM_MAX_CONCURRENCY", 2),
        },
        pricesFile:         env("PRICES_FILE", `${ROOT}prices.json`),
        twilioAccountSid:   env("TWILIO_ACCOUNT_SID"),
        twilioAuthToken,
        twilioMessagingSid: env("TWILIO_MESSAGING_SERVICE_SID"),
        twilioDefaultFrom:  env("TWILIO_DEFAULT_FROM"),
        validateTwilio:     flag("TWILIO_VALIDATE_SIGNATURES", twilioAuthToken !== ""),
        resendApiKey:       env("RESEND_API_KEY"),
        // SMTP_* for any mail host; GMAIL_USER / GMAIL_APP_PASSWORD (the Python app's names) mean Gmail.
        smtp:               {
            host:     env("SMTP_HOST", env("GMAIL_USER") ? "smtp.gmail.com" : ""),
            port:     int("SMTP_PORT", 587),
            user:     env("SMTP_USER", env("GMAIL_USER")),
            password: env("SMTP_PASSWORD", env("GMAIL_APP_PASSWORD")),
        },
        emailFrom:          env("EMAIL_FROM", env("NOTIFY_FROM_EMAIL", env("GMAIL_USER", "Front Desk <assistant@example.com>"))),
        deepgramApiKey:     env("DEEPGRAM_API_KEY"),
        termsUrl:           env("TERMS_URL", "https://inventiq.net/terms.html"),
        privacyUrl:         env("PRIVACY_URL", "https://inventiq.net/privacy.html"),
        defaultSlug:        env("DEFAULT_SLUG"),
        dashboardUrl:       env("DASHBOARD_URL").replace(/\/$/, ""),
        sessionIdleMinutes: int("SESSION_IDLE_MINUTES", 30),
        runWorker:          flag("RUN_WORKER", true),
        onboardingCode:     env("ONBOARDING_CODE"),
        onboardingNotifyEmail: env("ONBOARDING_NOTIFY_EMAIL"),
        stripe:             {
            secretKey:      env("STRIPE_SECRET_KEY"),
            webhookSecret:  env("STRIPE_WEBHOOK_SECRET"),
            priceBase:      env("STRIPE_PRICE_BASE"),
            priceExtraLine: env("STRIPE_PRICE_EXTRA_LINE"),
            priceSmsAlerts: env("STRIPE_PRICE_SMS_ALERTS"),
            topUpAmounts:   env("STRIPE_TOPUP_AMOUNTS", "25,50,100").split(",").map(Number).filter(n => n > 0),
        },
        // Signs business-owner sessions. Derived from the admin token when not set, so it is never empty in practice.
        sessionSecret:      env("SESSION_SECRET") || createHash("sha256").update(`session:${env("ADMIN_TOKEN")}`).digest("hex"),
        voicePipeline:      (["media_streams", "media_streams_local"] as const).find(p => p === env("VOICE_PIPELINE").toLowerCase()) ?? "twilio_gather",
        openaiTtsModel:     env("OPENAI_TTS_MODEL", "gpt-4o-mini-tts"),
        openaiTtsVoice:     env("OPENAI_TTS_VOICE", "coral"),
        localSpeech:        {
            sttBaseUrl:        env("LOCAL_STT_BASE_URL"),
            sttApiKey:         env("LOCAL_STT_API_KEY"),
            sttModel:          env("LOCAL_STT_MODEL", "Systran/faster-whisper-large-v3"),
            sttMaxConcurrency: int("LOCAL_STT_MAX_CONCURRENCY", 8),
            sttTimeoutMs:      int("LOCAL_STT_TIMEOUT_MS", 4000),
            ttsBaseUrl:        env("LOCAL_TTS_BASE_URL"),
            ttsApiKey:         env("LOCAL_TTS_API_KEY"),
            ttsModel:          env("LOCAL_TTS_MODEL", "kokoro"),
            ttsVoice:          env("LOCAL_TTS_VOICE", "af_heart"),
            ttsLanguages:      env("LOCAL_TTS_LANGUAGES", "en,es,fr,pt,zh").split(",").map(l => l.trim()).filter(Boolean),
        },
    };
}
