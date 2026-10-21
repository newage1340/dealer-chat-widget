/**
 * Runtime configuration, read from environment variables once at startup.
 */

import { fileURLToPath } from "node:url";

export const ROOT = fileURLToPath(new URL("../", import.meta.url));

/**
 * `primary` picks which route is tried first: "openai" or "local".
 * The local route is any OpenAI-compatible server (Ollama, llama.cpp, vLLM) at `localBaseUrl`;
 * `localMaxConcurrency` caps in-flight requests so overflow spills to OpenAI.
 */
export type LlmConfig = {
    readonly primary:             "openai" | "local";
    readonly openaiApiKey:        string;
    readonly openaiModels:        { readonly small: string; readonly fast: string; readonly capable: string };
    readonly localBaseUrl:        string;
    readonly localApiKey:         string;
    readonly localModels:         { readonly small: string; readonly fast: string; readonly capable: string };
    readonly localMaxConcurrency: number;
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
    readonly emailFrom:            string;
    readonly deepgramApiKey:       string;
    readonly termsUrl:             string;
    readonly privacyUrl:           string;
    readonly defaultSlug:          string;
    readonly dashboardUrl:         string;
    readonly sessionIdleMinutes:   number;
    readonly runWorker:            boolean;
};

function env(name: string, fallback = ""): string {
    return (process.env[name] ?? fallback).trim();
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
    const localModel = env("LOCAL_LLM_MODEL");
    const twilioAuthToken = env("TWILIO_AUTH_TOKEN");
    const embedded = env("EMBEDDED_DB_DIR", `${ROOT}data/pglite`);
    return {
        port:               int("PORT", 5001),
        publicBaseUrl:      env("PUBLIC_BASE_URL").replace(/\/$/, ""),
        databaseUrl:        env("DATABASE_URL"),
        embeddedDbDir:      embedded === "memory" ? null : embedded,
        adminToken:         env("ADMIN_TOKEN"),
        uploadToken:        env("UPLOAD_TOKEN", env("INVENTORY_UPLOAD_TOKEN")),
        llm: {
            primary:             env("LLM_PRIMARY", "openai").toLowerCase() === "local" ? "local" : "openai",
            openaiApiKey:        env("OPENAI_API_KEY"),
            openaiModels:        {
                small:   env("OPENAI_MODEL_SMALL", "gpt-4o-mini"),
                fast:    env("OPENAI_MODEL_FAST", "gpt-4o-mini"),
                capable: env("OPENAI_MODEL_CAPABLE", "gpt-4o-mini"),
            },
            localBaseUrl:        env("LOCAL_LLM_BASE_URL"),
            localApiKey:         env("LOCAL_LLM_API_KEY"),
            localModels:         {
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
        emailFrom:          env("EMAIL_FROM", "Front Desk <assistant@example.com>"),
        deepgramApiKey:     env("DEEPGRAM_API_KEY"),
        termsUrl:           env("TERMS_URL", "https://inventiq.net/terms.html"),
        privacyUrl:         env("PRIVACY_URL", "https://inventiq.net/privacy.html"),
        defaultSlug:        env("DEFAULT_SLUG"),
        dashboardUrl:       env("DASHBOARD_URL").replace(/\/$/, ""),
        sessionIdleMinutes: int("SESSION_IDLE_MINUTES", 30),
        runWorker:          flag("RUN_WORKER", true),
    };
}
