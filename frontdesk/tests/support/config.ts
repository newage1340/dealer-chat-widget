/**
 * A complete Config for tests: no vendors, embedded in-memory database, admin token "admin".
 */

import type { Config } from "../../src/config.ts";

export const TEST_CONFIG: Config = {
    port: 0, publicBaseUrl: "", databaseUrl: "", embeddedDbDir: null, adminToken: "admin", uploadToken: "upload",
    llm: { primary: "openai", openaiApiKey: "", openaiModels: { small: "m", fast: "m", capable: "m" }, localBaseUrl: "", localApiKey: "", localModels: { small: "", fast: "", capable: "" }, localMaxConcurrency: 1 },
    pricesFile: new URL("../../prices.json", import.meta.url).pathname,
    twilioAccountSid: "", twilioAuthToken: "", twilioMessagingSid: "", twilioDefaultFrom: "", validateTwilio: false,
    resendApiKey: "", smtp: { host: "", port: 587, user: "", password: "" }, emailFrom: "test@example.com", deepgramApiKey: "", termsUrl: "https://t", privacyUrl: "https://p",
    defaultSlug: "", dashboardUrl: "", sessionIdleMinutes: 30, runWorker: false, onboardingCode: "",
    voicePipeline: "twilio_gather", openaiTtsModel: "tts", openaiTtsVoice: "coral",
    localSpeech: {
        sttBaseUrl: "", sttApiKey: "", sttModel: "whisper", sttMaxConcurrency: 1, sttTimeoutMs: 1000,
        ttsBaseUrl: "", ttsApiKey: "", ttsModel: "kokoro", ttsVoice: "v", ttsLanguages: ["en"],
    },
};
