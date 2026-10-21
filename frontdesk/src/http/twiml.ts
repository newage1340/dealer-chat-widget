/**
 * A small TwiML builder (Twilio's XML for calls and messages), and the voices used per language.
 */

import type { Language } from "../core/index.ts";

/**
 * How to listen and speak in each language on Twilio: the <Gather> recognizer language and the <Say> voice.
 * Languages without a natural voice of their own fall back to the closest one Twilio offers.
 */
export const TWILIO_LANGUAGES: Readonly<Record<Language, { readonly gather: string; readonly voice: string; readonly say: string }>> = {
    en: { gather: "en-US", voice: "Polly.Joanna-Neural", say: "en-US" },
    es: { gather: "es-US", voice: "Polly.Lupe-Neural", say: "es-US" },
    zh: { gather: "zh-CN", voice: "Polly.Zhiyu-Neural", say: "cmn-CN" },
    vi: { gather: "vi-VN", voice: "Google.vi-VN-Standard-A", say: "vi-VN" },
    tl: { gather: "fil-PH", voice: "Google.fil-PH-Standard-A", say: "fil-PH" },
    ar: { gather: "ar-SA", voice: "Polly.Hala-Neural", say: "arb" },
    ko: { gather: "ko-KR", voice: "Polly.Seoyeon-Neural", say: "ko-KR" },
    ru: { gather: "ru-RU", voice: "Google.ru-RU-Standard-A", say: "ru-RU" },
    fr: { gather: "fr-FR", voice: "Polly.Lea-Neural", say: "fr-FR" },
    ht: { gather: "fr-FR", voice: "Polly.Lea-Neural", say: "fr-FR" },
    pt: { gather: "pt-BR", voice: "Polly.Camila-Neural", say: "pt-BR" },
};

/**
 * XML escaping for TwiML. Apostrophes stay as they are, so the TwiML (and its logs) read naturally.
 */
function escape(text: string): string {
    return text
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll("\"", "&quot;");
}

function attrs(values: Readonly<Record<string, string | number | boolean | undefined>>): string {
    return Object.entries(values)
        .filter(([, v]) => v !== undefined && v !== "")
        .map(([k, v]) => ` ${k}="${escape(String(v))}"`)
        .join("");
}

/**
 * Speech text may carry a little SSML (<break/>, <emphasis>), which is passed through inside <Say>;
 * everything else is escaped.
 */
function speech(text: string): string {
    const allowed = /<\/?(break|emphasis|prosody|say-as|sub|p|s)\b[^>]*>/gi;
    const parts: string[] = [];
    let last = 0;
    for (const m of text.matchAll(allowed)) {
        parts.push(escape(text.slice(last, m.index)), m[0]);
        last = (m.index ?? 0) + m[0].length;
    }
    parts.push(escape(text.slice(last)));
    return parts.join("").replace(/^<speak>|<\/speak>$/gi, "");
}

export class VoiceResponse {
    readonly #parts: string[] = [];

    say(text: string, language: Language): this {
        if (text.trim()) {
            const l = TWILIO_LANGUAGES[language];
            this.#parts.push(`<Say${attrs({ voice: l.voice, language: l.say })}>${speech(text)}</Say>`);
        }
        return this;
    }

    pause(seconds: number): this {
        this.#parts.push(`<Pause${attrs({ length: seconds })}/>`);
        return this;
    }

    /**
     * Speak inside a speech <Gather> so the caller can talk over (barge in on) the assistant.
     */
    gather(options: {
        readonly action:   string;
        readonly language: Language;
        readonly prompt:   string;
        readonly hints:    readonly string[];
        readonly timeout:  number;
    }): this
    {
        const l = TWILIO_LANGUAGES[options.language];
        const inner = options.prompt.trim()
            ? `<Say${attrs({ voice: l.voice, language: l.say })}>${speech(options.prompt)}</Say>`
            : "";
        this.#parts.push(`<Gather${attrs({
            input:              "speech",
            action:             options.action,
            method:             "POST",
            speechTimeout:      "auto",
            timeout:            options.timeout,
            language:           l.gather,
            hints:              options.hints.slice(0, 100).join(","),
            bargeIn:            true,
            actionOnEmptyResult: true,
        })}>${inner}</Gather>`);
        return this;
    }

    dial(number: string, options: { readonly action?: string; readonly timeout?: number } = {}): this {
        this.#parts.push(`<Dial${attrs({ action: options.action, timeout: options.timeout })}>${escape(number)}</Dial>`);
        return this;
    }

    record(options: { readonly action: string; readonly statusCallback: string; readonly maxLength: number }): this {
        this.#parts.push(`<Record${attrs({
            action:                        options.action,
            recordingStatusCallback:       options.statusCallback,
            recordingStatusCallbackMethod: "POST",
            maxLength:                     options.maxLength,
            playBeep:                      true,
            timeout:                       5,
        })}/>`);
        return this;
    }

    /**
     * Hand the call's audio to our WebSocket (both directions). The next verb runs when we close the socket.
     */
    stream(url: string, parameters: Readonly<Record<string, string>>): this {
        const params = Object.entries(parameters)
            .filter(([, value]) => value !== "")
            .map(([name, value]) => `<Parameter${attrs({ name, value })}/>`)
            .join("");
        this.#parts.push(`<Connect><Stream${attrs({ url })}>${params}</Stream></Connect>`);
        return this;
    }

    redirect(url: string): this {
        this.#parts.push(`<Redirect method="POST">${escape(url)}</Redirect>`);
        return this;
    }

    reject(): this {
        this.#parts.push(`<Reject reason="busy"/>`);
        return this;
    }

    hangup(): this {
        this.#parts.push("<Hangup/>");
        return this;
    }

    toString(): string {
        return `<?xml version="1.0" encoding="UTF-8"?><Response>${this.#parts.join("")}</Response>`;
    }
}

export function messageResponse(text: string | null): string {
    return text
        ? `<?xml version="1.0" encoding="UTF-8"?><Response><Message>${escape(text)}</Message></Response>`
        : `<?xml version="1.0" encoding="UTF-8"?><Response></Response>`;
}
