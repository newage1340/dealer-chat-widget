import { beforeEach, describe, expect, it } from "vitest";

import { FakeLlm, FakeStt, FakeTts, LoggingEmail, LoggingMessaging, type LlmRequest, type ScriptedReply } from "../../src/providers/index.ts";
import { buildApp, type App } from "../../src/server.ts";
import { openStore } from "../../src/storage/index.ts";
import { PENDING_DIRECTIVES, SentenceSplitter, StreamCall } from "../../src/voice/stream.ts";
import { TEST_CONFIG } from "../support/config.ts";
import { CUSTOMER, PLUMBER_NUMBER, plumber } from "../support/fixtures.ts";
import { lastUser } from "../support/harness.ts";

function script(request: LlmRequest): ScriptedReply | string {
    const said = lastUser(request);
    if (said.includes("explode")) {
        throw new Error("a bug in the turn");
    }
    if (said.includes("real person")) {
        return { text: "Okay, connecting you now.", tools: [{ name: "transfer_call", arguments: { reason: "asked for a person" } }] };
    }
    if (said.includes("hola")) {
        return "Claro, con mucho gusto le ayudo. ¿Qué problema tiene con la plomería?";
    }
    return "Sure thing, we can help with that. What's the address for the visit?";
}

type Sent = { event: string; media?: { payload: string }; mark?: { name: string } };

let app: App;
let stt: FakeStt;
let tts: FakeTts;

async function until(check: () => boolean, ms = 3000): Promise<void> {
    const end = Date.now() + ms;
    while (!check()) {
        if (Date.now() > end) {
            throw new Error("timed out waiting");
        }
        await new Promise(r => setTimeout(r, 10));
    }
}

async function post(path: string, body: Record<string, string>) {
    return await app.http.request(path, { method: "POST", body: new URLSearchParams(body) });
}

/**
 * Starts a call the way Twilio does: the /voice webhook, then a media stream for it.
 */
async function call(callId: string) {
    const sent: Sent[] = [];
    let closed = false;
    const twiml = await (await post("/voice", { CallSid: callId, From: CUSTOMER, To: PLUMBER_NUMBER })).text();
    const pipeline = { kind: "media_streams" as const, stt, voices: [tts] };
    const stream = new StreamCall({ services: app.services, engine: app.engine, pipeline }, {
        send:  m => sent.push(m as Sent),
        close: () => { closed = true; },
    });
    await stream.handle(JSON.stringify({ event: "start", start: { streamSid: "MZ1", callSid: callId, customParameters: { call: callId, tenant: "plumber", from: CUSTOMER } } }));
    // Twilio echoes each mark once that audio has played.
    const playAll = () => {
        for (const m of sent.filter(s => s.event === "mark" && !s.mark!.name.startsWith("done"))) {
            void stream.handle(JSON.stringify({ event: "mark", mark: { name: m.mark!.name } }));
            m.mark!.name = `done-${m.mark!.name}`;
        }
    };
    // Wait for audio to finish being sent (its mark), then play it.
    const play = async () => {
        await until(() => sent.some(m => m.event === "mark" && !m.mark!.name.startsWith("done")));
        playAll();
    };
    return { twiml, sent, stream, playAll, play, closed: () => closed };
}

beforeEach(async () => {
    const store = await openStore("", null);
    await store.tenants.save(plumber({ settings: { transferTo: "317-555-0777", calls: { silenceSeconds: 3, stillTherePrompts: 1 } } }));
    stt = new FakeStt();
    tts = new FakeTts();
    app = await buildApp({ ...TEST_CONFIG, publicBaseUrl: "https://frontdesk.example" }, {
        store, llm: new FakeLlm(script), messaging: new LoggingMessaging(), email: new LoggingEmail(), transcriber: null, fetchRecording: null,
        voice: { kind: "media_streams", stt, voices: [tts] },
    });
});

describe("SentenceSplitter", () => {
    it("emits whole sentences as text streams in, keeping very short ones with the next", () => {
        const s = new SentenceSplitter();
        expect(s.push("Okay. We can come out to")).toEqual([]);
        expect(s.push("morrow. What time works? Morning or")).toEqual(["Okay. We can come out tomorrow.", "What time works?"]);
        expect(s.flush()).toBe("Morning or");
    });
});

describe("media streams", () => {
    it("hands the call to our WebSocket instead of <Gather>", async () => {
        const { twiml } = await call("CA1");
        expect(twiml).toContain('<Connect><Stream url="wss://frontdesk.example/voice/stream">');
        expect(twiml).toContain('<Parameter name="tenant" value="plumber"/>');
        expect(twiml).toContain("/voice/after?call=CA1</Redirect>");
        expect(twiml).not.toContain("<Gather");
    });

    it("greets, answers each utterance as audio, and stops talking when the caller interrupts", async () => {
        const c = await call("CA2");
        await c.play();
        expect(c.sent.some(s => s.event === "media")).toBe(true);

        stt.hear("my kitchen sink is leaking");
        await until(() => tts.spoken.some(t => t.includes("we can help")));
        expect(tts.spoken.at(-1)).toContain("What's the address");
        await c.play();

        stt.hear("hola, hablo español");
        await until(() => tts.spoken.some(t => t.startsWith("Claro")));
        stt.startSpeaking();
        await until(() => c.sent.some(s => s.event === "clear"));
        expect(stt.languages).toContain("es");
    });

    it("plays the reply, then transfers through /voice/after", async () => {
        const c = await call("CA3");
        await c.play();
        stt.hear("I want a real person please");
        await until(() => tts.spoken.some(t => t.includes("connecting you")));
        await c.play();
        await until(() => c.closed());
        expect(PENDING_DIRECTIVES.get("CA3")).toMatchObject({ kind: "transfer" });
        const after = await (await post("/voice/after?call=CA3", { CallSid: "CA3" })).text();
        expect(after).toContain("<Dial");
        expect(after).toContain("+13175550777");
        const usage = await app.store.spend.summary(null, new Date(0));
        expect(usage.some(u => u.provider === "twilio-media-streams")).toBe(true);
        expect(usage.some(u => u.provider === "fake-tts")).toBe(true);
    });

    it("keeps answering after a turn fails", async () => {
        const c = await call("CA5");
        await c.play();
        stt.hear("please explode");
        await c.play();
        stt.hear("my kitchen sink is leaking");
        await until(() => tts.spoken.some(t => t.includes("we can help")));
    });

    it("asks if the caller is still there, then says goodbye and hangs up", async () => {
        const c = await call("CA4");
        await c.play();
        await until(() => tts.spoken.length === 2, 5000);
        expect(tts.spoken[1]).toMatch(/still there/i);
        await c.play();
        await until(() => tts.spoken.length === 3, 5000);
        await c.play();
        await until(() => c.closed(), 2000);
        expect(PENDING_DIRECTIVES.get("CA4")).toEqual({ kind: "end" });
    }, 20_000);
});
