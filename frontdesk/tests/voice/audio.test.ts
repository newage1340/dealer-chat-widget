import { describe, expect, it } from "vitest";

import { MuLaw, Pcm, VoiceActivity } from "../../src/providers/audio.ts";

function tone(ms: number, amplitude: number, rate = 8000): Int16Array {
    const n = Math.round((ms / 1000) * rate);
    return Int16Array.from({ length: n }, (_, i) => Math.round(amplitude * Math.sin((2 * Math.PI * 440 * i) / rate)));
}

describe("mu-law", () => {
    it("round-trips speech-level samples within mu-law's precision", () => {
        const samples = Int16Array.from([0, 100, -100, 1000, -1000, 8000, -8000, 30000, -30000]);
        const back = MuLaw.decode(MuLaw.encode(samples));
        samples.forEach((s, i) => expect(Math.abs(back[i]! - s)).toBeLessThanOrEqual(Math.max(16, Math.abs(s) * 0.04)));
    });

    it("decodes silence byte 0xFF to zero", () => {
        expect(MuLaw.decode(Uint8Array.from([0xff]))[0]).toBe(0);
    });
});

describe("PCM", () => {
    it("downsamples 24 kHz to 8 kHz and writes a WAV header", () => {
        const down = Pcm.downsample(tone(100, 5000, 24000), 24000, 8000);
        expect(down.length).toBe(800);
        const wav = Pcm.wav(down, 8000);
        expect(new TextDecoder().decode(wav.slice(0, 4))).toBe("RIFF");
        expect(wav.length).toBe(44 + 1600);
    });
});

describe("VoiceActivity", () => {
    it("finds an utterance between silences and ignores clicks", () => {
        const vad = new VoiceActivity({ silenceMs: 400 });
        const frames = (audio: Int16Array) => Array.from({ length: audio.length / 160 }, (_, i) => audio.slice(i * 160, (i + 1) * 160));
        const events: (string | number)[] = [];
        for (const f of [...frames(tone(300, 0)), ...frames(tone(60, 8000)), ...frames(tone(600, 0)), ...frames(tone(800, 8000)), ...frames(tone(600, 0))]) {
            const r = vad.push(f);
            if (r === "start") {
                events.push("start");
            } else if (r !== null) {
                events.push(r.length);
            }
        }
        // The 60 ms click starts and is dropped; the 800 ms of speech comes out with its trailing quiet.
        expect(events[0]).toBe("start");
        expect(events.filter(e => typeof e === "number")).toHaveLength(1);
        expect(events.at(-1)).toBeGreaterThan(800 * 8);
    });
});
