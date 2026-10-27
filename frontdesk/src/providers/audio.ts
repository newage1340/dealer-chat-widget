/**
 * Phone audio helpers: G.711 mu-law (what Twilio streams, 8 kHz mono), PCM conversion,
 * resampling, WAV packaging, and a simple energy-based voice activity detector.
 */

const BIAS = 0x84;
const CLIP = 32635;

const DECODE: Int16Array = (() => {
    const table = new Int16Array(256);
    for (let i = 0; i < 256; i++) {
        const u = ~i & 0xff;
        const sign = u & 0x80;
        const exponent = (u >> 4) & 0x07;
        const mantissa = u & 0x0f;
        let sample = ((mantissa << 3) + BIAS) << exponent;
        sample -= BIAS;
        table[i] = sign ? -sample : sample;
    }
    return table;
})();

export const MuLaw = {
    decode(bytes: Uint8Array): Int16Array {
        const out = new Int16Array(bytes.length);
        for (let i = 0; i < bytes.length; i++) {
            out[i] = DECODE[bytes[i]!]!;
        }
        return out;
    },

    encode(samples: Int16Array): Uint8Array {
        const out = new Uint8Array(samples.length);
        for (let i = 0; i < samples.length; i++) {
            let s = samples[i]!;
            const sign = s < 0 ? 0x80 : 0;
            if (sign) {
                s = -s;
            }
            s = Math.min(s, CLIP) + BIAS;
            let exponent = 7;
            for (let mask = 0x4000; (s & mask) === 0 && exponent > 0; mask >>= 1) {
                exponent--;
            }
            const mantissa = (s >> (exponent + 3)) & 0x0f;
            out[i] = ~(sign | (exponent << 4) | mantissa) & 0xff;
        }
        return out;
    },
} as const;

export const Pcm = {
    /**
     * Little-endian 16-bit PCM bytes as samples.
     */
    fromBytes(bytes: Uint8Array): Int16Array {
        const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength - (bytes.byteLength % 2));
        const out = new Int16Array(view.byteLength / 2);
        for (let i = 0; i < out.length; i++) {
            out[i] = view.getInt16(i * 2, true);
        }
        return out;
    },

    /**
     * Downsample by averaging (a crude low-pass, fine for speech going to an 8 kHz phone line).
     */
    downsample(samples: Int16Array, fromRate: number, toRate: number): Int16Array {
        if (fromRate === toRate) {
            return samples;
        }
        const ratio = fromRate / toRate;
        const out = new Int16Array(Math.floor(samples.length / ratio));
        for (let i = 0; i < out.length; i++) {
            const start = Math.floor(i * ratio);
            const end = Math.min(samples.length, Math.floor((i + 1) * ratio));
            let sum = 0;
            for (let j = start; j < end; j++) {
                sum += samples[j]!;
            }
            out[i] = Math.round(sum / Math.max(1, end - start));
        }
        return out;
    },

    /**
     * A mono 16-bit WAV file, for speech-to-text servers that want a file upload.
     */
    wav(samples: Int16Array, sampleRate: number): Uint8Array {
        const data = samples.length * 2;
        const buffer = new ArrayBuffer(44 + data);
        const v = new DataView(buffer);
        const text = (offset: number, s: string) => [...s].forEach((ch, i) => v.setUint8(offset + i, ch.charCodeAt(0)));
        text(0, "RIFF");
        v.setUint32(4, 36 + data, true);
        text(8, "WAVE");
        text(12, "fmt ");
        v.setUint32(16, 16, true);
        v.setUint16(20, 1, true);
        v.setUint16(22, 1, true);
        v.setUint32(24, sampleRate, true);
        v.setUint32(28, sampleRate * 2, true);
        v.setUint16(32, 2, true);
        v.setUint16(34, 16, true);
        text(36, "data");
        v.setUint32(40, data, true);
        samples.forEach((s, i) => v.setInt16(44 + i * 2, s, true));
        return new Uint8Array(buffer);
    },

    rms(samples: Int16Array): number {
        if (samples.length === 0) {
            return 0;
        }
        let sum = 0;
        for (const s of samples) {
            sum += s * s;
        }
        return Math.sqrt(sum / samples.length);
    },
} as const;

/**
 * Splits a stream of 8 kHz audio frames into utterances: speech starts when a frame is louder than
 * `threshold`, and ends after `silenceMs` of quiet. Utterances shorter than `minSpeechMs` are noise.
 */
export class VoiceActivity {
    readonly #threshold: number;
    readonly #silenceMs: number;
    readonly #minSpeechMs: number;
    readonly #maxMs: number;
    #frames: Int16Array[] = [];
    #speechMs = 0;
    #quietMs = 0;
    #speaking = false;

    constructor(options: { threshold?: number; silenceMs?: number; minSpeechMs?: number; maxMs?: number } = {}) {
        this.#threshold   = options.threshold ?? 700;
        this.#silenceMs   = options.silenceMs ?? 700;
        this.#minSpeechMs = options.minSpeechMs ?? 250;
        this.#maxMs       = options.maxMs ?? 30_000;
    }

    /**
     * Feed one frame. Returns "start" when speech begins, the utterance's samples when it ends, or null.
     */
    push(frame: Int16Array, sampleRate = 8000): "start" | Int16Array | null {
        const ms = (frame.length / sampleRate) * 1000;
        const loud = Pcm.rms(frame) >= this.#threshold;
        if (!this.#speaking) {
            if (!loud) {
                return null;
            }
            this.#speaking = true;
            this.#frames = [frame];
            this.#speechMs = ms;
            this.#quietMs = 0;
            return "start";
        }
        this.#frames.push(frame);
        this.#speechMs += ms;
        this.#quietMs = loud ? 0 : this.#quietMs + ms;
        if (this.#quietMs < this.#silenceMs && this.#speechMs < this.#maxMs) {
            return null;
        }
        this.#speaking = false;
        const voiced = this.#speechMs - this.#quietMs;
        const frames = this.#frames;
        this.#frames = [];
        if (voiced < this.#minSpeechMs) {
            return null;
        }
        const out = new Int16Array(frames.reduce((n, f) => n + f.length, 0));
        let offset = 0;
        for (const f of frames) {
            out.set(f, offset);
            offset += f.length;
        }
        return out;
    }
}
