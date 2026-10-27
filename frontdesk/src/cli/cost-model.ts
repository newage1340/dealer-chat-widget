/**
 * What a minute of answered call costs on each voice pipeline, and what that means per business and
 * for a fleet, using the same prices.json the app meters with.
 *
 * Usage: node src/cli/cost-model.ts [--minutes 300] [--tenants 20] [--machine 1800] [--machine-months 36]
 *                                   [--watts 250] [--kwh 0.1453] [--toll-free] [--markdown]
 *
 * Every assumption is printed with the results; change them with the flags above.
 */

import { readFile } from "node:fs/promises";

import { Plan, DEFAULT_PRICING } from "../core/index.ts";
import { ROOT } from "../config.ts";

type Prices = Record<string, Record<string, Record<string, number>>>;

const args = process.argv.slice(2);
const flag = (name: string, fallback: number) => {
    const i = args.indexOf(`--${name}`);
    const value = i >= 0 ? Number(args[i + 1]) : NaN;
    return Number.isFinite(value) ? value : fallback;
};

const prices = JSON.parse(await readFile(`${ROOT}prices.json`, "utf8")) as Prices;
const price = (provider: string, model: string, unit: string) => prices[provider]?.[model]?.[unit] ?? prices[provider]?.[""]?.[unit] ?? 0;

/**
 * Assumptions about one minute of a typical answered call.
 */
const A = {
    tollFree:        args.includes("--toll-free"),
    turnsPerMinute:  3,       // caller utterances per minute (each is one billed <Gather> on Twilio)
    assistantChars:  400,     // characters the assistant speaks per minute (about 45% of the minute at ~15 chars/s)
    callerShare:     0.4,     // share of the minute the caller is speaking (what local Whisper transcribes)
    llmPerTurn:      0.0005,  // measured: $0.0004 warm-cache to $0.0007 cold, gpt-4o-mini, 4-5k-token prompt
    backupShare:     0.05,    // share of local transcriptions that fall back to Deepgram
    minutes:         flag("minutes", 300),
    tenants:         flag("tenants", 20),
    machine:         flag("machine", 1800),
    machineMonths:   flag("machine-months", 36),
    watts:           flag("watts", 250),
    kwh:             flag("kwh", 0.1453),
};

const callMinute = 60 * price("twilio", A.tollFree ? "toll_free" : "", "call_seconds");
const llm = A.turnsPerMinute * A.llmPerTurn;

type Line = { readonly label: string; readonly usd: number };
type Pipeline = { readonly id: string; readonly name: string; readonly lines: readonly Line[] };

const pipelines: Pipeline[] = [
    {
        id:    "twilio_gather",
        name:  "Twilio Gather + Polly (today)",
        lines: [
            { label: "Twilio call minute", usd: callMinute },
            { label: `Twilio speech recognition (${A.turnsPerMinute} listens)`, usd: A.turnsPerMinute * price("twilio-speech", "", "requests") },
            { label: `Polly neural voice (${A.assistantChars} chars)`, usd: A.assistantChars * price("twilio-tts", "polly-neural", "characters") },
            { label: "AI replies", usd: llm },
        ],
    },
    {
        id:    "media_streams",
        name:  "Media Streams + Deepgram",
        lines: [
            { label: "Twilio call minute", usd: callMinute },
            { label: "Twilio Media Streams", usd: 60 * price("twilio-media-streams", "", "call_seconds") },
            { label: "Deepgram live transcription", usd: 60 * price("deepgram", "nova-3-streaming", "audio_seconds") },
            { label: `Deepgram Aura-2 voice (${A.assistantChars} chars)`, usd: A.assistantChars * price("deepgram", "aura-2", "characters") },
            { label: "AI replies", usd: llm },
        ],
    },
    {
        id:    "media_streams_local",
        name:  "Media Streams + local Whisper and voice",
        lines: [
            { label: "Twilio call minute", usd: callMinute },
            { label: "Twilio Media Streams", usd: 60 * price("twilio-media-streams", "", "call_seconds") },
            { label: `Deepgram backup (${A.backupShare * 100}% of caller speech)`, usd: A.backupShare * A.callerShare * 60 * price("deepgram", "nova-3", "audio_seconds") },
            { label: "Local Whisper and voice (machine cost below)", usd: 0 },
            { label: "AI replies", usd: llm },
        ],
    },
    {
        id:    "media_streams_local_llm",
        name:  "All local (Whisper, voice and AI on the GPU)",
        lines: [
            { label: "Twilio call minute", usd: callMinute },
            { label: "Twilio Media Streams", usd: 60 * price("twilio-media-streams", "", "call_seconds") },
            { label: `Deepgram backup (${A.backupShare * 100}% of caller speech)`, usd: A.backupShare * A.callerShare * 60 * price("deepgram", "nova-3", "audio_seconds") },
            { label: "Local Whisper, voice and AI (machine cost below)", usd: 0 },
            { label: "AI replies overflowing to OpenAI (10%)", usd: 0.1 * llm },
        ],
    },
];

const perMinute = (p: Pipeline) => p.lines.reduce((sum, l) => sum + l.usd, 0);
const machineMonthly = A.machine / A.machineMonths + (A.watts / 1000) * 730 * A.kwh;
const plan = { pricing: DEFAULT_PRICING, subscription: { extraLines: 0, smsNotifications: false, topUpsUsd: 0 } };
const cap = Plan.assistantCapUsd(plan);
const usd = (n: number, digits = 2) => `$${n.toFixed(digits)}`;
const pct = (n: number) => `${Math.round(n * 100)}%`;
const base = perMinute(pipelines[0]!);
const isLocal = (p: Pipeline) => p.id.startsWith("media_streams_local");

const out: string[] = [];
out.push("### Cost per answered call minute", "");
out.push("| Pipeline | Per minute | vs today | Minutes a $99 plan's AI allowance covers |", "|---|---|---|---|");
for (const p of pipelines) {
    out.push(`| ${p.name} | ${usd(perMinute(p), 4)} | ${p === pipelines[0] ? "-" : `${pct(1 - perMinute(p) / base)} less`} | ${Math.floor(cap / perMinute(p)).toLocaleString("en-US")} |`);
}
out.push("", `The AI allowance is ${usd(cap)} of each $${Plan.monthlyRevenueUsd(plan)} plan (the share of revenue the platform spends on service costs). Local pipelines also carry the machine cost below, shared by every business.`, "");

out.push("### Breakdown per minute", "");
for (const p of pipelines) {
    out.push(`**${p.name}**: ${p.lines.filter(l => l.usd > 0 || l.label.startsWith("Local")).map(l => `${l.label} ${usd(l.usd, 4)}`).join("; ")}.`, "");
}

out.push(`### A fleet of ${A.tenants} businesses at ${A.minutes} call minutes a month each`, "");
out.push("| Pipeline | Per business | Fleet per month | Fleet per year | Saved per year vs today |", "|---|---|---|---|---|");
for (const p of pipelines) {
    const fleet = perMinute(p) * A.minutes * A.tenants + (isLocal(p) ? machineMonthly : 0);
    const today = base * A.minutes * A.tenants;
    out.push(`| ${p.name} | ${usd(fleet / A.tenants)} | ${usd(fleet)} | ${usd(fleet * 12)} | ${p === pipelines[0] ? "-" : usd((today - fleet) * 12)} |`);
}
out.push("", `Machine: ${usd(A.machine, 0)} over ${A.machineMonths} months plus ${A.watts} W average at ${usd(A.kwh, 4)}/kWh = ${usd(machineMonthly)} a month.`, "");

const local = pipelines.find(p => p.id === "media_streams_local")!;
const savingVsCloud = perMinute(pipelines[1]!) - perMinute(local);
const savingVsToday = base - perMinute(local);
out.push("### When the local machine pays for itself", "");
out.push(`- Against today's pipeline it saves ${usd(savingVsToday, 4)} a minute: ${Math.ceil(machineMonthly / savingVsToday).toLocaleString("en-US")} call minutes a month cover the machine (${Math.ceil(machineMonthly / savingVsToday / A.minutes)} businesses at ${A.minutes} minutes).`);
out.push(`- Against Media Streams + Deepgram it saves ${usd(savingVsCloud, 4)} a minute: ${Math.ceil(machineMonthly / savingVsCloud).toLocaleString("en-US")} call minutes a month cover it (${Math.ceil(machineMonthly / savingVsCloud / A.minutes)} businesses at ${A.minutes} minutes).`);
out.push("", "### Assumptions", "");
out.push(`- ${A.tollFree ? "Toll-free" : "Local"} numbers; ${A.turnsPerMinute} caller turns a minute; the assistant speaks about ${A.assistantChars} characters a minute; the caller speaks ${pct(A.callerShare)} of the time.`);
out.push(`- AI replies ${usd(A.llmPerTurn, 4)} per turn (measured with the shared prompt cache); ${pct(A.backupShare)} of local transcriptions fall back to Deepgram.`);
out.push("- Texts, emails and summaries cost the same on every pipeline and are left out.");

console.log(out.join("\n"));
