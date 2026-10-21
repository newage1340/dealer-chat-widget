# Voice pipeline costs

Every answered call minute pays for four things: the phone line, hearing the caller, speaking back, and the AI's
replies. Today's pipeline (Twilio `<Gather>` and `<Say>`) pays Twilio for hearing and speaking, and the hearing
part alone is about three quarters of the bill. The app now supports three pipelines, picked with `VOICE_PIPELINE`:

| Setting | What hears the caller | What speaks | Needs |
|---|---|---|---|
| `twilio_gather` (default) | Twilio, billed per listen | Twilio Polly neural voices | nothing new |
| `media_streams` | Deepgram live transcription | Deepgram Aura (English, Spanish), OpenAI for other languages | `DEEPGRAM_API_KEY` |
| `media_streams_local` | Whisper on our GPU machine, with Deepgram as backup | local voice server, then Deepgram/OpenAI | the GPU machine; Deepgram key recommended |

Separately, `LLM_PRIMARY=local` runs the AI replies on the GPU machine, with OpenAI taking overflow and failures.
Classification already prefers the local model whenever one is configured. The default stays OpenAI.

Both streamed pipelines also answer faster: the first sentence is spoken while the rest is still being written.
And callers can interrupt the assistant.

The figures below come from `pnpm cost-model`, which reads the same `prices.json` the app meters with.
Re-run it with different assumptions, e.g. `node src/cli/cost-model.ts --minutes 500 --tenants 40 --machine 2500`.

## The numbers

### Cost per answered call minute

| Pipeline | Per minute | vs today | Minutes a $99 plan's AI allowance covers |
|---|---|---|---|
| Twilio Gather + Polly (today) | $0.0828 | - | 454 |
| Media Streams + Deepgram | $0.0312 | 62% less | 1,205 |
| Media Streams + local Whisper and voice | $0.0145 | 83% less | 2,596 |
| All local (Whisper, voice and AI on the GPU) | $0.0131 | 84% less | 2,863 |

The AI allowance is $37.62 of each $99 plan (the share of revenue the platform spends on service costs). Local pipelines also carry the machine cost below, shared by every business.

### Breakdown per minute

**Twilio Gather + Polly (today)**: Twilio call minute $0.0085; Twilio speech recognition (3 listens) $0.0600; Polly neural voice (400 chars) $0.0128; AI replies $0.0015.

**Media Streams + Deepgram**: Twilio call minute $0.0085; Twilio Media Streams $0.0044; Deepgram live transcription $0.0048; Deepgram Aura-2 voice (400 chars) $0.0120; AI replies $0.0015.

**Media Streams + local Whisper and voice**: Twilio call minute $0.0085; Twilio Media Streams $0.0044; Deepgram backup (5% of caller speech) $0.0001; Local Whisper and voice (machine cost below) $0.0000; AI replies $0.0015.

**All local (Whisper, voice and AI on the GPU)**: Twilio call minute $0.0085; Twilio Media Streams $0.0044; Deepgram backup (5% of caller speech) $0.0001; Local Whisper, voice and AI (machine cost below) $0.0000; AI replies overflowing to OpenAI (10%) $0.0002.

### A fleet of 20 businesses at 300 call minutes a month each

| Pipeline | Per business | Fleet per month | Fleet per year | Saved per year vs today |
|---|---|---|---|---|
| Twilio Gather + Polly (today) | $24.84 | $496.80 | $5961.61 | - |
| Media Streams + Deepgram | $9.36 | $187.20 | $2246.40 | $3715.21 |
| Media Streams + local Whisper and voice | $8.17 | $163.43 | $1961.20 | $4000.42 |
| All local (Whisper, voice and AI on the GPU) | $7.77 | $155.33 | $1864.00 | $4097.62 |

Machine: $1800 over 36 months plus 250 W average at $0.1453/kWh = $76.52 a month.

### When the local machine pays for itself

- Against today's pipeline it saves $0.0683 a minute: 1,121 call minutes a month cover the machine (4 businesses at 300 minutes).
- Against Media Streams + Deepgram it saves $0.0167 a minute: 4,579 call minutes a month cover it (16 businesses at 300 minutes).

### Assumptions

- Local numbers; 3 caller turns a minute; the assistant speaks about 400 characters a minute; the caller speaks 40% of the time.
- AI replies $0.0005 per turn (measured with the shared prompt cache); 5% of local transcriptions fall back to Deepgram.
- Texts, emails and summaries cost the same on every pipeline and are left out.

### At 50 businesses

| Pipeline | Per business | Fleet per month | Fleet per year | Saved per year vs today |
|---|---|---|---|---|
| Twilio Gather + Polly (today) | $24.84 | $1242.00 | $14904.04 | - |
| Media Streams + Deepgram | $9.36 | $468.00 | $5616.00 | $9288.04 |
| Media Streams + local Whisper and voice | $5.88 | $293.81 | $3525.69 | $11378.35 |
| All local (Whisper, voice and AI on the GPU) | $5.47 | $273.56 | $3282.69 | $11621.35 |


## The pitch for local

- **Today, a $99 plan's AI allowance covers about 450 call minutes. Media Streams + Deepgram stretches that to about
  1,200, and the local machine to about 2,600.** The same plan can promise "never unanswered" to much busier
  businesses, or keep far more margin on quiet ones.
- **Moving off `<Gather>` is the big step: 62% less per minute with no hardware.** Deepgram does the work, and it
  only needs an API key and a change to `VOICE_PIPELINE`.
- **Local takes it to 83% less.** The machine (about $77 a month, amortized with electricity) pays for itself against
  today's pipeline at about 4 businesses. Against Deepgram it pays off at about 16 businesses at 300 minutes, and
  every business after that is almost pure saving.
- **Nothing breaks when the machine is busy or off.** Transcription falls back to Deepgram and AI replies fall back
  to OpenAI automatically. Local is a cost lever, not a single point of failure.
- **What stays the same on every pipeline:** the phone line itself ($0.0085 a minute local, $0.022 toll-free),
  texts, emails and summaries.

## Caveats

- **Deepgram's Nova-3 streaming rate ($0.0048 a minute) is marked as a limited-time promotion.** The regular rate
  could be somewhat higher, but streaming would still be several times cheaper than `<Gather>`.
- **GPU capacity is an estimate, not a measurement.** Published benchmarks suggest one RTX 4090 transcribes 15–30
  phone calls at once with faster-whisper, and an RTX 3090 about 60% of that. Twenty businesses rarely have more
  than a few calls at the same moment. Measure on the real machine before relying on it.
- **Power: a 3090 draws 250–300 W under inference and a 4090 300–400 W, and less when idle.** The model assumes 250 W
  average at the US commercial rate (EIA, July 2026: 14.53¢/kWh).
- **Voice quality differs between providers** (Polly neural, Deepgram Aura, OpenAI, Kokoro). Listen to each before
  switching a live business.
- **The streamed pipelines are built and tested with simulated calls, but not yet on a live phone call.** They also
  need a Deepgram key and a public wss:// address.

## Sources (retrieved 2026-10-21)

| Price | Source |
|---|---|
| Twilio voice: $0.0085/min local, $0.022/min toll-free; `<Gather>` $0.02 per use; `<Say>` neural $0.0032 per 100 chars; Media Streams $0.0044/min | twilio.com/en-us/voice/pricing/us (current as of August 2026) |
| Twilio SMS: $0.0083 per segment plus carrier fees of about $0.0025–0.007 | twilio.com/en-us/sms/pricing/us (October 2026) |
| Deepgram: Nova-3 streaming $0.0048/min, pre-recorded $0.0043/min, Aura-2 $0.030 per 1,000 chars | deepgram.com/pricing |
| OpenAI: gpt-4o-mini $0.15 / $0.075 cached / $0.60 per 1M tokens; gpt-4o-mini-tts about $0.015/min | developers.openai.com/api/docs/pricing |
| Electricity: 14.53¢/kWh commercial, 18.31¢ residential (July 2026) | eia.gov, Electric Power Monthly table 5.3 |
| faster-whisper throughput | github.com/SYSTRAN/faster-whisper benchmarks (RTX 3070 Ti); 3090/4090 figures extrapolated |
