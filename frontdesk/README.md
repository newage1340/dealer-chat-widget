# Front Desk

An AI front desk for small businesses: it answers web chat, texts and phone calls, books appointments,
takes messages and voicemails, and emails the business a transcript of every conversation.
Business types (plumber, car dealer, salon, ...) are configuration: a universal base prompt plus a
domain pack per business type.

TypeScript on Node 24+ (Node runs the `.ts` files directly; there is no build step), Postgres, Hono.
The Python version in `../src` and the original `../app.py` are kept as references; nothing here depends on them.

## Run it locally

```bash
pnpm install
export OPENAI_API_KEY=...            # or LOCAL_LLM_BASE_URL + LOCAL_LLM_MODEL for a self-hosted model
pnpm import-tenants                  # loads ../data/tenants/*.json (old or new format)
pnpm start                           # http://localhost:5001 lists every widget
```

Without `DATABASE_URL` the app uses an embedded Postgres (PGlite) in `data/pglite` - fine for development,
**not for production**: on a host whose disk is wiped on deploy, that data would not survive.
Without Twilio, Resend or Deepgram keys, texts and emails are logged instead of sent and voicemails are not transcribed.

Useful commands:

```bash
pnpm check                                         # type-check + tests
pnpm prompt demo-plumber voice "my water heater is leaking"   # the exact prompt a turn gets
pnpm validate-pack                                 # check every domain pack
node src/main.ts author-domain "nail salon"        # have the model write a new pack (validated, one repair round)
pnpm eval                                          # scripted conversations against the configured model
```

## How a conversation flows

```
Web widget / Twilio SMS / Twilio voice / Vapi  ->  Inbound (channel, sender, tenant, text, dedupe key)
   -> TurnEngine
        dedupe (retried webhook? answer from the first result)
        resolve tenant, contact, conversation, session
        policies: STOP/START/HELP, blocked callers, web profile gate
        budget: allowance left? wind-down in progress?
        prompt (stable cacheable part + per-turn part) -> model, streaming, with tools
        tools record facts: details, bookings, links, callbacks, language switch, flags, transfer/end call
        commit in one transaction: messages, session, contact changes, events, and the jobs they cause
   -> reply streams back to the channel
Worker (same process): texts, emails, staff notices, reminders, follow-ups, voicemail transcription,
   classification of closed sessions, transcript emails, idle-session sweep, monthly fixed costs
```

Sessions close when a call ends, a chat or text thread goes quiet (30 minutes), or a wind-down finishes.
Every closed session is classified by a small model (outcome, topic, urgency, sentiment, language, a
one-line gist in the staff's language) and its transcript is emailed to staff.

## Money

- A tenant's service costs can never exceed **40% of what they paid this month** (`costShare` in `src/core/plan.ts`).
  Add-ons and top-ups raise the allowance in proportion. 5% of it is held back so callers can still leave voicemails.
- Every external call is metered into the `usage` table and priced from `prices.json` (verify those prices).
- The assistant answers around the clock until the allowance runs out. At 80% staff get a warning;
  when it is used up, calls go to voicemail, then get a free busy signal.
- Long or unproductive conversations get a 3-turn polite wind-down; two abusive sessions in 30 days
  block the caller for 24 hours (staff can block, extend or lift via the admin API).

Outside the code, also: bill up front (Stripe, with the web app), and set vendor-side limits
(an OpenAI project budget, Twilio usage triggers) as a backstop.

## Configuration

| Variable | Purpose |
|---|---|
| `PORT` | HTTP port (default 5001) |
| `PUBLIC_BASE_URL` | The public URL Twilio calls (needed for signature checks and absolute callbacks) |
| `DATABASE_URL` | Postgres connection string (production). Unset: embedded database in `EMBEDDED_DB_DIR` (`memory` for none) |
| `ADMIN_TOKEN` | Enables `/admin/*` (send as `Authorization: Bearer ...`) |
| `UPLOAD_TOKEN` (or `INVENTORY_UPLOAD_TOKEN`) | Lets the scraper upload catalogs |
| `OPENAI_API_KEY`, `OPENAI_MODEL_SMALL` / `_FAST` / `_CAPABLE` | OpenAI route (all default to the pinned `gpt-4o-mini-2024-07-18`; upgrade on purpose after evals pass) |
| `LOCAL_LLM_BASE_URL`, `LOCAL_LLM_MODEL` (or `_SMALL` / `_FAST` / `_CAPABLE`), `LOCAL_LLM_API_KEY` | Any OpenAI-compatible server, e.g. Ollama `http://gpu-box:11434/v1` |
| `LLM_PRIMARY` | `openai` (default) or `local`: which route is tried first. Classification always tries local first |
| `LOCAL_LLM_MAX_CONCURRENCY` | In-flight cap for the local route; overflow spills to OpenAI (default 2) |
| `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_MESSAGING_SERVICE_SID`, `TWILIO_DEFAULT_FROM` | Texts, recordings, webhook signatures |
| `TWILIO_VALIDATE_SIGNATURES` | Defaults to on whenever `TWILIO_AUTH_TOKEN` is set |
| `RESEND_API_KEY`, `EMAIL_FROM` | Transcript and staff emails |
| `DEEPGRAM_API_KEY` | Voicemail transcription |
| `PRICES_FILE` | Price table (default `prices.json`) |
| `TERMS_URL`, `PRIVACY_URL`, `DEFAULT_SLUG`, `DASHBOARD_URL` | Widget links, `/` redirect, link in transcript emails |
| `SESSION_IDLE_MINUTES` | When quiet chats and text threads close (default 30) |
| `RUN_WORKER` | Run the job worker in this process (default on) |

## Webhooks

**Twilio number** (Phone Numbers > Manage > your number):
- Messaging, "A message comes in": `POST {PUBLIC_BASE_URL}/sms`
- Voice, "A call comes in": `POST {PUBLIC_BASE_URL}/voice`
- Voice, "Call status changes": `POST {PUBLIC_BASE_URL}/voice/status` (closes the session and meters the call)

**Vapi assistant**: Custom LLM URL `{PUBLIC_BASE_URL}/vapi/chat/completions`, Server URL `{PUBLIC_BASE_URL}/vapi/events`,
with the `endCall` and `transferCall` tools enabled.

Each tenant lists its numbers under `endpoints`. The business keeps its own number and forwards calls to ours
however suits them (always, when nobody answers, or while closed); calls staff answer never reach us.

## Tenants

Tenants live in the database. Create or update one with `PUT /admin/tenants` and a JSON body like:

```json
{
  "id": "circle-city", "slug": "circle-city", "name": "Circle City Plumbing", "domain": "plumbing",
  "timezone": "America/Indiana/Indianapolis", "phone": "317-555-0142",
  "hoursText": "Mon-Fri 7am-6pm, Sat 8am-2pm, Sun closed",
  "endpoints": { "sms": ["+13175550200"], "voice": ["+13175550200"] },
  "staff": { "emails": ["owner@example.com"], "phones": ["317-555-0888"] },
  "profile": { "service_area": "Indianapolis and 25 miles around" },
  "options": { "free_estimates": true },
  "plan": { "extraLines": 0, "smsNotifications": false, "topUpsUsd": 0 },
  "settings": { "transferTo": "317-555-0777", "coldFollowupMinutes": 45, "recordingDisclosure": false }
}
```

Everything except `id`, `slug`, `name`, `domain` and `timezone` has a default; see `src/tenants/schema.ts`.
`pnpm import-tenants <dir>` loads a directory of these (or of the old Python-format files).

## Demo businesses

`pnpm seed-demo` loads eleven demo businesses from `tenants/demo/` and syncs their catalogs from `datasets/`
(re-running it keeps any phone numbers set on the settings page). `pnpm check-demo` validates them end to end.

| Slug | Business type | Languages | Catalog | Shows off |
|---|---|---|---|---|
| demo-hair-salon | Hair salon | en, es | 28 services (JSON) | patch-test rule, deposits |
| demo-nail-salon | Nail salon | en, vi, es | 30 services (JSON) | walk-ins, group bookings |
| demo-chinese-restaurant | Chinese restaurant | en, zh | 49 dishes (CSV, POS-style headers) | column mapping, pickup orders with totals |
| demo-auto-repair | Auto repair, Mexican-owned | en, es (staff summaries in Spanish) | 31 services (CSV) | bilingual catalog, Spanish follow-ups |
| demo-therapy-practice | Therapy practice | en, es | 15 services (JSON) | confidential mode, formal tone, honest about being virtual |
| demo-boxing-gym | Boxing gym | en, es | 26 classes and memberships (CSV) | casual tone, waivers, minors |
| demo-computer-repair | Computer and phone repair | en only | 32 services and refurbished devices (JSON) | single-language business, no passwords in chat |
| demo-esl-tutor | ESL for Spanish speakers | es first, en | 20 classes (JSON) | Spanish as the main language |
| demo-real-estate | Real estate agent | en, es | 24 listings (JSON) | listing and photo links, Fair Housing rules |
| demo-talent-agency | Talent agency for actors | en only | 19 programs (JSON) | no-fee representation, protecting minors |
| demo-plumbing | Plumbing | en, es | 13 services (JSON) | emergencies and live transfers |

Each demo has web chat at `/widget/<slug>`. To try one by phone, put the Twilio number under its Phone numbers on
the settings page; that moves the number away from whichever business had it.

## Settings website

`/settings` (sign in with `ADMIN_TOKEN`) edits everything a business owner tunes: business info, phone numbers,
tone, privacy, languages, schedule, catalog source, profile answers, notifications and blocked callers. It also
shows usage and conversations. Each section is its own form and saves through the same validation as `PUT /admin/tenants`.
The markup is bare on purpose (no classes, styles or scripts); `public/settings/settings.css` is an empty hook for styling.

## Catalogs: one interface for menus, price lists, listings and inventory

Every business has a catalog shaped by its domain pack (dishes, services, listings, vehicles). Where it comes from is a
per-business setting (`settings.catalog.source`):

- `manual`: rows uploaded via `POST /admin/catalog-upload` or pasted on the settings page (CSV or JSON);
- `dataset`: a file in `datasets/`;
- `csv_url`: any CSV link, e.g. a Google Sheet published as CSV, or a point-of-sale export;
- `json_url`: any JSON feed, with `itemsPath` pointing at the list.

`settings.catalog.fieldMap` renames the source's columns to the pack's fields (`"Dish": "name"`). Remote sources
refresh on their own every `refreshMinutes` (the worker checks once a minute) or right away with "Sync now".
A failed or empty fetch keeps the last good catalog. Whatever the source, the assistant sees the same catalog rows
and the same `search_catalog` tool. Adding another source (Square, Shopify, an MLS feed) is one connector in
`src/catalogs/connectors.ts`.

## Languages, tone and privacy

- **Languages.** `settings.languages` lists what a business offers; the assistant tells customers which languages are
  available when they ask for another one. Each reply's language is detected, and that is what sets the voice and the
  speech recognizer for the next turn, even when the model switches without calling `switch_language`. Customers'
  remembered languages can be reset from the settings page or with `POST /admin/tenants/:slug/languages/reset`.
- **Tone.** `settings.style.professionalism` (casual, friendly, polished, formal) and `honestAboutAi`; each defaults to the domain pack's value.
- **Privacy.** `settings.privacy.redact` scrubs card numbers, SSNs, bank account numbers and birth dates from what customers
  write, before the model sees it or anything is stored. `sensitive` adds confidentiality rules (never confirm who is a client,
  only collect what scheduling needs). Staff texts and transcript emails then carry no conversation content.
  `crisisSupport` is **off by default**: when a business turns it on, messages that suggest self-harm get 988/911
  guidance and staff get an urgent alert. When it's off, nothing in the prompts mentions crisis handling.

## Voice pipelines and local hosting

`VOICE_PIPELINE` picks how calls are heard and spoken:

- `twilio_gather` (default): Twilio `<Gather>` listens and `<Say>` speaks.
- `media_streams`: Twilio streams the audio to `/voice/stream`; Deepgram transcribes live, and Deepgram Aura or
  OpenAI speaks sentence by sentence. Callers can interrupt.
- `media_streams_local`: Whisper on our GPU machine (any OpenAI-compatible server, e.g. speaches) transcribes,
  with Deepgram as the backup. A local voice server (e.g. Kokoro-FastAPI) speaks when configured.

`LLM_PRIMARY=local` (with `LOCAL_LLM_BASE_URL`, e.g. Ollama or vLLM) runs AI replies on the GPU machine, with
OpenAI as overflow and fallback; the default is OpenAI. `docs/voice-costs.md` (generated with `pnpm cost-model`)
has per-minute costs for each option and when the local machine pays for itself.
The settings are listed in `.env.example`.

## Quality and evals

- `pnpm eval` runs the hand-written scenarios in `evals/`; `pnpm eval:packs` runs checks generated from every demo
  business's own data (a catalog price, something not offered, a closed day, a prank, switching languages and back);
  `pnpm eval:all` runs both three times each and reports anything that doesn't pass every time. Add `--variant <id>`
  to test a prompt variant. Results are stored, so pass rates per model and variant show at `/settings/quality`.
- Every live turn is metered: prompt variant, model, latency, time to first words, tools, tool errors, corrective
  rounds, fallbacks, tokens (cached included) and cost. Booking and lead rates come from the session summaries.
  See `/settings/quality` or `GET /admin/quality?days=7`.
- Prompt A/B tests: `src/prompts/variants/variants.json`. Each conversation is assigned a variant by a stable hash
  in proportion to its weight. A variant can swap base templates, temperature or tier.
- The model is pinned to a dated snapshot (`gpt-4o-mini-2024-07-18`); upgrade by changing it after `pnpm eval:all` passes.
- `.github/workflows/frontdesk-checks.yml` typechecks, tests and lints on every change, and runs the evals when the
  repository has an `OPENAI_API_KEY` secret.

## Admin API

All need `Authorization: Bearer $ADMIN_TOKEN` except `/health` and catalog upload (upload token).

| Endpoint | |
|---|---|
| `GET /health` | |
| `GET /admin/tenants`, `PUT /admin/tenants` | List / create or replace a tenant |
| `POST /admin/catalog-upload` (also `/admin/inventory-upload`) | `{slug or twilio_number, items or vehicles}`; refuses an empty list |
| `GET /admin/usage?hours=24&tenant=...` | Spend by provider and purpose, plus month-to-date |
| `GET /admin/prompt/:slug?channel=voice&message=...` | The exact prompt a turn would get |
| `GET /admin/transcripts?tenant=...&outcome=lead,booked`, `GET /admin/transcripts/:sessionId` | For the staff web app |
| `GET /admin/blocks?tenant=...`, `POST /admin/blocks`, `DELETE /admin/blocks/:contactId` | Bans |
| `GET /admin/tenants/:slug/catalog`, `POST /admin/tenants/:slug/catalog/sync` | Catalog, sync status, sync now |
| `POST /admin/tenants/:slug/languages/reset` | `{phone}` for one customer, empty for all |
| `GET /datasets/:file` | The sample datasets (public, for trying the CSV and JSON link sources) |

## Layout

```
src/core/       domain-neutral types and rules: tenants, contacts, sessions, calls, budget, plans, schedules, events, jobs
src/providers/  vendor-neutral interfaces + adapters (OpenAI-compatible LLM, router, Twilio, Resend, Deepgram, fakes)
src/domains/    domain pack schema and loader, catalog helpers, pack authoring
src/prompts/    base templates, composer, hours, speech formatting, fixed strings (with Spanish)
src/tools/      the model's tools
src/turn/       contracts: turn context, policies, tools, subscribers, outcomes
src/pipeline/   turn engine, policies, subscribers, session lifecycle, job worker
src/storage/    storage ports, Postgres implementation, migrations
src/catalogs/   catalog sources: connectors (dataset, CSV link, JSON link) and syncing
src/http/       channels: web widget, SMS, Twilio voice, Vapi, admin, settings website
src/evals/      scripted conversation evals
packs/          domain packs (TOML); evals/ scenario files; public/ widget front end
datasets/       sample catalogs; tenants/demo/ the demo businesses
```

## Deploying on Render

`render.yaml` describes a web service plus a managed Postgres database. The database costs extra
(a few dollars a month for the smallest plan) but is what keeps conversations, transcripts and usage safe across deploys.
Set the secrets in the Render dashboard, point the Twilio number and Vapi assistant at the service URL,
and import tenants with `PUT /admin/tenants` (or `pnpm import-tenants` with `DATABASE_URL` set).

The repository's root `Procfile` still starts the old Python `app.py`; switch Render to this service when ready.

## Not verified against live vendors yet

The tests use a scripted model and recording messengers. Before relying on production traffic, check:
Twilio speech-recognition language codes and voices per language (`src/http/twiml.ts`), Vapi's request and
end-of-call payload shapes (`src/http/vapi.ts`), the prices in `prices.json`, and Resend/Deepgram responses.
