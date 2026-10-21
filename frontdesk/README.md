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
| `OPENAI_API_KEY`, `OPENAI_MODEL_SMALL` / `_FAST` / `_CAPABLE` | OpenAI route (all default to `gpt-4o-mini`) |
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
src/http/       channels: web widget, SMS, Twilio voice, Vapi, admin
src/evals/      scripted conversation evals
packs/          domain packs (TOML); evals/ scenario files; public/ widget front end
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
