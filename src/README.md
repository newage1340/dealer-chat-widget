# Front Desk (redesign)

Domain-neutral rebuild of the monolithic `app.py`. Web chat, SMS, Twilio voice and Vapi all run
through one turn pipeline. Business types ("domains") are configuration: a universal base prompt
plus a domain pack per business type.

## Run it

```bash
uv sync                                  # installs deps into .venv
export OPENAI_API_KEY=...                # and/or LOCAL_LLM_* below
uv run python src/main.py                # dev server on :5001 (PORT to change)
# production:
gunicorn --chdir src "server:create_app()" --bind 0.0.0.0:$PORT --workers 1 --timeout 60
```

Open `http://localhost:5001/` for a list of tenant widgets. Without Twilio credentials,
outbound texts are logged instead of sent. Without any LLM configured, replies are fallbacks.

Useful commands:

```bash
uv run python src/main.py prompt gov voice "any trucks under 20k?"   # see the exact prompt
uv run python src/main.py validate-domain src/domains/packs/plumbing.toml
uv run python src/main.py author-domain "nail salon"                 # generate a new pack
uv run pytest
```

## Configuration (environment)

| Variable | Purpose |
|---|---|
| `OPENAI_API_KEY`, `OPENAI_MODEL_FAST`, `OPENAI_MODEL_CAPABLE` | OpenAI route (defaults `gpt-4o-mini` for both tiers; `OPENAI_VOICE_MODEL` / `OPENAI_MODEL` are honored as fallbacks) |
| `LOCAL_LLM_BASE_URL`, `LOCAL_LLM_MODEL` (or `_FAST` / `_CAPABLE`), `LOCAL_LLM_API_KEY` | Any OpenAI-compatible server, e.g. Ollama `http://gpu-box:11434/v1` |
| `LLM_PRIMARY` | `openai` (default) or `local`: which route is tried first; the other is the fallback |
| `LOCAL_LLM_MAX_CONCURRENCY` | In-flight cap for the local route; overflow spills to OpenAI (default 2) |
| `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_MESSAGING_SERVICE_SID`, `TWILIO_DEFAULT_FROM` | Outbound SMS |
| `TWILIO_VOICE` | Polly voice for Twilio calls (default `Polly.Joanna-Neural`) |
| `PUBLIC_BASE_URL` | Absolute base for Twilio `<Gather>` callbacks (relative URLs otherwise) |
| `DATA_DIR`, `TENANTS_DIR`, `DB_PATH`, `PRICES_FILE` | Data locations (default `data/`, `data/tenants/`, `data/frontdesk.db`, `data/prices.json`) |
| `WIDGET_DEFAULT_SLUG` | Where `/` redirects |
| `PRIMER_TERMS_URL`, `PRIMER_PRIVACY_URL` | Linked from the widget and SMS primer |
| `ADMIN_TOKEN` | Enables `/admin/*` (reload, usage, prompt preview) |
| `INVENTORY_UPLOAD_TOKEN` | Accepts catalog uploads at `/admin/catalog-upload` (and the legacy `/admin/inventory-upload`) |
| `FLASK_SECRET`, `DISABLE_SCHEDULER`, `REMINDER_LEAD_MINUTES` | Misc |

Webhooks to configure: Twilio SMS → `POST /sms`; Twilio voice → `POST /voice`;
Vapi custom LLM → `POST /vapi/chat/completions`. Each tenant lists its numbers in `endpoints`.

## Layout

```
core/        frozen dataclasses: Tenant, Contact, Message, CatalogItem, Appointment, Outcome, Actions, UsageRecord
providers/   vendor-neutral interfaces + adapters (OpenAI-compatible LLM, Twilio SMS), LLM router, price table
domains/     domain packs (packs/*.toml), loader/validator, catalog formatting/matching, authoring tools
prompts/     universal base templates, hours logic, the composer (base + domain + tenant + turn)
tenants/     tenant directory loaded from data/tenants/*.json
storage/     SQLite store (contacts, messages, state, appointments, notices, usage)
pipeline/    handlers (opt-out, web profile gate, LLM conversation), action executor, turn runner
channels/    Flask blueprints: web widget, SMS, Twilio voice, Vapi, admin
scheduler.py reminders + cold follow-ups      server.py app factory      main.py CLI
```

A turn: channel builds an `InboundMessage` → `TurnRunner` resolves tenant + contact and runs handlers
→ the first handler that returns `handled` produces the reply; every handler's `Actions` are applied by
`ActionExecutor` (texts, staff alerts, bookings) → channel renders the reply (JSON, TwiML, SSE).
New behavior is a new handler, not an edit to the conversation handler.

## Not yet ported from the legacy app

Done here: web widget (same front end), SMS, Twilio voice, Vapi, bookings with confirmations and staff
alerts, call summaries/handoffs/transfers, link and CARFAX sending, reminders, cold follow-ups,
opt-out, usage metering, catalog upload.

Still only in `app.py` (port as handlers or jobs when needed):
- Deterministic shortcut answers from `_process_message` (hours replies, MENU, price-threshold lists,
  reconfirmation/cancellation flows) and the reply rewriters (STEP 1.5 injection, day-in-time, name fix).
- Per-turn voice grounding directives (disambiguation, price, fuel, body style, absence, handoff, trade-in).
- Win-back follow-ups; the "called but didn't book" lead when a caller hangs up without a token
  (needs a Twilio status callback).
- Front-door call router (`/incoming`, ring-first / after-hours routing) and demo-line profile switching.
- Google Sheets as the tenant source (tenants are JSON files now), email notifications, admin UI pages,
  Twilio signature validation, SMS abuse rate limiting, prompt-cache warming.
