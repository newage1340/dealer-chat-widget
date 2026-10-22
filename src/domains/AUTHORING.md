# Domain packs

A domain pack teaches the assistant one kind of business (car dealership, plumber, nail salon, ...).
It is a single TOML file in `src/domains/packs/<id>.toml`. Tenants pick a pack with `"domain": "<id>"`.

## How a prompt is assembled

```
universal base (src/prompts/templates/base_chat.md or base_voice.md)
  + "=== <PACK NAME> SPECIFICS ==="  ← prompts.chat (+ prompts.voice on calls) + matching toggles
  + BUSINESS INFO                     ← tenant fields + answers to the pack's [[profile]] questions
  + <catalog heading>                 ← tenant catalog, formatted by [catalog]
  + TOP MATCHING DETAILS, CUSTOMER PROFILE, APPOINTMENT, DATE/HOURS (computed per turn)
```

The base already handles persona honesty, style, grounding, URLs, the booking steps,
META_JSON, call tokens and business hours. Packs supply only domain specifics. The base's
booking steps pull in the pack's `[booking]` settings (visit noun, intake questions,
whether the visit happens at the customer's address), and the call summary gets one
section per intake field.

## Adding a domain

1. **Generate it:** `python src/main.py author-domain "nail salon"` (uses the configured LLM;
   validates, gives the model one repair attempt, saves `packs/<id>.toml`).
   Or print the prompt leader and use any model you like:
   `python src/main.py author-prompt "nail salon" > leader.md`.
   To write one by hand, copy `packs/_template.toml`.
2. **Validate it:** `python src/main.py validate-domain src/domains/packs/<id>.toml`.
   This reports every problem: missing keys, wrong types, unknown placeholders,
   catalog templates that reference fields that don't exist.
3. **Add a tenant:** create `data/tenants/<slug>.json` with `"domain": "<id>"`, the profile
   answers, and a catalog whose items use the pack's field names (or its `[catalog.aliases]`).
   See `data/tenants/demo-plumber.json`.
4. **Inspect the prompt:** `python src/main.py prompt <slug> voice "a realistic first message"`.
5. **Reload without restarting:** `POST /admin/reload?token=$ADMIN_TOKEN`.

## Rules of thumb for pack text

- Write short imperative bullets under CAPS headings, with realistic quoted examples.
- Put industry-specific safety rules, never-claims and common questions in `prompts.chat`
  (used on every channel). Put phone-only manner in `prompts.voice`.
- Anything that varies per business goes in a `[[profile]]` question or a tenant setting
  (used through `{setting_x}` or a `[[prompts.toggles]]` block), not in the pack.
- Never restate base rules. If a rule applies to every business, it belongs in the base templates.
