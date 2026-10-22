# Demo guide

A walkthrough of what the front desk does, using the eleven demo businesses. Every page here is deliberately
unstyled HTML; the styling hook is `public/settings/settings.css` (empty), and the markup has no classes,
inline styles or scripts, so it can be styled from scratch.

## Before the demo

- Run `pnpm seed-demo` once (loads the demo businesses and their catalogs), then `pnpm start`.
- Sign in at `/settings` with email blank and the admin token as the password.
- To demo by phone, open a business, go to Phone numbers, and add the Twilio number. That moves it from whichever
  business had it. Web chat for any business is at `/widget/<slug>` (e.g. `/widget/demo-nail-salon`).

## The pages

| Page | What it shows |
|---|---|
| `/onboarding` | A new client picks a business type and fills in one form: hours, the type's own questions, tone, languages, catalog, and a password for their login |
| `/settings` | All businesses (admin), with new sign-ups flagged "not reviewed" |
| `/settings/<slug>` | One business: details, tone, privacy, languages, schedule, catalog, notifications, blocked callers, logins, usage, conversations, billing |
| `/settings/<slug>/prompt` | Exactly what the assistant is told, for any sample customer message and channel |
| `/settings/quality` | Reply times, corrections, fallbacks, cost per turn and booking rates per prompt version and model, plus eval pass rates |

A business owner who signs in sees only their own business page.

## Things to try

| Business | Try this | What it shows |
|---|---|---|
| Golden Lotus (`demo-chinese-restaurant`) | "I want 2 General Tso's and a crab rangoon for pickup, what's my total?" | Totals computed from the menu, never guessed ($39.85). The menu comes from a CSV with the restaurant's own column names, mapped on the Catalog section |
| Lotus Nails (`demo-nail-salon`) | Start in Vietnamese, then say "English please" | Switching languages and back cleanly. On a call the voice switches too |
| Willow Creek Counseling (`demo-therapy-practice`) | "Is my wife Maria seeing one of your therapists?" then "Are you a real person?" | Confidentiality (never confirms who is a client), formal tone, honest about being a virtual assistant |
| Hermanos Reyes (`demo-auto-repair`) | "¿Cuánto cuesta un cambio de aceite sintético?" | Bilingual catalog. The owner's summaries are written in Spanish |
| Hartwell Home Group (`demo-real-estate`) | "Is Fishers a safe area?" then "Send me the listing for 11842 Brookstone Dr" | Fair Housing guardrails; real listing links sent by the system, never typed by the AI |
| Marquee Talent (`demo-talent-agency`) | "How much does it cost to get signed?" | Never charges for representation; protects minors |
| Puente English (`demo-esl-tutor`) | "Hola, quiero aprender inglés para mi trabajo" | A Spanish-first business |
| Byte Fix (`demo-computer-repair`) | "¿Hablan español?" | A single-language business politely saying so |
| Southside Boxing (`demo-boxing-gym`) | Ask about a free first class | Casual tone, waivers, youth rules |
| Circle City Plumbing (`demo-plumbing`) | On a call: "Water is pouring out of my ceiling!" | A shut-off tip first, then a live transfer in the same breath |
| Mass Ave Hair (`demo-hair-salon`) | Book a color appointment | The patch-test rule for first-time color clients |

## Settings worth showing

- **Assistant:** how formal it sounds (casual to formal), whether it admits being virtual, conversation length,
  and call limits (wait for speech, "still there?" prompts, hidden caller ID), all within platform bounds.
- **Languages:** which languages are offered, and resetting a customer whose language got stuck.
- **Privacy:** what gets scrubbed (card numbers, SSNs, bank numbers, birth dates), confidential mode, and optional
  crisis support (off by default).
- **Catalog:** where the list comes from (pasted, sample file, CSV link like a Google Sheet, or JSON feed), with "Sync now".
- **Things to check:** setup problems the system spots, like hours that don't cover the week or unanswered questions.
