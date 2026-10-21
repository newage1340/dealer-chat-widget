You are writing a DOMAIN PACK for a customer-conversation assistant used by small businesses.
The assistant answers customers over web chat, SMS and phone, and books appointments.

The assistant's prompt is built in layers:
1. A UNIVERSAL BASE (already written - do not repeat it): persona honesty, communication style,
   "only use the data provided", no URLs, one question per message, replying in the customer's language,
   the booking steps (what it's for -> day and clock time -> intake questions -> email -> book with the tool),
   the tools (save details, book, send links, request a callback, transfer or end a call), business-hours
   handling, and staff-handoff rules.
2. YOUR DOMAIN PACK: everything specific to one kind of business - its vocabulary,
   what it offers (the catalog shape), what to ask before booking, how its staff talk,
   its common questions, pitfalls, safety rules, and objection handling.
3. Per-business facts (name, hours, catalog, policy answers) filled in at runtime.

Write a complete domain pack for: **{business_type}**
{extra_context}

REQUIREMENTS
- Output ONE TOML document and nothing else (no commentary before or after, no code fence).
- Follow the TEMPLATE's shape exactly: every table and key it shows, same names and types.
- Only use the placeholders listed in the template's header comment. Write literal braces as {{ and }}.
- `[domain].id` must be lowercase letters/underscores.
- Profile questions: 4-7 questions an owner of this business would answer once (pricing policy, service area,
  cancellation/deposit policy, credentials, specialties...). Keys are short snake_case.
- Catalog: model what customers ask about (services, menu items, packages, listings). Mark the fields
  customers search by as searchable. Give price fields an empty_text such as "Quoted in person".
- Booking: pick `location = "customer"` if staff travel to the customer, otherwise "business".
  Intake: 2-4 questions this business genuinely needs before an appointment, each with clear "ask" guidance
  (what to ask, when to skip, never re-ask what they already said).
- Widget topics: 6-10 shortcuts a customer would actually click, grouped into 2-4 sections.
- Classification topics ([[topics]]): 5-10 categories the owner would sort finished conversations by
  (e.g. emergency, pricing, a main service line). Ids are snake_case; never include "other" (it is automatic).
- Follow-ups: confirm, reminder and cold texts. Give each as a table with English (en) and Spanish (es).
- prompts.chat and prompts.voice: 15-40 short, concrete, imperative bullets each, grouped under CAPS headings.
  Include: domain vocabulary, the most common customer questions and how to answer them from the data,
  things the assistant must NEVER claim or promise in this industry, safety or emergency handling if relevant,
  and how staff in this industry naturally talk on the phone. Use realistic examples in quotes.
- Do NOT restate universal rules (listed above in layer 1).

TEMPLATE (the required shape, with comments explaining each key):
{template}

A COMPLETE EXAMPLE PACK (plumbing) for reference - match its depth and tone, not its content:
{example}
