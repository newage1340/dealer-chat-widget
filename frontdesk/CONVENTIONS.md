# Conventions

## Data and behavior ("impl" blocks)

Data is plain, readonly, JSON-shaped `type`s. Behavior for a type lives in a same-named
`const` object, the TypeScript stand-in for a Rust `impl` block:

```ts
export type Contact = {
    readonly firstName: string;
    readonly lastName:  string;
};

export const Contact = {
    displayName(contact: Contact): string { ... },
} as const;

Contact.displayName(c);        // like Contact::display_name(&c)
```

TypeScript keeps types and values in separate namespaces, so one name serves both.
Data never carries methods, so it stores, serializes and crosses process boundaries unchanged.

## Enums

Rust-style enums with data are tagged unions on a `kind` field. Exhaustive handling is a
`switch` the compiler checks (`assertNever` in the default case), or ts-pattern's
`match(...).exhaustive()`. Plain value enums are `as const` objects plus `ValueOf`:

```ts
export const Channel = { Web: "web", Sms: "sms", Voice: "voice" } as const;
export type Channel = ValueOf<typeof Channel>;   // "web" | "sms" | "voice"
```

TypeScript `enum`, `namespace` and constructor parameter properties are not used
(`erasableSyntaxOnly`), so Node runs the source directly.

## Traits

Capabilities that have several implementations (providers, storage, policies, tools) are
`interface`s. Stateful implementations are classes that `implements` them; stateless ones can be
object literals checked with `satisfies`.

## Types at the edges

Ids are branded strings (`TenantId`, `PhoneNumber`), made only through their constructor
functions. Anything from outside the process (webhooks, model output, database rows, pack
files) is parsed with zod before it becomes a core type. Expected failures are returned as
`Result<T, E>`; exceptions are for bugs and for provider failures (`ProviderError`).

## Shared building blocks

Reach for these before writing the same thing inline:

- `z.enum(X)` validates an `as const` enum object directly; `enumValue(X, raw)` reads one from untrusted
  input (query strings, env vars), returning null for anything else.
- `Problem` (`{ path, message }`, `Problem.fromIssues`, `Problem.format`) for everything wrong with a file or form;
  parsers return `Result<T, Problem[]>`.
- `jsonBody(c, Schema)` and `formBody(c)` with `one`/`many`/`on`/`num` (http/) read request bodies.
- `providerFetch`, `checkStatus` and `readJson` (providers/http.ts) call vendor APIs, and
  `UsageRecord.measured` meters them.
- `publishEvents` / `eventsAndJobs` / `recordMessage` (pipeline/services.ts) write events and messages outside a turn.

Where to add things:

- A prompt section: a function in prompts/sections.ts and an entry in `STABLE_SECTIONS` or `PER_TURN_SECTIONS`.
  A placeholder: its name in domains/template.ts; the compiler then asks for its value in prompts/values.ts.
- A settings section or form: an entry in `SETTINGS_SECTIONS` (http/settings/sections.ts); its route is generated.
- A store: a port in storage/ports.ts and a file under storage/postgres/, wired in storage/postgres.ts.
  JSON columns are read with a zod schema from storage/postgres/columns.ts.
- A tenant setting: the zod `TenantFile`, `toTenant` and `fromTenant` (tests/tenants checks they agree).

## Style

- Doc comments open with `/**` on its own line and wrap at clause boundaries.
- Consecutive fields, enum members and assignments are column-aligned.
- Barrel files export one name per line.
- 4-space indentation. No auto-formatter: it would undo the alignment.
