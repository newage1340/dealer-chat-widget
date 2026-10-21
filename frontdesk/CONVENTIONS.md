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

## Style

- Doc comments open with `/**` on its own line and wrap at clause boundaries.
- Consecutive fields, enum members and assignments are column-aligned.
- Barrel files export one name per line.
- 4-space indentation. No auto-formatter: it would undo the alignment.
