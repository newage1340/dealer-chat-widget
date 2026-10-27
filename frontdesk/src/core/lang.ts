/**
 * Small type-level tools the rest of the codebase leans on.
 */

declare const brand: unique symbol;

/**
 * A nominal type over a primitive.
 * At runtime a `Brand<string, "TenantId">` is just a string,
 * but the compiler will not let one stand in for another brand or a plain string.
 */
export type Brand<T, Name extends string> = T & { readonly [brand]: Name };

/**
 * The union of a const object's values.
 * Pairs with the "enum object" pattern: `const Channel = {...} as const; type Channel = ValueOf<typeof Channel>`.
 */
export type ValueOf<T> = T[keyof T];

/**
 * `raw` as a member of an `as const` enum object, or null when it isn't one (for untrusted input).
 */
export function enumValue<E extends Readonly<Record<string, string>>>(values: E, raw: unknown): ValueOf<E> | null {
    return (Object.values(values) as ValueOf<E>[]).find(v => v === raw) ?? null;
}

/**
 * A writable copy of a readonly object type, for building one up field by field.
 */
export type Mutable<T> = { -readonly [K in keyof T]: T[K] };

/**
 * The member of a tagged union whose `kind` is `K`.
 */
export type Variant<
	U extends { readonly kind: string }, K extends U["kind"]> = Extract<U, { readonly kind: K }>;

/**
 * Marks a branch the compiler has proven unreachable.
 * Use as the default of a `switch` over a union to make it exhaustive.
 */
export function assertNever(value: never, message = "unexpected variant"): never {
    throw new Error(`${message}: ${JSON.stringify(value)}`);
}

/**
 * The message of a caught value, for logs and error results.
 */
export function errorMessage(error: unknown): string {
    return error instanceof Error
        ? error.message
        : String(error);
}
