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
