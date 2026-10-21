/**
 * An explicit success-or-failure value, for errors callers are expected to handle.
 * Exceptions remain for bugs and for failures nobody can act on locally.
 */

export type Result<T, E> =
    | { readonly ok: true;
        readonly value: T
    }
    | { readonly ok: false;
        readonly error: E
    };

export const Result = {
    ok<T>(value: T): Result<T, never> {
        return { ok: true, value };
    },

    err<E>(error: E): Result<never, E> {
        return { ok: false, error };
    },

    map<T, U, E>(result: Result<T, E>, f: (value: T) => U): Result<U, E> {
        return result.ok
			? Result.ok(f(result.value))
			: result;
    },

    andThen<T, U, E>(result: Result<T, E>, f: (value: T) => Result<U, E>): Result<U, E> {
        return result.ok
			? f(result.value)
			: result;
    },

    unwrapOr<T, E>(result: Result<T, E>, fallback: T): T {
        return result.ok
			? result.value
			: fallback;
    },
} as const;
