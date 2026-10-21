/**
 * Failures from external services, classified so the router knows whether to try another route.
 */

import type { ValueOf } from "../core/index.ts";

export const ProviderFailure = {
    Timeout:     "timeout",
    Unavailable: "unavailable",
    RateLimited: "rate_limited",
    Auth:        "auth",
    BadRequest:  "bad_request",
    BadResponse: "bad_response",
} as const;
export type ProviderFailure = ValueOf<typeof ProviderFailure>;

const RETRYABLE: ReadonlySet<ProviderFailure> = new Set([
    ProviderFailure.Timeout,
    ProviderFailure.Unavailable,
    ProviderFailure.RateLimited,
]);

export class ProviderError extends Error {
    readonly provider: string;
    readonly failure:  ProviderFailure;

    constructor(
        provider: string,
        failure: ProviderFailure,
        message: string,
        options?: ErrorOptions
    ) {
        super(`${provider}: ${message}`, options);
        this.name     = "ProviderError";
        this.provider = provider;
        this.failure  = failure;
    }

    /**
     * Whether another route (or a later attempt) might succeed.
     */
    get retryable(): boolean {
        return RETRYABLE.has(this.failure);
    }
}
