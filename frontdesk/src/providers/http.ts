/**
 * Calling vendor HTTP APIs the same way in every adapter: a deadline that still honors the caller's own signal,
 * and failures classified as ProviderErrors, so the router knows whether another route might work.
 */

import type { z } from "zod";

import { ProviderError, ProviderFailure } from "./errors.ts";

/**
 * `timeoutMs` is the deadline for the response to start; `signal` is the caller's (a barge-in, a hang-up),
 * which can cancel sooner.
 */
export type Deadline = {
    readonly timeoutMs: number;
    readonly signal?:   AbortSignal | undefined;
};

/**
 * A signal that aborts at the deadline, or earlier when the caller's does.
 */
export function deadlineSignal(deadline: Deadline): AbortSignal {
    const timeout = AbortSignal.timeout(deadline.timeoutMs);
    return deadline.signal === undefined
        ? timeout
        : AbortSignal.any([deadline.signal, timeout]);
}

/**
 * How to treat an HTTP error status: worth trying elsewhere or later, or a mistake on our side.
 */
export function failureForStatus(status: number): ProviderFailure {
    if (status === 429) {
        return ProviderFailure.RateLimited;
    }
    if (status === 401 || status === 403) {
        return ProviderFailure.Auth;
    }
    if (status === 408 || status >= 500) {
        return ProviderFailure.Unavailable;
    }
    return ProviderFailure.BadRequest;
}

/**
 * `fetch` with a deadline. Network failures and timeouts throw ProviderErrors;
 * error statuses come back as responses, so the caller can read the vendor's message before `checkStatus`.
 */
export async function providerFetch(
    provider: string,
    url: string,
    init: Omit<RequestInit, "signal">,
    deadline: Deadline,
    fetchImpl: typeof fetch = fetch
): Promise<Response>
{
    try {
        return await fetchImpl(url, { ...init, signal: deadlineSignal(deadline) });
    } catch (e) {
        const timedOut = e instanceof DOMException && e.name === "TimeoutError";
        throw new ProviderError(provider, timedOut ? ProviderFailure.Timeout : ProviderFailure.Unavailable, String(e), { cause: e });
    }
}

/**
 * Throw a ProviderError for an error status. `message` is the vendor's explanation, when it gave one.
 */
export function checkStatus(provider: string, response: Response, message?: string | null): void {
    if (!response.ok) {
        throw new ProviderError(provider, failureForStatus(response.status), message || `HTTP ${response.status}`);
    }
}

/**
 * The response's JSON, checked against `schema` (whose fields should be optional, so error bodies fit too).
 * A body that doesn't fit is the vendor's error status if it sent one, else a bad response.
 */
export async function readJson<Schema extends z.ZodType>(
    provider: string,
    response: Response,
    schema: Schema
): Promise<z.output<Schema>>
{
    const parsed = schema.safeParse(await response.json().catch(() => ({})));
    if (parsed.success) {
        return parsed.data;
    }
    checkStatus(provider, response);
    throw new ProviderError(provider, ProviderFailure.BadResponse, `unexpected reply: ${parsed.error.message}`);
}
