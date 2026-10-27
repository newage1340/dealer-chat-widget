/**
 * Picks which LLM provider serves each request, and meters every call.
 *
 * Routing rules, in order:
 * - each tier has an ordered list of routes (e.g. local GPU first, OpenAI second);
 * - a route at its concurrency limit is skipped while another route has room (load spillover);
 * - a retryable failure before any output falls through to the next route;
 *   once text has been streamed to the caller, a failure is raised instead (it cannot be taken back).
 */

import { ProviderError } from "./errors.ts";
import type { LlmProvider, LlmRequest, LlmStreamPart, ModelTier } from "./llm.ts";
import { PriceTable, type UsageSink } from "./usage.ts";

/**
 * `maxConcurrency` caps in-flight requests to this provider; null means unlimited.
 * A self-hosted GPU typically gets a small cap so overflow spills to a cloud route.
 */
export type Route = {
    readonly provider:       LlmProvider;
    readonly maxConcurrency: number | null;
};

export class LlmRouter implements LlmProvider {
    readonly name = "router";
    readonly #routes: Readonly<Partial<Record<ModelTier, readonly Route[]>>>;
    readonly #sink: UsageSink | null;
    readonly #prices: PriceTable;
    readonly #inflight = new Map<string, number>();

    constructor(
        routes: Readonly<Partial<Record<ModelTier, readonly Route[]>>>,
        sink: UsageSink | null,
        prices: PriceTable
    ) {
        this.#routes = routes;
        this.#sink   = sink;
        this.#prices = prices;
    }

    hasRoutes(): boolean {
        return Object.values(this.#routes).some(r => (r?.length ?? 0) > 0);
    }

    async *stream(request: LlmRequest, signal?: AbortSignal): AsyncIterable<LlmStreamPart> {
        const routes = this.#routes[request.tier] ?? this.#routes.fast ?? [];
        if (routes.length === 0) {
            throw new ProviderError(this.name, "unavailable", "no LLM providers configured");
        }
        const free = routes.filter(r => !this.#saturated(r));
        const order = free.length > 0
            ? free
            : routes;

        let lastError: ProviderError | null = null;
        for (const route of order) {
            const name = route.provider.name;
            let emitted = false;
            this.#inflight.set(name, (this.#inflight.get(name) ?? 0) + 1);
            try {
                for await (const part of route.provider.stream(request, signal)) {
                    if (part.kind === "finish") {
                        const usage = { ...part.usage, costUsd: PriceTable.estimate(this.#prices, part.usage) };
                        await this.#sink?.record(usage);
                        yield { ...part, usage };
                    } else {
                        emitted = true;
                        yield part;
                    }
                }
                return;
            } catch (e) {
                const error = e instanceof ProviderError
                    ? e
                    : new ProviderError(name, "bad_response", String(e), { cause: e });
                if (emitted || !error.retryable) {
                    throw error;
                }
                lastError = error;
            } finally {
                this.#inflight.set(name, (this.#inflight.get(name) ?? 1) - 1);
            }
        }
        throw lastError ?? new ProviderError(this.name, "unavailable", "all routes failed");
    }

    #saturated(route: Route): boolean {
        return route.maxConcurrency !== null
            && (this.#inflight.get(route.provider.name) ?? 0) >= route.maxConcurrency;
    }
}
