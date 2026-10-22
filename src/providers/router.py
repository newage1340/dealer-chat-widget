"""
Picks which LLM provider serves each request, and meters every call.

Routing rules, in order:
- each tier has an ordered list of routes (e.g. local GPU first, OpenAI second);
- a route that is at its concurrency limit is skipped while another route has room (load spillover);
- a retryable failure falls through to the next route; a non-retryable one is raised.
"""

import threading
import time
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace

from .errors import ProviderError, ProviderUnavailable
from .llm import LLMProvider, LLMRequest, LLMResponse, ModelTier
from .usage import PriceTable, UsageSink


@dataclass(frozen=True, slots=True)
class Route:
    """
    `max_concurrency` caps in-flight requests to this provider; None means unlimited.
    A self-hosted GPU typically gets a small cap so overflow spills to a cloud route.
    """

    provider:        LLMProvider
    max_concurrency: int | None = None


class LLMRouter:
    name = "router"

    def __init__(
        self,
        routes: Mapping[ModelTier, Sequence[Route]],
        *,
        sink:   UsageSink | None = None,
        prices: PriceTable | None = None,
    ):
        self._routes = {tier: tuple(rs) for tier, rs in routes.items() if rs}
        self._sink = sink
        self._prices = prices or PriceTable()
        self._inflight: dict[str, int] = {}
        self._lock = threading.Lock()

    def complete(self, request: LLMRequest) -> LLMResponse:
        routes = self._routes.get(request.tier) or self._routes.get(ModelTier.FAST) or ()
        if not routes:
            raise ProviderUnavailable(self.name, "no LLM providers configured")

        available = [r for r in routes if not self._saturated(r)] or list(routes)
        last_error: ProviderError | None = None
        for route in available:
            started = time.monotonic()
            try:
                with self._slot(route):
                    response = route.provider.complete(request)
            except ProviderError as e:
                last_error = e
                if e.retryable:
                    continue
                raise
            return self._meter(response, (time.monotonic() - started) * 1000)
        assert last_error is not None
        raise last_error

    def _saturated(self, route: Route) -> bool:
        if route.max_concurrency is None:
            return False
        with self._lock:
            return self._inflight.get(route.provider.name, 0) >= route.max_concurrency

    @contextmanager
    def _slot(self, route: Route):
        name = route.provider.name
        with self._lock:
            self._inflight[name] = self._inflight.get(name, 0) + 1
        try:
            yield
        finally:
            with self._lock:
                self._inflight[name] -= 1

    def _meter(self, response: LLMResponse, latency_ms: float) -> LLMResponse:
        usage = response.usage
        usage = replace(
            usage,
            cost_usd   = self._prices.estimate(response.provider, response.model, usage.units),
            latency_ms = usage.latency_ms if usage.latency_ms is not None else latency_ms,
        )
        if self._sink is not None:
            self._sink.record(usage)
        return replace(response, usage=usage)
