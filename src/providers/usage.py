"""
Where usage records go, and how they are priced.

Providers report raw units only (tokens, segments, seconds).
Cost is estimated separately from a PriceTable,
so updating prices or comparing a self-hosted option never touches a provider.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Protocol

from core import UsageRecord, UsageUnit


class UsageSink(Protocol):
    def record(self, record: UsageRecord) -> None: ...


class ListUsageSink:
    """
    Keeps records in memory, for tests and local development.
    """

    def __init__(self) -> None:
        self.records: list[UsageRecord] = []

    def record(self, record: UsageRecord) -> None:
        self.records.append(record)


@dataclass(frozen=True, slots=True)
class PriceTable:
    """
    USD per single unit, keyed by (provider, model).

    Use model "" for services without models (e.g. SMS).
    Self-hosted providers can carry an amortized per-unit cost here
    so they compare fairly against per-call vendors.
    """

    prices: Mapping[tuple[str, str], Mapping[UsageUnit, float]] = field(default_factory=dict)

    def estimate(self, provider: str, model: str, units: Mapping[UsageUnit, float]) -> float | None:
        """
        Total cost of `units`, or None if any unit has no known price.
        """
        rates = self.prices.get((provider, model)) or self.prices.get((provider, ""))
        if rates is None or any(unit not in rates for unit in units):
            return None
        return sum(rates[unit] * amount for unit, amount in units.items())
