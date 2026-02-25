from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass

from project_phantom.core.types import Direction, LiquidationUpdate


@dataclass
class LiquidationProximity:
    long_score: float
    short_score: float
    long_distance_pct: float | None
    short_distance_pct: float | None
    short_cluster_p90: float
    long_cluster_p90: float


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    clamped = max(0.0, min(1.0, percentile))
    ordered = sorted(values)
    index = clamped * (len(ordered) - 1)
    low = int(math.floor(index))
    high = int(math.ceil(index))
    if low == high:
        return ordered[low]
    weight = index - low
    return ordered[low] * (1.0 - weight) + ordered[high] * weight


class LiquidationBook:
    def __init__(
        self,
        window_minutes: int = 90,
        bin_size: float = 100.0,
        decay_minutes: float = 45.0,
    ) -> None:
        self._events: deque[LiquidationUpdate] = deque()
        self._window_ms = int(window_minutes * 60_000)
        self._bin_size = bin_size
        self._decay_minutes = decay_minutes

    def add(self, event: LiquidationUpdate) -> None:
        self._events.append(event)
        self.prune(event.ts_ms)

    def prune(self, now_ms: int) -> None:
        cutoff = now_ms - self._window_ms
        while self._events and self._events[0].ts_ms < cutoff:
            self._events.popleft()

    def _bucket(self, price: float) -> float:
        return math.floor(price / self._bin_size) * self._bin_size

    def _decayed_bins(self, target_side: Direction, now_ms: int) -> dict[float, float]:
        buckets: dict[float, float] = {}
        for event in self._events:
            if event.liquidated_side != target_side:
                continue
            age_minutes = max(0.0, (now_ms - event.ts_ms) / 60_000.0)
            decay = math.exp(-age_minutes / self._decay_minutes)
            bucket = self._bucket(event.price)
            buckets[bucket] = buckets.get(bucket, 0.0) + (event.notional * decay)
        return buckets

    def _direction_score(
        self,
        bins: dict[float, float],
        current_price: float,
        need_above: bool,
    ) -> tuple[float, float | None, float]:
        if not bins:
            return (0.0, None, 0.0)

        p90 = _percentile(list(bins.values()), 0.90)
        if p90 <= 0.0:
            return (0.0, None, 0.0)

        best = 0.0
        best_distance: float | None = None
        for bucket_price, weighted_notional in bins.items():
            if need_above and bucket_price < current_price:
                continue
            if not need_above and bucket_price > current_price:
                continue

            distance_pct = abs(bucket_price - current_price) / current_price
            base = min(1.0, weighted_notional / p90)
            score = base * math.exp(-(distance_pct / 0.004))
            if score > best:
                best = score
                best_distance = distance_pct

        return (best, best_distance, p90)

    def proximity_scores(self, current_price: float, now_ms: int) -> LiquidationProximity:
        self.prune(now_ms)
        short_liq_bins = self._decayed_bins("SHORT", now_ms)
        long_liq_bins = self._decayed_bins("LONG", now_ms)

        long_score, long_distance, short_p90 = self._direction_score(
            short_liq_bins, current_price=current_price, need_above=True
        )
        short_score, short_distance, long_p90 = self._direction_score(
            long_liq_bins, current_price=current_price, need_above=False
        )

        return LiquidationProximity(
            long_score=long_score,
            short_score=short_score,
            long_distance_pct=long_distance,
            short_distance_pct=short_distance,
            short_cluster_p90=short_p90,
            long_cluster_p90=long_p90,
        )
