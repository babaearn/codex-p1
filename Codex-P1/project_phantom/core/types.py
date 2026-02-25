from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, AsyncIterator, Literal, Optional, Protocol

Direction = Literal["LONG", "SHORT"]
EventType = Literal["TRAP_SETUP_EVENT"]


@dataclass
class ExchangeSnapshot:
    exchange: str
    symbol: str
    open_interest: Optional[float]
    funding_rate: Optional[float]
    mark_price: Optional[float]
    ts_ms: int
    active: bool = True
    error: Optional[str] = None


@dataclass
class OIObservation:
    ts_ms: int
    open_interest: float


@dataclass
class LiquidationUpdate:
    exchange: str
    symbol: str
    price: float
    quantity: float
    notional: float
    liquidated_side: Direction
    ts_ms: int


@dataclass
class SignalBreakdown:
    liquidation_long: float
    liquidation_short: float
    funding_oi_long: float
    funding_oi_short: float
    oi_divergence: float

    def for_direction(self, direction: Direction) -> tuple[float, float, float]:
        if direction == "LONG":
            return (self.liquidation_long, self.funding_oi_long, self.oi_divergence)
        return (self.liquidation_short, self.funding_oi_short, self.oi_divergence)


@dataclass
class TrapSetupEvent:
    event_type: EventType
    event_id: str
    ts_ms: int
    symbol: str
    direction: Direction
    score: float
    passed: bool
    components: SignalBreakdown
    raw: dict[str, Any]
    degraded: bool
    degrade_reason: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["components"] = asdict(self.components)
        return payload


@dataclass
class HealthCounters:
    reconnects: dict[str, int] = field(default_factory=dict)
    stale_cycles: int = 0
    queue_drops: int = 0
    emitted_events: int = 0

    def increment_reconnect(self, exchange: str) -> None:
        self.reconnects[exchange] = self.reconnects.get(exchange, 0) + 1


class ExchangeClient(Protocol):
    name: str

    async def fetch_snapshot(self, symbol: str) -> ExchangeSnapshot:
        ...

    async def stream_liquidations(self, symbol: str) -> AsyncIterator[LiquidationUpdate]:
        ...

    async def close(self) -> None:
        ...
