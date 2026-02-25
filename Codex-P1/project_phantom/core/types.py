from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, AsyncIterator, Literal, Optional, Protocol

Direction = Literal["LONG", "SHORT"]
TrapEventType = Literal["TRAP_SETUP_EVENT"]
AbsorptionEventType = Literal["ABSORPTION_EVENT"]
PrePumpEventType = Literal["PRE_PUMP_EVENT"]
ExecutionEventType = Literal["EXECUTION_EVENT"]
EventType = Literal["TRAP_SETUP_EVENT", "ABSORPTION_EVENT", "PRE_PUMP_EVENT", "EXECUTION_EVENT"]


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
    event_type: TrapEventType
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
    last_emitted_ts_ms: int | None = None

    def increment_reconnect(self, exchange: str) -> None:
        self.reconnects[exchange] = self.reconnects.get(exchange, 0) + 1

    def mark_emitted(self, ts_ms: int) -> None:
        self.emitted_events += 1
        self.last_emitted_ts_ms = ts_ms


@dataclass
class TradeTick:
    exchange: str
    symbol: str
    price: float
    quantity: float
    is_buyer_maker: bool
    ts_ms: int

    @property
    def notional(self) -> float:
        return self.price * self.quantity


@dataclass
class StablecoinFlowObservation:
    source: str
    inflow_usd: float
    ts_ms: int


@dataclass
class AbsorptionBreakdown:
    whale_net_flow_long: float
    whale_net_flow_short: float
    twap_uniformity_long: float
    twap_uniformity_short: float
    cvd_long: float
    cvd_short: float
    stablecoin_inflow: float
    hidden_divergence_long: bool
    hidden_divergence_short: bool

    def score_components(self, direction: Direction) -> tuple[float, float, float, float]:
        if direction == "LONG":
            return (
                self.whale_net_flow_long,
                self.twap_uniformity_long,
                self.cvd_long,
                self.stablecoin_inflow,
            )
        return (
            self.whale_net_flow_short,
            self.twap_uniformity_short,
            self.cvd_short,
            self.stablecoin_inflow,
        )

    def hidden_divergence_for(self, direction: Direction) -> bool:
        if direction == "LONG":
            return self.hidden_divergence_long
        return self.hidden_divergence_short


@dataclass
class AbsorptionEvent:
    event_type: AbsorptionEventType
    event_id: str
    ts_ms: int
    symbol: str
    direction: Direction
    score: float
    passed: bool
    source_trap_event_id: str
    components: AbsorptionBreakdown
    raw: dict[str, Any]
    degraded: bool
    degrade_reason: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["components"] = asdict(self.components)
        return payload


@dataclass
class Candle:
    open_time_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time_ms: int


@dataclass
class IgnitionBreakdown:
    choch: bool
    order_block: bool
    absorption_strength: bool
    trap_strength: bool
    momentum: bool
    confirmations: int

    def for_direction(self, direction: Direction) -> tuple[bool, bool, bool, bool, bool]:
        # Layer 2 confirmations are directional at evaluation time.
        _ = direction
        return (
            self.choch,
            self.order_block,
            self.absorption_strength,
            self.trap_strength,
            self.momentum,
        )


@dataclass
class PrePumpEvent:
    event_type: PrePumpEventType
    event_id: str
    ts_ms: int
    symbol: str
    direction: Direction
    score: float
    passed: bool
    source_absorption_event_id: str
    source_trap_event_id: str
    components: IgnitionBreakdown
    raw: dict[str, Any]
    degraded: bool
    degrade_reason: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["components"] = asdict(self.components)
        return payload


@dataclass
class ExecutionPlan:
    entry: float
    sl: float
    tp1: float
    tp2: float
    rr: float
    sl_pct: float
    tp1_pct: float
    tp2_pct: float
    quantity: float
    risk_amount: float


@dataclass
class ExecutionEvent:
    event_type: ExecutionEventType
    event_id: str
    ts_ms: int
    symbol: str
    direction: Direction
    passed: bool
    source_pre_pump_event_id: str
    plan: ExecutionPlan
    order_ids: dict[str, str]
    raw: dict[str, Any]
    degraded: bool
    degrade_reason: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["plan"] = asdict(self.plan)
        return payload


class ExchangeClient(Protocol):
    name: str

    async def fetch_snapshot(self, symbol: str) -> ExchangeSnapshot:
        ...

    async def stream_liquidations(self, symbol: str) -> AsyncIterator[LiquidationUpdate]:
        ...

    async def close(self) -> None:
        ...


class TradeStreamClient(Protocol):
    name: str

    async def stream_trades(self, symbol: str) -> AsyncIterator[TradeTick]:
        ...

    async def close(self) -> None:
        ...


class StablecoinFlowClient(Protocol):
    name: str

    async def fetch_inflow_usd(self) -> StablecoinFlowObservation:
        ...

    async def close(self) -> None:
        ...


class CandleClient(Protocol):
    name: str

    async def fetch_candles(self, symbol: str, interval: str, limit: int) -> list[Candle]:
        ...

    async def close(self) -> None:
        ...


class SMCDetector(Protocol):
    name: str

    async def detect(self, candles: list[Candle], direction: Direction) -> tuple[bool, bool, dict[str, Any]]:
        ...


class FuturesExecutionClient(Protocol):
    name: str

    async def futures_create_order(self, **kwargs: Any) -> dict[str, Any]:
        ...

    async def close(self) -> None:
        ...


class TelegramNotifier(Protocol):
    name: str

    async def send_message(self, text: str) -> None:
        ...

    async def close(self) -> None:
        ...
