from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class SignalWeights:
    liquidation: float = 0.40
    funding_oi: float = 0.30
    oi_divergence: float = 0.30


@dataclass
class ThresholdConfig:
    score_threshold: float = 0.70
    component_threshold: float = 0.50
    oi_div_spread_floor: float = 0.4
    oi_div_spread_span: float = 1.6
    rv_low_vol_threshold: float = 0.008
    compression_return_cap: float = 0.01
    funding_scale: float = 0.0005
    oi_pct_scale: float = 1.5
    oi_accel_scale: float = 1.0


@dataclass
class BackoffConfig:
    min_seconds: float = 2.0
    max_seconds: float = 60.0


@dataclass
class ExchangeEndpoints:
    binance_rest: str = "https://fapi.binance.com"
    binance_ws: str = "wss://fstream.binance.com/stream?streams=!forceOrder@arr"
    bybit_rest: str = "https://api.bybit.com"
    bybit_ws: str = "wss://stream.bybit.com/v5/public/linear"
    okx_rest: str = "https://www.okx.com"


@dataclass
class Layer0Config:
    symbol: str = "BTCUSDT"
    cadence_seconds: float = 15.0
    rest_poll_interval_seconds: float = 15.0
    snapshot_staleness_seconds: float = 45.0
    warmup_minutes: int = 5
    queue_maxsize: int = 200
    cluster_window_minutes: int = 90
    cluster_bin_size: float = 100.0
    cluster_decay_minutes: float = 45.0
    enable_binance: bool = True
    enable_bybit: bool = True
    enable_okx: bool = True
    enable_okx_liquidations: bool = False
    weights: SignalWeights = field(default_factory=SignalWeights)
    thresholds: ThresholdConfig = field(default_factory=ThresholdConfig)
    backoff: BackoffConfig = field(default_factory=BackoffConfig)
    endpoints: ExchangeEndpoints = field(default_factory=ExchangeEndpoints)

    @property
    def warmup_ms(self) -> int:
        return int(self.warmup_minutes * 60_000)

    @property
    def staleness_ms(self) -> int:
        return int(self.snapshot_staleness_seconds * 1000)
