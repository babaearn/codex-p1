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
    binance_trade_ws: str = "wss://fstream.binance.com/ws"
    bybit_rest: str = "https://api.bybit.com"
    bybit_ws: str = "wss://stream.bybit.com/v5/public/linear"
    okx_rest: str = "https://www.okx.com"
    whale_alert_rest: str = "https://api.whale-alert.io/v1"


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


@dataclass
class Layer1Weights:
    whale_net_flow: float = 0.35
    twap_uniformity: float = 0.20
    cvd: float = 0.30
    stablecoin_inflow: float = 0.15


@dataclass
class Layer1ThresholdConfig:
    score_threshold: float = 0.60
    component_threshold: float = 0.50
    whale_notional_usd: float = 100_000.0
    whale_flow_scale_usd: float = 2_000_000.0
    twap_interval_cv_limit: float = 0.35
    cvd_scale_usd: float = 2_000_000.0
    stablecoin_inflow_scale_usd: float = 50_000_000.0
    min_component_hits: int = 2


@dataclass
class WhaleAlertConfig:
    enabled: bool = False
    api_key: str | None = None
    poll_interval_seconds: float = 20.0
    min_transfer_usd: float = 1_000_000.0


@dataclass
class Layer1Config:
    symbol: str = "BTCUSDT"
    cadence_seconds: float = 1.0
    trade_window_seconds: int = 180
    setup_ttl_seconds: int = 180
    queue_maxsize: int = 200
    min_trades_for_metrics: int = 20
    enable_binance_trades: bool = True
    thresholds: Layer1ThresholdConfig = field(default_factory=Layer1ThresholdConfig)
    weights: Layer1Weights = field(default_factory=Layer1Weights)
    backoff: BackoffConfig = field(default_factory=BackoffConfig)
    endpoints: ExchangeEndpoints = field(default_factory=ExchangeEndpoints)
    whale_alert: WhaleAlertConfig = field(default_factory=WhaleAlertConfig)

    @property
    def trade_window_ms(self) -> int:
        return self.trade_window_seconds * 1000

    @property
    def setup_ttl_ms(self) -> int:
        return self.setup_ttl_seconds * 1000


@dataclass
class Layer2ThresholdConfig:
    min_confirmations: int = 3
    absorption_score_min: float = 0.60
    trap_score_min: float = 0.70
    momentum_lookback_bars: int = 5
    momentum_min_return_pct: float = 0.0015


@dataclass
class Layer2Config:
    symbol: str = "BTCUSDT"
    cadence_seconds: float = 2.0
    rest_poll_interval_seconds: float = 3.0
    setup_ttl_seconds: int = 180
    queue_maxsize: int = 200
    candle_interval: str = "1m"
    candle_limit: int = 200
    enable_smartmoneyconcepts: bool = True
    thresholds: Layer2ThresholdConfig = field(default_factory=Layer2ThresholdConfig)
    backoff: BackoffConfig = field(default_factory=BackoffConfig)
    endpoints: ExchangeEndpoints = field(default_factory=ExchangeEndpoints)

    @property
    def setup_ttl_ms(self) -> int:
        return self.setup_ttl_seconds * 1000
