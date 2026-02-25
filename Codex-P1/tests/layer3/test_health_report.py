from __future__ import annotations

from project_phantom.core.types import HealthCounters
from project_phantom.layer3.health_report import format_health_report


def test_format_health_report_contains_pipeline_and_api_sections() -> None:
    report = format_health_report(
        symbol="BTCUSDT",
        mode="paper",
        queue_sizes={"l0": 1, "l1": 2, "l2": 3, "l3": 4},
        counters={
            "layer0": HealthCounters(emitted_events=5),
            "layer1": HealthCounters(emitted_events=6),
            "layer2": HealthCounters(emitted_events=7),
            "layer3": HealthCounters(emitted_events=8),
        },
        api_checks={
            "BINANCE_PUBLIC": (True, "reachable"),
            "BYBIT_PUBLIC": (False, "http_403"),
            "OKX_PUBLIC": (True, "reachable"),
            "WHALE_ALERT": (True, "disabled"),
        },
        binance_auth_check=(True, "skipped_non_live"),
        env_presence={
            "TG_BOT_TOKEN": True,
            "TG_CHAT_ID": True,
            "BINANCE_API_KEY": True,
            "BINANCE_API_SECRET": True,
        },
    )
    assert "PHANTOM HEALTH - BTCUSDT" in report
    assert "BINANCE_PUBLIC" in report
    assert "BYBIT_PUBLIC" in report
    assert "q_layer3             : 4" in report
    assert "layer3              : emitted=8" in report
