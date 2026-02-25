from __future__ import annotations

from typing import Any

from project_phantom.core.types import ExecutionPlan, PrePumpEvent


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _pct(value: float) -> str:
    return f"{value * 100:+.2f}%"


def _check(flag: bool) -> str:
    return "✅" if flag else "❌"


def _extract_source_trap_raw(event: PrePumpEvent) -> dict[str, Any]:
    source_absorption_raw = event.raw.get("source_absorption_raw")
    if not isinstance(source_absorption_raw, dict):
        return {}
    source_trap_raw = source_absorption_raw.get("source_trap_raw")
    if not isinstance(source_trap_raw, dict):
        return {}
    return source_trap_raw


def _extract_absorption_components(event: PrePumpEvent) -> dict[str, Any]:
    data = event.raw.get("source_absorption_components")
    if isinstance(data, dict):
        return data
    return {}


def format_telegram_signal(
    event: PrePumpEvent,
    plan: ExecutionPlan,
    *,
    order_ids: dict[str, str] | None = None,
) -> str:
    trap_raw = _extract_source_trap_raw(event)
    absorption_components = _extract_absorption_components(event)

    liq_swept = _to_float(trap_raw.get("short_cluster_p90_notional") or trap_raw.get("long_cluster_p90_notional"))
    funding_rate = _to_float(trap_raw.get("avg_funding"))
    oi_note = trap_raw.get("oi_spread_pct")
    oi_spread = _to_float(oi_note)
    oi_note_text = f"{oi_spread:.2f}%" if oi_note is not None else "n/a"
    choch_flag = _check(bool(event.components.choch))

    cvd_long = _to_float(absorption_components.get("cvd_long"))
    cvd_short = _to_float(absorption_components.get("cvd_short"))
    hidden_long = bool(absorption_components.get("hidden_divergence_long", False))
    hidden_short = bool(absorption_components.get("hidden_divergence_short", False))
    if event.direction == "LONG":
        cvd_flag = _check(cvd_long >= 0.5 or hidden_long)
    else:
        cvd_flag = _check(cvd_short >= 0.5 or hidden_short)

    score_100 = round(event.score * 100)
    direction_title = "REVERSAL IMMINENT"
    if event.direction == "SHORT":
        direction_title = "REVERSAL DOWN IMMINENT"

    liq_flag = _check(liq_swept > 0)
    funding_flag = _check(abs(funding_rate) > 0)
    oi_flag = _check(oi_spread > 0.4)
    funding_state = "reset"
    if funding_rate < 0:
        funding_state = "neg"
    elif funding_rate > 0:
        funding_state = "pos"

    ids_line = ""
    if order_ids:
        entry_id = order_ids.get("entry", "n/a")
        sl_id = order_ids.get("sl", "n/a")
        tp1_id = order_ids.get("tp1", "n/a")
        tp2_id = order_ids.get("tp2", "n/a")
        ids_line = f"\nORDERS: E#{entry_id} SL#{sl_id} TP1#{tp1_id} TP2#{tp2_id}\n"

    return (
        "<pre>"
        f"🎯 PHANTOM SIGNAL - {event.symbol}\n"
        "================================\n"
        f"TRAP DETECTED -> {direction_title}\n\n"
        f"Liquidation Swept : ${liq_swept:,.0f} {liq_flag}\n"
        f"Funding Rate      : {funding_rate:+.4%} -> {funding_state} {funding_flag}\n"
        f"Cross-Exchange OI : {oi_note_text} {oi_flag}\n"
        f"CHoCH 5m          : {choch_flag}\n"
        f"CVD Divergence    : {cvd_flag}\n\n"
        f"SCORE: {score_100}/100\n\n"
        f"Entry : ${plan.entry:,.2f}\n"
        f"SL    : ${plan.sl:,.2f} ({_pct(plan.sl_pct)})\n"
        f"TP1   : ${plan.tp1:,.2f} ({_pct(plan.tp1_pct)})\n"
        f"TP2   : ${plan.tp2:,.2f} ({_pct(plan.tp2_pct)})\n"
        f"R:R   : 1:{plan.rr:.2f}"
        f"{ids_line}"
        "================================"
        "</pre>"
    )
