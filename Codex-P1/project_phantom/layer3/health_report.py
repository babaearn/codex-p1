from __future__ import annotations

import time
from typing import Any

import aiohttp

from project_phantom.config import ExchangeEndpoints
from project_phantom.core.types import HealthCounters


def _status_line(label: str, ok: bool, detail: str) -> str:
    icon = "OK" if ok else "FAIL"
    return f"{label:<20} : {icon:<4} {detail}"


def _safe_reconnect_total(counters: HealthCounters) -> int:
    return sum(counters.reconnects.values())


async def _simple_get_json(session: aiohttp.ClientSession, url: str, params: dict[str, Any] | None = None) -> tuple[bool, str]:
    try:
        async with session.get(url, params=params, timeout=8) as response:
            if response.status >= 400:
                return (False, f"http_{response.status}")
            await response.text()
            return (True, "reachable")
    except Exception as exc:
        return (False, exc.__class__.__name__)


async def _check_with_retries(
    session: aiohttp.ClientSession,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    retries: int = 2,
) -> tuple[bool, str]:
    last_detail = "unknown"
    for _ in range(max(1, retries)):
        ok, detail = await _simple_get_json(session, url, params=params)
        if ok:
            return (True, detail)
        last_detail = detail
    return (False, last_detail)


async def _okx_check_with_fallback(session: aiohttp.ClientSession, primary_rest: str) -> tuple[bool, str]:
    primary_url = f"{primary_rest.rstrip('/')}/api/v5/public/time"
    ok, detail = await _check_with_retries(session, primary_url, retries=2)
    if ok:
        return (True, "reachable")
    if detail.lower() != "timeouterror":
        return (False, detail)

    fallback_url = "https://my.okx.com/api/v5/public/time"
    fallback_ok, fallback_detail = await _check_with_retries(session, fallback_url, retries=2)
    if fallback_ok:
        return (True, "reachable_fallback")
    return (False, f"primary_timeout+fallback_{fallback_detail}")


async def run_public_api_checks(
    endpoints: ExchangeEndpoints,
    *,
    whale_alert_enabled: bool,
    whale_alert_api_key: str | None,
) -> dict[str, tuple[bool, str]]:
    async with aiohttp.ClientSession() as session:
        binance = await _check_with_retries(session, f"{endpoints.binance_rest.rstrip('/')}/fapi/v1/ping", retries=2)
        bybit = await _check_with_retries(session, f"{endpoints.bybit_rest.rstrip('/')}/v5/market/time", retries=2)
        okx = await _okx_check_with_fallback(session, endpoints.okx_rest.rstrip("/"))

        whale_alert: tuple[bool, str]
        if not whale_alert_enabled:
            whale_alert = (True, "disabled")
        elif not whale_alert_api_key:
            whale_alert = (False, "missing_api_key")
        else:
            whale_alert = await _simple_get_json(
                session,
                f"{endpoints.whale_alert_rest.rstrip('/')}/status",
                params={"api_key": whale_alert_api_key},
            )

    return {
        "BINANCE_PUBLIC": binance,
        "BYBIT_PUBLIC": bybit,
        "OKX_PUBLIC": okx,
        "WHALE_ALERT": whale_alert,
    }


async def run_binance_auth_check(
    *,
    enabled: bool,
    mode: str,
    api_key: str | None,
    api_secret: str | None,
    testnet: bool,
) -> tuple[bool, str]:
    if not enabled or mode.lower() != "live":
        return (True, "skipped_non_live")
    if not api_key or not api_secret:
        return (False, "missing_keys")
    try:
        from binance import AsyncClient  # type: ignore
    except ModuleNotFoundError:
        return (False, "python_binance_missing")

    client = None
    try:
        client = await AsyncClient.create(api_key=api_key, api_secret=api_secret, testnet=testnet)
        payload = await client.futures_account_balance()
        return (True, f"ok_balances={len(payload)}")
    except Exception as exc:
        return (False, exc.__class__.__name__)
    finally:
        if client is not None:
            try:
                await client.close_connection()
            except Exception:
                pass


def format_health_report(
    *,
    symbol: str,
    mode: str,
    queue_sizes: dict[str, int],
    counters: dict[str, HealthCounters],
    api_checks: dict[str, tuple[bool, str]],
    binance_auth_check: tuple[bool, str],
    env_presence: dict[str, bool],
) -> str:
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    lines = [
        f"PHANTOM HEALTH - {symbol}",
        "================================",
        f"timestamp            : {ts}",
        f"mode                 : {mode}",
        "",
        "ENV",
        _status_line("TG_BOT_TOKEN", env_presence.get("TG_BOT_TOKEN", False), ""),
        _status_line("TG_CHAT_ID", env_presence.get("TG_CHAT_ID", False), ""),
        _status_line("BINANCE_API_KEY", env_presence.get("BINANCE_API_KEY", False), ""),
        _status_line("BINANCE_API_SECRET", env_presence.get("BINANCE_API_SECRET", False), ""),
        "",
        "API",
    ]
    for key in ("BINANCE_PUBLIC", "BYBIT_PUBLIC", "OKX_PUBLIC", "WHALE_ALERT"):
        ok, detail = api_checks.get(key, (False, "not_checked"))
        lines.append(_status_line(key, ok, detail))
    lines.append(_status_line("BINANCE_AUTH", binance_auth_check[0], binance_auth_check[1]))
    lines += [
        "",
        "PIPELINE",
        f"q_layer0             : {queue_sizes.get('l0', 0)}",
        f"q_layer1             : {queue_sizes.get('l1', 0)}",
        f"q_layer2             : {queue_sizes.get('l2', 0)}",
        f"q_layer3             : {queue_sizes.get('l3', 0)}",
    ]

    for layer_name in ("layer0", "layer1", "layer2", "layer3"):
        counter = counters[layer_name]
        lines.append(
            f"{layer_name:<20}: emitted={counter.emitted_events} "
            f"reconnects={_safe_reconnect_total(counter)} queue_drops={counter.queue_drops}"
        )
    lines.append("================================")
    return "<pre>" + "\n".join(lines) + "</pre>"
