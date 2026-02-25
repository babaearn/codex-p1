from __future__ import annotations

from typing import Any

from project_phantom.core.types import Candle, Direction


def _bool_from_directional_value(value: Any, direction: Direction) -> bool:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    if direction == "LONG":
        return number > 0
    return number < 0


def _heuristic_signals(candles: list[Candle], direction: Direction) -> tuple[bool, bool, dict[str, Any]]:
    if len(candles) < 12:
        return (False, False, {"backend": "heuristic", "reason": "INSUFFICIENT_CANDLES"})

    highs = [item.high for item in candles]
    lows = [item.low for item in candles]
    closes = [item.close for item in candles]
    opens = [item.open for item in candles]

    recent_high = max(highs[-7:-1])
    recent_low = min(lows[-7:-1])
    last_close = closes[-1]

    if direction == "LONG":
        choch = last_close > recent_high
        order_block = any(
            closes[idx] < opens[idx] and closes[idx + 1] > highs[idx]
            for idx in range(len(candles) - 8, len(candles) - 1)
        )
    else:
        choch = last_close < recent_low
        order_block = any(
            closes[idx] > opens[idx] and closes[idx + 1] < lows[idx]
            for idx in range(len(candles) - 8, len(candles) - 1)
        )

    return (
        choch,
        order_block,
        {
            "backend": "heuristic",
            "recent_high": recent_high,
            "recent_low": recent_low,
            "last_close": last_close,
        },
    )


class SmartMoneyConceptsDetector:
    name = "smartmoneyconcepts"

    async def detect(self, candles: list[Candle], direction: Direction) -> tuple[bool, bool, dict[str, Any]]:
        if len(candles) < 12:
            return _heuristic_signals(candles, direction)

        try:
            import pandas as pd  # type: ignore
            from smartmoneyconcepts import smc  # type: ignore
        except ModuleNotFoundError:
            return _heuristic_signals(candles, direction)

        df = pd.DataFrame(
            {
                "open": [item.open for item in candles],
                "high": [item.high for item in candles],
                "low": [item.low for item in candles],
                "close": [item.close for item in candles],
                "volume": [item.volume for item in candles],
            }
        )

        try:
            swings = smc.swing_highs_lows(df, swing_length=5)
            bos_choch = smc.bos_choch(df, swings)
            order_blocks = smc.ob(df, swings)
        except Exception:
            return _heuristic_signals(candles, direction)

        choch_signal = False
        order_block_signal = False

        choch_col = next(
            (col for col in bos_choch.columns if str(col).lower().replace("_", "") == "choch"),
            None,
        )
        if choch_col is not None:
            last_choch = bos_choch[choch_col].dropna()
            if not last_choch.empty:
                choch_signal = _bool_from_directional_value(last_choch.iloc[-1], direction)

        ob_col = next(
            (col for col in order_blocks.columns if str(col).lower().replace("_", "") in {"ob", "orderblock"}),
            None,
        )
        if ob_col is not None:
            last_ob = order_blocks[ob_col].dropna()
            if not last_ob.empty:
                order_block_signal = _bool_from_directional_value(last_ob.iloc[-1], direction)

        return (
            choch_signal,
            order_block_signal,
            {
                "backend": "smartmoneyconcepts",
                "choch_col": choch_col,
                "order_block_col": ob_col,
            },
        )
