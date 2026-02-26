from __future__ import annotations

from project_phantom.run_bot import _apply_fanout_cap


def test_apply_fanout_cap_trims_when_above_limit() -> None:
    symbols = [f"S{i}USDT" for i in range(30)]
    trimmed = _apply_fanout_cap(symbols, hard_cap_symbols=25, allow_unsafe_fanout=False)
    assert len(trimmed) == 25
    assert trimmed == symbols[:25]


def test_apply_fanout_cap_allows_when_unsafe_enabled() -> None:
    symbols = [f"S{i}USDT" for i in range(30)]
    output = _apply_fanout_cap(symbols, hard_cap_symbols=25, allow_unsafe_fanout=True)
    assert output == symbols
