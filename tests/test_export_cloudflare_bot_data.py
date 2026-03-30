from __future__ import annotations

import pandas as pd

from scripts.export_cloudflare_bot_data import _resolve_entry_day


class _DummySelector:
    @staticmethod
    def _execution_pos(index: pd.Index, signal_day: pd.Timestamp, execution_timing: str) -> int:
        if execution_timing == "next_open":
            return len(index)
        return len(index) - 1


def test_resolve_entry_day_falls_back_to_next_business_day_when_next_open_bar_is_missing() -> None:
    index = pd.DatetimeIndex(["2026-03-27"])

    entry_day = _resolve_entry_day(
        _DummySelector(),
        index,
        pd.Timestamp("2026-03-27"),
        "next_open",
    )

    assert str(entry_day) == "2026-03-30"
