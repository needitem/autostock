from __future__ import annotations

from pathlib import Path
from typing import Any


def build_symbol_strategy_importable_config(profile: dict[str, Any]) -> dict[str, Any]:
    nautilus = profile.get("nautilus", {}) if isinstance(profile.get("nautilus"), dict) else {}
    return {
        "strategy_path": "nautilus_trader.examples.strategies.ema_cross:EMACross",
        "config_path": "nautilus_trader.examples.strategies.ema_cross:EMACrossConfig",
        "config": {
            "instrument_id": nautilus.get("instrument_id"),
            "bar_type": nautilus.get("bar_type"),
            "trade_size": str(nautilus.get("trade_size", "10")),
            "fast_ema_period": int(nautilus.get("fast_ema_period", 10)),
            "slow_ema_period": int(nautilus.get("slow_ema_period", 20)),
            "subscribe_quote_ticks": bool(nautilus.get("subscribe_quote_ticks", False)),
            "subscribe_trade_ticks": bool(nautilus.get("subscribe_trade_ticks", True)),
            "request_bars": bool(nautilus.get("request_bars", True)),
        },
    }


def build_symbol_data_paths(base_dir: str | Path, symbol: str) -> dict[str, str]:
    root = Path(base_dir)
    slug = str(symbol).lower()
    return {
        "news_events": str(root / f"{slug}_news_events.jsonl"),
        "macro_events": str(root / f"{slug}_macro_events.jsonl"),
        "signal_snapshot": str(root / f"{slug}_signal_snapshot.json"),
        "bars_csv": str(root / f"{slug}_bars.csv"),
    }


def build_symbol_backtest_stub(base_dir: str | Path, profile: dict[str, Any]) -> dict[str, Any]:
    """
    Build a Nautilus-oriented run-config stub.

    This is intentionally a JSON-serializable template rather than a live executable
    BacktestRunConfig object, so it remains usable even when `nautilus_trader` is not
    installed in the current repo environment.
    """

    symbol = str(profile.get("primary_symbol", "TSLA")).upper()
    return {
        "engine": "nautilus_trader",
        "mode": f"{symbol.lower()}_ema_cross_backtest_stub",
        "data_paths": build_symbol_data_paths(base_dir, symbol),
        "strategy": build_symbol_strategy_importable_config(profile),
        "notes": [
            f"Import exported {symbol} bars into a Nautilus ParquetDataCatalog.",
            "Use the official NautilusTrader EMACross example strategy for the backtest.",
            "Keep the exported runtime and signal snapshot as Telegram/UI context only.",
        ],
    }
