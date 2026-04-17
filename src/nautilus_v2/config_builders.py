from __future__ import annotations

from pathlib import Path
from typing import Any


def build_symbol_strategy_importable_config(profile: dict[str, Any]) -> dict[str, Any]:
    nautilus = profile.get("nautilus", {}) if isinstance(profile.get("nautilus"), dict) else {}
    return {
        "strategy_path": "nautilus_v2.strategy:TslaEventStrategy",
        "config_path": "nautilus_v2.strategy:TslaEventStrategyConfig",
        "config": {
            "instrument_id": nautilus.get("instrument_id"),
            "bar_type": nautilus.get("bar_type"),
            "trade_size": str(nautilus.get("trade_size", "10")),
            "custom_data_client_id": nautilus.get("custom_data_client_id", "CUSTOM"),
            "news_buy_threshold": float(nautilus.get("news_buy_threshold", 0.85)),
            "news_sell_threshold": float(nautilus.get("news_sell_threshold", -0.85)),
            "min_volume_ratio_for_entry": float(nautilus.get("min_volume_ratio_for_entry", 1.5)),
            "allow_entries_in_risk_off": bool(nautilus.get("allow_entries_in_risk_off", False)),
            "signal_name": nautilus.get("signal_name"),
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
        "mode": f"{symbol.lower()}_event_backtest_stub",
        "data_paths": build_symbol_data_paths(base_dir, symbol),
        "strategy": build_symbol_strategy_importable_config(profile),
        "notes": [
            f"Import exported {symbol} bars into a Nautilus ParquetDataCatalog or wrangler pipeline.",
            f"Import custom {symbol} news/macro events as custom data and subscribe to them in strategy.on_data.",
            "Use the exported signal snapshot only as a sanity-check artifact, not as a trading input.",
        ],
    }
