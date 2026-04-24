from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any

from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.config import BacktestEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import RiskEngineConfig
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.objects import Money
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog
from nautilus_trader.persistence.catalog.singleton import clear_singleton_instances
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.examples.strategies.ema_cross import EMACross
from nautilus_trader.examples.strategies.ema_cross import EMACrossConfig

from nautilus_v2.loader import load_tsla_bars_csv
from event_profile import load_event_profile


def _s(value: Any) -> str:
    return str(value or "").strip()


def tsla_instrument():
    return TestInstrumentProvider.equity(symbol="TSLA", venue="XNAS")


def tsla_bar_type() -> BarType:
    instrument = tsla_instrument()
    return BarType.from_str(f"{instrument.id}-1-DAY-LAST-EXTERNAL")


def _csv_rows_to_bars(bars_csv: str | Path) -> list[Bar]:
    frame = load_tsla_bars_csv(bars_csv)
    bar_type = tsla_bar_type()
    bars: list[Bar] = []
    for _, row in frame.iterrows():
        bars.append(
            Bar.from_dict(
                {
                    "type": "Bar",
                    "bar_type": str(bar_type),
                    "open": f"{float(row['open']):.2f}",
                    "high": f"{float(row['high']):.2f}",
                    "low": f"{float(row['low']):.2f}",
                    "close": f"{float(row['close']):.2f}",
                    "volume": str(int(float(row["volume"]))),
                    "ts_event": int(row["ts_event"]),
                    "ts_init": int(row["ts_event"]),
                }
            )
        )
    return bars


def setup_catalog(path: str | Path) -> ParquetDataCatalog:
    catalog_path = Path(path).resolve()
    clear_singleton_instances(ParquetDataCatalog)
    catalog = ParquetDataCatalog(path=catalog_path.as_posix(), fs_protocol="file")
    if not catalog.fs.exists(catalog.path):
        catalog.fs.mkdir(catalog.path, create_parents=True)
    return catalog


def import_tsla_bundle_to_catalog(bundle_dir: str | Path, catalog_dir: str | Path) -> dict[str, Any]:
    bundle = Path(bundle_dir).resolve()
    catalog = setup_catalog(catalog_dir)
    instrument = tsla_instrument()
    bars = _csv_rows_to_bars(bundle / "tsla_bars.csv")

    catalog.write_data([instrument])
    catalog.write_data(bars)

    return {
        "catalog_path": str(Path(catalog.path)),
        "bar_count": len(bars),
        "news_count": 0,
        "macro_count": 0,
        "instrument_id": str(instrument.id),
        "bar_type": str(tsla_bar_type()),
    }


def run_tsla_backtest_in_memory(bundle_dir: str | Path) -> dict[str, Any]:
    bundle = Path(bundle_dir).resolve()
    profile = load_event_profile("tsla")
    nautilus = profile.get("nautilus", {}) if isinstance(profile.get("nautilus"), dict) else {}
    instrument = tsla_instrument()
    bar_type = tsla_bar_type()
    bars = _csv_rows_to_bars(bundle / "tsla_bars.csv")

    engine = BacktestEngine(
        config=BacktestEngineConfig(
            logging=LoggingConfig(log_level="INFO", bypass_logging=True),
            risk_engine=RiskEngineConfig(bypass=True),
        )
    )
    engine.add_venue(
        venue=Venue("XNAS"),
        oms_type=OmsType.NETTING,
        account_type=AccountType.CASH,
        base_currency=USD,
        starting_balances=[Money(100_000, USD)],
        book_type=BookType.L1_MBP,
        bar_execution=True,
    )
    engine.add_instrument(instrument)
    engine.add_data(bars)

    strategy = EMACross(
        EMACrossConfig(
            instrument_id=instrument.id,
            bar_type=bar_type,
            trade_size=Decimal(str(nautilus.get("trade_size", "10"))),
            fast_ema_period=int(nautilus.get("fast_ema_period", 10)),
            slow_ema_period=int(nautilus.get("slow_ema_period", 20)),
            subscribe_quote_ticks=bool(nautilus.get("subscribe_quote_ticks", False)),
            subscribe_trade_ticks=bool(nautilus.get("subscribe_trade_ticks", True)),
            request_bars=bool(nautilus.get("request_bars", True)),
        )
    )
    engine.add_strategy(strategy)
    engine.run()
    result = engine.get_result()
    summary: dict[str, Any] = {
        "engine": "BacktestEngine",
        "strategy": "official_ema_cross",
        "bar_count": len(bars),
        "news_count": 0,
        "macro_count": 0,
        "fast_ema_period": int(nautilus.get("fast_ema_period", 10)),
        "slow_ema_period": int(nautilus.get("slow_ema_period", 20)),
    }
    if result is not None:
        summary["result_type"] = type(result).__name__
        for attr in ("run_id", "run_config_id", "instance_id"):
            if hasattr(result, attr):
                summary[attr] = _s(getattr(result, attr))
        if hasattr(result, "stats_pnls"):
            summary["stats_pnls"] = getattr(result, "stats_pnls")
        if hasattr(result, "stats_returns"):
            summary["stats_returns"] = getattr(result, "stats_returns")
        summary["repr"] = repr(result)
    return summary
