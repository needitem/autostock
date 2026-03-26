from __future__ import annotations

import importlib.util
import json
import os
import sys
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
TARGET_TS = ROOT / "cloudflare" / "telegram-bot" / "src" / "snapshot-data.ts"
TARGET_JSON = ROOT / "cloudflare" / "telegram-bot" / "live" / "snapshot.json"
FALLBACK_STRATEGY_METRICS: dict[str, dict[str, Any]] = {
    "v2": {
        "label": "Strategy V2",
        "cagr": 31.24,
        "benchmarkCagr": 18.96,
        "drawdown": -39.93,
        "turnover": 0.272,
        "pAlphaGt0": 0.957,
    },
    "v14": {
        "label": "Strategy V14",
        "cagr": 29.18,
        "benchmarkCagr": 18.96,
        "drawdown": -36.64,
        "turnover": 0.289,
        "pAlphaGt0": 0.929,
    },
}


def _load_module(path: Path, name: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _latest_json(pattern: str) -> Path | None:
    files = sorted((ROOT / "data" / "runs").glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        return None
    return files[0]


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_bool(raw: str) -> bool:
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _build_current_signal(strategy_key: str, runner_name: str) -> dict[str, Any]:
    sys.path.insert(0, str(ROOT / "scripts"))
    runner = _load_module(ROOT / "scripts" / runner_name, f"runner_{strategy_key}_cf")
    runner._apply_defaults()
    os.environ["AI_END_DATE"] = str(date.today())
    selector = _load_module(ROOT / "scripts" / "backtest_ai_portfolio_selector.py", f"selector_{strategy_key}_cf")

    symbols = selector._parse_symbols(os.getenv("AI_SYMBOLS", ""))
    prefetch = []
    for spec in (
        os.getenv("AI_REGIME_RISK_ON", "TQQQ"),
        os.getenv("AI_REGIME_RISK_ON_ALT", "QLD"),
        os.getenv("AI_REGIME_NEUTRAL", "QLD"),
        os.getenv("AI_REGIME_RECOVERY", ""),
        os.getenv("AI_REGIME_RISK_OFF", "GLD"),
        os.getenv("AI_REGIME_CRASH", "GLD"),
        os.getenv("AI_REGIME_FILTER_SAFE", "BIL"),
        os.getenv("AI_REGIME_RISK_OFF_FALLBACK", ""),
        os.getenv("AI_REGIME_CRASH_FALLBACK", ""),
    ):
        prefetch.extend(selector._parse_allocation_spec(spec).keys())
    for raw in (os.getenv("AI_REGIME_RISK_OFF_POOL", ""), os.getenv("AI_REGIME_CRASH_POOL", "")):
        prefetch.extend(selector._parse_symbols(raw))
    for sym in (
        selector._normalize_symbol(os.getenv("AI_REGIME_SOURCE", selector.BENCH)),
        selector._normalize_symbol(os.getenv("AI_REGIME_FILTER_ASSET", "")),
    ):
        if sym:
            prefetch.append(sym)

    download_symbols = list(dict.fromkeys(symbols + prefetch + [selector.BENCH, selector.VIX]))
    frames = selector._build_frames(download_symbols)
    latest_market_day = pd.Timestamp(frames[selector.BENCH].index.max()).normalize()
    signal_day = max(d for d in selector._snapshot_dates() if d <= latest_market_day)
    entry_pos = selector._execution_pos(frames[selector.BENCH].index, signal_day, "next_open")
    entry_day = pd.Timestamp(frames[selector.BENCH].index[entry_pos]).date() if entry_pos >= 0 else None

    by_symbol: dict[str, dict[str, Any]] = {}
    by_symbol_latest: dict[str, dict[str, Any]] = {}
    for sym, frame in frames.items():
        if frame is None or frame.empty:
            continue
        ind = selector._indicator_frame(frame)
        if ind.empty:
            continue
        sd = selector._last_day(ind, signal_day)
        if sd is not None:
            row = ind.loc[sd]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[-1]
            data = row.to_dict()
            data["symbol"] = sym
            by_symbol[sym] = data
        latest_ref = selector._last_day(ind, latest_market_day)
        if latest_ref is not None:
            latest_row = ind.loc[latest_ref]
            if isinstance(latest_row, pd.DataFrame):
                latest_row = latest_row.iloc[-1]
            latest_data = latest_row.to_dict()
            latest_data["symbol"] = sym
            by_symbol_latest[sym] = latest_data

    out = selector._regime_portfolio_from_features(
        by_symbol=by_symbol,
        prev_state="risk_off",
        regime_source=selector._normalize_symbol(os.getenv("AI_REGIME_SOURCE", selector.BENCH)) or selector.BENCH,
        ma_fast=int(os.getenv("AI_REGIME_MA_FAST", "100")),
        ma_slow=int(os.getenv("AI_REGIME_MA_SLOW", "200")),
        mom_lb=int(os.getenv("AI_REGIME_MOM_LB", "21")),
        mom_thr=float(os.getenv("AI_REGIME_MOM_THR", "0.0")),
        risk_on_alloc=selector._parse_allocation_spec(os.getenv("AI_REGIME_RISK_ON", "TQQQ")),
        risk_on_alt_alloc=selector._parse_allocation_spec(os.getenv("AI_REGIME_RISK_ON_ALT", "QLD")),
        neutral_alloc=selector._parse_allocation_spec(os.getenv("AI_REGIME_NEUTRAL", "QLD")),
        recovery_alloc=selector._parse_allocation_spec(os.getenv("AI_REGIME_RECOVERY", "")),
        risk_off_alloc=selector._parse_allocation_spec(os.getenv("AI_REGIME_RISK_OFF", "GLD")),
        crash_alloc=selector._parse_allocation_spec(os.getenv("AI_REGIME_CRASH", "GLD")),
        vol_cap=float(os.getenv("AI_REGIME_VOL_CAP", "0.05")),
        vol_low=float(os.getenv("AI_REGIME_VOL_LOW", "0.035")),
        vol_mid=float(os.getenv("AI_REGIME_VOL_MID", "0.04")),
        mom_strong=float(os.getenv("AI_REGIME_MOM_STRONG", "0.06")),
        crash_vol=float(os.getenv("AI_REGIME_CRASH_VOL", "0.06")),
        crash_dd=float(os.getenv("AI_REGIME_CRASH_DD", "-0.2")),
        hysteresis=float(os.getenv("AI_REGIME_HYSTERESIS", "0.0")),
        recovery_slow_buffer=float(os.getenv("AI_REGIME_RECOVERY_SLOW_BUFFER", "0.03")),
        recovery_min_mom=float(os.getenv("AI_REGIME_RECOVERY_MIN_MOM", "0.015")),
        recovery_max_vol=float(os.getenv("AI_REGIME_RECOVERY_MAX_VOL", "0.045")),
        recovery_dd_floor=float(os.getenv("AI_REGIME_RECOVERY_DD_FLOOR", "-0.12")),
        risk_on_filter_asset=selector._normalize_symbol(os.getenv("AI_REGIME_FILTER_ASSET", "")),
        risk_on_filter_ma=int(os.getenv("AI_REGIME_FILTER_MA", "50")),
        risk_on_filter_safe_alloc=selector._parse_allocation_spec(os.getenv("AI_REGIME_FILTER_SAFE", "")),
        risk_off_dynamic=_parse_bool(os.getenv("AI_REGIME_RISK_OFF_DYNAMIC", "0")),
        risk_off_pool_symbols=selector._parse_symbols(os.getenv("AI_REGIME_RISK_OFF_POOL", "")),
        risk_off_top_n=int(os.getenv("AI_REGIME_RISK_OFF_TOP_N", "1")),
        risk_off_min_ma_gap=float(os.getenv("AI_REGIME_RISK_OFF_MIN_MA_GAP", "0.0")),
        risk_off_min_ret21=float(os.getenv("AI_REGIME_RISK_OFF_MIN_RET21", "0.0")),
        risk_off_min_ret63=float(os.getenv("AI_REGIME_RISK_OFF_MIN_RET63", "0.0")),
        risk_off_max_vol=float(os.getenv("AI_REGIME_RISK_OFF_MAX_VOL", "0.0")),
        risk_off_min_dd252=float(os.getenv("AI_REGIME_RISK_OFF_MIN_DD252", "-1.0")),
        risk_off_weight_mode=str(os.getenv("AI_REGIME_RISK_OFF_WEIGHT_MODE", "inv_vol")),
        risk_off_fallback_alloc=selector._parse_allocation_spec(os.getenv("AI_REGIME_RISK_OFF_FALLBACK", "")),
        crash_dynamic=_parse_bool(os.getenv("AI_REGIME_CRASH_DYNAMIC", "0")),
        crash_pool_symbols=selector._parse_symbols(os.getenv("AI_REGIME_CRASH_POOL", "")),
        crash_top_n=int(os.getenv("AI_REGIME_CRASH_TOP_N", "1")),
        crash_min_ma_gap=float(os.getenv("AI_REGIME_CRASH_MIN_MA_GAP", "0.0")),
        crash_min_ret21=float(os.getenv("AI_REGIME_CRASH_MIN_RET21", "0.0")),
        crash_min_ret63=float(os.getenv("AI_REGIME_CRASH_MIN_RET63", "0.0")),
        crash_max_vol=float(os.getenv("AI_REGIME_CRASH_MAX_VOL", "0.0")),
        crash_min_dd252=float(os.getenv("AI_REGIME_CRASH_MIN_DD252", "-1.0")),
        crash_weight_mode=str(os.getenv("AI_REGIME_CRASH_WEIGHT_MODE", "inv_vol")),
        crash_fallback_alloc=selector._parse_allocation_spec(os.getenv("AI_REGIME_CRASH_FALLBACK", "")),
    )

    source_symbol = selector._normalize_symbol(os.getenv("AI_REGIME_SOURCE", selector.BENCH)) or selector.BENCH
    source = by_symbol.get(source_symbol, {})
    latest_source = by_symbol_latest.get(source_symbol, {})
    latest_vix = by_symbol_latest.get("^VIX") or {}
    position_price_refs: list[dict[str, Any]] = []
    entry_ts = pd.Timestamp(entry_day) if entry_day else None
    for position in out.get("positions", []):
        symbol = str((position or {}).get("symbol", "") or "").strip().upper()
        if not symbol:
            continue
        frame = frames.get(symbol)
        latest_close = None
        entry_open = None
        if frame is not None and not frame.empty:
            latest_ref_day = selector._last_day(frame, latest_market_day)
            if latest_ref_day is not None:
                latest_close = float(frame.loc[latest_ref_day]["Close"])
            if entry_ts is not None:
                entry_ref_day = selector._last_day(frame, entry_ts)
                if entry_ref_day is not None:
                    entry_open = float(frame.loc[entry_ref_day]["Open"])
        position_price_refs.append(
            {
                "symbol": symbol,
                "weightPct": round(float((position or {}).get("weight_pct", 0.0) or 0.0), 4),
                "entryDay": str(entry_day) if entry_day else "",
                "entryDayOpen": round(entry_open, 2) if entry_open is not None else None,
                "latestMarketDay": str(latest_market_day.date()),
                "latestClose": round(latest_close, 2) if latest_close is not None else None,
            }
        )
    return {
        "latestMarketDay": str(latest_market_day.date()),
        "signalDay": str(signal_day.date()),
        "entryDay": str(entry_day),
        "regimeState": str(out.get("_regime_state", "")),
        "regimeReason": str(out.get("_regime_reason", "")),
        "positions": out.get("positions", []),
        "positionPriceRefs": position_price_refs,
        "signalQqqClose": round(float(source.get("close", 0.0) or 0.0), 2),
        "signalQqqMa200Gap": round(float(source.get("ma200_gap", 0.0) or 0.0), 4),
        "signalQqqReturn21d": round(float(source.get("return_21d", 0.0) or 0.0), 4),
        "signalQqqReturn63d": round(float(source.get("return_63d", 0.0) or 0.0), 4),
        "signalVixClose": round(float((by_symbol.get("^VIX") or {}).get("close", 0.0) or 0.0), 2),
        "latestQqqClose": round(float(latest_source.get("close", 0.0) or 0.0), 2),
        "latestQqqMa200Gap": round(float(latest_source.get("ma200_gap", 0.0) or 0.0), 4),
        "latestQqqReturn21d": round(float(latest_source.get("return_21d", 0.0) or 0.0), 4),
        "latestQqqReturn63d": round(float(latest_source.get("return_63d", 0.0) or 0.0), 4),
        "latestVixClose": round(float(latest_vix.get("close", 0.0) or 0.0), 2),
    }


def _strategy_metrics(pattern: str, strategy_key: str) -> dict[str, Any]:
    fallback = dict(FALLBACK_STRATEGY_METRICS[strategy_key])
    path = _latest_json(pattern)
    if path is None:
        return fallback
    payload = _load_json(path)
    metrics = payload.get("metrics") or {}
    alpha = payload.get("alpha") or {}
    turnover = payload.get("turnover") or {}
    ai = (metrics.get("ai_portfolio") or {}) if isinstance(metrics, dict) else {}
    benchmark = (metrics.get("benchmark") or {}) if isinstance(metrics, dict) else {}
    turnover_ai = (turnover.get("ai") or {}) if isinstance(turnover, dict) else {}
    return {
        "label": fallback["label"],
        "cagr": round(float(ai.get("cagr_pct", fallback["cagr"]) or fallback["cagr"]), 2),
        "benchmarkCagr": round(
            float(benchmark.get("cagr_pct", fallback["benchmarkCagr"]) or fallback["benchmarkCagr"]),
            2,
        ),
        "drawdown": round(float(ai.get("max_drawdown_pct", fallback["drawdown"]) or fallback["drawdown"]), 2),
        "turnover": round(float(turnover_ai.get("mean", fallback["turnover"]) or fallback["turnover"]), 3),
        "pAlphaGt0": round(float(alpha.get("nw_p_gt0", fallback["pAlphaGt0"]) or fallback["pAlphaGt0"]), 3),
    }


def _build_payload() -> dict[str, Any]:
    return {
        "generatedAt": str(pd.Timestamp.utcnow()),
        "strategies": {
            "v2": _strategy_metrics("ai_portfolio_backtest_verification_strategy_v2*.json", "v2"),
            "v14": _strategy_metrics("ai_portfolio_backtest_verification_strategy_v14*.json", "v14"),
        },
        "rebalance": {
            "v2": _build_current_signal("v2", "run_strategy_v2_baseline.py"),
            "v14": _build_current_signal("v14", "run_strategy_v14_regime_gld_dynamic_defense.py"),
        },
    }


def main() -> None:
    payload = _build_payload()
    TARGET_TS.parent.mkdir(parents=True, exist_ok=True)
    TARGET_JSON.parent.mkdir(parents=True, exist_ok=True)
    TARGET_TS.write_text(
        "export const SNAPSHOT = " + json.dumps(payload, ensure_ascii=False, indent=2) + " as const;\n",
        encoding="utf-8",
    )
    TARGET_JSON.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"Saved: {TARGET_TS.relative_to(ROOT)}")
    print(f"Saved: {TARGET_JSON.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
