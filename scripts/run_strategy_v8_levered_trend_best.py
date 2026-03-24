from __future__ import annotations

import importlib.util
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
RUNS_DIR = ROOT / "data" / "runs"
RESEARCH_SCRIPT = ROOT / "scripts" / "research_weekly_levered_trend.py"


DEFAULT_ENV: dict[str, str] = {
    "LT_BEST_JSON": "data/runs/levered_trend_best_lt_v1_grid_20260305.json",
    "LT_START_DATE": "2016-03-01",
    "LT_END_DATE": "2026-03-01",
    "LT_BENCHMARK": "QQQ",
    "LT_VIX": "^VIX",
    "LT_TRADE_COST_BPS": "20",
}


def _apply_defaults() -> None:
    for key, value in DEFAULT_ENV.items():
        os.environ.setdefault(key, value)
    os.environ.setdefault(
        "AI_RUN_TAG",
        f"strategy_v8_levered_trend_best_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
    )


def _load_module(path: Path, name: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _risk_metrics(series_pct: pd.Series, periods_per_year: int = 52) -> dict[str, float]:
    s = pd.to_numeric(series_pct, errors="coerce").dropna().astype(float)
    if len(s) == 0:
        return {
            "periods": 0,
            "mean_period_return_pct": 0.0,
            "cagr_pct": 0.0,
            "vol_annual_pct": 0.0,
            "sharpe": 0.0,
            "sortino": 0.0,
            "max_drawdown_pct": 0.0,
            "win_rate_pct": 0.0,
            "total_return_pct": 0.0,
        }
    r = s / 100.0
    n = len(r)
    c = (1.0 + r).cumprod()
    tot = float(c.iloc[-1] - 1.0)
    cagr = float(c.iloc[-1] ** (periods_per_year / n) - 1.0) if c.iloc[-1] > 0 else 0.0
    sd = float(r.std(ddof=1)) if n > 1 else 0.0
    sharpe = float((r.mean() / sd) * np.sqrt(periods_per_year)) if sd > 1e-12 else 0.0
    dn = r[r < 0]
    sortino = 0.0
    if len(dn) > 1:
        dsd = float(dn.std(ddof=1))
        if dsd > 1e-12:
            sortino = float((r.mean() / dsd) * np.sqrt(periods_per_year))
    dd = (c / c.cummax()) - 1.0
    return {
        "periods": int(n),
        "mean_period_return_pct": float(r.mean() * 100.0),
        "cagr_pct": float(cagr * 100.0),
        "vol_annual_pct": float(sd * np.sqrt(periods_per_year) * 100.0) if sd > 1e-12 else 0.0,
        "sharpe": float(sharpe),
        "sortino": float(sortino),
        "max_drawdown_pct": float(dd.min() * 100.0),
        "win_rate_pct": float((r > 0).mean() * 100.0),
        "total_return_pct": float(tot * 100.0),
    }


def _load_best_payload(path: Path) -> dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(obj, dict)
    return obj


def _open_price_on(frame: pd.DataFrame, day_text: str) -> float:
    day = pd.Timestamp(day_text)
    row = frame.loc[frame.index == day]
    if row.empty:
        return float("nan")
    return float(pd.to_numeric(row["Open"], errors="coerce").iloc[0])


def _build_risk_only_comparator(df: pd.DataFrame, risk_frame: pd.DataFrame, risk_symbol: str, trade_cost_bps: float) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    prev = ""
    cost_pct = float(trade_cost_bps) / 100.0
    for rec in df.to_dict(orient="records"):
        entry_day = str(rec["entry_day"])
        exit_day = str(rec["exit_day"])
        p0 = _open_price_on(risk_frame, entry_day)
        p1 = _open_price_on(risk_frame, exit_day)
        if not np.isfinite(p0) or not np.isfinite(p1) or p0 <= 0 or p1 <= 0:
            gross_pct = 0.0
        else:
            gross_pct = float((p1 / p0 - 1.0) * 100.0)
        turn = 0.0 if prev == risk_symbol and prev else 1.0
        net_pct = float(gross_pct - cost_pct * turn)
        rows.append(
            {
                "mom_gross_return_pct": gross_pct,
                "mom_net_return_pct": net_pct,
                "mom_turnover": float(turn),
                "mom_positions": json.dumps({risk_symbol: 100.0}, ensure_ascii=True),
            }
        )
        prev = risk_symbol
    return pd.DataFrame(rows)


def main() -> None:
    _apply_defaults()
    run_tag = os.environ["AI_RUN_TAG"]
    best_json = Path(os.getenv("LT_BEST_JSON", DEFAULT_ENV["LT_BEST_JSON"]))
    if not best_json.is_absolute():
        best_json = ROOT / best_json
    payload = _load_best_payload(best_json)
    best = payload.get("best") or {}
    cfg = best.get("config") or {}

    research = _load_module(RESEARCH_SCRIPT, f"research_weekly_levered_trend_std_{run_tag}")
    trend_cfg = research.TrendConfig(
        risk=str(cfg.get("risk") or "QLD"),
        safe=str(cfg.get("safe") or "GLD"),
        ma_window=int(cfg.get("ma_window") or 125),
        qqq_confirm=bool(cfg.get("qqq_confirm", False)),
        vix_max=float(cfg.get("vix_max") or 0.0),
    )

    symbols = sorted({research.BENCH, research.VIX, trend_cfg.risk, trend_cfg.safe})
    frames = research._download(symbols)
    snaps = research._snapshots()
    raw_df, summary = research._run_config(trend_cfg, frames, snaps)
    if raw_df.empty:
        raise RuntimeError("No levered-trend periods produced")

    trade_cost_bps = float(os.getenv("LT_TRADE_COST_BPS", DEFAULT_ENV["LT_TRADE_COST_BPS"]))
    cost_pct = float(trade_cost_bps) / 100.0

    df = raw_df.copy()
    df["date"] = df["entry_day"]
    df["execution_timing"] = "next_open"
    df["market_regime"] = np.where(df["asset"].astype(str) == str(trend_cfg.risk), "risk_on", "risk_off")
    df["market_regime_base"] = df["market_regime"]
    df["regime_state"] = df["market_regime"]
    df["regime_reason"] = "levered_trend_best"
    df["positions"] = df["asset"].astype(str).map(lambda sym: json.dumps({sym: 100.0}, ensure_ascii=True))
    df["ai_core_positions"] = df["positions"]
    df["turnover"] = (df["asset"].astype(str) != df["asset"].astype(str).shift(1)).astype(float)
    if not df.empty:
        df.loc[df.index[0], "turnover"] = 1.0
    df["gross_return_pct"] = pd.to_numeric(df["strategy_ret"], errors="coerce").fillna(0.0) * 100.0 + cost_pct * pd.to_numeric(df["turnover"], errors="coerce").fillna(0.0)
    df["net_return_pct"] = pd.to_numeric(df["strategy_ret"], errors="coerce").fillna(0.0) * 100.0
    df["benchmark_return_pct"] = pd.to_numeric(df["bench_ret"], errors="coerce").fillna(0.0) * 100.0
    df["cash_pct"] = 0.0
    df["ai_core_cash_pct"] = 0.0
    df["exposure_pct"] = 100.0
    df["ai_fallback"] = False
    df["target_positions"] = 1
    df["sit_out"] = False
    df["periods_per_year"] = 52
    df["decision_engine"] = "trend"
    df["prompt_version"] = "v1_levered_trend_best"
    df["breadth_source_mode"] = "custom"
    df["momentum_blend_ratio_applied"] = 0.0

    risk_cmp = _build_risk_only_comparator(df, frames[trend_cfg.risk], trend_cfg.risk, trade_cost_bps)
    df = pd.concat([df.reset_index(drop=True), risk_cmp.reset_index(drop=True)], axis=1)

    result_cols = [
        "date",
        "signal_day",
        "entry_day",
        "exit_day",
        "execution_timing",
        "market_regime",
        "market_regime_base",
        "regime_state",
        "regime_reason",
        "positions",
        "ai_core_positions",
        "cash_pct",
        "ai_core_cash_pct",
        "exposure_pct",
        "turnover",
        "gross_return_pct",
        "net_return_pct",
        "benchmark_return_pct",
        "mom_positions",
        "mom_turnover",
        "mom_gross_return_pct",
        "mom_net_return_pct",
        "periods_per_year",
        "decision_engine",
        "prompt_version",
        "vix_close",
        "asset",
    ]
    df_out = df[result_cols].copy()

    pm = {
        "ai_portfolio": _risk_metrics(df_out["net_return_pct"], 52),
        "ai_portfolio_gross": _risk_metrics(df_out["gross_return_pct"], 52),
        "momentum_topk": _risk_metrics(df_out["mom_net_return_pct"], 52),
        "momentum_topk_gross": _risk_metrics(df_out["mom_gross_return_pct"], 52),
        "benchmark": _risk_metrics(df_out["benchmark_return_pct"], 52),
    }

    summary_json = {
        "run_tag": run_tag,
        "decision_engine": "trend",
        "universe": "custom",
        "symbols": int(len(symbols)),
        "benchmark_symbol": str(research.BENCH),
        "vix_symbol": str(research.VIX),
        "start_date": str(research.START),
        "end_date": str(research.END),
        "snapshot_freq": "weekly",
        "trade_cost_bps": float(trade_cost_bps),
        "trend_best_source_json": str(best_json),
        "trend_best_config": cfg,
        "portfolio_metrics": pm,
    }

    out_csv = RUNS_DIR / f"ai_portfolio_backtest_results_{run_tag}.csv"
    out_summary = RUNS_DIR / f"ai_portfolio_backtest_summary_{run_tag}.json"
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out_csv, index=False)
    out_summary.write_text(json.dumps(summary_json, ensure_ascii=False, indent=2), encoding="utf-8")

    print("Running Strategy V8 levered-trend best...")
    print(f"  AI_RUN_TAG={run_tag}")
    print(f"  LT_BEST_JSON={best_json}")
    print(f"  risk={trend_cfg.risk}")
    print(f"  safe={trend_cfg.safe}")
    print(f"  ma_window={trend_cfg.ma_window}")
    print(f"  qqq_confirm={trend_cfg.qqq_confirm}")
    print(f"  vix_max={trend_cfg.vix_max}")
    print(f"Saved: {out_csv.relative_to(ROOT)} ({len(df_out)} rows)")
    print(f"Saved: {out_summary.relative_to(ROOT)}")
    print(
        "AI portfolio -> "
        f"CAGR {pm['ai_portfolio']['cagr_pct']:.2f}% | "
        f"Sharpe {pm['ai_portfolio']['sharpe']:.2f} | "
        f"MDD {pm['ai_portfolio']['max_drawdown_pct']:.2f}%"
    )


if __name__ == "__main__":
    main()
