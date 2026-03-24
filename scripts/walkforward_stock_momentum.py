from __future__ import annotations

import importlib.util
import json
import math
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
RUNS_DIR = ROOT / "data" / "runs"
RUN_TAG = (os.getenv("WF_RUN_TAG") or "").strip() or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

START = (os.getenv("WF_START_DATE") or "2006-03-01").strip()
END = (os.getenv("WF_END_DATE") or "2026-03-11").strip()
TRAIN_YEARS = max(3, int(float(os.getenv("WF_TRAIN_YEARS") or "5")))
TEST_YEARS = max(1, int(float(os.getenv("WF_TEST_YEARS") or "1")))
MIN_TEST_WEEKS = max(20, int(float(os.getenv("WF_MIN_TEST_WEEKS") or "40")))
TRADE_COST_BPS = float(os.getenv("WF_TRADE_COST_BPS") or "20")

FOLDS_CSV = RUNS_DIR / f"walkforward_stock_momentum_folds_{RUN_TAG}.csv"
OOS_CSV = RUNS_DIR / f"walkforward_stock_momentum_oos_{RUN_TAG}.csv"
SUMMARY_JSON = RUNS_DIR / f"walkforward_stock_momentum_summary_{RUN_TAG}.json"
SUMMARY_MD = RUNS_DIR / f"walkforward_stock_momentum_summary_{RUN_TAG}.md"


@dataclass(frozen=True)
class StockConfig:
    top_k: int = 5
    max_weight_pct: float = 20.0
    min_overlap: int = 4
    top_k_neutral: int = 5
    top_k_risk_off: int = 0
    weight_mode: str = "equal"
    min_positions_for_invest: int = 2
    max_per_sector: int = 2
    sector_bonus: float = 0.15
    pit_bonus: float = 0.0
    pit_max_filing_age: int = 180


def _load_bt_module():
    os.environ["AI_START_DATE"] = START
    os.environ["AI_END_DATE"] = END
    os.environ["AI_DATA_START_DATE"] = str((pd.Timestamp(START) - pd.DateOffset(years=3)).date())
    os.environ["AI_DATA_END_DATE"] = str((pd.Timestamp(END) + pd.DateOffset(days=1)).date())
    os.environ["AI_SNAPSHOT_FREQ"] = "weekly"
    os.environ["AI_HORIZON_MODE"] = "next_snapshot"
    os.environ["AI_EXECUTION_TIMING"] = "next_open"
    os.environ["AI_STOCK_MOMO_PIT_BONUS"] = "0.1"
    os.environ["AI_STOCK_MOMO_PIT_MAX_FILING_AGE"] = "180"
    path = ROOT / "scripts" / "backtest_ai_portfolio_selector.py"
    spec = importlib.util.spec_from_file_location("backtest_ai_portfolio_selector_walkforward", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _risk_metrics(series_pct: pd.Series, periods_per_year: int = 52) -> dict[str, float]:
    s = pd.to_numeric(series_pct, errors="coerce").dropna().astype(float)
    if len(s) == 0:
        return {
            "periods": 0,
            "cagr_pct": 0.0,
            "total_return_pct": 0.0,
            "sharpe": 0.0,
            "max_drawdown_pct": 0.0,
        }
    r = s / 100.0
    n = len(r)
    c = (1.0 + r).cumprod()
    total = float(c.iloc[-1] - 1.0)
    cagr = float(c.iloc[-1] ** (periods_per_year / n) - 1.0) if c.iloc[-1] > 0 else 0.0
    sd = float(r.std(ddof=1)) if n > 1 else 0.0
    sharpe = float((r.mean() / sd) * np.sqrt(periods_per_year)) if sd > 1e-12 else 0.0
    dd = (c / c.cummax()) - 1.0
    return {
        "periods": int(n),
        "cagr_pct": float(cagr * 100.0),
        "total_return_pct": float(total * 100.0),
        "sharpe": float(sharpe),
        "max_drawdown_pct": float(dd.min() * 100.0),
    }


def _newey_west(alpha_pct: pd.Series) -> dict[str, float]:
    a = pd.to_numeric(alpha_pct, errors="coerce").dropna().astype(float)
    n = len(a)
    if n < 5:
        return {"nw_t": 0.0, "nw_p_two": 1.0, "nw_p_gt0": 0.5}
    mu = float(a.mean())
    x = a.to_numpy(dtype=float)
    eps = x - mu
    lag = int(round(4.0 * ((n / 100.0) ** (2.0 / 9.0))))
    lag = max(1, min(lag, n - 1))
    gamma0 = float(np.dot(eps, eps) / n)
    lrv = gamma0
    for l in range(1, lag + 1):
        cov = float(np.dot(eps[l:], eps[:-l]) / n)
        weight = 1.0 - (l / (lag + 1.0))
        lrv += 2.0 * weight * cov
    lrv = max(0.0, lrv)
    se = math.sqrt(lrv / n) if n > 0 else 0.0
    t = mu / se if se > 1e-12 else 0.0
    return {
        "nw_t": float(t),
        "nw_p_two": float(math.erfc(abs(t) / math.sqrt(2.0))),
        "nw_p_gt0": float(0.5 * (1.0 + math.erf(t / math.sqrt(2.0)))),
    }


def _selection_key(metrics: dict[str, float]) -> tuple[Any, ...]:
    cagr_diff = float(metrics.get("cagr_diff_pct", 0.0))
    mdd_diff = float(metrics.get("mdd_diff_pct", 0.0))
    turnover = float(metrics.get("avg_turnover", 0.0))
    sharpe_diff = float(metrics.get("sharpe", 0.0)) - float(metrics.get("benchmark_sharpe", 0.0))
    return (
        1 if cagr_diff > 0 else 0,
        1 if mdd_diff >= 0 else 0,
        1 if turnover <= 0.30 else 0,
        float(sharpe_diff),
        float(cagr_diff),
        float(mdd_diff),
        -float(turnover),
    )


def _build_snapshot_records(bt: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    snaps = bt._snapshot_dates()
    universe_by_date = bt._load_universe_by_date(ROOT / "data" / "universe" / "nasdaq100_by_date_weekly_2006_2026.json")
    universe_lookup = bt._build_universe_asof_lookup(universe_by_date)
    requested_symbols = sorted({s for arr in universe_by_date.values() for s in arr})
    sector_lookup = bt._load_sector_lookup()
    frames = bt._build_frames(requested_symbols)
    if bt.BENCH not in frames:
        raise RuntimeError(f"Missing benchmark frame: {bt.BENCH}")
    ind_by_symbol: dict[str, pd.DataFrame] = {}
    for sym, frame in frames.items():
        if frame is None or frame.empty:
            continue
        try:
            ind = bt._indicator_frame(frame)
        except Exception:
            continue
        if ind is None or ind.empty:
            continue
        ind_by_symbol[sym] = ind
    bench_ind = ind_by_symbol.get(bt.BENCH)
    if bench_ind is None or bench_ind.empty:
        raise RuntimeError("Benchmark indicator frame empty")

    records: list[dict[str, Any]] = []
    for i, qdt in enumerate(snaps):
        mkt = bt._market_ctx(frames[bt.BENCH], frames.get(bt.VIX), qdt, use_benchmark_features=True)
        if not mkt:
            continue
        next_mkt = None
        if bt.HORIZON_MODE == "next_snapshot":
            if i + 1 >= len(snaps):
                continue
            next_mkt = bt._market_ctx(frames[bt.BENCH], frames.get(bt.VIX), snaps[i + 1], use_benchmark_features=True)
            if not next_mkt:
                continue

        signal_day = pd.Timestamp(mkt["day"])
        bench_signal_pos = bt._asof_pos(bench_ind.index, signal_day)
        if bench_signal_pos < bt.MIN_HISTORY_DAYS:
            continue

        exit_signal_day = pd.Timestamp(next_mkt["day"]) if next_mkt is not None else None
        bench_frame = frames[bt.BENCH]
        bench_entry_pos = bt._execution_pos(bench_frame.index, signal_day, "next_open")
        bench_exit_pos = bt._execution_pos(bench_frame.index, exit_signal_day, "next_open") if exit_signal_day is not None else -1
        if (
            bench_entry_pos < 0
            or bench_exit_pos <= bench_entry_pos
            or bench_exit_pos >= len(bench_frame)
            or bench_entry_pos >= len(bench_frame)
        ):
            continue
        bench_entry_px = bt._execution_price(bench_frame, bench_entry_pos, "next_open")
        bench_exit_px = bt._execution_price(bench_frame, bench_exit_pos, "next_open")
        if bench_entry_px <= 0 or bench_exit_px <= 0:
            continue

        snap = str(qdt.date())
        raw_universe = bt._resolve_universe_asof(universe_lookup, snap, requested_symbols)
        active_symbols = [s for s in raw_universe if s in ind_by_symbol]
        feats: list[dict[str, Any]] = []
        fwd_ret_by_symbol: dict[str, float] = {}
        for symbol in active_symbols:
            ind_df = ind_by_symbol.get(symbol)
            if ind_df is None or ind_df.empty:
                continue
            signal_pos = bt._asof_pos(ind_df.index, signal_day)
            if signal_pos < bt.MIN_HISTORY_DAYS or signal_pos >= len(ind_df):
                continue
            raw_df = frames.get(symbol)
            if raw_df is None or raw_df.empty:
                continue
            entry_pos = bt._execution_pos(raw_df.index, signal_day, "next_open")
            exit_pos = bt._execution_pos(raw_df.index, exit_signal_day, "next_open") if exit_signal_day is not None else -1
            if entry_pos < 0 or exit_pos <= entry_pos or exit_pos >= len(raw_df) or entry_pos >= len(raw_df):
                continue
            entry_px = bt._execution_price(raw_df, entry_pos, "next_open")
            exit_px = bt._execution_price(raw_df, exit_pos, "next_open")
            if entry_px <= 0 or exit_px <= 0:
                continue

            forward_return_pct = (exit_px / entry_px - 1.0) * 100.0
            ind_row = ind_df.iloc[signal_pos]
            rs63 = bt._f(ind_row.get("return_63d")) - bt._f(mkt["bench_r63"])
            rs21 = bt._f(ind_row.get("return_21d")) - bt._f(mkt["bench_r21"])
            tt = bt._trend_template_checks(ind_row, rs63, rs63_min=0.0)
            sector_meta = sector_lookup.get(symbol, {})
            feats.append(
                {
                    "symbol": symbol,
                    "sector": str(sector_meta.get("sector") or "Unknown"),
                    "industry": str(sector_meta.get("industry") or "Unknown"),
                    "relative_strength_63d": float(rs63),
                    "relative_strength_21d": float(rs21),
                    "return_63d": bt._f(ind_row.get("return_63d")),
                    "return_21d": bt._f(ind_row.get("return_21d")),
                    "vol_20": bt._f(ind_row.get("vol_20")),
                    "atr_pct": bt._f(ind_row.get("atr_pct")),
                    "ma50_gap": bt._f(ind_row.get("ma50_gap")),
                    "ma200_gap": bt._f(ind_row.get("ma200_gap")),
                    "volume_ratio": bt._f(ind_row.get("volume_ratio"), 1.0),
                    "trend_template_pass": bool(tt.get("pass", False)),
                }
            )
            fwd_ret_by_symbol[symbol] = float(forward_return_pct)

        safe_feats = [x for x in feats if bool(x.get("trend_template_pass", False))]
        breadth = bt._market_breadth(bt._resolve_breadth_features(feats, safe_feats, "universe"))
        sector_scores = bt._sector_strength_scores(safe_feats)
        pit_bonus_by_symbol = bt._pit_symbol_bonus(safe_feats, max_filing_age_days=180)
        records.append(
            {
                "snap": snap,
                "entry_day": str(pd.Timestamp(bench_frame.index[bench_entry_pos]).date()),
                "exit_day": str(pd.Timestamp(bench_frame.index[bench_exit_pos]).date()),
                "market_regime": str(mkt.get("regime", "neutral")),
                "benchmark_return_pct": float((bench_exit_px / bench_entry_px - 1.0) * 100.0),
                "features": feats,
                "safe_features": safe_feats,
                "breadth": breadth,
                "sector_scores": sector_scores,
                "pit_bonus_by_symbol": pit_bonus_by_symbol,
                "fwd_ret_by_symbol": fwd_ret_by_symbol,
            }
        )
    meta = {
        "snapshots": int(len(snaps)),
        "records": int(len(records)),
        "symbols": int(len(requested_symbols)),
    }
    return records, meta


def _evaluate_config(bt: Any, records: list[dict[str, Any]], cfg: StockConfig) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    prev_port: dict[str, float] = {"__CASH__": 1.0}
    trade_cost_pct = float(TRADE_COST_BPS) / 100.0

    for rec in records:
        safe_feats = list(rec["safe_features"])
        if not safe_feats:
            rows.append(
                {
                    "entry_day": rec["entry_day"],
                    "exit_day": rec["exit_day"],
                    "net_return_pct": 0.0,
                    "benchmark_return_pct": float(rec["benchmark_return_pct"]),
                    "turnover": 0.0,
                    "positions": "{}",
                }
            )
            prev_port = {"__CASH__": 1.0}
            continue

        held_syms = [
            sym
            for sym, weight in prev_port.items()
            if isinstance(sym, str)
            and sym != "__CASH__"
            and float(weight) > 0
            and any(str(x.get("symbol")) == sym for x in safe_feats)
        ]
        target_top_k = bt._dynamic_position_target(
            safe_feats,
            base_top_k=int(cfg.top_k),
            market_regime=str(rec["market_regime"]),
            safe_mode=True,
            require_risk_on=False,
            min_positions=1,
        )
        if str(rec["market_regime"]).lower() == "neutral":
            target_top_k = min(target_top_k, int(cfg.top_k_neutral))
        elif str(rec["market_regime"]).lower() == "risk_off":
            target_top_k = min(target_top_k, int(cfg.top_k_risk_off))

        candidates = bt._select_candidates_with_includes(
            safe_feats,
            30,
            "top_rs63",
            held_syms,
        )
        allowed = {x["symbol"] for x in candidates if isinstance(x, dict) and x.get("symbol")}
        weights_pct: dict[str, float] = {}
        cash_pct = 100.0
        if target_top_k > 0 and allowed:
            out = bt._stock_momentum_portfolio(
                candidates,
                top_k=target_top_k,
                weight_mode=cfg.weight_mode,
                min_positions_for_invest=cfg.min_positions_for_invest,
                    max_per_sector=cfg.max_per_sector,
                    sector_bonus_mult=cfg.sector_bonus,
                    sector_scores=rec["sector_scores"],
                    symbol_bonus={sym: float(cfg.pit_bonus * rec["pit_bonus_by_symbol"].get(sym, 0.0)) for sym in rec["pit_bonus_by_symbol"]},
                )
            weights_pct, cash_pct = bt._portfolio_from_ai(
                out,
                allowed=allowed,
                top_k=target_top_k,
                max_weight_pct=cfg.max_weight_pct,
            )
            feats_by_symbol = {str(x.get("symbol")): x for x in rec["features"] if isinstance(x, dict) and x.get("symbol")}
            weights_pct = bt._enforce_min_overlap(
                weights_pct,
                prev_port=prev_port,
                allowed=allowed,
                feats_by_symbol=feats_by_symbol,
                min_overlap=cfg.min_overlap,
                top_k=target_top_k,
            )
            weights_pct = {k: float(v) for k, v in weights_pct.items() if float(v) > 0 and k in allowed}
            if weights_pct:
                weights_pct, cash_pct = bt._portfolio_from_ai(
                    {"positions": [{"symbol": k, "weight_pct": v} for k, v in weights_pct.items()]},
                    allowed=allowed,
                    top_k=target_top_k,
                    max_weight_pct=cfg.max_weight_pct,
                )

        port = {sym: float(weight) / 100.0 for sym, weight in weights_pct.items()}
        port["__CASH__"] = float(cash_pct) / 100.0
        gross = 0.0
        for sym, weight in port.items():
            if sym == "__CASH__":
                continue
            gross += float(weight) * float(rec["fwd_ret_by_symbol"].get(sym, 0.0))
        turn = bt._turnover(prev_port, port)
        net = float(gross - (trade_cost_pct * turn))
        rows.append(
            {
                "entry_day": rec["entry_day"],
                "exit_day": rec["exit_day"],
                "net_return_pct": float(net),
                "benchmark_return_pct": float(rec["benchmark_return_pct"]),
                "turnover": float(turn),
                "positions": json.dumps({sym: round(weight * 100.0, 4) for sym, weight in port.items() if sym != "__CASH__" and weight > 0}, ensure_ascii=True),
            }
        )
        prev_port = dict(port)

    df = pd.DataFrame(rows)
    ai = _risk_metrics(df["net_return_pct"])
    bench = _risk_metrics(df["benchmark_return_pct"])
    alpha = pd.to_numeric(df["net_return_pct"], errors="coerce") - pd.to_numeric(df["benchmark_return_pct"], errors="coerce")
    nw = _newey_west(alpha)
    summary = {
        "config": asdict(cfg),
        "periods": int(len(df)),
        "cagr_pct": float(ai["cagr_pct"]),
        "benchmark_cagr_pct": float(bench["cagr_pct"]),
        "cagr_diff_pct": float(ai["cagr_pct"] - bench["cagr_pct"]),
        "sharpe": float(ai["sharpe"]),
        "benchmark_sharpe": float(bench["sharpe"]),
        "mdd_pct": float(ai["max_drawdown_pct"]),
        "benchmark_mdd_pct": float(bench["max_drawdown_pct"]),
        "mdd_diff_pct": float(ai["max_drawdown_pct"] - bench["max_drawdown_pct"]),
        "avg_turnover": float(pd.to_numeric(df["turnover"], errors="coerce").mean()),
        **nw,
    }
    return df, summary


def _candidate_grid() -> list[StockConfig]:
    out: list[StockConfig] = []
    for min_overlap in (3, 4):
        for top_k_neutral in (3, 5):
            for weight_mode in ("equal", "score"):
                for max_per_sector in (0, 2):
                    for sector_bonus in (0.0, 0.15, 0.25):
                        out.append(
                            StockConfig(
                                top_k=5,
                                max_weight_pct=20.0,
                                min_overlap=int(min_overlap),
                                top_k_neutral=int(top_k_neutral),
                                top_k_risk_off=0,
                                weight_mode=str(weight_mode),
                                min_positions_for_invest=2,
                                max_per_sector=int(max_per_sector),
                                sector_bonus=float(sector_bonus),
                                pit_bonus=0.0,
                                pit_max_filing_age=180,
                            )
                        )
                        out.append(
                            StockConfig(
                                top_k=5,
                                max_weight_pct=20.0,
                                min_overlap=int(min_overlap),
                                top_k_neutral=int(top_k_neutral),
                                top_k_risk_off=0,
                                weight_mode=str(weight_mode),
                                min_positions_for_invest=2,
                                max_per_sector=int(max_per_sector),
                                sector_bonus=float(sector_bonus),
                                pit_bonus=0.1,
                                pit_max_filing_age=180,
                            )
                        )
    return out


def _walkforward_folds(df_records: pd.DataFrame) -> list[tuple[int, int, int]]:
    years = sorted(df_records["entry_year"].dropna().astype(int).unique().tolist())
    folds: list[tuple[int, int, int]] = []
    for test_year in years:
        train_start = test_year - TRAIN_YEARS
        train_end = test_year - 1
        test_end = test_year + TEST_YEARS - 1
        train_mask = df_records["entry_year"].between(train_start, train_end)
        test_mask = df_records["entry_year"].between(test_year, test_end)
        if int(train_mask.sum()) < TRAIN_YEARS * 40:
            continue
        if int(test_mask.sum()) < MIN_TEST_WEEKS:
            continue
        folds.append((train_start, train_end, test_year))
    return folds


def run() -> None:
    bt = _load_bt_module()
    records, meta = _build_snapshot_records(bt)
    if not records:
        raise RuntimeError("No snapshot records generated")

    df_records = pd.DataFrame(
        {
            "entry_day": pd.to_datetime([r["entry_day"] for r in records], errors="coerce"),
        }
    )
    df_records["entry_year"] = df_records["entry_day"].dt.year.astype(int)
    folds = _walkforward_folds(df_records)
    if not folds:
        raise RuntimeError("No walk-forward folds available")

    candidate_grid = _candidate_grid()
    baseline_cfg = StockConfig()

    fold_rows: list[dict[str, Any]] = []
    oos_selected_parts: list[pd.DataFrame] = []
    oos_static_parts: list[pd.DataFrame] = []

    for train_start, train_end, test_year in folds:
        train_records = [r for r in records if train_start <= pd.Timestamp(r["entry_day"]).year <= train_end]
        test_records = [r for r in records if test_year <= pd.Timestamp(r["entry_day"]).year <= (test_year + TEST_YEARS - 1)]

        best_cfg = baseline_cfg
        best_train_summary: dict[str, Any] | None = None
        best_key: tuple[Any, ...] | None = None
        for cfg in candidate_grid:
            _, train_summary = _evaluate_config(bt, train_records, cfg)
            key = _selection_key(train_summary)
            if best_key is None or key > best_key:
                best_key = key
                best_cfg = cfg
                best_train_summary = train_summary

        selected_df, selected_test = _evaluate_config(bt, test_records, best_cfg)
        static_df, static_test = _evaluate_config(bt, test_records, baseline_cfg)
        oos_selected_parts.append(selected_df)
        oos_static_parts.append(static_df)

        fold_rows.append(
            {
                "test_year": int(test_year),
                "train_start": int(train_start),
                "train_end": int(train_end),
                "selected_config": json.dumps(asdict(best_cfg), ensure_ascii=True),
                "train_cagr_diff_pct": float((best_train_summary or {}).get("cagr_diff_pct", 0.0)),
                "train_mdd_diff_pct": float((best_train_summary or {}).get("mdd_diff_pct", 0.0)),
                "train_turnover": float((best_train_summary or {}).get("avg_turnover", 0.0)),
                "test_cagr_diff_pct": float(selected_test.get("cagr_diff_pct", 0.0)),
                "test_mdd_diff_pct": float(selected_test.get("mdd_diff_pct", 0.0)),
                "test_turnover": float(selected_test.get("avg_turnover", 0.0)),
                "static_test_cagr_diff_pct": float(static_test.get("cagr_diff_pct", 0.0)),
                "static_test_mdd_diff_pct": float(static_test.get("mdd_diff_pct", 0.0)),
                "static_test_turnover": float(static_test.get("avg_turnover", 0.0)),
            }
        )

    wf_df = pd.concat(oos_selected_parts, ignore_index=True)
    static_df = pd.concat(oos_static_parts, ignore_index=True)

    def _summary_from_oos(df: pd.DataFrame, label: str) -> dict[str, Any]:
        ai = _risk_metrics(df["net_return_pct"])
        bench = _risk_metrics(df["benchmark_return_pct"])
        alpha = pd.to_numeric(df["net_return_pct"], errors="coerce") - pd.to_numeric(df["benchmark_return_pct"], errors="coerce")
        nw = _newey_west(alpha)
        return {
            "label": label,
            "periods": int(len(df)),
            "cagr_pct": float(ai["cagr_pct"]),
            "benchmark_cagr_pct": float(bench["cagr_pct"]),
            "cagr_diff_pct": float(ai["cagr_pct"] - bench["cagr_pct"]),
            "sharpe": float(ai["sharpe"]),
            "benchmark_sharpe": float(bench["sharpe"]),
            "mdd_pct": float(ai["max_drawdown_pct"]),
            "benchmark_mdd_pct": float(bench["max_drawdown_pct"]),
            "mdd_diff_pct": float(ai["max_drawdown_pct"] - bench["max_drawdown_pct"]),
            "avg_turnover": float(pd.to_numeric(df["turnover"], errors="coerce").mean()),
            **nw,
        }

    wf_selected_summary = _summary_from_oos(wf_df, "walkforward_selected")
    wf_static_summary = _summary_from_oos(static_df, "walkforward_static_baseline")

    folds_df = pd.DataFrame(fold_rows)
    FOLDS_CSV.parent.mkdir(parents=True, exist_ok=True)
    folds_df.to_csv(FOLDS_CSV, index=False)
    wf_df.to_csv(OOS_CSV, index=False)

    summary = {
        "run_tag": RUN_TAG,
        "inputs": {
            "start_date": START,
            "end_date": END,
            "train_years": int(TRAIN_YEARS),
            "test_years": int(TEST_YEARS),
            "min_test_weeks": int(MIN_TEST_WEEKS),
            "trade_cost_bps": float(TRADE_COST_BPS),
            "candidate_count": int(len(candidate_grid)),
            **meta,
        },
        "baseline_config": asdict(baseline_cfg),
        "folds": fold_rows,
        "oos_selected": wf_selected_summary,
        "oos_static_baseline": wf_static_summary,
        "paths": {
            "folds_csv": str(FOLDS_CSV.relative_to(ROOT)),
            "oos_csv": str(OOS_CSV.relative_to(ROOT)),
        },
    }
    SUMMARY_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    lines: list[str] = []
    lines.append("# Walk-Forward Stock Momentum Validation")
    lines.append("")
    lines.append(f"- start: `{START}`")
    lines.append(f"- end: `{END}`")
    lines.append(f"- train/test: **{TRAIN_YEARS}y / {TEST_YEARS}y**")
    lines.append(f"- candidate count: **{len(candidate_grid)}**")
    lines.append("")
    lines.append("## OOS Selected")
    lines.append(
        f"- CAGR {_fmt_pct(wf_selected_summary['cagr_pct'])} vs QQQ {_fmt_pct(wf_selected_summary['benchmark_cagr_pct'])} "
        f"| diff {_fmt_pct(wf_selected_summary['cagr_diff_pct'])} "
        f"| Sharpe {_fmt_num(wf_selected_summary['sharpe'])} vs {_fmt_num(wf_selected_summary['benchmark_sharpe'])} "
        f"| MDD {_fmt_pct(wf_selected_summary['mdd_pct'])} vs {_fmt_pct(wf_selected_summary['benchmark_mdd_pct'])}"
    )
    lines.append(
        f"- NW p(two-sided) {_fmt_num(wf_selected_summary['nw_p_two'], 3)} | "
        f"P(alpha>0) {_fmt_num(wf_selected_summary['nw_p_gt0'], 3)} | "
        f"turnover {_fmt_num(wf_selected_summary['avg_turnover'], 3)}"
    )
    lines.append("")
    lines.append("## OOS Static Baseline")
    lines.append(
        f"- CAGR {_fmt_pct(wf_static_summary['cagr_pct'])} vs QQQ {_fmt_pct(wf_static_summary['benchmark_cagr_pct'])} "
        f"| diff {_fmt_pct(wf_static_summary['cagr_diff_pct'])} "
        f"| Sharpe {_fmt_num(wf_static_summary['sharpe'])} vs {_fmt_num(wf_static_summary['benchmark_sharpe'])} "
        f"| MDD {_fmt_pct(wf_static_summary['mdd_pct'])} vs {_fmt_pct(wf_static_summary['benchmark_mdd_pct'])}"
    )
    lines.append("")
    lines.append("## Folds")
    lines.append("```text")
    lines.append(folds_df.to_string(index=False))
    lines.append("```")
    lines.append("")
    SUMMARY_MD.write_text("\n".join(lines), encoding="utf-8")

    print(f"Saved: {FOLDS_CSV.relative_to(ROOT)}")
    print(f"Saved: {OOS_CSV.relative_to(ROOT)}")
    print(f"Saved: {SUMMARY_JSON.relative_to(ROOT)}")
    print(f"Saved: {SUMMARY_MD.relative_to(ROOT)}")
    print(
        "Walk-forward OOS selected -> "
        f"CAGR {wf_selected_summary['cagr_pct']:.2f}% vs QQQ {wf_selected_summary['benchmark_cagr_pct']:.2f}% "
        f"| diff {wf_selected_summary['cagr_diff_pct']:+.2f}pp "
        f"| Sharpe {wf_selected_summary['sharpe']:.2f} vs {wf_selected_summary['benchmark_sharpe']:.2f} "
        f"| MDD {wf_selected_summary['mdd_pct']:.2f}% vs {wf_selected_summary['benchmark_mdd_pct']:.2f}% "
        f"| NW p2 {wf_selected_summary['nw_p_two']:.3f}"
    )


def _fmt_pct(x: Any) -> str:
    return f"{float(x):.2f}%"


def _fmt_num(x: Any, digits: int = 2) -> str:
    return f"{float(x):.{digits}f}"


if __name__ == "__main__":
    run()
