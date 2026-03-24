from __future__ import annotations

import importlib.util
import json
import math
import os
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from core.sec_pit import SecPointInTimeStore


RUNS_DIR = ROOT / "data" / "runs"
RUN_TAG = (os.getenv("HYP_RUN_TAG") or "").strip() or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

START = (os.getenv("HYP_START_DATE") or "2011-03-01").strip()
END = (os.getenv("HYP_END_DATE") or "2026-03-01").strip()
OOS_START = int(float(os.getenv("HYP_OOS_START_YEAR") or "2016"))
TRADE_COST_BPS = float(os.getenv("HYP_TRADE_COST_BPS") or "20")

SUMMARY_JSON = RUNS_DIR / f"stock_hypothesis_research_{RUN_TAG}.json"
SUMMARY_MD = RUNS_DIR / f"stock_hypothesis_research_{RUN_TAG}.md"


@dataclass(frozen=True)
class Hypothesis:
    name: str
    freq: str
    engine: str
    top_k: int
    top_k_neutral: int
    top_k_risk_off: int
    weight_mode: str
    min_positions_for_invest: int
    max_per_sector: int
    sector_bonus: float
    min_overlap: int
    pit_bonus: float = 0.0
    pit_max_filing_age: int = 240
    quality_weight: float = 0.60
    momentum_weight: float = 0.40
    min_quality_score: float = -1.5
    min_momentum_score: float = 0.0
    safe_use_trend_template: bool = True
    safe_min_volume_ratio: float = 0.0
    require_trend_template: bool = False
    pit_veto_threshold: float = -999.0
    pit_veto_max_filing_age: int = 180
    pit_veto_new_only: bool = False
    pit_veto_regimes: tuple[str, ...] = ()
    breadth_source_mode: str = "universe"
    neutral_min_breadth_up200: float = -1.0
    neutral_min_breadth_positive63: float = -1.0
    neutral_max_positions_when_weak: int = -1
    sector_focus_top_n: int = 0
    sector_focus_new_only: bool = False
    sector_focus_regimes: tuple[str, ...] = ()
    breadth_entry_source_mode: str = "universe"
    neutral_entry_min_breadth_up200: float = -1.0
    neutral_entry_min_breadth_positive63: float = -1.0
    neutral_max_new_names_when_weak: int = -1


def _load_bt_module(freq: str):
    os.environ["AI_START_DATE"] = START
    os.environ["AI_END_DATE"] = END
    os.environ["AI_DATA_START_DATE"] = str((pd.Timestamp(START) - pd.DateOffset(years=3)).date())
    os.environ["AI_DATA_END_DATE"] = str((pd.Timestamp(END) + pd.DateOffset(days=1)).date())
    os.environ["AI_SNAPSHOT_FREQ"] = str(freq)
    os.environ["AI_HORIZON_MODE"] = "next_snapshot"
    os.environ["AI_EXECUTION_TIMING"] = "next_open"
    path = ROOT / "scripts" / "backtest_ai_portfolio_selector.py"
    name = f"backtest_ai_portfolio_selector_hyp_{freq}"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _risk_metrics(series_pct: pd.Series, periods_per_year: int) -> dict[str, float]:
    s = pd.to_numeric(series_pct, errors="coerce").dropna().astype(float)
    if len(s) == 0:
        return {"periods": 0, "cagr_pct": 0.0, "sharpe": 0.0, "max_drawdown_pct": 0.0}
    r = s / 100.0
    n = len(r)
    c = (1.0 + r).cumprod()
    cagr = float(c.iloc[-1] ** (periods_per_year / n) - 1.0) * 100.0 if c.iloc[-1] > 0 else 0.0
    sd = float(r.std(ddof=1)) if n > 1 else 0.0
    sharpe = float((r.mean() / sd) * np.sqrt(periods_per_year)) if sd > 1e-12 else 0.0
    dd = (c / c.cummax()) - 1.0
    return {
        "periods": int(n),
        "cagr_pct": float(cagr),
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


def _build_records(bt: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    snaps = bt._snapshot_dates()
    universe_by_date = bt._load_universe_by_date(ROOT / "data" / "universe" / "nasdaq100_by_date_weekly_2006_2026.json")
    universe_lookup = bt._build_universe_asof_lookup(universe_by_date)
    requested_symbols = sorted({s for arr in universe_by_date.values() for s in arr})
    sector_lookup = bt._load_sector_lookup()
    pit_store = SecPointInTimeStore(requested_symbols)
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
        if not mkt or i + 1 >= len(snaps):
            continue
        next_mkt = bt._market_ctx(frames[bt.BENCH], frames.get(bt.VIX), snaps[i + 1], use_benchmark_features=True)
        if not next_mkt:
            continue

        signal_day = pd.Timestamp(mkt["day"])
        bench_signal_pos = bt._asof_pos(bench_ind.index, signal_day)
        if bench_signal_pos < bt.MIN_HISTORY_DAYS:
            continue

        exit_signal_day = pd.Timestamp(next_mkt["day"])
        bench_frame = frames[bt.BENCH]
        bench_entry_pos = bt._execution_pos(bench_frame.index, signal_day, "next_open")
        bench_exit_pos = bt._execution_pos(bench_frame.index, exit_signal_day, "next_open")
        if bench_entry_pos < 0 or bench_exit_pos <= bench_entry_pos or bench_exit_pos >= len(bench_frame):
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
            exit_pos = bt._execution_pos(raw_df.index, exit_signal_day, "next_open")
            if entry_pos < 0 or exit_pos <= entry_pos or exit_pos >= len(raw_df):
                continue
            entry_px = bt._execution_price(raw_df, entry_pos, "next_open")
            exit_px = bt._execution_price(raw_df, exit_pos, "next_open")
            if entry_px <= 0 or exit_px <= 0:
                continue

            ind_row = ind_df.iloc[signal_pos]
            rs63 = bt._f(ind_row.get("return_63d")) - bt._f(mkt["bench_r63"])
            rs21 = bt._f(ind_row.get("return_21d")) - bt._f(mkt["bench_r21"])
            sector_meta = sector_lookup.get(symbol, {})
            pit_meta = pit_store.features_asof(symbol, pd.Timestamp(signal_day).date())
            tt = bt._trend_template_checks(ind_row, rs63, rs63_min=0.0)
            feats.append(
                {
                    "symbol": symbol,
                    "sector": str(sector_meta.get("sector") or "Unknown"),
                    "industry": str(sector_meta.get("industry") or "Unknown"),
                    "close": bt._f(ind_row.get("close")),
                    "relative_strength_63d": float(rs63),
                    "relative_strength_21d": float(rs21),
                    "return_126d": bt._f(ind_row.get("return_126d")),
                    "return_63d": bt._f(ind_row.get("return_63d")),
                    "return_21d": bt._f(ind_row.get("return_21d")),
                    "vol_20": bt._f(ind_row.get("vol_20")),
                    "atr_pct": bt._f(ind_row.get("atr_pct")),
                    "dd_63": bt._f(ind_row.get("dd_63")),
                    "dd_252": bt._f(ind_row.get("dd_252")),
                    "ma50_gap": bt._f(ind_row.get("ma50_gap")),
                    "ma200_gap": bt._f(ind_row.get("ma200_gap")),
                    "ma200_trend_pct": bt._f(ind_row.get("ma200_trend_pct")),
                    "volume_ratio": bt._f(ind_row.get("volume_ratio"), 1.0),
                    "trend_template_pass": bool(tt.get("pass", False)),
                    **pit_meta,
                }
            )
            fwd_ret_by_symbol[symbol] = float((exit_px / entry_px - 1.0) * 100.0)

        records.append(
            {
                "entry_day": str(pd.Timestamp(bench_frame.index[bench_entry_pos]).date()),
                "exit_day": str(pd.Timestamp(bench_frame.index[bench_exit_pos]).date()),
                "market_regime": str(mkt.get("regime", "neutral")),
                "benchmark_return_pct": float((bench_exit_px / bench_entry_px - 1.0) * 100.0),
                "features": feats,
                "fwd_ret_by_symbol": fwd_ret_by_symbol,
            }
        )

    return records, {"snapshots": int(len(snaps)), "records": int(len(records)), "symbols": int(len(requested_symbols))}


def _safe_features(feats: list[dict[str, Any]], hypothesis: Hypothesis) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for row in feats:
        if hypothesis.safe_use_trend_template and not bool(row.get("trend_template_pass", False)):
            continue
        if float(row.get("volume_ratio", 0.0)) < float(hypothesis.safe_min_volume_ratio):
            continue
        filtered.append(row)
    return filtered


def _apply_pit_quality_veto(
    bt: Any,
    feats: list[dict[str, Any]],
    hypothesis: Hypothesis,
    exempt_symbols: set[str] | None = None,
) -> list[dict[str, Any]]:
    threshold = float(hypothesis.pit_veto_threshold)
    if threshold <= -100.0 or not feats:
        return feats

    bonus_by_symbol = bt._pit_symbol_bonus(feats, max_filing_age_days=hypothesis.pit_veto_max_filing_age)
    if not bonus_by_symbol:
        return feats

    exempt = {str(sym) for sym in (exempt_symbols or set()) if isinstance(sym, str) and sym}
    filtered = [
        row
        for row in feats
        if str(row.get("symbol")) in exempt or float(bonus_by_symbol.get(str(row.get("symbol")), 999.0)) >= threshold
    ]
    min_required = max(1, int(hypothesis.min_positions_for_invest))
    return filtered if len(filtered) >= min_required else feats


def _pit_veto_enabled_for_regime(hypothesis: Hypothesis, market_regime: str) -> bool:
    regimes = tuple(str(x).strip().lower() for x in hypothesis.pit_veto_regimes if str(x).strip())
    if not regimes:
        return True
    return str(market_regime).strip().lower() in set(regimes)


def _sector_focus_enabled_for_regime(hypothesis: Hypothesis, market_regime: str) -> bool:
    regimes = tuple(str(x).strip().lower() for x in hypothesis.sector_focus_regimes if str(x).strip())
    if not regimes:
        return True
    return str(market_regime).strip().lower() in set(regimes)


def _apply_sector_focus(
    feats: list[dict[str, Any]],
    hypothesis: Hypothesis,
    sector_scores: dict[str, float],
    exempt_symbols: set[str] | None = None,
) -> list[dict[str, Any]]:
    top_n = int(hypothesis.sector_focus_top_n)
    if top_n <= 0 or not feats:
        return feats

    ranked = sorted(
        ((str(sector), float(score)) for sector, score in dict(sector_scores or {}).items()),
        key=lambda item: item[1],
        reverse=True,
    )
    allowed_sectors = {sector for sector, _ in ranked[: max(0, top_n)] if sector}
    if not allowed_sectors:
        return feats

    exempt = {str(sym) for sym in (exempt_symbols or set()) if isinstance(sym, str) and sym}
    filtered = [
        row
        for row in feats
        if str(row.get("symbol")) in exempt or str(row.get("sector") or "Unknown") in allowed_sectors
    ]
    min_required = max(1, int(hypothesis.min_positions_for_invest))
    return filtered if len(filtered) >= min_required else feats


def _apply_breadth_new_entry_gate(
    bt: Any,
    feats: list[dict[str, Any]],
    safe_feats: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
    hypothesis: Hypothesis,
    market_regime: str,
    held_syms: list[str],
) -> list[dict[str, Any]]:
    if str(market_regime).strip().lower() != "neutral":
        return candidates
    max_new = int(hypothesis.neutral_max_new_names_when_weak)
    if max_new < 0:
        return candidates

    up200_min = float(hypothesis.neutral_entry_min_breadth_up200)
    pos63_min = float(hypothesis.neutral_entry_min_breadth_positive63)
    if up200_min < 0 and pos63_min < 0:
        return candidates

    source_mode = str(hypothesis.breadth_entry_source_mode or "universe").strip().lower() or "universe"
    breadth_feats = bt._resolve_breadth_features(feats, safe_feats, source_mode)
    breadth = bt._market_breadth(breadth_feats)
    up200_now = float(bt._f(breadth.get("up200"), 0.0))
    pos63_now = float(bt._f(breadth.get("positive_63d"), 0.0))

    weak = False
    if up200_min >= 0:
        weak = weak or up200_now < up200_min
    if pos63_min >= 0:
        weak = weak or pos63_now < pos63_min
    if not weak:
        return candidates

    held_set = {str(sym) for sym in held_syms if isinstance(sym, str) and sym}
    held_rows = [row for row in candidates if str(row.get("symbol")) in held_set]
    new_rows = [row for row in candidates if str(row.get("symbol")) not in held_set]
    return held_rows + new_rows[: max(0, max_new)]


def _apply_breadth_topk_gate(
    bt: Any,
    feats: list[dict[str, Any]],
    safe_feats: list[dict[str, Any]],
    hypothesis: Hypothesis,
    market_regime: str,
    target_top_k: int,
) -> int:
    current = max(0, int(target_top_k))
    if current <= 0:
        return 0
    if str(market_regime).strip().lower() != "neutral":
        return current
    weak_cap = int(hypothesis.neutral_max_positions_when_weak)
    if weak_cap < 0:
        return current

    up200_min = float(hypothesis.neutral_min_breadth_up200)
    pos63_min = float(hypothesis.neutral_min_breadth_positive63)
    if up200_min < 0 and pos63_min < 0:
        return current

    source_mode = str(hypothesis.breadth_source_mode or "universe").strip().lower() or "universe"
    breadth_feats = bt._resolve_breadth_features(feats, safe_feats, source_mode)
    breadth = bt._market_breadth(breadth_feats)
    up200_now = float(bt._f(breadth.get("up200"), 0.0))
    pos63_now = float(bt._f(breadth.get("positive_63d"), 0.0))
    weak = False
    if up200_min >= 0:
        weak = weak or up200_now < up200_min
    if pos63_min >= 0:
        weak = weak or pos63_now < pos63_min
    if not weak:
        return current
    return min(current, max(0, weak_cap))


def _evaluate(bt: Any, records: list[dict[str, Any]], hypothesis: Hypothesis) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    prev_port: dict[str, float] = {"__CASH__": 1.0}
    trade_cost_pct = float(TRADE_COST_BPS) / 100.0

    for rec in records:
        feats = list(rec["features"])
        safe_feats = _safe_features(feats, hypothesis)
        veto_active = _pit_veto_enabled_for_regime(hypothesis, str(rec["market_regime"]))
        sector_focus_active = _sector_focus_enabled_for_regime(hypothesis, str(rec["market_regime"]))
        if veto_active and not hypothesis.pit_veto_new_only:
            safe_feats = _apply_pit_quality_veto(bt, safe_feats, hypothesis)
        if sector_focus_active and not hypothesis.sector_focus_new_only:
            sector_scores_for_focus = bt._sector_strength_scores(safe_feats)
            safe_feats = _apply_sector_focus(safe_feats, hypothesis, sector_scores_for_focus)
        if not safe_feats:
            rows.append(
                {
                    "entry_day": rec["entry_day"],
                    "net_return_pct": 0.0,
                    "benchmark_return_pct": float(rec["benchmark_return_pct"]),
                    "turnover": 0.0,
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
            base_top_k=hypothesis.top_k,
            market_regime=str(rec["market_regime"]),
            safe_mode=True,
            require_risk_on=False,
            min_positions=1,
        )
        regime = str(rec["market_regime"]).lower()
        if regime == "neutral":
            target_top_k = min(target_top_k, hypothesis.top_k_neutral)
        elif regime == "risk_off":
            target_top_k = min(target_top_k, hypothesis.top_k_risk_off)
        target_top_k = _apply_breadth_topk_gate(
            bt=bt,
            feats=feats,
            safe_feats=safe_feats,
            hypothesis=hypothesis,
            market_regime=str(rec["market_regime"]),
            target_top_k=target_top_k,
        )

        candidates = bt._select_candidates_with_includes(safe_feats, 40, "top_rs63", held_syms)
        if veto_active and hypothesis.pit_veto_new_only:
            candidates = _apply_pit_quality_veto(bt, candidates, hypothesis, exempt_symbols=set(held_syms))
        if sector_focus_active and hypothesis.sector_focus_new_only:
            sector_scores_for_focus = bt._sector_strength_scores(safe_feats)
            candidates = _apply_sector_focus(candidates, hypothesis, sector_scores_for_focus, exempt_symbols=set(held_syms))
        candidates = _apply_breadth_new_entry_gate(
            bt=bt,
            feats=feats,
            safe_feats=safe_feats,
            candidates=candidates,
            hypothesis=hypothesis,
            market_regime=str(rec["market_regime"]),
            held_syms=held_syms,
        )
        allowed = {x["symbol"] for x in candidates if isinstance(x, dict) and x.get("symbol")}
        weights_pct: dict[str, float] = {}
        cash_pct = 100.0
        if target_top_k > 0 and allowed:
            sector_scores = bt._sector_strength_scores(safe_feats)
            if hypothesis.engine == "quality_momentum":
                out = bt._quality_momentum_portfolio(
                    candidates,
                    top_k=target_top_k,
                    weight_mode=hypothesis.weight_mode,
                    min_positions_for_invest=hypothesis.min_positions_for_invest,
                    max_per_sector=hypothesis.max_per_sector,
                    sector_bonus_mult=hypothesis.sector_bonus,
                    sector_scores=sector_scores,
                    max_filing_age_days=hypothesis.pit_max_filing_age,
                    quality_weight=hypothesis.quality_weight,
                    momentum_weight=hypothesis.momentum_weight,
                    min_quality_score=hypothesis.min_quality_score,
                    min_momentum_score=hypothesis.min_momentum_score,
                    require_trend_template=hypothesis.require_trend_template,
                )
            else:
                pit_bonus_by_symbol = bt._pit_symbol_bonus(safe_feats, max_filing_age_days=hypothesis.pit_max_filing_age)
                out = bt._stock_momentum_portfolio(
                    candidates,
                    top_k=target_top_k,
                    weight_mode=hypothesis.weight_mode,
                    min_positions_for_invest=hypothesis.min_positions_for_invest,
                    max_per_sector=hypothesis.max_per_sector,
                    sector_bonus_mult=hypothesis.sector_bonus,
                    sector_scores=sector_scores,
                    symbol_bonus={
                        sym: float(hypothesis.pit_bonus * pit_bonus_by_symbol.get(sym, 0.0))
                        for sym in pit_bonus_by_symbol
                    },
                )
            weights_pct, cash_pct = bt._portfolio_from_ai(out, allowed=allowed, top_k=target_top_k, max_weight_pct=20.0)
            feats_by_symbol = {str(x.get("symbol")): x for x in feats if isinstance(x, dict) and x.get("symbol")}
            weights_pct = bt._enforce_min_overlap(
                weights_pct,
                prev_port=prev_port,
                allowed=allowed,
                feats_by_symbol=feats_by_symbol,
                min_overlap=hypothesis.min_overlap,
                top_k=target_top_k,
            )
            weights_pct = {k: float(v) for k, v in weights_pct.items() if float(v) > 0 and k in allowed}
            if weights_pct:
                weights_pct, cash_pct = bt._portfolio_from_ai(
                    {"positions": [{"symbol": k, "weight_pct": v} for k, v in weights_pct.items()]},
                    allowed=allowed,
                    top_k=target_top_k,
                    max_weight_pct=20.0,
                )

        port = {sym: float(weight) / 100.0 for sym, weight in weights_pct.items()}
        port["__CASH__"] = float(cash_pct) / 100.0
        gross = sum(float(weight) * float(rec["fwd_ret_by_symbol"].get(sym, 0.0)) for sym, weight in port.items() if sym != "__CASH__")
        turn = bt._turnover(prev_port, port)
        net = float(gross - (trade_cost_pct * turn))
        rows.append(
            {
                "entry_day": rec["entry_day"],
                "net_return_pct": net,
                "benchmark_return_pct": float(rec["benchmark_return_pct"]),
                "turnover": float(turn),
            }
        )
        prev_port = dict(port)

    return pd.DataFrame(rows)


def _summarize(df: pd.DataFrame, periods_per_year: int) -> dict[str, float]:
    ai = _risk_metrics(df["net_return_pct"], periods_per_year)
    bench = _risk_metrics(df["benchmark_return_pct"], periods_per_year)
    alpha = pd.to_numeric(df["net_return_pct"], errors="coerce") - pd.to_numeric(df["benchmark_return_pct"], errors="coerce")
    nw = _newey_west(alpha)
    return {
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


def _hypotheses() -> list[Hypothesis]:
    return [
        Hypothesis(
            name="weekly_baseline_v4",
            freq="weekly",
            engine="stock_momentum",
            top_k=5,
            top_k_neutral=5,
            top_k_risk_off=0,
            weight_mode="equal",
            min_positions_for_invest=2,
            max_per_sector=2,
            sector_bonus=0.15,
            min_overlap=4,
            pit_bonus=0.0,
            safe_use_trend_template=True,
        ),
        Hypothesis(
            name="weekly_score_lightq",
            freq="weekly",
            engine="stock_momentum",
            top_k=5,
            top_k_neutral=5,
            top_k_risk_off=0,
            weight_mode="score",
            min_positions_for_invest=2,
            max_per_sector=2,
            sector_bonus=0.15,
            min_overlap=4,
            pit_bonus=0.10,
            safe_use_trend_template=True,
        ),
        Hypothesis(
            name="weekly_score_noq",
            freq="weekly",
            engine="stock_momentum",
            top_k=5,
            top_k_neutral=5,
            top_k_risk_off=0,
            weight_mode="score",
            min_positions_for_invest=2,
            max_per_sector=2,
            sector_bonus=0.15,
            min_overlap=4,
            pit_bonus=0.0,
            safe_use_trend_template=True,
        ),
        Hypothesis(
            name="weekly_veto_recentq_soft",
            freq="weekly",
            engine="stock_momentum",
            top_k=5,
            top_k_neutral=5,
            top_k_risk_off=0,
            weight_mode="equal",
            min_positions_for_invest=2,
            max_per_sector=2,
            sector_bonus=0.15,
            min_overlap=4,
            pit_bonus=0.0,
            safe_use_trend_template=True,
            pit_veto_threshold=-4.0,
            pit_veto_max_filing_age=180,
        ),
        Hypothesis(
            name="weekly_veto_recentq_mid",
            freq="weekly",
            engine="stock_momentum",
            top_k=5,
            top_k_neutral=5,
            top_k_risk_off=0,
            weight_mode="equal",
            min_positions_for_invest=2,
            max_per_sector=2,
            sector_bonus=0.15,
            min_overlap=4,
            pit_bonus=0.0,
            safe_use_trend_template=True,
            pit_veto_threshold=-2.5,
            pit_veto_max_filing_age=180,
        ),
        Hypothesis(
            name="weekly_veto_recentq_soft_bonus",
            freq="weekly",
            engine="stock_momentum",
            top_k=5,
            top_k_neutral=5,
            top_k_risk_off=0,
            weight_mode="equal",
            min_positions_for_invest=2,
            max_per_sector=2,
            sector_bonus=0.15,
            min_overlap=4,
            pit_bonus=0.05,
            safe_use_trend_template=True,
            pit_veto_threshold=-4.0,
            pit_veto_max_filing_age=180,
        ),
        Hypothesis(
            name="weekly_veto_recentq_newonly_soft",
            freq="weekly",
            engine="stock_momentum",
            top_k=5,
            top_k_neutral=5,
            top_k_risk_off=0,
            weight_mode="equal",
            min_positions_for_invest=2,
            max_per_sector=2,
            sector_bonus=0.15,
            min_overlap=4,
            pit_bonus=0.0,
            safe_use_trend_template=True,
            pit_veto_threshold=-4.0,
            pit_veto_max_filing_age=180,
            pit_veto_new_only=True,
        ),
        Hypothesis(
            name="weekly_veto_recentq_newonly_soft_bonus",
            freq="weekly",
            engine="stock_momentum",
            top_k=5,
            top_k_neutral=5,
            top_k_risk_off=0,
            weight_mode="equal",
            min_positions_for_invest=2,
            max_per_sector=2,
            sector_bonus=0.15,
            min_overlap=4,
            pit_bonus=0.05,
            safe_use_trend_template=True,
            pit_veto_threshold=-4.0,
            pit_veto_max_filing_age=180,
            pit_veto_new_only=True,
        ),
        Hypothesis(
            name="weekly_veto_recentq_newonly_nrisk_soft_bonus",
            freq="weekly",
            engine="stock_momentum",
            top_k=5,
            top_k_neutral=5,
            top_k_risk_off=0,
            weight_mode="equal",
            min_positions_for_invest=2,
            max_per_sector=2,
            sector_bonus=0.15,
            min_overlap=4,
            pit_bonus=0.05,
            safe_use_trend_template=True,
            pit_veto_threshold=-4.0,
            pit_veto_max_filing_age=180,
            pit_veto_new_only=True,
            pit_veto_regimes=("neutral", "risk_off"),
        ),
        Hypothesis(
            name="weekly_veto_recentq_newonly_neutral_soft_bonus",
            freq="weekly",
            engine="stock_momentum",
            top_k=5,
            top_k_neutral=5,
            top_k_risk_off=0,
            weight_mode="equal",
            min_positions_for_invest=2,
            max_per_sector=2,
            sector_bonus=0.15,
            min_overlap=4,
            pit_bonus=0.05,
            safe_use_trend_template=True,
            pit_veto_threshold=-4.0,
            pit_veto_max_filing_age=180,
            pit_veto_new_only=True,
            pit_veto_regimes=("neutral",),
        ),
        Hypothesis(
            name="weekly_veto_recentq_newonly_neutral_soft_bonus_ro2",
            freq="weekly",
            engine="stock_momentum",
            top_k=5,
            top_k_neutral=5,
            top_k_risk_off=2,
            weight_mode="equal",
            min_positions_for_invest=2,
            max_per_sector=2,
            sector_bonus=0.15,
            min_overlap=4,
            pit_bonus=0.05,
            safe_use_trend_template=True,
            pit_veto_threshold=-4.0,
            pit_veto_max_filing_age=180,
            pit_veto_new_only=True,
            pit_veto_regimes=("neutral",),
        ),
        Hypothesis(
            name="weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto35",
            freq="weekly",
            engine="stock_momentum",
            top_k=5,
            top_k_neutral=5,
            top_k_risk_off=2,
            weight_mode="equal",
            min_positions_for_invest=2,
            max_per_sector=2,
            sector_bonus=0.15,
            min_overlap=4,
            pit_bonus=0.05,
            safe_use_trend_template=True,
            pit_veto_threshold=-3.5,
            pit_veto_max_filing_age=180,
            pit_veto_new_only=True,
            pit_veto_regimes=("neutral",),
        ),
        Hypothesis(
            name="weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto325",
            freq="weekly",
            engine="stock_momentum",
            top_k=5,
            top_k_neutral=5,
            top_k_risk_off=2,
            weight_mode="equal",
            min_positions_for_invest=2,
            max_per_sector=2,
            sector_bonus=0.15,
            min_overlap=4,
            pit_bonus=0.05,
            safe_use_trend_template=True,
            pit_veto_threshold=-3.25,
            pit_veto_max_filing_age=180,
            pit_veto_new_only=True,
            pit_veto_regimes=("neutral",),
        ),
        Hypothesis(
            name="weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto325_bgcap2",
            freq="weekly",
            engine="stock_momentum",
            top_k=5,
            top_k_neutral=5,
            top_k_risk_off=2,
            weight_mode="equal",
            min_positions_for_invest=2,
            max_per_sector=2,
            sector_bonus=0.15,
            min_overlap=4,
            pit_bonus=0.05,
            safe_use_trend_template=True,
            pit_veto_threshold=-3.25,
            pit_veto_max_filing_age=180,
            pit_veto_new_only=True,
            pit_veto_regimes=("neutral",),
            breadth_source_mode="universe",
            neutral_min_breadth_up200=0.50,
            neutral_min_breadth_positive63=0.45,
            neutral_max_positions_when_weak=2,
        ),
        Hypothesis(
            name="weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto325_bgcap2_tight",
            freq="weekly",
            engine="stock_momentum",
            top_k=5,
            top_k_neutral=5,
            top_k_risk_off=2,
            weight_mode="equal",
            min_positions_for_invest=2,
            max_per_sector=2,
            sector_bonus=0.15,
            min_overlap=4,
            pit_bonus=0.05,
            safe_use_trend_template=True,
            pit_veto_threshold=-3.25,
            pit_veto_max_filing_age=180,
            pit_veto_new_only=True,
            pit_veto_regimes=("neutral",),
            breadth_source_mode="universe",
            neutral_min_breadth_up200=0.55,
            neutral_min_breadth_positive63=0.50,
            neutral_max_positions_when_weak=2,
        ),
        Hypothesis(
            name="weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto325_sector3",
            freq="weekly",
            engine="stock_momentum",
            top_k=5,
            top_k_neutral=5,
            top_k_risk_off=2,
            weight_mode="equal",
            min_positions_for_invest=2,
            max_per_sector=2,
            sector_bonus=0.15,
            min_overlap=4,
            pit_bonus=0.05,
            safe_use_trend_template=True,
            pit_veto_threshold=-3.25,
            pit_veto_max_filing_age=180,
            pit_veto_new_only=True,
            pit_veto_regimes=("neutral",),
            sector_focus_top_n=3,
            sector_focus_new_only=False,
            sector_focus_regimes=("neutral",),
        ),
        Hypothesis(
            name="weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto325_sector3new",
            freq="weekly",
            engine="stock_momentum",
            top_k=5,
            top_k_neutral=5,
            top_k_risk_off=2,
            weight_mode="equal",
            min_positions_for_invest=2,
            max_per_sector=2,
            sector_bonus=0.15,
            min_overlap=4,
            pit_bonus=0.05,
            safe_use_trend_template=True,
            pit_veto_threshold=-3.25,
            pit_veto_max_filing_age=180,
            pit_veto_new_only=True,
            pit_veto_regimes=("neutral",),
            sector_focus_top_n=3,
            sector_focus_new_only=True,
            sector_focus_regimes=("neutral",),
        ),
        Hypothesis(
            name="weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto325_entryfreeze0",
            freq="weekly",
            engine="stock_momentum",
            top_k=5,
            top_k_neutral=5,
            top_k_risk_off=2,
            weight_mode="equal",
            min_positions_for_invest=2,
            max_per_sector=2,
            sector_bonus=0.15,
            min_overlap=4,
            pit_bonus=0.05,
            safe_use_trend_template=True,
            pit_veto_threshold=-3.25,
            pit_veto_max_filing_age=180,
            pit_veto_new_only=True,
            pit_veto_regimes=("neutral",),
            breadth_entry_source_mode="universe",
            neutral_entry_min_breadth_up200=0.50,
            neutral_entry_min_breadth_positive63=0.45,
            neutral_max_new_names_when_weak=0,
        ),
        Hypothesis(
            name="weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto325_entryfreeze1",
            freq="weekly",
            engine="stock_momentum",
            top_k=5,
            top_k_neutral=5,
            top_k_risk_off=2,
            weight_mode="equal",
            min_positions_for_invest=2,
            max_per_sector=2,
            sector_bonus=0.15,
            min_overlap=4,
            pit_bonus=0.05,
            safe_use_trend_template=True,
            pit_veto_threshold=-3.25,
            pit_veto_max_filing_age=180,
            pit_veto_new_only=True,
            pit_veto_regimes=("neutral",),
            breadth_entry_source_mode="universe",
            neutral_entry_min_breadth_up200=0.50,
            neutral_entry_min_breadth_positive63=0.45,
            neutral_max_new_names_when_weak=1,
        ),
        Hypothesis(
            name="weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto325_entryfreeze1_pb007",
            freq="weekly",
            engine="stock_momentum",
            top_k=5,
            top_k_neutral=5,
            top_k_risk_off=2,
            weight_mode="equal",
            min_positions_for_invest=2,
            max_per_sector=2,
            sector_bonus=0.15,
            min_overlap=4,
            pit_bonus=0.07,
            safe_use_trend_template=True,
            pit_veto_threshold=-3.25,
            pit_veto_max_filing_age=180,
            pit_veto_new_only=True,
            pit_veto_regimes=("neutral",),
            breadth_entry_source_mode="universe",
            neutral_entry_min_breadth_up200=0.50,
            neutral_entry_min_breadth_positive63=0.45,
            neutral_max_new_names_when_weak=1,
        ),
        Hypothesis(
            name="weekly_veto_recentq_newonly_nrisk_soft_bonus_ro2",
            freq="weekly",
            engine="stock_momentum",
            top_k=5,
            top_k_neutral=5,
            top_k_risk_off=2,
            weight_mode="equal",
            min_positions_for_invest=2,
            max_per_sector=2,
            sector_bonus=0.15,
            min_overlap=4,
            pit_bonus=0.05,
            safe_use_trend_template=True,
            pit_veto_threshold=-4.0,
            pit_veto_max_filing_age=180,
            pit_veto_new_only=True,
            pit_veto_regimes=("neutral", "risk_off"),
        ),
        Hypothesis(
            name="monthly_score_noq",
            freq="monthly",
            engine="stock_momentum",
            top_k=8,
            top_k_neutral=5,
            top_k_risk_off=0,
            weight_mode="score",
            min_positions_for_invest=4,
            max_per_sector=2,
            sector_bonus=0.15,
            min_overlap=6,
            pit_bonus=0.0,
            safe_use_trend_template=True,
        ),
        Hypothesis(
            name="monthly_score_lightq",
            freq="monthly",
            engine="stock_momentum",
            top_k=8,
            top_k_neutral=5,
            top_k_risk_off=0,
            weight_mode="score",
            min_positions_for_invest=4,
            max_per_sector=2,
            sector_bonus=0.15,
            min_overlap=6,
            pit_bonus=0.10,
            safe_use_trend_template=True,
        ),
        Hypothesis(
            name="monthly_quality_soft",
            freq="monthly",
            engine="quality_momentum",
            top_k=8,
            top_k_neutral=5,
            top_k_risk_off=0,
            weight_mode="score",
            min_positions_for_invest=4,
            max_per_sector=2,
            sector_bonus=0.15,
            min_overlap=6,
            quality_weight=0.20,
            momentum_weight=0.80,
            min_quality_score=-4.0,
            min_momentum_score=0.0,
            safe_use_trend_template=False,
            require_trend_template=False,
        ),
        Hypothesis(
            name="monthly_quality_balance",
            freq="monthly",
            engine="quality_momentum",
            top_k=8,
            top_k_neutral=5,
            top_k_risk_off=0,
            weight_mode="inv_vol",
            min_positions_for_invest=4,
            max_per_sector=2,
            sector_bonus=0.15,
            min_overlap=6,
            quality_weight=0.65,
            momentum_weight=0.35,
            min_quality_score=-1.0,
            min_momentum_score=0.0,
            safe_use_trend_template=False,
            require_trend_template=False,
        ),
        Hypothesis(
            name="monthly_score_notrend_lightq",
            freq="monthly",
            engine="stock_momentum",
            top_k=8,
            top_k_neutral=5,
            top_k_risk_off=0,
            weight_mode="score",
            min_positions_for_invest=4,
            max_per_sector=2,
            sector_bonus=0.15,
            min_overlap=6,
            pit_bonus=0.10,
            safe_use_trend_template=False,
        ),
    ]


def run() -> None:
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    bt_by_freq = {freq: _load_bt_module(freq) for freq in ("weekly", "monthly")}
    records_by_freq = {}
    meta_by_freq = {}
    for freq, bt in bt_by_freq.items():
        records, meta = _build_records(bt)
        records_by_freq[freq] = records
        meta_by_freq[freq] = meta

    rows: list[dict[str, Any]] = []
    for hypothesis in _hypotheses():
        bt = bt_by_freq[hypothesis.freq]
        records = records_by_freq[hypothesis.freq]
        df = _evaluate(bt, records, hypothesis)
        full = _summarize(df, bt._periods_per_year(hypothesis.freq))
        df_oos = df.loc[pd.to_datetime(df["entry_day"], errors="coerce").dt.year >= int(OOS_START)].copy()
        oos = _summarize(df_oos, bt._periods_per_year(hypothesis.freq)) if not df_oos.empty else {}
        rows.append(
            {
                "name": hypothesis.name,
                "freq": hypothesis.freq,
                "engine": hypothesis.engine,
                "config": asdict(hypothesis),
                "full": full,
                "oos": oos,
            }
        )

    rows.sort(
        key=lambda item: (
            1 if float(item["oos"].get("cagr_diff_pct", -999.0)) > 0 else 0,
            1 if float(item["oos"].get("mdd_diff_pct", -999.0)) >= 0 else 0,
            float(item["oos"].get("cagr_diff_pct", -999.0)),
            float(item["oos"].get("mdd_diff_pct", -999.0)),
            float(item["oos"].get("sharpe", -999.0)) - float(item["oos"].get("benchmark_sharpe", -999.0)),
            -float(item["oos"].get("avg_turnover", 999.0)),
        ),
        reverse=True,
    )

    summary = {
        "run_tag": RUN_TAG,
        "inputs": {
            "start_date": START,
            "end_date": END,
            "oos_start_year": int(OOS_START),
            "trade_cost_bps": float(TRADE_COST_BPS),
            "hypothesis_count": int(len(rows)),
            "meta_by_freq": meta_by_freq,
        },
        "results": rows,
    }
    SUMMARY_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# Stock Hypothesis Research",
        "",
        f"- start: `{START}`",
        f"- end: `{END}`",
        f"- OOS start year: `{OOS_START}`",
        f"- trade cost: `{TRADE_COST_BPS:.0f}bps`",
        "",
        "## Ranked Results",
    ]
    for item in rows:
        full = item["full"]
        oos = item["oos"]
        lines.append(
            f"- `{item['name']}` [{item['freq']}/{item['engine']}] "
            f"| OOS diff {oos.get('cagr_diff_pct', 0.0):+.2f}pp "
            f"| OOS MDD diff {oos.get('mdd_diff_pct', 0.0):+.2f}pp "
            f"| OOS Sharpe {oos.get('sharpe', 0.0):.2f} vs {oos.get('benchmark_sharpe', 0.0):.2f} "
            f"| Full diff {full.get('cagr_diff_pct', 0.0):+.2f}pp"
        )
    SUMMARY_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"Saved: {SUMMARY_JSON.relative_to(ROOT)}")
    print(f"Saved: {SUMMARY_MD.relative_to(ROOT)}")
    if rows:
        best = rows[0]
        print(
            "Best OOS candidate -> "
            f"{best['name']} | OOS CAGR diff {best['oos'].get('cagr_diff_pct', 0.0):+.2f}pp "
            f"| OOS MDD diff {best['oos'].get('mdd_diff_pct', 0.0):+.2f}pp"
        )


if __name__ == "__main__":
    run()
