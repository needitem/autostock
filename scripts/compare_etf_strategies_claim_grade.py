from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd
import yfinance as yf


ROOT = Path(__file__).resolve().parents[1]
RUNS_DIR = ROOT / "data" / "runs"
RUN_TAG = (os.getenv("STRAT_RUN_TAG") or "").strip() or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

START = (os.getenv("STRAT_START_DATE") or "2018-01-01").strip()
END = (os.getenv("STRAT_END_DATE") or "2025-12-31").strip()
FREQ = (os.getenv("STRAT_SNAPSHOT_FREQ") or "weekly").strip().lower()
BENCH = (os.getenv("STRAT_BENCHMARK") or "QQQ").strip().upper()
SAFE = (os.getenv("STRAT_SAFE_ASSET") or "BIL").strip().upper()
TRADE_COST_BPS = float(os.getenv("STRAT_TRADE_COST_BPS") or "20")

CSV_OUT = RUNS_DIR / f"etf_strategy_compare_{RUN_TAG}.csv"
JSON_OUT = RUNS_DIR / f"etf_strategy_compare_{RUN_TAG}.json"
MD_OUT = RUNS_DIR / f"etf_strategy_compare_{RUN_TAG}.md"


def _f(x: float | int | None, d: float = 0.0) -> float:
    try:
        y = float(x)
        return d if np.isnan(y) or np.isinf(y) else y
    except Exception:
        return d


def _snapshot_dates() -> list[pd.Timestamp]:
    start = pd.Timestamp(START)
    end = pd.Timestamp(END)
    if end < start:
        start, end = end, start
    if FREQ in {"weekly", "w", "week"}:
        ds = pd.date_range(start=start, end=end, freq="W-FRI")
    elif FREQ in {"monthly", "m", "month"}:
        ds = pd.date_range(start=start, end=end, freq="ME")
    else:
        ds = pd.date_range(start=start, end=end, freq="W-FRI")
    return [pd.Timestamp(d).normalize() for d in ds]


def _asof_pos(idx: pd.Index, dt: pd.Timestamp) -> int:
    try:
        p = int(idx.searchsorted(pd.Timestamp(dt), side="right")) - 1
        return p if p >= 0 else -1
    except Exception:
        return -1


def _ret_lookback(close: pd.Series, pos: int, days: int) -> float:
    if pos < 0 or pos - days < 0 or pos >= len(close):
        return float("nan")
    p0 = _f(close.iloc[pos - days], np.nan)
    p1 = _f(close.iloc[pos], np.nan)
    if not np.isfinite(p0) or not np.isfinite(p1) or p0 <= 0 or p1 <= 0:
        return float("nan")
    return p1 / p0 - 1.0


def _sma(close: pd.Series, pos: int, window: int) -> float:
    if pos + 1 < window:
        return float("nan")
    v = pd.to_numeric(close.iloc[pos - window + 1 : pos + 1], errors="coerce").dropna()
    if len(v) < window:
        return float("nan")
    return float(v.mean())


def _vol(close: pd.Series, pos: int, window: int) -> float:
    if pos < window:
        return float("nan")
    r = pd.to_numeric(close.iloc[pos - window : pos + 1], errors="coerce").pct_change().dropna()
    if len(r) < max(5, window // 2):
        return float("nan")
    return float(r.std(ddof=1))


def _risk_metrics(returns: pd.Series, periods_per_year: int) -> dict[str, float]:
    r = pd.to_numeric(returns, errors="coerce").dropna().astype(float)
    if len(r) == 0:
        return {
            "periods": 0,
            "cagr_pct": 0.0,
            "total_return_pct": 0.0,
            "sharpe": 0.0,
            "max_drawdown_pct": 0.0,
        }
    n = len(r)
    c = (1.0 + r).cumprod()
    total = float(c.iloc[-1] - 1.0)
    cagr = float(c.iloc[-1] ** (periods_per_year / n) - 1.0) if c.iloc[-1] > 0 else 0.0
    sd = float(r.std(ddof=1)) if n > 1 else 0.0
    sharpe = float((r.mean() / sd) * np.sqrt(periods_per_year)) if sd > 1e-12 else 0.0
    dd = (c / c.cummax()) - 1.0
    return {
        "periods": int(n),
        "cagr_pct": cagr * 100.0,
        "total_return_pct": total * 100.0,
        "sharpe": sharpe,
        "max_drawdown_pct": float(dd.min() * 100.0),
    }


def _newey_west_t(alpha: pd.Series, lag: int | None = None) -> dict[str, float]:
    a = pd.to_numeric(alpha, errors="coerce").dropna().astype(float)
    n = len(a)
    if n < 5:
        return {"nw_t": 0.0, "nw_p_two_sided": 1.0, "nw_p_gt0": 0.5, "nw_lag": 0}
    mu = float(a.mean())
    x = a.to_numpy(dtype=float)
    eps = x - mu
    if lag is None:
        lag = int(round(4.0 * ((n / 100.0) ** (2.0 / 9.0))))
    lag = max(1, min(lag, n - 1))
    gamma0 = float(np.dot(eps, eps) / n)
    lrv = gamma0
    for l in range(1, lag + 1):
        cov = float(np.dot(eps[l:], eps[:-l]) / n)
        w = 1.0 - (l / (lag + 1.0))
        lrv += 2.0 * w * cov
    lrv = max(0.0, lrv)
    se = math.sqrt(lrv / n) if n > 0 else 0.0
    t = mu / se if se > 1e-12 else 0.0
    p2 = float(math.erfc(abs(t) / math.sqrt(2.0)))
    pgt0 = float(0.5 * (1.0 + math.erf(t / math.sqrt(2.0))))
    return {"nw_t": float(t), "nw_p_two_sided": p2, "nw_p_gt0": pgt0, "nw_lag": int(lag)}


def _block_bootstrap_cagr_diff(
    strat_r: pd.Series,
    bench_r: pd.Series,
    periods_per_year: int,
    block_len: int = 3,
    samples: int = 5000,
    seed: int = 7,
) -> dict[str, float | list[float]]:
    a = pd.to_numeric(strat_r, errors="coerce").dropna().astype(float).to_numpy()
    b = pd.to_numeric(bench_r, errors="coerce").dropna().astype(float).to_numpy()
    n = min(len(a), len(b))
    if n < 5:
        return {"p_cagr_diff_gt0": 0.5, "cagr_diff_ci95": [0.0, 0.0]}
    a = a[:n]
    b = b[:n]
    rng = np.random.default_rng(seed)

    def _sample_idx() -> np.ndarray:
        idx: list[int] = []
        while len(idx) < n:
            s = int(rng.integers(0, n))
            idx.extend(range(s, min(n, s + block_len)))
        return np.array(idx[:n], dtype=int)

    diffs = np.empty(samples, dtype=float)
    for i in range(samples):
        idx = _sample_idx()
        sa = a[idx]
        sb = b[idx]
        ca = float(np.prod(1.0 + sa))
        cb = float(np.prod(1.0 + sb))
        cagr_a = (ca ** (periods_per_year / n) - 1.0) if ca > 0 else -1.0
        cagr_b = (cb ** (periods_per_year / n) - 1.0) if cb > 0 else -1.0
        diffs[i] = cagr_a - cagr_b

    lo, hi = np.quantile(diffs, [0.025, 0.975])
    return {
        "p_cagr_diff_gt0": float((diffs > 0).mean()),
        "cagr_diff_ci95": [float(lo * 100.0), float(hi * 100.0)],
    }


def _turnover(prev_w: dict[str, float], new_w: dict[str, float]) -> float:
    keys = set(prev_w) | set(new_w)
    return float(0.5 * sum(abs(new_w.get(k, 0.0) - prev_w.get(k, 0.0)) for k in keys))


def _normalize_long_only(weights: dict[str, float], safe_asset: str) -> dict[str, float]:
    w = {k: max(0.0, float(v)) for k, v in weights.items() if k and k != "__CASH__"}
    s = float(sum(w.values()))
    out: dict[str, float] = {}
    if s > 1e-12:
        out = {k: float(v / s) for k, v in w.items()}
    else:
        out[safe_asset] = 1.0
    out["__CASH__"] = max(0.0, 1.0 - float(sum(v for k, v in out.items() if k != "__CASH__")))
    return out


def _safe_weight(safe_asset: str) -> dict[str, float]:
    return {safe_asset: 1.0, "__CASH__": 0.0}


def _dual_momentum(
    close_by_symbol: dict[str, pd.Series],
    pos: int,
    safe_asset: str,
    universe: list[str],
    lookback: int,
) -> dict[str, float]:
    scores: list[tuple[str, float]] = []
    for s in universe:
        c = close_by_symbol.get(s)
        if c is None:
            continue
        r = _ret_lookback(c, pos, lookback)
        if np.isfinite(r):
            scores.append((s, float(r)))
    if not scores:
        return _safe_weight(safe_asset)
    scores.sort(key=lambda x: x[1], reverse=True)
    best, sc = scores[0]
    if sc <= 0:
        return _safe_weight(safe_asset)
    return _normalize_long_only({best: 1.0}, safe_asset)


def _taa_weighted_momentum(
    close_by_symbol: dict[str, pd.Series],
    pos: int,
    safe_asset: str,
    universe: list[str],
    top_n: int,
) -> dict[str, float]:
    scores: list[tuple[str, float]] = []
    for s in universe:
        c = close_by_symbol.get(s)
        if c is None:
            continue
        r21 = _ret_lookback(c, pos, 21)
        r63 = _ret_lookback(c, pos, 63)
        r126 = _ret_lookback(c, pos, 126)
        r252 = _ret_lookback(c, pos, 252)
        if not all(np.isfinite(x) for x in (r21, r63, r126, r252)):
            continue
        score = 12.0 * r21 + 4.0 * r63 + 2.0 * r126 + 1.0 * r252
        scores.append((s, float(score)))
    if not scores:
        return _safe_weight(safe_asset)
    scores.sort(key=lambda x: x[1], reverse=True)
    picked = [s for s, sc in scores if sc > 0][: max(1, int(top_n))]
    if not picked:
        return _safe_weight(safe_asset)
    w = 1.0 / len(picked)
    return _normalize_long_only({s: w for s in picked}, safe_asset)


def _qqq_trend_filter(close_by_symbol: dict[str, pd.Series], pos: int, safe_asset: str, ma_window: int) -> dict[str, float]:
    c = close_by_symbol.get("QQQ")
    if c is None or pos < ma_window or pos >= len(c):
        return _safe_weight(safe_asset)
    price = _f(c.iloc[pos], np.nan)
    ma = _sma(c, pos, ma_window)
    if np.isfinite(price) and np.isfinite(ma) and price > ma:
        return _normalize_long_only({"QQQ": 1.0}, safe_asset)
    return _safe_weight(safe_asset)


def _asset_trend_filter(
    close_by_symbol: dict[str, pd.Series],
    pos: int,
    safe_asset: str,
    asset: str,
    ma_window: int,
) -> dict[str, float]:
    c = close_by_symbol.get(asset)
    if c is None or pos < ma_window or pos >= len(c):
        return _safe_weight(safe_asset)
    price = _f(c.iloc[pos], np.nan)
    ma = _sma(c, pos, ma_window)
    if np.isfinite(price) and np.isfinite(ma) and price > ma:
        return _normalize_long_only({asset: 1.0}, safe_asset)
    return _safe_weight(safe_asset)


def _sector_rotation(
    close_by_symbol: dict[str, pd.Series],
    pos: int,
    safe_asset: str,
    sectors: list[str],
    top_n: int,
    lookback: int = 126,
    ma_window: int = 200,
) -> dict[str, float]:
    scores: list[tuple[str, float]] = []
    for s in sectors:
        c = close_by_symbol.get(s)
        if c is None:
            continue
        r = _ret_lookback(c, pos, lookback)
        ma = _sma(c, pos, ma_window)
        price = _f(c.iloc[pos], np.nan) if pos < len(c) else np.nan
        if not np.isfinite(r) or not np.isfinite(ma) or not np.isfinite(price):
            continue
        if r <= 0 or price <= ma:
            continue
        scores.append((s, float(r)))
    if not scores:
        return _safe_weight(safe_asset)
    scores.sort(key=lambda x: x[1], reverse=True)
    picked = [s for s, _ in scores[: max(1, int(top_n))]]
    w = 1.0 / len(picked)
    return _normalize_long_only({s: w for s in picked}, safe_asset)


def _risk_parity_trend(
    close_by_symbol: dict[str, pd.Series],
    pos: int,
    safe_asset: str,
    assets: list[str],
    vol_window: int = 20,
    ma_window: int = 200,
    mom_window: int = 126,
) -> dict[str, float]:
    inv_vol: dict[str, float] = {}
    for s in assets:
        c = close_by_symbol.get(s)
        if c is None:
            continue
        price = _f(c.iloc[pos], np.nan) if pos < len(c) else np.nan
        ma = _sma(c, pos, ma_window)
        mom = _ret_lookback(c, pos, mom_window)
        v = _vol(c, pos, vol_window)
        if not all(np.isfinite(x) for x in (price, ma, mom, v)):
            continue
        if price <= ma or mom <= 0 or v <= 1e-8:
            continue
        inv_vol[s] = float(1.0 / v)
    if not inv_vol:
        return _safe_weight(safe_asset)
    tot = float(sum(inv_vol.values()))
    raw = {s: v / tot for s, v in inv_vol.items()}
    return _normalize_long_only(raw, safe_asset)


def _pairs_mean_reversion(
    close_by_symbol: dict[str, pd.Series],
    pos: int,
    z_entry: float = 1.0,
    lookback: int = 60,
) -> dict[str, float]:
    q = close_by_symbol.get("QQQ")
    s = close_by_symbol.get("SPY")
    if q is None or s is None or pos < lookback:
        return {"__CASH__": 1.0}
    qq = pd.to_numeric(q.iloc[: pos + 1], errors="coerce").astype(float)
    ss = pd.to_numeric(s.iloc[: pos + 1], errors="coerce").astype(float)
    ratio = np.log(qq / ss).replace([np.inf, -np.inf], np.nan).dropna()
    if len(ratio) < lookback:
        return {"__CASH__": 1.0}
    w = ratio.iloc[-lookback:]
    mu = float(w.mean())
    sd = float(w.std(ddof=1))
    if sd <= 1e-12:
        return {"__CASH__": 1.0}
    z = float((ratio.iloc[-1] - mu) / sd)
    if z >= float(z_entry):
        return {"QQQ": -0.5, "SPY": 0.5, "__CASH__": 0.0}
    if z <= -float(z_entry):
        return {"QQQ": 0.5, "SPY": -0.5, "__CASH__": 0.0}
    return {"__CASH__": 1.0}


@dataclass(frozen=True)
class StrategySpec:
    name: str
    mode: str  # long_only | long_short
    fn: Callable[[dict[str, pd.Series], int], dict[str, float]]


def _build_specs() -> list[StrategySpec]:
    risk_assets = ["QQQ", "SPY", "IWM", "EFA", "EEM"]
    taa_assets = ["QQQ", "SPY", "EFA", "EEM", "IEF", "TLT", "GLD", "VNQ"]
    sectors = ["XLK", "XLF", "XLV", "XLI", "XLY", "XLP", "XLE", "XLB", "XLU", "XLC", "XLRE"]
    rp_assets = ["QQQ", "IEF", "TLT", "GLD"]
    lev_assets = ["TQQQ", "QQQ", "SPY", "TLT"]
    return [
        StrategySpec(
            name="qqq_200ma_filter",
            mode="long_only",
            fn=lambda c, p: _qqq_trend_filter(c, p, SAFE, ma_window=200),
        ),
        StrategySpec(
            name="dual_momentum_12m",
            mode="long_only",
            fn=lambda c, p: _dual_momentum(c, p, SAFE, risk_assets, lookback=252),
        ),
        StrategySpec(
            name="dual_momentum_6m",
            mode="long_only",
            fn=lambda c, p: _dual_momentum(c, p, SAFE, risk_assets, lookback=126),
        ),
        StrategySpec(
            name="trend_tqqq_200ma",
            mode="long_only",
            fn=lambda c, p: _asset_trend_filter(c, p, SAFE, asset="TQQQ", ma_window=200),
        ),
        StrategySpec(
            name="trend_tqqq_150ma",
            mode="long_only",
            fn=lambda c, p: _asset_trend_filter(c, p, SAFE, asset="TQQQ", ma_window=150),
        ),
        StrategySpec(
            name="trend_tqqq_100ma",
            mode="long_only",
            fn=lambda c, p: _asset_trend_filter(c, p, SAFE, asset="TQQQ", ma_window=100),
        ),
        StrategySpec(
            name="trend_qld_200ma",
            mode="long_only",
            fn=lambda c, p: _asset_trend_filter(c, p, SAFE, asset="QLD", ma_window=200),
        ),
        StrategySpec(
            name="dual_momentum_levered_12m",
            mode="long_only",
            fn=lambda c, p: _dual_momentum(c, p, SAFE, lev_assets, lookback=252),
        ),
        StrategySpec(
            name="dual_momentum_levered_6m",
            mode="long_only",
            fn=lambda c, p: _dual_momentum(c, p, SAFE, lev_assets, lookback=126),
        ),
        StrategySpec(
            name="taa_weighted_top3",
            mode="long_only",
            fn=lambda c, p: _taa_weighted_momentum(c, p, SAFE, taa_assets, top_n=3),
        ),
        StrategySpec(
            name="taa_weighted_top2",
            mode="long_only",
            fn=lambda c, p: _taa_weighted_momentum(c, p, SAFE, taa_assets, top_n=2),
        ),
        StrategySpec(
            name="sector_rotation_top3",
            mode="long_only",
            fn=lambda c, p: _sector_rotation(c, p, SAFE, sectors, top_n=3),
        ),
        StrategySpec(
            name="sector_rotation_top2",
            mode="long_only",
            fn=lambda c, p: _sector_rotation(c, p, SAFE, sectors, top_n=2),
        ),
        StrategySpec(
            name="risk_parity_trend",
            mode="long_only",
            fn=lambda c, p: _risk_parity_trend(c, p, SAFE, rp_assets),
        ),
        StrategySpec(
            name="pairs_qqq_spy_z1",
            mode="long_short",
            fn=lambda c, p: _pairs_mean_reversion(c, p, z_entry=1.0),
        ),
        StrategySpec(
            name="pairs_qqq_spy_z1_5",
            mode="long_short",
            fn=lambda c, p: _pairs_mean_reversion(c, p, z_entry=1.5),
        ),
    ]


def _download_frames(symbols: list[str]) -> dict[str, pd.DataFrame]:
    raw = yf.download(
        tickers=sorted(set(symbols)),
        start="2010-01-01",
        end="2026-12-31",
        auto_adjust=False,
        progress=False,
        group_by="ticker",
        threads=True,
    )
    out: dict[str, pd.DataFrame] = {}
    if not isinstance(raw.columns, pd.MultiIndex):
        return out
    for s in sorted(set(symbols)):
        if s not in raw.columns.get_level_values(0):
            continue
        f = raw[s].copy()
        req = ["Open", "Close"]
        if any(c not in f.columns for c in req):
            continue
        out[s] = f[["Open", "Close"]].dropna().sort_index()
    return out


def _backtest_strategy(
    spec: StrategySpec,
    frames: dict[str, pd.DataFrame],
    snaps: list[pd.Timestamp],
    cost_bps: float,
    periods_per_year: int = 52,
) -> dict[str, float | str | list[float]]:
    if BENCH not in frames:
        raise RuntimeError(f"Missing benchmark frame: {BENCH}")
    bench = frames[BENCH]
    close_by_symbol = {k: v["Close"] for k, v in frames.items()}

    prev_w: dict[str, float] = {"__CASH__": 1.0}
    strat_r: list[float] = []
    bench_r: list[float] = []
    turns: list[float] = []
    valid_periods = 0

    for i in range(len(snaps) - 1):
        sdt = snaps[i]
        edt = snaps[i + 1]
        signal_pos_b = _asof_pos(bench.index, sdt)
        next_signal_pos_b = _asof_pos(bench.index, edt)
        if signal_pos_b < 252 or next_signal_pos_b < 0:
            continue
        entry_pos_b = signal_pos_b + 1
        exit_pos_b = next_signal_pos_b + 1
        if entry_pos_b <= 0 or exit_pos_b <= entry_pos_b or exit_pos_b >= len(bench):
            continue
        b_e = _f(bench.iloc[entry_pos_b]["Open"], np.nan)
        b_x = _f(bench.iloc[exit_pos_b]["Open"], np.nan)
        if not np.isfinite(b_e) or not np.isfinite(b_x) or b_e <= 0 or b_x <= 0:
            continue
        b_ret = b_x / b_e - 1.0

        w = spec.fn(close_by_symbol, signal_pos_b)
        if spec.mode == "long_only":
            w = _normalize_long_only(w, SAFE)
        else:
            # Keep provided signed weights; if cash missing, set residual cash by net exposure.
            if "__CASH__" not in w:
                net = float(sum(v for k, v in w.items() if k != "__CASH__"))
                w["__CASH__"] = float(1.0 - net)

        gross = 0.0
        for sym, wt in w.items():
            if sym == "__CASH__":
                continue
            f = frames.get(sym)
            if f is None:
                continue
            signal_pos = _asof_pos(f.index, sdt)
            next_signal_pos = _asof_pos(f.index, edt)
            if signal_pos < 0 or next_signal_pos < 0:
                continue
            epos = signal_pos + 1
            xpos = next_signal_pos + 1
            if epos <= 0 or xpos <= epos or xpos >= len(f):
                continue
            p0 = _f(f.iloc[epos]["Open"], np.nan)
            p1 = _f(f.iloc[xpos]["Open"], np.nan)
            if not np.isfinite(p0) or not np.isfinite(p1) or p0 <= 0 or p1 <= 0:
                continue
            r = p1 / p0 - 1.0
            gross += float(wt) * float(r)

        trn = _turnover(prev_w, w)
        cost = (float(cost_bps) / 10000.0) * trn
        net = gross - cost

        strat_r.append(float(net))
        bench_r.append(float(b_ret))
        turns.append(float(trn))
        prev_w = dict(w)
        valid_periods += 1

    if valid_periods == 0:
        return {
            "strategy": spec.name,
            "mode": spec.mode,
            "periods": 0,
            "error": "no_valid_periods",
        }

    s_ser = pd.Series(strat_r, dtype=float)
    b_ser = pd.Series(bench_r, dtype=float)
    alpha = s_ser - b_ser

    sm = _risk_metrics(s_ser, periods_per_year=periods_per_year)
    bm = _risk_metrics(b_ser, periods_per_year=periods_per_year)
    nw = _newey_west_t(alpha)
    boot = _block_bootstrap_cagr_diff(s_ser, b_ser, periods_per_year=periods_per_year)

    return {
        "strategy": spec.name,
        "mode": spec.mode,
        "periods": int(valid_periods),
        "cagr_pct": float(sm["cagr_pct"]),
        "benchmark_cagr_pct": float(bm["cagr_pct"]),
        "cagr_diff_pctp": float(sm["cagr_pct"] - bm["cagr_pct"]),
        "total_return_pct": float(sm["total_return_pct"]),
        "benchmark_total_return_pct": float(bm["total_return_pct"]),
        "total_diff_pctp": float(sm["total_return_pct"] - bm["total_return_pct"]),
        "sharpe": float(sm["sharpe"]),
        "benchmark_sharpe": float(bm["sharpe"]),
        "sharpe_diff": float(sm["sharpe"] - bm["sharpe"]),
        "max_drawdown_pct": float(sm["max_drawdown_pct"]),
        "benchmark_max_drawdown_pct": float(bm["max_drawdown_pct"]),
        "mdd_diff_pctp": float(sm["max_drawdown_pct"] - bm["max_drawdown_pct"]),
        "alpha_mean_pct_per_period": float(alpha.mean() * 100.0),
        "nw_t": float(nw["nw_t"]),
        "nw_p_two_sided": float(nw["nw_p_two_sided"]),
        "nw_p_gt0": float(nw["nw_p_gt0"]),
        "nw_lag": int(nw["nw_lag"]),
        "bootstrap_p_cagr_diff_gt0": float(boot["p_cagr_diff_gt0"]),
        "bootstrap_cagr_diff_ci95_lo": float(boot["cagr_diff_ci95"][0]),
        "bootstrap_cagr_diff_ci95_hi": float(boot["cagr_diff_ci95"][1]),
        "avg_turnover": float(np.mean(turns)) if turns else 0.0,
    }


def run() -> None:
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    specs = _build_specs()
    symbols = sorted({BENCH, SAFE, "SPY", "IWM", "EFA", "EEM", "IEF", "TLT", "GLD", "VNQ", "TQQQ", "QLD", "UPRO"} | {
        "XLK",
        "XLF",
        "XLV",
        "XLI",
        "XLY",
        "XLP",
        "XLE",
        "XLB",
        "XLU",
        "XLC",
        "XLRE",
    })
    frames = _download_frames(symbols)
    snaps = _snapshot_dates()
    rows: list[dict[str, float | str | list[float]]] = []
    for sp in specs:
        res = _backtest_strategy(sp, frames, snaps, cost_bps=TRADE_COST_BPS, periods_per_year=52)
        rows.append(res)
        if "error" in res:
            print(f"[FAIL] {sp.name}: {res['error']}")
        else:
            print(
                f"[OK] {sp.name}: CAGR diff {float(res['cagr_diff_pctp']):+.2f}pp | "
                f"NW t {float(res['nw_t']):+.2f} | p2 {float(res['nw_p_two_sided']):.3f}"
            )

    df = pd.DataFrame(rows)
    if "error" in df.columns:
        mask_err = df["error"].fillna("").astype(str).str.len().astype(bool)
        ok = df[~mask_err].copy()
    else:
        ok = df.copy()
    if ok.empty:
        raise RuntimeError("No successful strategy runs")
    ok = ok.sort_values(["nw_t", "cagr_diff_pctp", "sharpe_diff"], ascending=[False, False, False]).reset_index(
        drop=True
    )

    # Primary pick rule: statistically stronger alpha first, then return.
    sig = ok[(ok["nw_p_two_sided"] <= 0.10) & (ok["cagr_diff_pctp"] > 0)].copy()
    if not sig.empty:
        pick = sig.sort_values(["nw_t", "cagr_diff_pctp"], ascending=[False, False]).iloc[0]
        pick_reason = "significant_alpha"
    else:
        pick = ok.iloc[0]
        pick_reason = "best_available_non_significant"

    df.to_csv(CSV_OUT, index=False)
    payload = {
        "run_tag": RUN_TAG,
        "start_date": START,
        "end_date": END,
        "snapshot_freq": FREQ,
        "benchmark": BENCH,
        "safe_asset": SAFE,
        "trade_cost_bps": float(TRADE_COST_BPS),
        "tested_strategies": int(len(ok)),
        "pick_reason": pick_reason,
        "best_strategy": pick.to_dict(),
        "top5": ok.head(5).to_dict(orient="records"),
        "all_results_csv": str(CSV_OUT.relative_to(ROOT)),
    }
    JSON_OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines: list[str] = []
    lines.append("# ETF Strategy Compare (Claim-Grade Protocol)\n")
    lines.append(f"- period: {START} ~ {END} ({FREQ})")
    lines.append(f"- benchmark: {BENCH} (open-to-open)")
    lines.append(f"- execution: next-open")
    lines.append(f"- transaction cost: {TRADE_COST_BPS:.1f} bps")
    lines.append(f"- tested strategies: {len(ok)}\n")
    lines.append(f"## Best Strategy: **{pick['strategy']}**")
    lines.append(f"- reason: `{pick_reason}`")
    lines.append(
        f"- CAGR diff: {float(pick['cagr_diff_pctp']):+.2f}pp | total diff: {float(pick['total_diff_pctp']):+.2f}pp"
    )
    lines.append(
        f"- NW t: {float(pick['nw_t']):+.3f} | p(two-sided): {float(pick['nw_p_two_sided']):.4f} | "
        f"P(alpha>0): {float(pick['nw_p_gt0']):.4f}"
    )
    lines.append(
        f"- bootstrap P(CAGR diff>0): {float(pick['bootstrap_p_cagr_diff_gt0']):.4f} | "
        f"CI95 [{float(pick['bootstrap_cagr_diff_ci95_lo']):+.2f}, {float(pick['bootstrap_cagr_diff_ci95_hi']):+.2f}]"
    )
    lines.append("")
    lines.append("## Top 5\n")
    cols = [
        "strategy",
        "mode",
        "cagr_diff_pctp",
        "total_diff_pctp",
        "sharpe_diff",
        "mdd_diff_pctp",
        "nw_t",
        "nw_p_two_sided",
        "bootstrap_p_cagr_diff_gt0",
    ]
    lines.append(ok[cols].head(5).to_string(index=False))
    MD_OUT.write_text("\n".join(lines), encoding="utf-8")

    print(f"Saved: {CSV_OUT.relative_to(ROOT)}")
    print(f"Saved: {JSON_OUT.relative_to(ROOT)}")
    print(f"Saved: {MD_OUT.relative_to(ROOT)}")
    print(
        f"Best: {pick['strategy']} | CAGR diff {float(pick['cagr_diff_pctp']):+.2f}pp | "
        f"NW t {float(pick['nw_t']):+.2f} (p2={float(pick['nw_p_two_sided']):.3f})"
    )


if __name__ == "__main__":
    run()
