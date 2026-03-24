from __future__ import annotations

import json
import math
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yfinance as yf


ROOT = Path(__file__).resolve().parents[1]
RUNS_DIR = ROOT / "data" / "runs"
RUN_TAG = (os.getenv("RR_RUN_TAG") or "").strip() or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

START = (os.getenv("RR_START_DATE") or "2016-03-01").strip()
END = (os.getenv("RR_END_DATE") or "2026-03-01").strip()
FREQ = (os.getenv("RR_SNAPSHOT_FREQ") or "weekly").strip().lower() or "weekly"
BENCH = (os.getenv("RR_BENCHMARK") or "QQQ").strip().upper() or "QQQ"
TRADE_COST_BPS = float(os.getenv("RR_TRADE_COST_BPS") or "20")
GRID_SEARCH = str(os.getenv("RR_GRID_SEARCH", "1")).strip().lower() in {"1", "true", "yes", "on", "y"}

PERIODS_CSV = RUNS_DIR / f"regime_rotation_periods_{RUN_TAG}.csv"
GRID_CSV = RUNS_DIR / f"regime_rotation_grid_{RUN_TAG}.csv"
BEST_JSON = RUNS_DIR / f"regime_rotation_best_{RUN_TAG}.json"
LADDER_CSV = RUNS_DIR / f"regime_rotation_ladder_{RUN_TAG}.csv"
YEARLY_CSV = RUNS_DIR / f"regime_rotation_yearly_{RUN_TAG}.csv"


ALLOCATIONS: dict[str, dict[str, float]] = {
    "QQQ": {"QQQ": 1.0},
    "QLD": {"QLD": 1.0},
    "TQQQ": {"TQQQ": 1.0},
    "UPRO": {"UPRO": 1.0},
    "BIL": {"BIL": 1.0},
    "TLT": {"TLT": 1.0},
    "IEF": {"IEF": 1.0},
    "GLD": {"GLD": 1.0},
    "PSQ50B50": {"PSQ": 0.5, "BIL": 0.5},
    "INV30B70": {"SQQQ": 0.3, "BIL": 0.7},
    "Q50T50": {"QQQ": 0.5, "TLT": 0.5},
    "Q50B50": {"QQQ": 0.5, "BIL": 0.5},
    "L2_50B50": {"QLD": 0.5, "BIL": 0.5},
    "GLD50B50": {"GLD": 0.5, "BIL": 0.5},
}


@dataclass(frozen=True)
class RegimeConfig:
    regime_source: str
    ma_fast: int
    ma_slow: int
    mom_lb: int
    mom_thr: float
    risk_on: str
    risk_on_alt: str
    neutral: str
    risk_off: str
    crash: str
    vol_cap: float
    vol_low: float
    vol_mid: float
    mom_strong: float
    crash_vol: float
    crash_dd: float
    hysteresis: float
    risk_on_filter_asset: str
    risk_on_filter_ma: int
    risk_on_filter_safe: str


def _f(x: Any, d: float = 0.0) -> float:
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
        dates = pd.date_range(start=start, end=end, freq="W-FRI")
    elif FREQ in {"monthly", "m", "month"}:
        dates = pd.date_range(start=start, end=end, freq="ME")
    else:
        dates = pd.date_range(start=start, end=end, freq="W-FRI")
    return [pd.Timestamp(d).normalize() for d in dates]


def _asof_pos(idx: pd.Index, dt: pd.Timestamp) -> int:
    try:
        p = int(idx.searchsorted(pd.Timestamp(dt), side="right")) - 1
        return p if p >= 0 else -1
    except Exception:
        return -1


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
        if any(c not in f.columns for c in ("Open", "Close")):
            continue
        out[s] = f[["Open", "Close"]].dropna().sort_index()
    return out


def _newey_west(alpha_pct: pd.Series) -> dict[str, float]:
    a = pd.to_numeric(alpha_pct, errors="coerce").dropna().astype(float)
    n = len(a)
    if n < 5:
        return {"nw_t": 0.0, "nw_p_two": 1.0, "nw_p_gt0": 0.5, "nw_lag": 0}
    mu = float(a.mean())
    x = a.to_numpy(dtype=float)
    eps = x - mu
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
    pgt = float(0.5 * (1.0 + math.erf(t / math.sqrt(2.0))))
    return {"nw_t": float(t), "nw_p_two": p2, "nw_p_gt0": pgt, "nw_lag": int(lag)}


def _risk_metrics(r: pd.Series, periods_per_year: int = 52) -> dict[str, float]:
    s = pd.to_numeric(r, errors="coerce").dropna().astype(float)
    if len(s) == 0:
        return {"periods": 0, "cagr_pct": 0.0, "total_return_pct": 0.0, "sharpe": 0.0, "max_drawdown_pct": 0.0}
    n = len(s)
    c = (1.0 + s).cumprod()
    total = float(c.iloc[-1] - 1.0)
    cagr = float(c.iloc[-1] ** (periods_per_year / n) - 1.0) if c.iloc[-1] > 0 else 0.0
    sd = float(s.std(ddof=1)) if n > 1 else 0.0
    sharpe = float((s.mean() / sd) * np.sqrt(periods_per_year)) if sd > 1e-12 else 0.0
    dd = (c / c.cummax()) - 1.0
    return {
        "periods": int(n),
        "cagr_pct": float(cagr * 100.0),
        "total_return_pct": float(total * 100.0),
        "sharpe": float(sharpe),
        "max_drawdown_pct": float(dd.min() * 100.0),
    }


def _turnover(prev_w: dict[str, float], new_w: dict[str, float]) -> float:
    keys = set(prev_w) | set(new_w)
    return float(0.5 * sum(abs(new_w.get(k, 0.0) - prev_w.get(k, 0.0)) for k in keys))


def _build_periods(
    frames: dict[str, pd.DataFrame],
    snaps: list[pd.Timestamp],
) -> tuple[pd.DataFrame, pd.Series]:
    if BENCH not in frames:
        raise RuntimeError(f"Missing benchmark frame: {BENCH}")

    bench = frames[BENCH]
    idx = bench.index
    close_qqq = pd.to_numeric(bench["Close"], errors="coerce")
    open_by_sym = {k: pd.to_numeric(v["Open"], errors="coerce").reindex(idx).ffill() for k, v in frames.items()}

    rows: list[dict[str, Any]] = []
    for i in range(len(snaps) - 1):
        sdt = snaps[i]
        edt = snaps[i + 1]
        signal_pos = _asof_pos(idx, sdt)
        next_signal_pos = _asof_pos(idx, edt)
        if signal_pos < 252 or next_signal_pos < 0:
            continue
        entry_pos = signal_pos + 1
        exit_pos = next_signal_pos + 1
        if entry_pos <= 0 or exit_pos <= entry_pos or exit_pos >= len(idx):
            continue
        b0 = _f(open_by_sym[BENCH].iloc[entry_pos], np.nan)
        b1 = _f(open_by_sym[BENCH].iloc[exit_pos], np.nan)
        if not np.isfinite(b0) or not np.isfinite(b1) or b0 <= 0 or b1 <= 0:
            continue

        row: dict[str, Any] = {
            "signal_day": str(pd.Timestamp(idx[signal_pos]).date()),
            "entry_day": str(pd.Timestamp(idx[entry_pos]).date()),
            "exit_day": str(pd.Timestamp(idx[exit_pos]).date()),
            "signal_pos": int(signal_pos),
            "bench_ret": float(b1 / b0 - 1.0),
        }

        valid = True
        for token, weights in ALLOCATIONS.items():
            gross = 0.0
            for sym, w in weights.items():
                if sym not in open_by_sym:
                    valid = False
                    break
                o0 = _f(open_by_sym[sym].iloc[entry_pos], np.nan)
                o1 = _f(open_by_sym[sym].iloc[exit_pos], np.nan)
                if not np.isfinite(o0) or not np.isfinite(o1) or o0 <= 0 or o1 <= 0:
                    valid = False
                    break
                gross += float(w) * float(o1 / o0 - 1.0)
            if not valid:
                break
            row[f"ret_{token}"] = float(gross)
        if valid:
            rows.append(row)

    if not rows:
        raise RuntimeError("No valid periods generated")
    return pd.DataFrame(rows), close_qqq


def _decide_token(
    cfg: RegimeConfig,
    signal_pos: int,
    close_qqq: pd.Series,
    cache: dict[str, pd.Series],
    prev_token: str | None = None,
) -> str:
    source = str(cfg.regime_source or "QQQ").strip().upper() or "QQQ"
    src_close = cache.get(f"close_{source}", close_qqq)
    src_ma_fast = cache.get(f"ma_{source}_{cfg.ma_fast}", cache.get(f"ma_{cfg.ma_fast}"))
    src_ma_slow = cache.get(f"ma_{source}_{cfg.ma_slow}", cache.get(f"ma_{cfg.ma_slow}"))
    src_mom = cache.get(f"mom_{source}_{cfg.mom_lb}", cache.get(f"mom_{cfg.mom_lb}"))
    src_vol = cache.get(f"vol_{source}_21", cache.get("vol_21"))
    src_dd = cache.get(f"dd_{source}_252", cache.get("dd_252"))

    price = _f(src_close.iloc[signal_pos], np.nan) if isinstance(src_close, pd.Series) else np.nan
    ma_fast = _f(src_ma_fast.iloc[signal_pos], np.nan) if isinstance(src_ma_fast, pd.Series) else np.nan
    ma_slow = _f(src_ma_slow.iloc[signal_pos], np.nan) if isinstance(src_ma_slow, pd.Series) else np.nan
    mom = _f(src_mom.iloc[signal_pos], np.nan) if isinstance(src_mom, pd.Series) else np.nan
    vol21 = _f(src_vol.iloc[signal_pos], np.nan) if isinstance(src_vol, pd.Series) else np.nan
    dd252 = _f(src_dd.iloc[signal_pos], np.nan) if isinstance(src_dd, pd.Series) else np.nan

    if not all(np.isfinite(x) for x in (price, ma_fast, ma_slow, mom, vol21, dd252)):
        return cfg.crash

    risk_on_core = price > ma_fast and ma_fast > ma_slow and mom > cfg.mom_thr
    risk_on_cond = risk_on_core and vol21 <= cfg.vol_cap
    crash_cond = (price < ma_slow and dd252 <= cfg.crash_dd) or (vol21 >= cfg.crash_vol)
    risk_off_cond = price < ma_slow or mom < -max(0.0, cfg.mom_thr)

    if crash_cond:
        token = cfg.crash
    elif risk_on_cond:
        if vol21 <= cfg.vol_low and mom >= cfg.mom_strong:
            token = cfg.risk_on
        elif vol21 <= cfg.vol_mid:
            token = cfg.risk_on_alt
        else:
            token = cfg.neutral
    elif risk_off_cond:
        token = cfg.risk_off
    else:
        token = cfg.neutral

    h = max(0.0, float(cfg.hysteresis))
    if prev_token:
        risk_on_tokens = {cfg.risk_on, cfg.risk_on_alt}
        if prev_token in risk_on_tokens and token not in risk_on_tokens:
            if price > ma_slow and mom > (cfg.mom_thr - h) and vol21 <= (cfg.vol_mid + 0.005):
                token = prev_token if prev_token == cfg.risk_on_alt else cfg.risk_on_alt
        elif prev_token in {cfg.risk_off, cfg.crash} and token == cfg.risk_on:
            if mom < (cfg.mom_strong + h):
                token = cfg.risk_on_alt
        elif prev_token == cfg.neutral and token in {cfg.risk_off, cfg.crash}:
            if mom > -h and price >= ma_slow * 0.99:
                token = cfg.neutral

    filt_asset = str(cfg.risk_on_filter_asset or "").strip().upper()
    filt_ma = int(cfg.risk_on_filter_ma)
    filt_safe = str(cfg.risk_on_filter_safe or "").strip().upper()
    if filt_asset and filt_ma > 0 and token in {cfg.risk_on, cfg.risk_on_alt}:
        c = cache.get(f"close_{filt_asset}")
        m = cache.get(f"ma_{filt_asset}_{filt_ma}")
        px = _f(c.iloc[signal_pos], np.nan) if isinstance(c, pd.Series) and signal_pos < len(c) else np.nan
        mx = _f(m.iloc[signal_pos], np.nan) if isinstance(m, pd.Series) and signal_pos < len(m) else np.nan
        if not np.isfinite(px) or not np.isfinite(mx) or px <= mx:
            token = filt_safe if filt_safe in ALLOCATIONS else cfg.neutral

    return token


def _backtest_config(
    cfg: RegimeConfig,
    periods: pd.DataFrame,
    close_qqq: pd.Series,
    cache: dict[str, pd.Series],
    cost_bps: float,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    prev_w: dict[str, float] = {"__CASH__": 1.0}
    prev_token: str | None = None
    for row in periods.to_dict(orient="records"):
        signal_pos = int(row["signal_pos"])
        token = _decide_token(cfg, signal_pos, close_qqq, cache, prev_token=prev_token)
        alloc = dict(ALLOCATIONS[token])
        alloc["__CASH__"] = max(0.0, 1.0 - float(sum(v for k, v in alloc.items() if k != "__CASH__")))
        turn = _turnover(prev_w, alloc)
        gross = _f(row.get(f"ret_{token}"), 0.0)
        cost = float(cost_bps) / 10000.0 * turn
        net = gross - cost
        rows.append(
            {
                "signal_day": row["signal_day"],
                "entry_day": row["entry_day"],
                "exit_day": row["exit_day"],
                "regime_token": token,
                "turnover": float(turn),
                "gross_return_pct": float(gross * 100.0),
                "net_return_pct": float(net * 100.0),
                "benchmark_return_pct": float(_f(row.get("bench_ret"), 0.0) * 100.0),
            }
        )
        prev_w = alloc
        prev_token = token

    df = pd.DataFrame(rows)
    if df.empty:
        return df, {"error": "no_period_rows"}

    s_ret = pd.to_numeric(df["net_return_pct"], errors="coerce").astype(float) / 100.0
    b_ret = pd.to_numeric(df["benchmark_return_pct"], errors="coerce").astype(float) / 100.0
    alpha_pct = (s_ret - b_ret) * 100.0

    sm = _risk_metrics(s_ret, periods_per_year=52)
    bm = _risk_metrics(b_ret, periods_per_year=52)
    nw = _newey_west(alpha_pct)

    summary = {
        "config": asdict(cfg),
        "periods": int(len(df)),
        "cagr_pct": float(sm["cagr_pct"]),
        "benchmark_cagr_pct": float(bm["cagr_pct"]),
        "cagr_diff_pctp": float(sm["cagr_pct"] - bm["cagr_pct"]),
        "total_return_pct": float(sm["total_return_pct"]),
        "benchmark_total_return_pct": float(bm["total_return_pct"]),
        "total_diff_pctp": float(sm["total_return_pct"] - bm["total_return_pct"]),
        "sharpe": float(sm["sharpe"]),
        "benchmark_sharpe": float(bm["sharpe"]),
        "max_drawdown_pct": float(sm["max_drawdown_pct"]),
        "benchmark_max_drawdown_pct": float(bm["max_drawdown_pct"]),
        "mdd_diff_pctp": float(sm["max_drawdown_pct"] - bm["max_drawdown_pct"]),
        "alpha_mean_pct_per_period": float(alpha_pct.mean()),
        "avg_turnover": float(pd.to_numeric(df["turnover"], errors="coerce").mean()),
        **nw,
    }
    return df, summary


def _horizon_ladder(df: pd.DataFrame) -> pd.DataFrame:
    out: list[dict[str, Any]] = []
    d = df.copy()
    d["entry_day"] = pd.to_datetime(d["entry_day"], errors="coerce")
    d = d.dropna(subset=["entry_day"]).sort_values("entry_day")
    if d.empty:
        return pd.DataFrame()
    end = pd.Timestamp(d["entry_day"].max())
    for years in range(1, 11):
        st = end - pd.DateOffset(years=years)
        s = d[d["entry_day"] > st]
        if len(s) < 20:
            continue
        sr = pd.to_numeric(s["net_return_pct"], errors="coerce").astype(float) / 100.0
        br = pd.to_numeric(s["benchmark_return_pct"], errors="coerce").astype(float) / 100.0
        sm = _risk_metrics(sr, periods_per_year=52)
        bm = _risk_metrics(br, periods_per_year=52)
        alpha_pct = (sr - br) * 100.0
        nw = _newey_west(alpha_pct)
        cdiff = float(sm["cagr_pct"] - bm["cagr_pct"])
        out.append(
            {
                "horizon_years": int(years),
                "periods": int(len(s)),
                "start": str(pd.Timestamp(s["entry_day"].min()).date()),
                "end": str(pd.Timestamp(s["entry_day"].max()).date()),
                "cagr_diff_pctp": cdiff,
                "mdd_diff_pctp": float(sm["max_drawdown_pct"] - bm["max_drawdown_pct"]),
                "nw_t": float(nw["nw_t"]),
                "nw_p_two": float(nw["nw_p_two"]),
                "nw_p_gt0": float(nw["nw_p_gt0"]),
                "meaningful_loose": bool(cdiff > 0 and nw["nw_p_gt0"] >= 0.90),
                "meaningful_strict": bool(cdiff > 0 and nw["nw_p_two"] < 0.10),
            }
        )
    return pd.DataFrame(out)


def _calendar_yearly(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["entry_day"] = pd.to_datetime(d["entry_day"], errors="coerce")
    d = d.dropna(subset=["entry_day"]).sort_values("entry_day")
    if d.empty:
        return pd.DataFrame()
    d["year"] = d["entry_day"].dt.year.astype(int)
    rows: list[dict[str, Any]] = []
    for year, g in d.groupby("year"):
        if len(g) < 8:
            continue
        sr = pd.to_numeric(g["net_return_pct"], errors="coerce").astype(float) / 100.0
        br = pd.to_numeric(g["benchmark_return_pct"], errors="coerce").astype(float) / 100.0
        total_s = float((1.0 + sr).prod() - 1.0) * 100.0
        total_b = float((1.0 + br).prod() - 1.0) * 100.0
        cdiff = float(_risk_metrics(sr)["cagr_pct"] - _risk_metrics(br)["cagr_pct"])
        nw = _newey_west((sr - br) * 100.0)
        rows.append(
            {
                "year": int(year),
                "weeks": int(len(g)),
                "strategy_total_pct": total_s,
                "benchmark_total_pct": total_b,
                "total_diff_pctp": float(total_s - total_b),
                "cagr_diff_pctp": cdiff,
                "nw_t": float(nw["nw_t"]),
                "nw_p_two": float(nw["nw_p_two"]),
                "nw_p_gt0": float(nw["nw_p_gt0"]),
                "meaningful_loose": bool(cdiff > 0 and nw["nw_p_gt0"] >= 0.90),
                "meaningful_strict": bool(cdiff > 0 and nw["nw_p_two"] < 0.10),
            }
        )
    return pd.DataFrame(rows).sort_values("year")


def _grid_configs() -> list[RegimeConfig]:
    regime_source_list = ["QQQ", "TQQQ"]
    ma_fast_list = [100]
    ma_slow_list = [200]
    mom_lb_list = [21]
    mom_thr_list = [0.0]
    risk_on = ["TQQQ"]
    risk_on_alt = ["QLD"]
    neutral = ["QLD", "QQQ"]
    risk_off = ["GLD", "TLT"]
    crash = ["INV30B70", "GLD"]
    vol_cap = [0.05]
    vol_low = [0.03, 0.035]
    vol_mid = [0.04, 0.05]
    mom_strong = [0.04, 0.06]
    crash_vol = [0.06]
    crash_dd = [-0.20, -0.28]
    hysteresis = [0.0, 0.01]
    risk_on_filter_asset = ["TQQQ"]
    risk_on_filter_ma = [0, 125, 150, 175]
    risk_on_filter_safe = ["BIL", "GLD", "Q50B50"]

    out: list[RegimeConfig] = []
    for src in regime_source_list:
        for maf in ma_fast_list:
            for mas in ma_slow_list:
                if maf >= mas:
                    continue
                for mlb in mom_lb_list:
                    for mth in mom_thr_list:
                        for on in risk_on:
                            for on_alt in risk_on_alt:
                                for ne in neutral:
                                    for off in risk_off:
                                        for cr in crash:
                                            for vc in vol_cap:
                                                for vl in vol_low:
                                                    for vm in vol_mid:
                                                        if not (0 < vl <= vm <= vc):
                                                            continue
                                                        for ms in mom_strong:
                                                            if ms < mth:
                                                                continue
                                                            for cv in crash_vol:
                                                                if cv <= vc:
                                                                    continue
                                                                for cdd in crash_dd:
                                                                    for hy in hysteresis:
                                                                        for fa in risk_on_filter_asset:
                                                                            for fm in risk_on_filter_ma:
                                                                                for fs in risk_on_filter_safe:
                                                                                    out.append(
                                                                                        RegimeConfig(
                                                                                            regime_source=str(src),
                                                                                            ma_fast=int(maf),
                                                                                            ma_slow=int(mas),
                                                                                            mom_lb=int(mlb),
                                                                                            mom_thr=float(mth),
                                                                                            risk_on=on,
                                                                                            risk_on_alt=on_alt,
                                                                                            neutral=ne,
                                                                                            risk_off=off,
                                                                                            crash=cr,
                                                                                            vol_cap=float(vc),
                                                                                            vol_low=float(vl),
                                                                                            vol_mid=float(vm),
                                                                                            mom_strong=float(ms),
                                                                                            crash_vol=float(cv),
                                                                                            crash_dd=float(cdd),
                                                                                            hysteresis=float(hy),
                                                                                            risk_on_filter_asset=str(fa),
                                                                                            risk_on_filter_ma=int(fm),
                                                                                            risk_on_filter_safe=str(fs),
                                                                                        )
                                                                                    )
    return out


def run() -> None:
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    all_symbols = sorted({BENCH, "QQQ"} | {s for w in ALLOCATIONS.values() for s in w.keys()})
    frames = _download_frames(all_symbols)
    if BENCH not in frames:
        raise RuntimeError(f"Benchmark not loaded: {BENCH}")
    snaps = _snapshot_dates()
    periods, qclose = _build_periods(frames, snaps)

    cache: dict[str, pd.Series] = {}
    for sym in ("QQQ", "TQQQ", "QLD", "UPRO"):
        if sym not in frames:
            continue
        c = pd.to_numeric(frames[sym]["Close"], errors="coerce").reindex(qclose.index).ffill()
        cache[f"close_{sym}"] = c
        for w in (50, 100, 125, 150, 175, 200):
            cache[f"ma_{sym}_{w}"] = c.rolling(w).mean()
        for lb in (21, 63):
            cache[f"mom_{sym}_{lb}"] = c.pct_change(lb)
        cache[f"vol_{sym}_21"] = c.pct_change().rolling(21).std()
        cache[f"dd_{sym}_252"] = (c / c.rolling(252).max()) - 1.0

    # backward-compatible aliases (QQQ source)
    cache["ma_50"] = cache.get("ma_QQQ_50", qclose.rolling(50).mean())
    cache["ma_100"] = cache.get("ma_QQQ_100", qclose.rolling(100).mean())
    cache["ma_150"] = cache.get("ma_QQQ_150", qclose.rolling(150).mean())
    cache["ma_200"] = cache.get("ma_QQQ_200", qclose.rolling(200).mean())
    cache["mom_21"] = cache.get("mom_QQQ_21", qclose.pct_change(21))
    cache["mom_63"] = cache.get("mom_QQQ_63", qclose.pct_change(63))
    cache["vol_21"] = cache.get("vol_QQQ_21", qclose.pct_change().rolling(21).std())
    cache["dd_252"] = cache.get("dd_QQQ_252", (qclose / qclose.rolling(252).max()) - 1.0)

    if GRID_SEARCH:
        configs = _grid_configs()
    else:
        configs = [
            RegimeConfig(
                regime_source=str(os.getenv("RR_REGIME_SOURCE", "QQQ")).strip().upper(),
                ma_fast=int(float(os.getenv("RR_MA_FAST", "100"))),
                ma_slow=int(float(os.getenv("RR_MA_SLOW", "200"))),
                mom_lb=int(float(os.getenv("RR_MOM_LB", "21"))),
                mom_thr=float(os.getenv("RR_MOM_THR", "0.0")),
                risk_on=str(os.getenv("RR_RISK_ON", "TQQQ")).strip().upper(),
                risk_on_alt=str(os.getenv("RR_RISK_ON_ALT", "QLD")).strip().upper(),
                neutral=str(os.getenv("RR_NEUTRAL", "QLD")).strip().upper(),
                risk_off=str(os.getenv("RR_RISK_OFF", "GLD")).strip().upper(),
                crash=str(os.getenv("RR_CRASH", "INV30B70")).strip().upper(),
                vol_cap=float(os.getenv("RR_VOL_CAP", "0.05")),
                vol_low=float(os.getenv("RR_VOL_LOW", "0.03")),
                vol_mid=float(os.getenv("RR_VOL_MID", "0.05")),
                mom_strong=float(os.getenv("RR_MOM_STRONG", "0.04")),
                crash_vol=float(os.getenv("RR_CRASH_VOL", "0.06")),
                crash_dd=float(os.getenv("RR_CRASH_DD", "-0.20")),
                hysteresis=float(os.getenv("RR_HYSTERESIS", "0.01")),
                risk_on_filter_asset=str(os.getenv("RR_RISK_ON_FILTER_ASSET", "TQQQ")).strip().upper(),
                risk_on_filter_ma=int(float(os.getenv("RR_RISK_ON_FILTER_MA", "150"))),
                risk_on_filter_safe=str(os.getenv("RR_RISK_ON_FILTER_SAFE", "BIL")).strip().upper(),
            )
        ]

    grid_rows: list[dict[str, Any]] = []
    best_df: pd.DataFrame | None = None
    best_summary: dict[str, Any] | None = None
    best_key: tuple[Any, ...] | None = None

    for i, cfg in enumerate(configs, start=1):
        if (
            cfg.risk_on not in ALLOCATIONS
            or cfg.risk_on_alt not in ALLOCATIONS
            or cfg.neutral not in ALLOCATIONS
            or cfg.risk_off not in ALLOCATIONS
            or cfg.crash not in ALLOCATIONS
            or (int(cfg.risk_on_filter_ma) > 0 and cfg.risk_on_filter_safe not in ALLOCATIONS)
        ):
            continue
        df, sm = _backtest_config(cfg, periods, qclose, cache, cost_bps=TRADE_COST_BPS)
        if "error" in sm:
            continue

        ladder = _horizon_ladder(df)
        yearly = _calendar_yearly(df)
        loose = int(ladder["meaningful_loose"].sum()) if not ladder.empty else 0
        strict = int(ladder["meaningful_strict"].sum()) if not ladder.empty else 0
        year_win = int((pd.to_numeric(yearly.get("total_diff_pctp"), errors="coerce") > 0).sum()) if not yearly.empty else 0
        full_years = yearly[yearly["year"].between(2017, 2025)] if not yearly.empty else pd.DataFrame()
        full_year_pos = int((pd.to_numeric(full_years.get("total_diff_pctp"), errors="coerce") > 0).sum()) if not full_years.empty else 0
        full_year_loose = int(full_years["meaningful_loose"].sum()) if not full_years.empty else 0
        full_year_strict = int(full_years["meaningful_strict"].sum()) if not full_years.empty else 0
        qqq_down = full_years[pd.to_numeric(full_years.get("benchmark_total_pct"), errors="coerce") < 0] if not full_years.empty else pd.DataFrame()
        down_years = int(len(qqq_down))
        down_year_out = int((pd.to_numeric(qqq_down.get("total_diff_pctp"), errors="coerce") > 0).sum()) if down_years > 0 else 0

        row = {
            **asdict(cfg),
            "periods": int(sm["periods"]),
            "cagr_diff_pctp": float(sm["cagr_diff_pctp"]),
            "mdd_diff_pctp": float(sm["mdd_diff_pctp"]),
            "nw_t": float(sm["nw_t"]),
            "nw_p_two": float(sm["nw_p_two"]),
            "nw_p_gt0": float(sm["nw_p_gt0"]),
            "avg_turnover": float(sm["avg_turnover"]),
            "loose10": loose,
            "strict10": strict,
            "year_win": year_win,
            "full_year_pos": full_year_pos,
            "full_year_loose": full_year_loose,
            "full_year_strict": full_year_strict,
            "down_years": down_years,
            "down_year_outperform": down_year_out,
        }
        grid_rows.append(row)

        # prioritize annual robustness first, then horizon consistency, then significance/return
        key = (
            int(full_year_strict),
            int(full_year_pos),
            int(down_year_out),
            int(full_year_loose),
            int(loose),
            int(strict),
            int(year_win),
            1 if float(sm["nw_p_two"]) < 0.10 else 0,
            float(sm["nw_p_gt0"]),
            1 if float(sm["mdd_diff_pctp"]) >= 0.0 else 0,
            1 if float(sm["mdd_diff_pctp"]) >= -5.0 else 0,
            float(sm["cagr_diff_pctp"]),
            float(sm["mdd_diff_pctp"]),
        )
        if best_key is None or key > best_key:
            best_key = key
            best_df = df
            best_summary = {
                "metrics": sm,
                "horizon_ladder": ladder.to_dict(orient="records"),
                "calendar_yearly": yearly.to_dict(orient="records"),
                "loose10": loose,
                "strict10": strict,
                "year_win": year_win,
                "full_year_pos": full_year_pos,
                "full_year_loose": full_year_loose,
                "full_year_strict": full_year_strict,
                "down_years": down_years,
                "down_year_outperform": down_year_out,
            }

        if i % 500 == 0:
            print(f"[{i}/{len(configs)}] scanned...")

    if not grid_rows or best_df is None or best_summary is None:
        raise RuntimeError("No successful configurations")

    grid_df = pd.DataFrame(grid_rows).sort_values(
        [
            "full_year_strict",
            "full_year_pos",
            "down_year_outperform",
            "full_year_loose",
            "loose10",
            "strict10",
            "year_win",
            "nw_p_gt0",
            "cagr_diff_pctp",
        ],
        ascending=[False, False, False, False, False, False, False, False, False],
    )
    grid_df.to_csv(GRID_CSV, index=False)
    best_df.to_csv(PERIODS_CSV, index=False)
    pd.DataFrame(best_summary["horizon_ladder"]).to_csv(LADDER_CSV, index=False)
    pd.DataFrame(best_summary["calendar_yearly"]).to_csv(YEARLY_CSV, index=False)

    payload = {
        "run_tag": RUN_TAG,
        "start_date": START,
        "end_date": END,
        "snapshot_freq": FREQ,
        "benchmark": BENCH,
        "trade_cost_bps": float(TRADE_COST_BPS),
        "grid_search": bool(GRID_SEARCH),
        "tested_configs": int(len(grid_df)),
        "best": best_summary,
        "paths": {
            "grid_csv": str(GRID_CSV.relative_to(ROOT)),
            "periods_csv": str(PERIODS_CSV.relative_to(ROOT)),
            "ladder_csv": str(LADDER_CSV.relative_to(ROOT)),
            "yearly_csv": str(YEARLY_CSV.relative_to(ROOT)),
        },
    }
    BEST_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    m = best_summary["metrics"]
    print(f"Saved: {GRID_CSV.relative_to(ROOT)} ({len(grid_df)} rows)")
    print(f"Saved: {PERIODS_CSV.relative_to(ROOT)}")
    print(f"Saved: {LADDER_CSV.relative_to(ROOT)}")
    print(f"Saved: {YEARLY_CSV.relative_to(ROOT)}")
    print(f"Saved: {BEST_JSON.relative_to(ROOT)}")
    print(
        "Best regime config -> "
        f"CAGR diff {float(m['cagr_diff_pctp']):+.2f}pp | "
        f"MDD diff {float(m['mdd_diff_pctp']):+.2f}pp | "
        f"NW t {float(m['nw_t']):+.2f} (p2={float(m['nw_p_two']):.3f}, pgt0={float(m['nw_p_gt0']):.3f}) | "
        f"FullYear pos/loose/strict {best_summary['full_year_pos']}/{best_summary['full_year_loose']}/{best_summary['full_year_strict']} | "
        f"Loose10 {best_summary['loose10']}/10 | Strict10 {best_summary['strict10']}/10"
    )


if __name__ == "__main__":
    run()
