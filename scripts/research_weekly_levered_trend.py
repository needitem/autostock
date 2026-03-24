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
RUN_TAG = (os.getenv("LT_RUN_TAG") or "").strip() or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

START = (os.getenv("LT_START_DATE") or "2016-03-01").strip()
END = (os.getenv("LT_END_DATE") or "2026-03-01").strip()
BENCH = (os.getenv("LT_BENCHMARK") or "QQQ").strip().upper() or "QQQ"
VIX = (os.getenv("LT_VIX") or "^VIX").strip() or "^VIX"
TRADE_COST_BPS = float(os.getenv("LT_TRADE_COST_BPS") or "20")

GRID_CSV = RUNS_DIR / f"levered_trend_grid_{RUN_TAG}.csv"
PERIODS_CSV = RUNS_DIR / f"levered_trend_periods_{RUN_TAG}.csv"
YEARLY_CSV = RUNS_DIR / f"levered_trend_yearly_{RUN_TAG}.csv"
BEST_JSON = RUNS_DIR / f"levered_trend_best_{RUN_TAG}.json"


@dataclass(frozen=True)
class TrendConfig:
    risk: str
    safe: str
    ma_window: int
    qqq_confirm: bool
    vix_max: float


def _f(x: Any, d: float = 0.0) -> float:
    try:
        y = float(x)
        return d if np.isnan(y) or np.isinf(y) else y
    except Exception:
        return d


def _asof_pos(idx: pd.Index, dt: pd.Timestamp) -> int:
    try:
        p = int(idx.searchsorted(pd.Timestamp(dt), side="right")) - 1
        return p if p >= 0 else -1
    except Exception:
        return -1


def _snapshots() -> list[pd.Timestamp]:
    start = pd.Timestamp(START)
    end = pd.Timestamp(END)
    if end < start:
        start, end = end, start
    return [pd.Timestamp(d).normalize() for d in pd.date_range(start=start, end=end, freq="W-FRI")]


def _download(symbols: list[str]) -> dict[str, pd.DataFrame]:
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


def _risk_metrics(returns: pd.Series) -> dict[str, float]:
    r = pd.to_numeric(returns, errors="coerce").dropna().astype(float)
    if r.empty:
        return {"cagr_pct": 0.0, "total_return_pct": 0.0, "max_drawdown_pct": 0.0, "sharpe": 0.0}
    n = len(r)
    c = (1.0 + r).cumprod()
    cagr = float(c.iloc[-1] ** (52 / n) - 1.0) if c.iloc[-1] > 0 else 0.0
    dd = (c / c.cummax()) - 1.0
    sd = float(r.std(ddof=1)) if n > 1 else 0.0
    sharpe = float((r.mean() / sd) * np.sqrt(52)) if sd > 1e-12 else 0.0
    return {
        "cagr_pct": float(cagr * 100.0),
        "total_return_pct": float((c.iloc[-1] - 1.0) * 100.0),
        "max_drawdown_pct": float(dd.min() * 100.0),
        "sharpe": float(sharpe),
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
        w = 1.0 - (l / (lag + 1.0))
        lrv += 2.0 * w * cov
    lrv = max(0.0, lrv)
    se = math.sqrt(lrv / n) if n > 0 else 0.0
    t = mu / se if se > 1e-12 else 0.0
    return {
        "nw_t": float(t),
        "nw_p_two": float(math.erfc(abs(t) / math.sqrt(2.0))),
        "nw_p_gt0": float(0.5 * (1.0 + math.erf(t / math.sqrt(2.0)))),
    }


def _yearly_table(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["entry_day"] = pd.to_datetime(d["entry_day"], errors="coerce")
    d = d.dropna(subset=["entry_day"])
    d["year"] = d["entry_day"].dt.year.astype(int)
    rows: list[dict[str, Any]] = []
    for y, g in d.groupby("year"):
        if len(g) < 8:
            continue
        sr = pd.to_numeric(g["strategy_ret"], errors="coerce").fillna(0.0)
        br = pd.to_numeric(g["bench_ret"], errors="coerce").fillna(0.0)
        s_tot = float((1.0 + sr).prod() - 1.0) * 100.0
        b_tot = float((1.0 + br).prod() - 1.0) * 100.0
        rows.append(
            {
                "year": int(y),
                "weeks": int(len(g)),
                "strategy_total_pct": s_tot,
                "benchmark_total_pct": b_tot,
                "total_diff_pctp": float(s_tot - b_tot),
            }
        )
    return pd.DataFrame(rows).sort_values("year")


def _turnover(prev: str, now: str) -> float:
    if prev == now:
        return 0.0
    if not prev:
        return 1.0
    return 1.0


def _run_config(cfg: TrendConfig, frames: dict[str, pd.DataFrame], snaps: list[pd.Timestamp]) -> tuple[pd.DataFrame, dict[str, Any]]:
    if any(s not in frames for s in (BENCH, cfg.risk, cfg.safe)):
        return pd.DataFrame(), {"error": "missing_frame"}
    bench = frames[BENCH]
    risk = frames[cfg.risk]
    safe = frames[cfg.safe]
    vix = frames.get(VIX)

    risk_close = pd.to_numeric(risk["Close"], errors="coerce")
    risk_ma = risk_close.rolling(cfg.ma_window).mean()
    qqq_close = pd.to_numeric(bench["Close"], errors="coerce")
    qqq_ma200 = qqq_close.rolling(200).mean()

    rows: list[dict[str, Any]] = []
    prev_asset = ""
    for i in range(len(snaps) - 1):
        sdt = snaps[i]
        edt = snaps[i + 1]

        sb = _asof_pos(bench.index, sdt)
        nb = _asof_pos(bench.index, edt)
        if sb < 252 or nb < 0:
            continue
        eb = sb + 1
        xb = nb + 1
        if eb <= 0 or xb <= eb or xb >= len(bench):
            continue
        b0 = _f(bench.iloc[eb]["Open"], np.nan)
        b1 = _f(bench.iloc[xb]["Open"], np.nan)
        if not np.isfinite(b0) or not np.isfinite(b1) or b0 <= 0 or b1 <= 0:
            continue

        sr = _asof_pos(risk.index, sdt)
        nr = _asof_pos(risk.index, edt)
        ss = _asof_pos(safe.index, sdt)
        ns = _asof_pos(safe.index, edt)
        if min(sr, nr, ss, ns) < 0:
            continue
        er = sr + 1
        xr = nr + 1
        es = ss + 1
        xs = ns + 1
        if er <= 0 or xr <= er or xr >= len(risk):
            continue
        if es <= 0 or xs <= es or xs >= len(safe):
            continue

        risk_px = _f(risk_close.iloc[sr], np.nan)
        risk_ma_v = _f(risk_ma.iloc[sr], np.nan)
        qqq_px = _f(qqq_close.iloc[sb], np.nan)
        qqq_ma_v = _f(qqq_ma200.iloc[sb], np.nan)
        vix_px = np.nan
        if vix is not None and not vix.empty:
            sv = _asof_pos(vix.index, sdt)
            if sv >= 0:
                vix_px = _f(vix.iloc[sv]["Close"], np.nan)

        cond = np.isfinite(risk_px) and np.isfinite(risk_ma_v) and risk_px > risk_ma_v
        if cfg.qqq_confirm:
            cond = cond and np.isfinite(qqq_px) and np.isfinite(qqq_ma_v) and qqq_px > qqq_ma_v
        if cfg.vix_max > 0 and np.isfinite(vix_px):
            cond = cond and (vix_px <= cfg.vix_max)

        if cond:
            p0 = _f(risk.iloc[er]["Open"], np.nan)
            p1 = _f(risk.iloc[xr]["Open"], np.nan)
            asset = cfg.risk
        else:
            p0 = _f(safe.iloc[es]["Open"], np.nan)
            p1 = _f(safe.iloc[xs]["Open"], np.nan)
            asset = cfg.safe
        if not np.isfinite(p0) or not np.isfinite(p1) or p0 <= 0 or p1 <= 0:
            continue

        gross = float(p1 / p0 - 1.0)
        cost = float(TRADE_COST_BPS / 10000.0) * _turnover(prev_asset, asset)
        net = gross - cost
        prev_asset = asset
        rows.append(
            {
                "signal_day": str(pd.Timestamp(sdt).date()),
                "entry_day": str(pd.Timestamp(bench.index[eb]).date()),
                "exit_day": str(pd.Timestamp(bench.index[xb]).date()),
                "asset": asset,
                "strategy_ret": net,
                "bench_ret": float(b1 / b0 - 1.0),
                "vix_close": float(vix_px) if np.isfinite(vix_px) else np.nan,
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df, {"error": "no_periods"}

    sr = pd.to_numeric(df["strategy_ret"], errors="coerce").dropna().astype(float)
    br = pd.to_numeric(df["bench_ret"], errors="coerce").dropna().astype(float)
    sm = _risk_metrics(sr)
    bm = _risk_metrics(br)
    nw = _newey_west((sr - br) * 100.0)
    yearly = _yearly_table(df)
    full = yearly[yearly["year"].between(2017, 2025)] if not yearly.empty else pd.DataFrame()
    full_pos = int((pd.to_numeric(full.get("total_diff_pctp"), errors="coerce") > 0).sum()) if not full.empty else 0

    summary = {
        "config": asdict(cfg),
        "periods": int(len(df)),
        "cagr_diff_pctp": float(sm["cagr_pct"] - bm["cagr_pct"]),
        "mdd_diff_pctp": float(sm["max_drawdown_pct"] - bm["max_drawdown_pct"]),
        "strategy_cagr_pct": float(sm["cagr_pct"]),
        "benchmark_cagr_pct": float(bm["cagr_pct"]),
        "strategy_mdd_pct": float(sm["max_drawdown_pct"]),
        "benchmark_mdd_pct": float(bm["max_drawdown_pct"]),
        "strategy_total_pct": float(sm["total_return_pct"]),
        "benchmark_total_pct": float(bm["total_return_pct"]),
        "full_year_pos": int(full_pos),
        "yearly": yearly.to_dict(orient="records"),
        **nw,
    }
    return df, summary


def _grid() -> list[TrendConfig]:
    risks = ["TQQQ", "QLD", "UPRO"]
    safes = ["BIL", "GLD", "TLT"]
    ma_window = [100, 125, 150, 175, 200]
    qqq_confirm = [False, True]
    vix_max = [0.0, 28.0, 32.0]
    out: list[TrendConfig] = []
    for r in risks:
        for s in safes:
            for w in ma_window:
                for qc in qqq_confirm:
                    for vx in vix_max:
                        out.append(TrendConfig(risk=r, safe=s, ma_window=int(w), qqq_confirm=bool(qc), vix_max=float(vx)))
    return out


def run() -> None:
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    configs = _grid()
    symbols = sorted({BENCH, VIX} | {c.risk for c in configs} | {c.safe for c in configs})
    frames = _download(symbols)
    if BENCH not in frames:
        raise RuntimeError(f"Missing benchmark frame: {BENCH}")
    snaps = _snapshots()

    grid_rows: list[dict[str, Any]] = []
    best_df: pd.DataFrame | None = None
    best_summary: dict[str, Any] | None = None
    best_key: tuple[Any, ...] | None = None

    for i, cfg in enumerate(configs, start=1):
        df, sm = _run_config(cfg, frames, snaps)
        if "error" in sm:
            continue
        row = {
            **asdict(cfg),
            "periods": int(sm["periods"]),
            "cagr_diff_pctp": float(sm["cagr_diff_pctp"]),
            "mdd_diff_pctp": float(sm["mdd_diff_pctp"]),
            "full_year_pos": int(sm["full_year_pos"]),
            "nw_t": float(sm["nw_t"]),
            "nw_p_two": float(sm["nw_p_two"]),
            "nw_p_gt0": float(sm["nw_p_gt0"]),
        }
        grid_rows.append(row)

        key = (
            1 if (float(sm["cagr_diff_pctp"]) > 0 and float(sm["mdd_diff_pctp"]) > 0) else 0,
            int(sm["full_year_pos"]),
            1 if float(sm["nw_p_two"]) < 0.10 else 0,
            float(sm["cagr_diff_pctp"]),
            float(sm["mdd_diff_pctp"]),
            float(sm["nw_p_gt0"]),
        )
        if best_key is None or key > best_key:
            best_key = key
            best_df = df
            best_summary = sm

        if i % 50 == 0:
            print(f"[{i}/{len(configs)}] scanned...")

    if not grid_rows or best_df is None or best_summary is None:
        raise RuntimeError("No successful configs")

    grid_df = pd.DataFrame(grid_rows).sort_values(
        ["full_year_pos", "cagr_diff_pctp", "mdd_diff_pctp", "nw_p_gt0"],
        ascending=[False, False, False, False],
    )
    yearly_df = pd.DataFrame(best_summary["yearly"])
    grid_df.to_csv(GRID_CSV, index=False)
    best_df.to_csv(PERIODS_CSV, index=False)
    yearly_df.to_csv(YEARLY_CSV, index=False)

    out = {
        "run_tag": RUN_TAG,
        "start_date": START,
        "end_date": END,
        "benchmark": BENCH,
        "trade_cost_bps": float(TRADE_COST_BPS),
        "tested_configs": int(len(grid_df)),
        "best": best_summary,
        "paths": {
            "grid_csv": str(GRID_CSV.relative_to(ROOT)),
            "periods_csv": str(PERIODS_CSV.relative_to(ROOT)),
            "yearly_csv": str(YEARLY_CSV.relative_to(ROOT)),
        },
    }
    BEST_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Saved: {GRID_CSV.relative_to(ROOT)} ({len(grid_df)} rows)")
    print(f"Saved: {PERIODS_CSV.relative_to(ROOT)}")
    print(f"Saved: {YEARLY_CSV.relative_to(ROOT)}")
    print(f"Saved: {BEST_JSON.relative_to(ROOT)}")
    print(
        "Best trend config -> "
        f"CAGR diff {float(best_summary['cagr_diff_pctp']):+.2f}pp | "
        f"MDD diff {float(best_summary['mdd_diff_pctp']):+.2f}pp | "
        f"FullYear+ {int(best_summary['full_year_pos'])}/9 | "
        f"NW p2 {float(best_summary['nw_p_two']):.3f}"
    )


if __name__ == "__main__":
    run()
