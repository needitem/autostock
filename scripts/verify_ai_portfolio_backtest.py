from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RESULTS_CSV = ROOT / "data" / "ai_portfolio_backtest_results.csv"
DEFAULT_SUMMARY_JSON = ROOT / "data" / "ai_portfolio_backtest_summary.json"
DEFAULT_OUT_MD = ROOT / "data" / "ai_portfolio_backtest_verification.md"
DEFAULT_OUT_JSON = ROOT / "data" / "ai_portfolio_backtest_verification.json"


def _f(x: Any, d: float = 0.0) -> float:
    try:
        y = float(x)
        return d if np.isnan(y) or np.isinf(y) else y
    except Exception:
        return d


def _load_summary(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _risk_metrics(series_pct: pd.Series, periods_per_year: int) -> dict[str, float]:
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
    ppy = max(1, int(periods_per_year))
    c = (1 + r).cumprod()
    tot = float(c.iloc[-1] - 1.0)
    cagr = float(c.iloc[-1] ** (ppy / n) - 1.0) if c.iloc[-1] > 0 else 0.0
    sharpe, vol = 0.0, 0.0
    if n > 1:
        sd = float(r.std(ddof=1))
        if sd > 1e-12:
            vol = float(sd * np.sqrt(ppy))
            sharpe = float((r.mean() / sd) * np.sqrt(ppy))
    sortino = 0.0
    dn = r[r < 0]
    if len(dn) > 1:
        dsd = float(dn.std(ddof=1))
        if dsd > 1e-12:
            sortino = float((r.mean() / dsd) * np.sqrt(ppy))
    dd = (c / c.cummax()) - 1.0
    return {
        "periods": int(n),
        "mean_period_return_pct": float(r.mean() * 100.0),
        "cagr_pct": float(cagr * 100.0),
        "vol_annual_pct": float(vol * 100.0),
        "sharpe": float(sharpe),
        "sortino": float(sortino),
        "max_drawdown_pct": float(dd.min() * 100.0),
        "win_rate_pct": float((r > 0).mean() * 100.0),
        "total_return_pct": float(tot * 100.0),
    }


def _cagr_from_returns(series_pct: pd.Series, periods_per_year: int) -> float:
    s = pd.to_numeric(series_pct, errors="coerce").dropna().astype(float)
    if len(s) == 0:
        return 0.0
    r = s / 100.0
    ppy = max(1, int(periods_per_year))
    c = (1 + r).cumprod()
    if float(c.iloc[-1]) <= 0:
        return 0.0
    return float(c.iloc[-1] ** (ppy / len(r)) - 1.0) * 100.0


def _breakeven_cost_bps(
    gross_return_pct: pd.Series,
    turnover: pd.Series,
    bench_return_pct: pd.Series,
    periods_per_year: int,
    hi_bps: float = 300.0,
) -> float:
    bench_cagr = _cagr_from_returns(bench_return_pct, periods_per_year)
    gross = pd.to_numeric(gross_return_pct, errors="coerce").astype(float)
    turn = pd.to_numeric(turnover, errors="coerce").astype(float)

    def _net_cagr(cost_bps: float) -> float:
        return _cagr_from_returns(gross - (float(cost_bps) / 100.0) * turn, periods_per_year)

    lo, hi = 0.0, float(hi_bps)
    for _ in range(60):
        mid = (lo + hi) / 2.0
        if _net_cagr(mid) > bench_cagr:
            lo = mid
        else:
            hi = mid
    return float(hi)


def _yearly_compounded(df: pd.DataFrame, col: str, date_col: str, label: str) -> pd.DataFrame:
    out = df.copy()
    out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
    out = out.dropna(subset=[date_col])
    out["year"] = out[date_col].dt.year.astype(int)

    def _comp_pct(x: pd.Series) -> float:
        r = pd.to_numeric(x, errors="coerce").dropna().astype(float) / 100.0
        if len(r) == 0:
            return float("nan")
        return float(((1 + r).prod() - 1.0) * 100.0)

    y = out.groupby("year").agg(n=(col, "count"), value=(col, _comp_pct)).reset_index()
    y = y.rename(columns={"value": label})
    return y


@dataclass(frozen=True)
class BootstrapConfig:
    blocks: int = 3
    samples: int = 5000
    seed: int = 7


def _block_bootstrap_indices(rng: np.random.Generator, n: int, block: int) -> np.ndarray:
    if n <= 0:
        return np.array([], dtype=int)
    b = max(1, int(block))
    starts = np.arange(n)
    idx: list[int] = []
    while len(idx) < n:
        s = int(rng.choice(starts))
        idx.extend(range(s, min(n, s + b)))
    return np.array(idx[:n], dtype=int)


def _bootstrap_alpha_ci(
    ai_net_pct: pd.Series,
    bench_pct: pd.Series,
    periods_per_year: int,
    cfg: BootstrapConfig,
) -> dict[str, Any]:
    ai = pd.to_numeric(ai_net_pct, errors="coerce").dropna().astype(float).to_numpy() / 100.0
    q = pd.to_numeric(bench_pct, errors="coerce").dropna().astype(float).to_numpy() / 100.0
    n = int(min(len(ai), len(q)))
    ai = ai[:n]
    q = q[:n]
    if n <= 3:
        return {"error": "not_enough_periods"}

    rng = np.random.default_rng(int(cfg.seed))
    B = max(200, int(cfg.samples))
    block = max(1, int(cfg.blocks))
    ppy = max(1, int(periods_per_year))

    mean_alpha = np.empty(B, dtype=float)
    cagr_diff = np.empty(B, dtype=float)
    for b in range(B):
        idx = _block_bootstrap_indices(rng, n, block)
        a = ai[idx]
        qq = q[idx]
        mean_alpha[b] = float((a - qq).mean()) * 100.0

        ca = float(np.prod(1.0 + a))
        cq = float(np.prod(1.0 + qq))
        cagr_a = (ca ** (ppy / n) - 1.0) * 100.0 if ca > 0 else 0.0
        cagr_q = (cq ** (ppy / n) - 1.0) * 100.0 if cq > 0 else 0.0
        cagr_diff[b] = float(cagr_a - cagr_q)

    def _ci(x: np.ndarray) -> tuple[float, float]:
        lo, hi = np.quantile(x, [0.025, 0.975])
        return float(lo), float(hi)

    ca_obs = float(np.prod(1.0 + ai))
    cq_obs = float(np.prod(1.0 + q))
    cagr_diff_obs = ((ca_obs ** (ppy / n) - 1.0) - (cq_obs ** (ppy / n) - 1.0)) * 100.0

    return {
        "n_periods": n,
        "bootstrap_samples": int(B),
        "block_len": int(block),
        "alpha_mean_obs_pct": float((ai - q).mean() * 100.0),
        "alpha_mean_ci95_pct": _ci(mean_alpha),
        "p_alpha_mean_gt0": float((mean_alpha > 0).mean()),
        "cagr_diff_obs_pct": float(cagr_diff_obs),
        "cagr_diff_ci95_pct": _ci(cagr_diff),
        "p_cagr_diff_gt0": float((cagr_diff > 0).mean()),
    }


def _turnover_stats(series: pd.Series) -> dict[str, float]:
    s = pd.to_numeric(series, errors="coerce").dropna().astype(float)
    if len(s) == 0:
        return {"mean": 0.0, "median": 0.0, "p90": 0.0, "p95": 0.0, "max": 0.0}
    return {
        "mean": float(s.mean()),
        "median": float(s.median()),
        "p90": float(s.quantile(0.90)),
        "p95": float(s.quantile(0.95)),
        "max": float(s.max()),
    }


def _extract_symbol_sets(df: pd.DataFrame, col: str) -> list[set[str]]:
    out: list[set[str]] = []
    for raw in df.get(col, pd.Series(dtype=str)).astype(str).fillna("").tolist():
        syms: set[str] = set()
        try:
            obj = json.loads(raw) if raw and raw != "nan" else {}
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if not isinstance(k, str):
                        continue
                    if k.strip() and _f(v, 0.0) > 0:
                        syms.add(k.strip().upper())
        except Exception:
            pass
        out.append(syms)
    return out


def _overlap_stats(sets: list[set[str]]) -> dict[str, float]:
    if len(sets) < 2:
        return {"avg_overlap_count": 0.0, "avg_jaccard": 0.0}
    overlaps: list[int] = []
    jacc: list[float] = []
    for a, b in zip(sets[:-1], sets[1:]):
        inter = len(a & b)
        uni = len(a | b)
        overlaps.append(inter)
        jacc.append(float(inter / uni) if uni else 0.0)
    return {
        "avg_overlap_count": float(np.mean(overlaps)) if overlaps else 0.0,
        "avg_jaccard": float(np.mean(jacc)) if jacc else 0.0,
    }


def _distinct_symbol_count(sets: list[set[str]]) -> int:
    union: set[str] = set()
    for s in sets:
        union |= set(s or set())
    return int(len(union))


def _avg_positions_held(sets: list[set[str]]) -> float:
    if not sets:
        return 0.0
    return float(np.mean([len(s) for s in sets]))


def _load_universe_by_date(path: Path) -> dict[str, list[str]]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(obj, dict):
        return {}
    raw = obj.get("dates", obj)
    if not isinstance(raw, dict):
        return {}
    out: dict[str, list[str]] = {}
    for k, v in raw.items():
        if not isinstance(k, str) or not isinstance(v, list):
            continue
        syms: list[str] = []
        seen: set[str] = set()
        for item in v:
            if not isinstance(item, str):
                continue
            sym = item.strip().upper().replace(".", "-")
            if not sym or sym in seen:
                continue
            seen.add(sym)
            syms.append(sym)
        if syms:
            out[k.strip()] = syms
    return out


def _alpha_stats(alpha_pct: pd.Series, periods_per_year: int) -> dict[str, float]:
    a = pd.to_numeric(alpha_pct, errors="coerce").dropna().astype(float)
    if len(a) == 0:
        return {"mean_pct": 0.0, "sd_pct": 0.0, "ir": 0.0, "t_stat": 0.0, "win_rate_pct": 0.0}
    mu = float(a.mean())
    sd = float(a.std(ddof=1)) if len(a) > 1 else 0.0
    ppy = max(1, int(periods_per_year))
    ir = float((mu / sd) * math.sqrt(ppy)) if sd > 1e-12 else 0.0
    t = float(mu / (sd / math.sqrt(len(a)))) if sd > 1e-12 else 0.0
    win = float((a > 0).mean() * 100.0)
    return {"mean_pct": mu, "sd_pct": sd, "ir": ir, "t_stat": t, "win_rate_pct": win}


def _subperiod_table(df: pd.DataFrame, periods_per_year: int) -> pd.DataFrame:
    d = df.copy()
    d["entry_day"] = pd.to_datetime(d["entry_day"], errors="coerce")
    d = d.dropna(subset=["entry_day"])
    cuts = [
        ("2016-2019", "2016-01-01", "2019-12-31"),
        ("2020-2022", "2020-01-01", "2022-12-31"),
        ("2023-2025", "2023-01-01", "2025-12-31"),
    ]
    rows: list[dict[str, Any]] = []
    for name, a, b in cuts:
        m = d[(d["entry_day"] >= a) & (d["entry_day"] <= b)]
        if m.empty:
            continue
        ai = _risk_metrics(m["net_return_pct"], periods_per_year)
        qqq = _risk_metrics(m["benchmark_return_pct"], periods_per_year)
        mom = _risk_metrics(m["mom_net_return_pct"], periods_per_year)
        alpha_mean = float(
            (pd.to_numeric(m["net_return_pct"], errors="coerce") - pd.to_numeric(m["benchmark_return_pct"], errors="coerce"))
            .dropna()
            .astype(float)
            .mean()
        )
        rows.append(
            {
                "period": name,
                "n_months": int(ai.get("periods", 0)),
                "ai_cagr_pct": float(ai.get("cagr_pct", 0.0)),
                "qqq_cagr_pct": float(qqq.get("cagr_pct", 0.0)),
                "mom_cagr_pct": float(mom.get("cagr_pct", 0.0)),
                "ai_mdd_pct": float(ai.get("max_drawdown_pct", 0.0)),
                "qqq_mdd_pct": float(qqq.get("max_drawdown_pct", 0.0)),
                "alpha_mean_pct": float(alpha_mean),
            }
        )
    return pd.DataFrame(rows)


def _cost_sweep_table(df: pd.DataFrame, periods_per_year: int, costs_bps: list[float]) -> pd.DataFrame:
    gross_ai = pd.to_numeric(df["gross_return_pct"], errors="coerce")
    turn_ai = pd.to_numeric(df["turnover"], errors="coerce")
    gross_mom = pd.to_numeric(df["mom_gross_return_pct"], errors="coerce")
    turn_mom = pd.to_numeric(df["mom_turnover"], errors="coerce")

    rows: list[dict[str, Any]] = []
    for bps in costs_bps:
        cost_pct = float(bps) / 100.0
        ai_net = gross_ai - cost_pct * turn_ai
        mom_net = gross_mom - cost_pct * turn_mom
        ai = _risk_metrics(ai_net, periods_per_year)
        mom = _risk_metrics(mom_net, periods_per_year)
        rows.append(
            {
                "cost_bps": float(bps),
                "ai_cagr_pct": float(ai.get("cagr_pct", 0.0)),
                "ai_sharpe": float(ai.get("sharpe", 0.0)),
                "ai_mdd_pct": float(ai.get("max_drawdown_pct", 0.0)),
                "mom_cagr_pct": float(mom.get("cagr_pct", 0.0)),
                "mom_sharpe": float(mom.get("sharpe", 0.0)),
                "mom_mdd_pct": float(mom.get("max_drawdown_pct", 0.0)),
            }
        )
    return pd.DataFrame(rows)


def _fmt_pct(x: Any, digits: int = 2) -> str:
    try:
        v = float(x)
        if not np.isfinite(v):
            return "NA"
        return f"{v:.{digits}f}%"
    except Exception:
        return "NA"


def _fmt_num(x: Any, digits: int = 2) -> str:
    try:
        v = float(x)
        if not np.isfinite(v):
            return "NA"
        return f"{v:.{digits}f}"
    except Exception:
        return "NA"


def _df_to_text_table(df: pd.DataFrame, float_digits: int = 2, max_rows: int = 0) -> str:
    if df is None or df.empty:
        return ""
    view = df.copy()
    if max_rows > 0 and len(view) > max_rows:
        view = view.head(max_rows)

    def _ff(x: float) -> str:
        try:
            return f"{float(x):.{int(float_digits)}f}"
        except Exception:
            return str(x)

    return view.to_string(index=False, float_format=_ff)


def main() -> None:
    results_path = Path(os.getenv("AI_PORTFOLIO_RESULTS_CSV", str(DEFAULT_RESULTS_CSV))).resolve()
    summary_path = Path(os.getenv("AI_PORTFOLIO_SUMMARY_JSON", str(DEFAULT_SUMMARY_JSON))).resolve()
    out_md = Path(os.getenv("AI_PORTFOLIO_VERIFY_MD", str(DEFAULT_OUT_MD))).resolve()
    out_json = Path(os.getenv("AI_PORTFOLIO_VERIFY_JSON", str(DEFAULT_OUT_JSON))).resolve()

    if not results_path.exists():
        raise FileNotFoundError(f"Missing results CSV: {results_path}")
    df = pd.read_csv(results_path)
    if df.empty:
        raise ValueError(f"Results CSV empty: {results_path}")

    summary = _load_summary(summary_path)
    periods_per_year = int(pd.to_numeric(df.get("periods_per_year", 12), errors="coerce").fillna(12).iloc[0])

    # Core metrics (recompute from CSV)
    pm_calc = {
        "ai_portfolio": _risk_metrics(df["net_return_pct"], periods_per_year),
        "ai_portfolio_gross": _risk_metrics(df["gross_return_pct"], periods_per_year),
        "momentum_topk": _risk_metrics(df["mom_net_return_pct"], periods_per_year),
        "momentum_topk_gross": _risk_metrics(df["mom_gross_return_pct"], periods_per_year),
        "benchmark": _risk_metrics(df["benchmark_return_pct"], periods_per_year),
    }

    pm_ref = (summary.get("portfolio_metrics") or {}) if isinstance(summary, dict) else {}
    matches_summary = True
    for k, calc in pm_calc.items():
        ref = pm_ref.get(k)
        if not isinstance(ref, dict):
            matches_summary = False
            continue
        for stat in ("cagr_pct", "sharpe", "max_drawdown_pct", "total_return_pct"):
            if abs(float(calc.get(stat, 0.0)) - float(ref.get(stat, 0.0))) > 1e-9:
                matches_summary = False

    # Alpha stats
    alpha_pct = pd.to_numeric(df["net_return_pct"], errors="coerce") - pd.to_numeric(
        df["benchmark_return_pct"], errors="coerce"
    )
    alpha = _alpha_stats(alpha_pct, periods_per_year)
    bs_cfg = BootstrapConfig(
        blocks=int(os.getenv("AI_VERIFY_BOOTSTRAP_BLOCK_LEN", "3")),
        samples=int(os.getenv("AI_VERIFY_BOOTSTRAP_SAMPLES", "5000")),
        seed=int(os.getenv("AI_VERIFY_BOOTSTRAP_SEED", "7")),
    )
    boot = _bootstrap_alpha_ci(df["net_return_pct"], df["benchmark_return_pct"], periods_per_year, bs_cfg)

    # Costs
    costs_bps = [float(x) for x in (os.getenv("AI_VERIFY_COST_SWEEP_BPS", "0,20,50,100,150").split(",")) if x.strip()]
    cost_sweep = _cost_sweep_table(df, periods_per_year, costs_bps)
    ai_breakeven_bps = _breakeven_cost_bps(
        df["gross_return_pct"], df["turnover"], df["benchmark_return_pct"], periods_per_year
    )
    mom_breakeven_bps = _breakeven_cost_bps(
        df["mom_gross_return_pct"], df["mom_turnover"], df["benchmark_return_pct"], periods_per_year
    )

    # Turnover and churn
    t_ai = _turnover_stats(df["turnover"])
    t_mom = _turnover_stats(df["mom_turnover"])
    ai_sets = _extract_symbol_sets(df, "positions")
    mom_sets = _extract_symbol_sets(df, "mom_positions")
    overlap_ai = _overlap_stats(ai_sets)
    overlap_mom = _overlap_stats(mom_sets)
    distinct_ai = _distinct_symbol_count(ai_sets)
    distinct_mom = _distinct_symbol_count(mom_sets)
    avg_pos_ai = _avg_positions_held(ai_sets)
    avg_pos_mom = _avg_positions_held(mom_sets)

    # Yearly compounded returns
    y_ai = _yearly_compounded(df, "net_return_pct", "entry_day", "ai_ret_pct")
    y_qqq = _yearly_compounded(df, "benchmark_return_pct", "entry_day", "qqq_ret_pct")
    y_mom = _yearly_compounded(df, "mom_net_return_pct", "entry_day", "mom_ret_pct")
    yearly = y_ai.merge(y_qqq, on=["year", "n"], how="outer").merge(y_mom, on=["year", "n"], how="outer")
    yearly["ai_minus_qqq_pct"] = yearly["ai_ret_pct"] - yearly["qqq_ret_pct"]

    # Subperiods
    subperiods = _subperiod_table(df, periods_per_year)

    # Universe coverage (best-effort, no external data fetch)
    by_date_path: Path | None = None
    by_date_env = (os.getenv("AI_UNIVERSE_BY_DATE_FILE") or "").strip()
    if by_date_env:
        p = Path(by_date_env)
        by_date_path = p if p.is_absolute() else (ROOT / p)
    else:
        # Heuristic: common path for nasdaq100 by-date monthly snapshots.
        guess = ROOT / "data" / "universe" / "nasdaq100_by_date_monthly.json"
        if guess.exists() and "nasdaq" in str(summary.get("universe", "")).lower() and "by_date" in str(
            summary.get("universe", "")
        ).lower():
            by_date_path = guess
        else:
            guess2 = ROOT / "data" / "universe" / "nasdaq100_by_date.json"
            if guess2.exists() and "nasdaq" in str(summary.get("universe", "")).lower() and "by_date" in str(
                summary.get("universe", "")
            ).lower():
                by_date_path = guess2

    coverage: dict[str, Any] = {}
    if by_date_path and by_date_path.exists():
        ub = _load_universe_by_date(by_date_path)
        want = set(pd.to_datetime(df["date"], errors="coerce").dt.date.astype(str).dropna().tolist())
        ub_in_run = {k: v for k, v in ub.items() if k in want}
        union_all = sorted({s for arr in ub.values() for s in (arr or [])})
        union_run = sorted({s for arr in ub_in_run.values() for s in (arr or [])})
        coverage = {
            "by_date_file": str(by_date_path),
            "snapshots_in_file": int(len(ub)),
            "snapshots_in_run": int(len(ub_in_run)),
            "union_symbols_all": int(len(union_all)),
            "union_symbols_run": int(len(union_run)),
            "symbols_used_in_backtest": int(_f(summary.get("symbols"), 0.0)),
            "missing_vs_union_run": int(max(0, len(union_run) - int(_f(summary.get("symbols"), 0.0)))),
        }

    # Summarize
    ai = pm_calc["ai_portfolio"]
    qqq = pm_calc["benchmark"]
    mom = pm_calc["momentum_topk"]

    report = {
        "inputs": {
            "results_csv": str(results_path),
            "summary_json": str(summary_path) if summary_path.exists() else "",
            "rows": int(len(df)),
            "periods_per_year": int(periods_per_year),
        },
        "integrity": {"matches_summary_json": bool(matches_summary)},
        "metrics": {"ai_portfolio": ai, "momentum_topk": mom, "benchmark": qqq},
        "alpha": alpha,
        "bootstrap": boot,
        "turnover": {
            "ai": t_ai,
            "momentum": t_mom,
            "ai_overlap": overlap_ai,
            "momentum_overlap": overlap_mom,
            "distinct_ai_symbols_held": int(distinct_ai),
            "distinct_momentum_symbols_held": int(distinct_mom),
            "avg_positions_ai": float(avg_pos_ai),
            "avg_positions_momentum": float(avg_pos_mom),
        },
        "costs": {
            "sweep_bps": costs_bps,
            "breakeven_bps_ai": float(ai_breakeven_bps),
            "breakeven_bps_momentum": float(mom_breakeven_bps),
        },
        "coverage": coverage,
        "yearly": yearly.to_dict(orient="records"),
        "subperiods": subperiods.to_dict(orient="records"),
    }

    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    # Human report
    lines: list[str] = []
    lines.append("# AI Portfolio Backtest Verification\n")
    lines.append(f"- results: `{results_path.relative_to(ROOT) if results_path.is_relative_to(ROOT) else results_path}`")
    if summary_path.exists():
        lines.append(f"- summary: `{summary_path.relative_to(ROOT) if summary_path.is_relative_to(ROOT) else summary_path}`")
    lines.append(f"- rows: **{len(df)}** | periods/year: **{periods_per_year}**")
    lines.append(f"- summary-metrics match: **{matches_summary}**\n")

    lines.append("## Headline\n")
    lines.append(
        f"- AI vs QQQ: CAGR {_fmt_pct(ai['cagr_pct'])} vs {_fmt_pct(qqq['cagr_pct'])} "
        f"| Sharpe {_fmt_num(ai['sharpe'])} vs {_fmt_num(qqq['sharpe'])} "
        f"| MDD {_fmt_pct(ai['max_drawdown_pct'])} vs {_fmt_pct(qqq['max_drawdown_pct'])}"
    )
    lines.append(
        f"- Momentum vs QQQ: CAGR {_fmt_pct(mom['cagr_pct'])} vs {_fmt_pct(qqq['cagr_pct'])} "
        f"| Sharpe {_fmt_num(mom['sharpe'])} vs {_fmt_num(qqq['sharpe'])} "
        f"| MDD {_fmt_pct(mom['max_drawdown_pct'])} vs {_fmt_pct(qqq['max_drawdown_pct'])}\n"
    )

    lines.append("## Alpha (AI - QQQ)\n")
    lines.append(
        f"- mean monthly alpha: {_fmt_pct(alpha['mean_pct'], 3)} | win-rate: {_fmt_pct(alpha['win_rate_pct'], 2)} "
        f"| IR: {_fmt_num(alpha['ir'], 3)} | t-stat: {_fmt_num(alpha['t_stat'], 3)}"
    )
    if isinstance(boot, dict) and "error" not in boot:
        ci_lo, ci_hi = boot["alpha_mean_ci95_pct"]
        cd_lo, cd_hi = boot["cagr_diff_ci95_pct"]
        lines.append(
            f"- bootstrap(block={boot['block_len']}, B={boot['bootstrap_samples']}): "
            f"mean alpha CI95 [{_fmt_num(ci_lo, 3)}, {_fmt_num(ci_hi, 3)}] | P(mean>0)={_fmt_num(boot['p_alpha_mean_gt0'], 3)}"
        )
        lines.append(
            f"- bootstrap: CAGR diff obs {_fmt_pct(boot['cagr_diff_obs_pct'], 3)} "
            f"CI95 [{_fmt_pct(cd_lo, 3)}, {_fmt_pct(cd_hi, 3)}] | P(diff>0)={_fmt_num(boot['p_cagr_diff_gt0'], 3)}\n"
        )
    else:
        lines.append("- bootstrap: skipped (not enough periods)\n")

    lines.append("## Turnover\n")
    lines.append(
        f"- AI turnover mean {_fmt_num(t_ai['mean'], 3)} | median {_fmt_num(t_ai['median'], 3)} | p95 {_fmt_num(t_ai['p95'], 3)}"
    )
    lines.append(
        f"- MOM turnover mean {_fmt_num(t_mom['mean'], 3)} | median {_fmt_num(t_mom['median'], 3)} | p95 {_fmt_num(t_mom['p95'], 3)}"
    )
    lines.append(f"- distinct symbols held (AI/MOM): **{distinct_ai}** / **{distinct_mom}**")
    lines.append(f"- avg positions held (AI/MOM): **{avg_pos_ai:.2f}** / **{avg_pos_mom:.2f}**")
    lines.append(
        f"- AI avg overlap(count): {_fmt_num(overlap_ai['avg_overlap_count'], 3)} | avg jaccard: {_fmt_num(overlap_ai['avg_jaccard'], 3)}"
    )
    lines.append(
        f"- MOM avg overlap(count): {_fmt_num(overlap_mom['avg_overlap_count'], 3)} | avg jaccard: {_fmt_num(overlap_mom['avg_jaccard'], 3)}\n"
    )

    if coverage:
        lines.append("## Universe Coverage (by-date union vs used)\n")
        p = Path(str(coverage.get("by_date_file", "")))
        show = p.relative_to(ROOT) if p.is_relative_to(ROOT) else p
        lines.append(f"- by-date file: `{show}`")
        lines.append(
            f"- union symbols (run snapshots): **{coverage.get('union_symbols_run', 0)}** | used in backtest: **{coverage.get('symbols_used_in_backtest', 0)}**"
        )
        lines.append(f"- missing vs union(run): **{coverage.get('missing_vs_union_run', 0)}**\n")

    lines.append("## Cost Sensitivity (turnover-based)\n")
    lines.append(f"- breakeven total cost (AI): **{ai_breakeven_bps:.2f} bps** per 100% turnover")
    lines.append(f"- breakeven total cost (MOM): **{mom_breakeven_bps:.2f} bps** per 100% turnover\n")
    lines.append("```text")
    lines.append(_df_to_text_table(cost_sweep, float_digits=2))
    lines.append("```")
    lines.append("")

    lines.append("## Yearly (compounded)\n")
    if not yearly.empty:
        lines.append("```text")
        lines.append(_df_to_text_table(yearly, float_digits=2))
        lines.append("```")
        lines.append("")

    lines.append("## Subperiods\n")
    if not subperiods.empty:
        lines.append("```text")
        lines.append(_df_to_text_table(subperiods, float_digits=2))
        lines.append("```")
        lines.append("")

    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")

    print(f"Saved: {out_json.relative_to(ROOT)}")
    print(f"Saved: {out_md.relative_to(ROOT)}")
    if not matches_summary:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
