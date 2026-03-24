from __future__ import annotations

import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
RUNS_DIR = ROOT / "data" / "runs"


def _f(x: Any, d: float = 0.0) -> float:
    try:
        y = float(x)
        return d if np.isnan(y) or np.isinf(y) else y
    except Exception:
        return d


def _load_json(path: Path) -> dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    return obj if isinstance(obj, dict) else {}


def _risk_metrics(series_pct: pd.Series, periods_per_year: int = 52) -> dict[str, float]:
    s = pd.to_numeric(series_pct, errors="coerce").dropna().astype(float)
    if len(s) == 0:
        return {"cagr_pct": 0.0, "sharpe": 0.0, "max_drawdown_pct": 0.0}
    r = s / 100.0
    n = len(r)
    c = (1.0 + r).cumprod()
    cagr = float(c.iloc[-1] ** (periods_per_year / n) - 1.0) * 100.0 if c.iloc[-1] > 0 else 0.0
    sd = float(r.std(ddof=1)) if n > 1 else 0.0
    sharpe = float((r.mean() / sd) * np.sqrt(periods_per_year)) if sd > 1e-12 else 0.0
    dd = (c / c.cummax()) - 1.0
    return {
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


def _horizon_slice(df: pd.DataFrame, years: int) -> pd.DataFrame:
    d = df.copy()
    d["entry_day"] = pd.to_datetime(d["entry_day"], errors="coerce")
    d = d.dropna(subset=["entry_day"])
    if d.empty:
        return d
    latest = pd.Timestamp(d["entry_day"].max())
    cutoff = latest - pd.DateOffset(years=int(years))
    return d.loc[d["entry_day"] >= cutoff].copy()


def _horizon_check(df: pd.DataFrame, years: int, periods_per_year: int) -> dict[str, Any]:
    sub = _horizon_slice(df, years)
    ai = _risk_metrics(sub["net_return_pct"], periods_per_year)
    bench = _risk_metrics(sub["benchmark_return_pct"], periods_per_year)
    alpha = pd.to_numeric(sub["net_return_pct"], errors="coerce") - pd.to_numeric(sub["benchmark_return_pct"], errors="coerce")
    nw = _newey_west(alpha)
    return {
        "years": int(years),
        "rows": int(len(sub)),
        "cagr_diff_pct": float(ai["cagr_pct"] - bench["cagr_pct"]),
        "nw_p_two": float(nw["nw_p_two"]),
        "passes": bool((float(ai["cagr_pct"] - bench["cagr_pct"]) > 0.0) and (float(nw["nw_p_two"]) < 0.10)),
    }


def main() -> None:
    verify_json = Path(os.getenv("PROMOTION_VERIFY_JSON", ""))
    results_csv = Path(os.getenv("PROMOTION_RESULTS_CSV", ""))
    run_tag = (os.getenv("PROMOTION_RUN_TAG") or f"promotion_check_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}").strip()
    min_cost_bps = float(os.getenv("PROMOTION_MIN_COST_BPS", "20"))
    max_mdd_worse_than_bench_pctp = float(os.getenv("PROMOTION_MAX_MDD_WORSE_PCTP", "10"))
    min_p_alpha_gt0 = float(os.getenv("PROMOTION_MIN_P_ALPHA_GT0", "0.90"))
    if not verify_json.is_absolute():
        verify_json = ROOT / verify_json
    if not results_csv.is_absolute():
        results_csv = ROOT / results_csv
    if not verify_json.exists():
        raise FileNotFoundError(f"Missing verification JSON: {verify_json}")
    if not results_csv.exists():
        raise FileNotFoundError(f"Missing results CSV: {results_csv}")

    verify = _load_json(verify_json)
    df = pd.read_csv(results_csv)
    summary_path_raw = str(((verify.get("inputs") or {}).get("summary_json") or "")).strip()
    summary_path = Path(summary_path_raw) if summary_path_raw else Path()
    if summary_path_raw and not summary_path.is_absolute():
        summary_path = ROOT / summary_path
    summary = _load_json(summary_path) if summary_path_raw and summary_path.exists() else {}
    periods_per_year = int(pd.to_numeric(df.get("periods_per_year", 52), errors="coerce").fillna(52).iloc[0])
    ai = (verify.get("metrics") or {}).get("ai_portfolio") or {}
    bench = (verify.get("metrics") or {}).get("benchmark") or {}
    alpha = verify.get("alpha") or {}
    turnover = (verify.get("turnover") or {}).get("ai") or {}

    horizon_checks = [_horizon_check(df, years=y, periods_per_year=periods_per_year) for y in (3, 5, 7)]
    summary_cost = _f(summary.get("trade_cost_bps"), np.nan)
    if not np.isfinite(summary_cost):
        summary_cost = _f(summary.get("trade_cost_bps_base"), np.nan)
    df_cost = _f(df.get("trade_cost_bps_base", pd.Series([np.nan])).iloc[0], np.nan) if "trade_cost_bps_base" in df.columns else np.nan
    effective_cost = summary_cost if np.isfinite(summary_cost) else (_f(df_cost, 0.0) if np.isfinite(df_cost) else 0.0)
    criterion_cost = bool(float(min_cost_bps) <= float(effective_cost))
    criterion_full = bool((_f(ai.get("cagr_pct")) - _f(bench.get("cagr_pct")) > 0.0) and (_f(alpha.get("nw_p_gt0"), 0.0) >= min_p_alpha_gt0))
    mdd_diff = float(_f(ai.get("max_drawdown_pct")) - _f(bench.get("max_drawdown_pct")))
    criterion_mdd = bool(mdd_diff >= -float(max_mdd_worse_than_bench_pctp))
    criterion_turnover = bool(_f(turnover.get("mean"), 999.0) <= 0.30)
    criteria = [
        {"name": "cost_at_least_20bps", "passes": criterion_cost, "detail": f"base_cost_bps={float(effective_cost):.1f}"},
        {"name": "horizon_3y", "passes": bool(horizon_checks[0]["passes"]), "detail": f"cagr_diff={horizon_checks[0]['cagr_diff_pct']:+.2f}pp nw_p2={horizon_checks[0]['nw_p_two']:.3f}"},
        {"name": "horizon_5y", "passes": bool(horizon_checks[1]["passes"]), "detail": f"cagr_diff={horizon_checks[1]['cagr_diff_pct']:+.2f}pp nw_p2={horizon_checks[1]['nw_p_two']:.3f}"},
        {"name": "horizon_7y", "passes": bool(horizon_checks[2]["passes"]), "detail": f"cagr_diff={horizon_checks[2]['cagr_diff_pct']:+.2f}pp nw_p2={horizon_checks[2]['nw_p_two']:.3f}"},
        {"name": "full_window_alpha", "passes": criterion_full, "detail": f"cagr_diff={(_f(ai.get('cagr_pct')) - _f(bench.get('cagr_pct'))):+.2f}pp p_alpha_gt0={_f(alpha.get('nw_p_gt0'), 0.0):.3f}"},
        {"name": "drawdown_guardrail", "passes": criterion_mdd, "detail": f"mdd_diff={mdd_diff:+.2f}pp"},
        {"name": "turnover_guardrail", "passes": criterion_turnover, "detail": f"turnover_mean={_f(turnover.get('mean'), 999.0):.3f}"},
    ]
    overall_pass = all(bool(item["passes"]) for item in criteria)

    report = {
        "run_tag": run_tag,
        "inputs": {
            "verify_json": str(verify_json),
            "results_csv": str(results_csv),
            "periods_per_year": int(periods_per_year),
        },
        "headline": {
            "strategy_cagr_pct": _f(ai.get("cagr_pct")),
            "benchmark_cagr_pct": _f(bench.get("cagr_pct")),
            "strategy_sharpe": _f(ai.get("sharpe")),
            "benchmark_sharpe": _f(bench.get("sharpe")),
            "strategy_mdd_pct": _f(ai.get("max_drawdown_pct")),
            "benchmark_mdd_pct": _f(bench.get("max_drawdown_pct")),
            "nw_p_two_sided": _f(alpha.get("nw_p_two_sided"), 1.0),
            "p_alpha_gt0": _f(alpha.get("nw_p_gt0"), 0.0),
            "turnover_mean": _f(turnover.get("mean"), 999.0),
        },
        "criteria": criteria,
        "overall_pass": bool(overall_pass),
    }

    out_json = RUNS_DIR / f"{run_tag}.json"
    out_md = RUNS_DIR / f"{run_tag}.md"
    out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        "# Strategy Promotion Check",
        "",
        f"- verification: `{verify_json.relative_to(ROOT) if verify_json.is_relative_to(ROOT) else verify_json}`",
        f"- results: `{results_csv.relative_to(ROOT) if results_csv.is_relative_to(ROOT) else results_csv}`",
        f"- overall pass: **{overall_pass}**",
        "",
        "## Criteria",
        "",
    ]
    for item in criteria:
        lines.append(f"- `{item['name']}`: **{item['passes']}** | {item['detail']}")
    lines.append("")
    out_md.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")

    print(f"Saved: {out_json.relative_to(ROOT)}")
    print(f"Saved: {out_md.relative_to(ROOT)}")
    print(f"Promotion check -> overall_pass={overall_pass}")


if __name__ == "__main__":
    main()
