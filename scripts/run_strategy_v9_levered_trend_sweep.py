from __future__ import annotations

import importlib.util
import json
import math
import os
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from itertools import product
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
RUNS_DIR = ROOT / "data" / "runs"
RESEARCH_SCRIPT = ROOT / "scripts" / "research_weekly_levered_trend.py"


DEFAULT_ENV: dict[str, str] = {
    "LT_BEST_JSON": "data/runs/levered_trend_best_lt_v1_grid_20260305.json",
    "LT_RECENT_YEARS": "3",
    "LT_TRADE_COST_BPS": "20",
}


@dataclass(frozen=True)
class SweepConfig:
    risk: str
    safe: str
    ma_window: int
    qqq_confirm: bool
    vix_max: float


def _apply_defaults() -> None:
    for key, value in DEFAULT_ENV.items():
        os.environ.setdefault(key, value)
    os.environ.setdefault(
        "LT_SWEEP_TAG",
        f"levered_trend_local_sweep_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
    )


def _load_module(path: Path, name: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _load_best_payload(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected object in {path}")
    return payload


def _slug(text: str) -> str:
    out = "".join(ch if ch.isalnum() else "_" for ch in str(text).strip().lower()).strip("_")
    return out or "run"


def _config_id(cfg: SweepConfig) -> str:
    vix_text = str(float(cfg.vix_max)).rstrip("0").rstrip(".")
    if not vix_text:
        vix_text = "0"
    return f"{_slug(cfg.risk)}_{_slug(cfg.safe)}_w{int(cfg.ma_window)}_qc{int(cfg.qqq_confirm)}_vx{_slug(vix_text)}"


def _candidate_configs(best_cfg: dict[str, Any]) -> list[SweepConfig]:
    risk = str(best_cfg.get("risk") or "QLD")
    safe = str(best_cfg.get("safe") or "GLD")
    ma_window = int(best_cfg.get("ma_window") or 125)
    qqq_confirm = bool(best_cfg.get("qqq_confirm", False))
    vix_max = float(best_cfg.get("vix_max") or 0.0)

    ma_values = sorted({max(25, ma_window - 25), ma_window, ma_window + 25}, key=lambda x: (abs(x - ma_window), x))
    qqq_values = [qqq_confirm, not qqq_confirm]
    vix_values = sorted({vix_max, 28.0, 32.0}, key=lambda x: (abs(x - vix_max), x))

    configs = [
        SweepConfig(
            risk=risk,
            safe=safe,
            ma_window=int(w),
            qqq_confirm=bool(qc),
            vix_max=float(vx),
        )
        for w, qc, vx in product(ma_values, qqq_values, vix_values)
    ]
    configs.sort(
        key=lambda cfg: (
            abs(cfg.ma_window - ma_window),
            int(cfg.qqq_confirm != qqq_confirm),
            abs(cfg.vix_max - vix_max),
            cfg.ma_window,
            int(cfg.qqq_confirm),
            cfg.vix_max,
        )
    )
    return configs


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
    se = float(np.sqrt(lrv / n)) if n > 0 else 0.0
    t = mu / se if se > 1e-12 else 0.0
    return {
        "nw_t": float(t),
        "nw_p_two": float(math.erfc(abs(t) / math.sqrt(2.0))),
        "nw_p_gt0": float(0.5 * (1.0 + math.erf(t / math.sqrt(2.0)))),
    }


def _period_frame(raw_df: pd.DataFrame) -> pd.DataFrame:
    df = raw_df.copy()
    df["entry_day"] = pd.to_datetime(df.get("entry_day"), errors="coerce")
    df["net_return_pct"] = pd.to_numeric(df.get("strategy_ret"), errors="coerce") * 100.0
    df["benchmark_return_pct"] = pd.to_numeric(df.get("bench_ret"), errors="coerce") * 100.0
    return df.dropna(subset=["entry_day", "net_return_pct", "benchmark_return_pct"]).copy()


def _window_slice(df: pd.DataFrame, *, start: str | pd.Timestamp | None = None, end: str | pd.Timestamp | None = None) -> pd.DataFrame:
    d = df.copy()
    d["entry_day"] = pd.to_datetime(d["entry_day"], errors="coerce")
    d = d.dropna(subset=["entry_day"])
    if start is not None:
        d = d.loc[d["entry_day"] >= pd.Timestamp(start)].copy()
    if end is not None:
        d = d.loc[d["entry_day"] <= pd.Timestamp(end)].copy()
    return d


def _window_summary(df: pd.DataFrame, *, label: str, periods_per_year: int = 52) -> dict[str, Any]:
    d = df.dropna(subset=["entry_day", "net_return_pct", "benchmark_return_pct"]).copy()
    if d.empty:
        return {
            "label": label,
            "periods": 0,
            "window_start": None,
            "window_end": None,
            "strategy": _risk_metrics(pd.Series(dtype=float), periods_per_year),
            "benchmark": _risk_metrics(pd.Series(dtype=float), periods_per_year),
            "alpha": {"cagr_diff_pctp": 0.0, "mdd_diff_pctp": 0.0, "total_diff_pctp": 0.0, **_newey_west(pd.Series(dtype=float))},
        }

    strategy = _risk_metrics(d["net_return_pct"], periods_per_year)
    benchmark = _risk_metrics(d["benchmark_return_pct"], periods_per_year)
    alpha = pd.to_numeric(d["net_return_pct"], errors="coerce") - pd.to_numeric(d["benchmark_return_pct"], errors="coerce")
    nw = _newey_west(alpha)
    return {
        "label": label,
        "periods": int(len(d)),
        "window_start": str(pd.Timestamp(d["entry_day"].min()).date()),
        "window_end": str(pd.Timestamp(d["entry_day"].max()).date()),
        "strategy": strategy,
        "benchmark": benchmark,
        "alpha": {
            "cagr_diff_pctp": float(strategy["cagr_pct"] - benchmark["cagr_pct"]),
            "mdd_diff_pctp": float(strategy["max_drawdown_pct"] - benchmark["max_drawdown_pct"]),
            "total_diff_pctp": float(strategy["total_return_pct"] - benchmark["total_return_pct"]),
            **nw,
        },
    }


def _flatten_window(prefix: str, summary: dict[str, Any]) -> dict[str, Any]:
    return {
        f"{prefix}_periods": int(summary["periods"]),
        f"{prefix}_window_start": summary["window_start"],
        f"{prefix}_window_end": summary["window_end"],
        f"{prefix}_strategy_cagr_pct": float(summary["strategy"]["cagr_pct"]),
        f"{prefix}_benchmark_cagr_pct": float(summary["benchmark"]["cagr_pct"]),
        f"{prefix}_cagr_diff_pctp": float(summary["alpha"]["cagr_diff_pctp"]),
        f"{prefix}_strategy_mdd_pct": float(summary["strategy"]["max_drawdown_pct"]),
        f"{prefix}_benchmark_mdd_pct": float(summary["benchmark"]["max_drawdown_pct"]),
        f"{prefix}_mdd_diff_pctp": float(summary["alpha"]["mdd_diff_pctp"]),
        f"{prefix}_strategy_total_pct": float(summary["strategy"]["total_return_pct"]),
        f"{prefix}_benchmark_total_pct": float(summary["benchmark"]["total_return_pct"]),
        f"{prefix}_total_diff_pctp": float(summary["alpha"]["total_diff_pctp"]),
        f"{prefix}_nw_t": float(summary["alpha"]["nw_t"]),
        f"{prefix}_nw_p_two": float(summary["alpha"]["nw_p_two"]),
        f"{prefix}_nw_p_gt0": float(summary["alpha"]["nw_p_gt0"]),
    }


def _compare_key(full_summary: dict[str, Any], recent_summary: dict[str, Any]) -> tuple[Any, ...]:
    full_alpha = full_summary["alpha"]
    recent_alpha = recent_summary["alpha"]
    full_cagr = float(full_alpha["cagr_diff_pctp"])
    recent_cagr = float(recent_alpha["cagr_diff_pctp"])
    return (
        1 if full_cagr > 0.0 and recent_cagr > 0.0 else 0,
        min(full_cagr, recent_cagr),
        full_cagr + recent_cagr,
        min(float(full_alpha["nw_p_gt0"]), float(recent_alpha["nw_p_gt0"])),
        min(float(full_alpha["mdd_diff_pctp"]), float(recent_alpha["mdd_diff_pctp"])),
    )


def _markdown_table(rows: list[dict[str, Any]], columns: list[tuple[str, str]]) -> str:
    header = "| " + " | ".join(title for title, _ in columns) + " |"
    divider = "| " + " | ".join("---" for _ in columns) + " |"
    lines = [header, divider]
    for row in rows:
        cells = []
        for title, key in columns:
            value = row.get(key, "")
            if isinstance(value, float):
                if "Rank" in title:
                    cells.append(str(int(value)))
                else:
                    cells.append(f"{value:+.2f}")
            elif isinstance(value, int) and "Rank" not in title:
                cells.append(f"{value:+d}")
            else:
                cells.append(str(value))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def _render_markdown(report: dict[str, Any]) -> str:
    rows = report["results"]
    columns = [
        ("Rank", "rank"),
        ("Config", "config_id"),
        ("Full CAGR diff", "full_cagr_diff_pctp"),
        ("Full MDD diff", "full_mdd_diff_pctp"),
        ("Recent 3Y CAGR diff", "recent_3y_cagr_diff_pctp"),
        ("Recent 3Y MDD diff", "recent_3y_mdd_diff_pctp"),
        ("Robust score", "robust_score"),
    ]
    lines = [
        "# Levered Trend Local Sweep",
        "",
        f"- source: `{report['inputs']['best_json']}`",
        f"- base config: `{report['base_config_id']}`",
        f"- sweep size: **{report['sweep_size']}**",
        f"- recent window: **{report['inputs']['recent_years']}y**",
        "",
        "## Best Candidates",
        "",
    ]
    best = report["best"]
    lines.append(f"- full window: `{best['full_window']['config_id']}`")
    lines.append(f"- recent 3y: `{best['recent_3y']['config_id']}`")
    lines.append(f"- robust: `{best['robust']['config_id']}`")
    lines.extend(["", "## Sweep Table", "", _markdown_table(rows, columns), ""])
    return "\n".join(lines)


def _evaluate_config(
    research: Any,
    cfg: SweepConfig,
    frames: dict[str, pd.DataFrame],
    snaps: list[pd.Timestamp],
    *,
    periods_per_year: int,
    recent_years: int,
    end_date: str,
) -> dict[str, Any]:
    trend_cfg = research.TrendConfig(**asdict(cfg))
    raw_df, _summary = research._run_config(trend_cfg, frames, snaps)
    if raw_df.empty:
        raise RuntimeError("No levered-trend periods produced")

    df = _period_frame(raw_df)
    full = _window_summary(df, label="full_window", periods_per_year=periods_per_year)
    cutoff = pd.Timestamp(end_date) - pd.DateOffset(years=int(recent_years))
    recent = _window_summary(_window_slice(df, start=cutoff, end=end_date), label="recent_3y", periods_per_year=periods_per_year)
    return {
        "config": asdict(cfg),
        "config_id": _config_id(cfg),
        "full_window": full,
        "recent_3y": recent,
        "compare_key": _compare_key(full, recent),
    }


def _seed_research_env(payload: dict[str, Any]) -> None:
    defaults = {
        "LT_START_DATE": str(payload.get("start_date") or "2016-03-01"),
        "LT_END_DATE": str(payload.get("end_date") or "2026-03-01"),
        "LT_BENCHMARK": str(payload.get("benchmark") or "QQQ"),
        "LT_VIX": "^VIX",
        "LT_TRADE_COST_BPS": str(payload.get("trade_cost_bps") or DEFAULT_ENV["LT_TRADE_COST_BPS"]),
    }
    for key, value in defaults.items():
        os.environ.setdefault(key, value)


def main() -> None:
    _apply_defaults()
    best_json = Path(os.getenv("LT_BEST_JSON", DEFAULT_ENV["LT_BEST_JSON"]))
    if not best_json.is_absolute():
        best_json = ROOT / best_json
    payload = _load_best_payload(best_json)
    best = payload.get("best") or {}
    best_cfg = best.get("config") or {}
    if not best_cfg:
        raise RuntimeError(f"Missing best config in {best_json}")
    _seed_research_env(payload)

    run_tag = os.environ["LT_SWEEP_TAG"]
    recent_years = int(float(os.getenv("LT_RECENT_YEARS", DEFAULT_ENV["LT_RECENT_YEARS"])))
    research = _load_module(RESEARCH_SCRIPT, f"research_weekly_levered_trend_sweep_{run_tag}")

    configs = _candidate_configs(best_cfg)
    trend_configs = [SweepConfig(**asdict(cfg)) for cfg in configs]
    base_cfg = SweepConfig(
        risk=str(best_cfg.get("risk") or "QLD"),
        safe=str(best_cfg.get("safe") or "GLD"),
        ma_window=int(best_cfg.get("ma_window") or 125),
        qqq_confirm=bool(best_cfg.get("qqq_confirm", False)),
        vix_max=float(best_cfg.get("vix_max") or 0.0),
    )

    symbols = sorted({research.BENCH, research.VIX, base_cfg.risk, base_cfg.safe})
    frames = research._download(symbols)
    snaps = research._snapshots()
    if research.BENCH not in frames:
        raise RuntimeError(f"Missing benchmark frame: {research.BENCH}")

    results: list[dict[str, Any]] = []
    for cfg in trend_configs:
        evaluated = _evaluate_config(
            research,
            cfg,
            frames,
            snaps,
            periods_per_year=52,
            recent_years=recent_years,
            end_date=str(os.environ["LT_END_DATE"]),
        )
        results.append(evaluated)

    full_sorted = sorted(results, key=lambda row: (
        1 if float(row["full_window"]["alpha"]["cagr_diff_pctp"]) > 0.0 else 0,
        1 if float(row["full_window"]["alpha"]["mdd_diff_pctp"]) > 0.0 else 0,
        float(row["full_window"]["alpha"]["cagr_diff_pctp"]),
        float(row["full_window"]["alpha"]["mdd_diff_pctp"]),
        float(row["full_window"]["alpha"]["nw_p_gt0"]),
    ), reverse=True)
    recent_sorted = sorted(results, key=lambda row: (
        1 if float(row["recent_3y"]["alpha"]["cagr_diff_pctp"]) > 0.0 else 0,
        1 if float(row["recent_3y"]["alpha"]["mdd_diff_pctp"]) > 0.0 else 0,
        float(row["recent_3y"]["alpha"]["cagr_diff_pctp"]),
        float(row["recent_3y"]["alpha"]["mdd_diff_pctp"]),
        float(row["recent_3y"]["alpha"]["nw_p_gt0"]),
    ), reverse=True)
    robust_sorted = sorted(results, key=lambda row: row["compare_key"], reverse=True)

    ranked_results: list[dict[str, Any]] = []
    full_rank = {row["config_id"]: idx + 1 for idx, row in enumerate(full_sorted)}
    recent_rank = {row["config_id"]: idx + 1 for idx, row in enumerate(recent_sorted)}
    robust_rank = {row["config_id"]: idx + 1 for idx, row in enumerate(robust_sorted)}
    for row in robust_sorted:
        flat = {
            "config_id": row["config_id"],
            **row["config"],
            **_flatten_window("full", row["full_window"]),
            **_flatten_window("recent_3y", row["recent_3y"]),
            "rank": int(robust_rank[row["config_id"]]),
            "full_rank": int(full_rank[row["config_id"]]),
            "recent_3y_rank": int(recent_rank[row["config_id"]]),
            "robust_rank": int(robust_rank[row["config_id"]]),
            "robust_score": float(row["compare_key"][1]),
            "stability_score": float(row["compare_key"][2]),
        }
        ranked_results.append(flat)

    summary = {
        "run_tag": run_tag,
        "inputs": {
            "best_json": str(best_json),
            "start_date": str(os.environ["LT_START_DATE"]),
            "end_date": str(os.environ["LT_END_DATE"]),
            "benchmark": str(os.environ["LT_BENCHMARK"]),
            "recent_years": int(recent_years),
            "trade_cost_bps": float(os.environ["LT_TRADE_COST_BPS"]),
        },
        "base_config": asdict(base_cfg),
        "base_config_id": _config_id(base_cfg),
        "sweep_size": int(len(results)),
        "results": ranked_results,
        "best": {
            "full_window": full_sorted[0],
            "recent_3y": recent_sorted[0],
            "robust": robust_sorted[0],
        },
        "rankings": {
            "full_window": [row["config_id"] for row in full_sorted],
            "recent_3y": [row["config_id"] for row in recent_sorted],
            "robust": [row["config_id"] for row in robust_sorted],
        },
    }

    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    out_json = RUNS_DIR / f"{run_tag}.json"
    out_csv = RUNS_DIR / f"{run_tag}.csv"
    out_md = RUNS_DIR / f"{run_tag}.md"
    out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame(ranked_results).to_csv(out_csv, index=False)
    out_md.write_text(_render_markdown(summary), encoding="utf-8")

    print(f"Saved: {out_json.relative_to(ROOT)}")
    print(f"Saved: {out_csv.relative_to(ROOT)}")
    print(f"Saved: {out_md.relative_to(ROOT)}")
    print(
        "Best -> "
        f"full={summary['best']['full_window']['config_id']} "
        f"recent_3y={summary['best']['recent_3y']['config_id']} "
        f"robust={summary['best']['robust']['config_id']}"
    )


if __name__ == "__main__":
    main()
