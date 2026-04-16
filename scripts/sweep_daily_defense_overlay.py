from __future__ import annotations

import itertools
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from run_strategy_v4_stock_momentum import DEFAULT_ENV


ROOT = Path(__file__).resolve().parents[1]
RUNS_DIR = ROOT / "data" / "runs"
SELECTOR = ROOT / "scripts" / "backtest_ai_portfolio_selector.py"


def _parse_float_grid(raw: str, default: list[float]) -> list[float]:
    items = [part.strip() for part in str(raw).split(",") if part.strip()]
    if not items:
        return list(default)
    return [float(item) for item in items]


def _slug_num(value: float) -> str:
    text = f"{float(value):+.2f}".replace("+", "").replace("-", "m").replace(".", "p")
    return text


def _run_case(run_tag: str, env_overrides: dict[str, str]) -> dict[str, Any]:
    env = os.environ.copy()
    env.update(DEFAULT_ENV)
    env.update(env_overrides)
    env["AI_RUN_TAG"] = run_tag
    proc = subprocess.run(
        [sys.executable, str(SELECTOR)],
        cwd=str(ROOT),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr or proc.stdout or f"selector failed: {run_tag}")

    summary_path = RUNS_DIR / f"ai_portfolio_backtest_summary_{run_tag}.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    metrics = summary.get("portfolio_metrics", {})
    ai = metrics.get("ai_portfolio", {})
    benchmark = metrics.get("benchmark", {})
    return {
        "run_tag": run_tag,
        "summary_json": str(summary_path.relative_to(ROOT)),
        "cagr_pct": float(ai.get("cagr_pct", 0.0)),
        "benchmark_cagr_pct": float(benchmark.get("cagr_pct", 0.0)),
        "cagr_diff_pct": float(ai.get("cagr_pct", 0.0)) - float(benchmark.get("cagr_pct", 0.0)),
        "sharpe": float(ai.get("sharpe", 0.0)),
        "mdd_pct": float(ai.get("max_drawdown_pct", 0.0)),
        "benchmark_mdd_pct": float(benchmark.get("max_drawdown_pct", 0.0)),
        "mdd_diff_pct": float(ai.get("max_drawdown_pct", 0.0)) - float(benchmark.get("max_drawdown_pct", 0.0)),
        "avg_turnover": float(summary.get("avg_turnover", 0.0)),
        "avg_daily_defense_exposure_pct": float(summary.get("avg_daily_defense_exposure_pct", 100.0)),
        "daily_defense_soft_segments_total": int(summary.get("daily_defense_soft_segments_total", 0)),
        "daily_defense_hard_segments_total": int(summary.get("daily_defense_hard_segments_total", 0)),
    }


def _ranking_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        float(row.get("sharpe", 0.0)),
        float(row.get("mdd_pct", -999.0)),
        float(row.get("cagr_pct", 0.0)),
        -float(row.get("avg_turnover", 999.0)),
    )


def main() -> None:
    run_tag = (
        os.getenv("DAILY_DEFENSE_SWEEP_TAG")
        or f"daily_defense_sweep_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    ).strip()
    start_date = (os.getenv("DAILY_DEFENSE_SWEEP_START_DATE") or DEFAULT_ENV["AI_START_DATE"]).strip()
    end_date = (os.getenv("DAILY_DEFENSE_SWEEP_END_DATE") or DEFAULT_ENV["AI_END_DATE"]).strip()

    soft_grid = _parse_float_grid(os.getenv("DAILY_DEFENSE_SOFT_GRID", ""), [80.0, 85.0, 90.0])
    hard_grid = _parse_float_grid(os.getenv("DAILY_DEFENSE_HARD_GRID", ""), [35.0, 40.0, 50.0])
    vix_soft_grid = _parse_float_grid(os.getenv("DAILY_DEFENSE_VIX_SOFT_GRID", ""), [24.0, 26.0, 28.0])
    vix_hard_grid = _parse_float_grid(os.getenv("DAILY_DEFENSE_VIX_HARD_GRID", ""), [30.0, 32.0, 34.0])
    return21_grid = _parse_float_grid(os.getenv("DAILY_DEFENSE_RETURN21_GRID", ""), [-2.0, -4.0, -6.0])

    rows: list[dict[str, Any]] = []
    for soft, hard, vix_soft, vix_hard, ret21 in itertools.product(
        soft_grid,
        hard_grid,
        vix_soft_grid,
        vix_hard_grid,
        return21_grid,
    ):
        if hard > soft or vix_hard < vix_soft:
            continue
        case_tag = f"{run_tag}__s{_slug_num(soft)}__h{_slug_num(hard)}__vs{_slug_num(vix_soft)}__vh{_slug_num(vix_hard)}__r{_slug_num(ret21)}"
        result = _run_case(
            case_tag,
            {
                "AI_START_DATE": start_date,
                "AI_END_DATE": end_date,
                "AI_DAILY_DEFENSE_OVERLAY": "1",
                "AI_DAILY_DEFENSE_SOFT_EXPOSURE_PCT": str(soft),
                "AI_DAILY_DEFENSE_HARD_EXPOSURE_PCT": str(hard),
                "AI_DAILY_DEFENSE_VIX_SOFT": str(vix_soft),
                "AI_DAILY_DEFENSE_VIX_HARD": str(vix_hard),
                "AI_DAILY_DEFENSE_RETURN21_SOFT": str(ret21),
                "AI_SKIP_LATEST_WRITE": "1",
            },
        )
        result.update(
            {
                "soft_exposure_pct": soft,
                "hard_exposure_pct": hard,
                "vix_soft": vix_soft,
                "vix_hard": vix_hard,
                "return21_soft": ret21,
            }
        )
        rows.append(result)
        print(
            f"{case_tag}: CAGR {result['cagr_pct']:.2f} | Sharpe {result['sharpe']:.2f} | MDD {result['mdd_pct']:.2f}"
        )

    rows.sort(key=_ranking_key, reverse=True)
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = RUNS_DIR / f"{run_tag}.csv"
    json_path = RUNS_DIR / f"{run_tag}.json"
    pd.DataFrame(rows).to_csv(csv_path, index=False)
    json_path.write_text(json.dumps({"run_tag": run_tag, "results": rows}, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Saved: {csv_path.relative_to(ROOT)}")
    print(f"Saved: {json_path.relative_to(ROOT)}")
    if rows:
        best = rows[0]
        print(
            "Best -> "
            f"soft={best['soft_exposure_pct']} hard={best['hard_exposure_pct']} "
            f"vix_soft={best['vix_soft']} vix_hard={best['vix_hard']} ret21={best['return21_soft']} "
            f"| CAGR {best['cagr_pct']:.2f} | Sharpe {best['sharpe']:.2f} | MDD {best['mdd_pct']:.2f}"
        )


if __name__ == "__main__":
    main()
