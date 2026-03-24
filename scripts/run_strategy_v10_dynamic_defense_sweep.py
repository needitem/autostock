from __future__ import annotations

import importlib.util
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import product
from pathlib import Path
from typing import Any

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from strategy_sweep_utils import (
    ROOT,
    RUNS_DIR,
    load_module,
    metric as _promotion_metric,
    run_backtest_verify_promotion,
    slug as _slug,
)
V10_RUNNER = ROOT / "scripts" / "run_strategy_v10_regime_dynamic_defense.py"


@dataclass(frozen=True)
class SweepVariant:
    neutral: str
    filter_safe: str
    hysteresis: float
    crash_dynamic: bool

def _variant_id(variant: SweepVariant) -> str:
    hysteresis = str(float(variant.hysteresis)).replace(".", "p")
    return (
        f"neutral_{_slug(variant.neutral)}__"
        f"filter_{_slug(variant.filter_safe)}__"
        f"h{hysteresis}__"
        f"crashdyn_{int(bool(variant.crash_dynamic))}"
    )


def _variants() -> list[SweepVariant]:
    out = [
        SweepVariant(
            neutral=str(neutral),
            filter_safe=str(filter_safe),
            hysteresis=float(hysteresis),
            crash_dynamic=bool(crash_dynamic),
        )
        for neutral, filter_safe, hysteresis, crash_dynamic in product(
            ("QQQ", "QLD"),
            ("QQQ", "QLD"),
            (0.0, 0.01),
            (False, True),
        )
    ]
    return out

def _ranking_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        int(row.get("criteria_pass_count", 0)),
        int(bool(row.get("horizon_3y_pass", False))),
        int(bool(row.get("drawdown_guardrail_pass", False))),
        float(row.get("full_p_alpha_gt0", 0.0)),
        float(row.get("full_cagr_diff_pct", -999.0)),
        -float(row.get("turnover_mean", 999.0)),
        float(row.get("horizon_3y_cagr_diff_pct", -999.0)),
        -float(row.get("horizon_3y_nw_p_two", 1.0)),
    )


def _build_markdown(summary: dict[str, Any], csv_rel: str) -> str:
    lines = [
        "# V10 Dynamic Defense Sweep",
        "",
        f"- variants: **{len(summary.get('results') or [])}**",
        f"- csv: `{csv_rel}`",
        "",
        "## Ranking",
        "",
    ]
    for row in summary.get("ranking") or []:
        lines.append(
            f"- `{row['variant_id']}` | pass {int(row.get('criteria_pass_count', 0))}/7 "
            f"| full diff {float(row.get('full_cagr_diff_pct', 0.0)):+.2f}pp "
            f"| p(alpha>0) {float(row.get('full_p_alpha_gt0', 0.0)):.3f} "
            f"| turnover {float(row.get('turnover_mean', 0.0)):.3f}"
        )
    lines.append("")
    return "\n".join(lines).strip() + "\n"


def main() -> None:
    run_tag = (
        os.getenv("V10_SWEEP_TAG")
        or f"strategy_v10_dynamic_defense_sweep_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    ).strip()
    base_runner = _load_module(V10_RUNNER, f"run_strategy_v10_dynamic_defense_sweep_{run_tag}")
    variants = _variants()
    rows: list[dict[str, Any]] = []

    for variant in variants:
        variant_id = _variant_id(variant)
        ai_run_tag = f"strategy_v10_sweep_{variant_id}_{run_tag}"
        verify_json = RUNS_DIR / f"ai_portfolio_backtest_verification_{ai_run_tag}.json"
        verify_md = RUNS_DIR / f"ai_portfolio_backtest_verification_{ai_run_tag}.md"
        results_csv = RUNS_DIR / f"ai_portfolio_backtest_results_{ai_run_tag}.csv"
        summary_json = RUNS_DIR / f"ai_portfolio_backtest_summary_{ai_run_tag}.json"
        promotion_tag = f"promotion_check_{ai_run_tag}"

        env_updates = dict(base_runner.DEFAULT_ENV)
        env_updates.update(
            {
                "AI_RUN_TAG": ai_run_tag,
                "AI_SKIP_LATEST_WRITE": "1",
                "AI_REGIME_NEUTRAL": str(variant.neutral),
                "AI_REGIME_FILTER_SAFE": str(variant.filter_safe),
                "AI_REGIME_HYSTERESIS": str(float(variant.hysteresis)),
                "AI_REGIME_CRASH_DYNAMIC": "1" if bool(variant.crash_dynamic) else "0",
            }
        )
        if not bool(variant.crash_dynamic):
            env_updates["AI_REGIME_CRASH"] = "BIL"
            env_updates["AI_REGIME_CRASH_FALLBACK"] = "BIL"

        _, report = run_backtest_verify_promotion(
            runner_script=V10_RUNNER,
            env_updates=env_updates,
            ai_run_tag=ai_run_tag,
            promotion_tag=promotion_tag,
        )
        promotion_json = RUNS_DIR / f"{promotion_tag}.json"
        criteria = {str(item.get("name")): bool(item.get("passes", False)) for item in report.get("criteria") or []}
        detail_by_name = {
            str(item.get("name")): str(item.get("detail", "")) for item in report.get("criteria") or []
        }
        rows.append(
            {
                "variant_id": variant_id,
                "neutral": variant.neutral,
                "filter_safe": variant.filter_safe,
                "hysteresis": float(variant.hysteresis),
                "crash_dynamic": bool(variant.crash_dynamic),
                "criteria_pass_count": int(sum(1 for passed in criteria.values() if passed)),
                "overall_pass": bool(report.get("overall_pass", False)),
                "full_cagr_diff_pct": _promotion_metric(report, "headline", "strategy_cagr_pct")
                - _promotion_metric(report, "headline", "benchmark_cagr_pct"),
                "full_p_alpha_gt0": _promotion_metric(report, "headline", "p_alpha_gt0"),
                "turnover_mean": _promotion_metric(report, "headline", "turnover_mean"),
                "drawdown_guardrail_pass": bool(criteria.get("drawdown_guardrail", False)),
                "horizon_3y_pass": bool(criteria.get("horizon_3y", False)),
                "horizon_3y_cagr_diff_pct": float(
                    detail_by_name.get("horizon_3y", "cagr_diff=+0.00pp").split("cagr_diff=")[1].split("pp")[0]
                ),
                "horizon_3y_nw_p_two": float(detail_by_name.get("horizon_3y", "nw_p2=1.000").split("nw_p2=")[1]),
                "paths_results_csv": str(results_csv.relative_to(ROOT)),
                "paths_summary_json": str(summary_json.relative_to(ROOT)),
                "paths_verify_json": str(verify_json.relative_to(ROOT)),
                "paths_promotion_json": str(promotion_json.relative_to(ROOT)),
            }
        )

    rows.sort(key=_ranking_key, reverse=True)
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    json_path = RUNS_DIR / f"{run_tag}.json"
    csv_path = RUNS_DIR / f"{run_tag}.csv"
    md_path = RUNS_DIR / f"{run_tag}.md"
    summary = {
        "run_tag": run_tag,
        "results": rows,
        "ranking": rows,
    }
    pd.DataFrame(rows).to_csv(csv_path, index=False)
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_build_markdown(summary, str(csv_path.relative_to(ROOT))), encoding="utf-8")

    print(f"Saved: {json_path.relative_to(ROOT)}")
    print(f"Saved: {csv_path.relative_to(ROOT)}")
    print(f"Saved: {md_path.relative_to(ROOT)}")
    if rows:
        best = rows[0]
        print(
            "Best V10 sweep variant -> "
            f"{best['variant_id']} | criteria_passed={best['criteria_pass_count']} "
            f"| full_diff={float(best['full_cagr_diff_pct']):+.2f}pp "
            f"| turnover={float(best['turnover_mean']):.3f}"
        )


if __name__ == "__main__":
    main()
