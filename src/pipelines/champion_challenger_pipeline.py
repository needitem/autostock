"""Run the weekly stock-momentum champion/challenger validation workflow."""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
RUNS_DIR = DATA_DIR / "runs"
SCRIPTS_DIR = ROOT_DIR / "scripts"

RUNNER_SCRIPT = SCRIPTS_DIR / "run_strategy_v4_stock_momentum.py"
VERIFY_SCRIPT = SCRIPTS_DIR / "verify_ai_portfolio_backtest.py"
WALKFORWARD_SCRIPT = SCRIPTS_DIR / "walkforward_stock_momentum.py"
RESEARCH_SCRIPT = SCRIPTS_DIR / "research_stock_hypotheses.py"

BASELINE_SUMMARY_JSON = DATA_DIR / "ai_portfolio_backtest_summary.json"
BASELINE_RESULTS_CSV = DATA_DIR / "ai_portfolio_backtest_results.csv"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def _load_script_module(path: Path, name: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _run_python_script(script_path: Path, extra_env: dict[str, str] | None = None) -> dict[str, Any]:
    env = os.environ.copy()
    if extra_env:
        env.update({k: str(v) for k, v in extra_env.items()})
    proc = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(ROOT_DIR),
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    result = {
        "script": str(script_path),
        "returncode": int(proc.returncode),
        "stdout": str(proc.stdout or "").strip(),
        "stderr": str(proc.stderr or "").strip(),
    }
    if proc.returncode != 0:
        detail = result["stderr"] or result["stdout"] or f"exit={proc.returncode}"
        raise RuntimeError(f"{script_path.name} failed: {detail}")
    return result


def _copy_required(src: Path, dst: Path) -> str:
    if not src.exists():
        raise FileNotFoundError(f"Missing expected artifact: {src}")
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    return str(dst)


def _flatten_result_row(row: dict[str, Any]) -> dict[str, Any]:
    full = row.get("full") or {}
    oos = row.get("oos") or {}
    config = row.get("config") or {}
    return {
        "name": row.get("name"),
        "max_per_sector": config.get("max_per_sector"),
        "min_overlap": config.get("min_overlap"),
        "weight_mode": config.get("weight_mode"),
        "full_cagr_diff_pct": full.get("cagr_diff_pct"),
        "full_mdd_diff_pct": full.get("mdd_diff_pct"),
        "full_avg_turnover": full.get("avg_turnover"),
        "oos_cagr_diff_pct": oos.get("cagr_diff_pct"),
        "oos_mdd_diff_pct": oos.get("mdd_diff_pct"),
        "oos_sharpe_diff": float(oos.get("sharpe", 0.0)) - float(oos.get("benchmark_sharpe", 0.0)),
        "oos_avg_turnover": oos.get("avg_turnover"),
        "oos_nw_p_two": oos.get("nw_p_two"),
    }


def _ranking_key(row: dict[str, Any]) -> tuple[Any, ...]:
    oos = row.get("oos") or {}
    cagr_diff = float(oos.get("cagr_diff_pct", -999.0))
    mdd_diff = float(oos.get("mdd_diff_pct", -999.0))
    turnover = float(oos.get("avg_turnover", 999.0))
    sharpe_diff = float(oos.get("sharpe", 0.0)) - float(oos.get("benchmark_sharpe", 0.0))
    return (
        1 if cagr_diff > 0 else 0,
        1 if mdd_diff >= 0 else 0,
        cagr_diff,
        mdd_diff,
        sharpe_diff,
        -turnover,
    )


def _build_sensitivity_suite(champion: Any) -> list[Any]:
    return [
        champion,
        replace(champion, name=f"{champion.name}__sector_free", max_per_sector=0),
        replace(champion, name=f"{champion.name}__sector_tight", max_per_sector=1),
        replace(champion, name=f"{champion.name}__sector_loose", max_per_sector=3),
        replace(champion, name=f"{champion.name}__turnover_loose", min_overlap=3),
        replace(champion, name=f"{champion.name}__turnover_tight", min_overlap=5),
        replace(
            champion,
            name=f"{champion.name}__sector_tight_turnover_tight",
            max_per_sector=1,
            min_overlap=5,
        ),
        replace(
            champion,
            name=f"{champion.name}__sector_free_turnover_loose",
            max_per_sector=0,
            min_overlap=3,
        ),
        replace(
            champion,
            name="weekly_veto_recentq_newonly_nrisk_soft_bonus",
            pit_bonus=0.05,
            pit_veto_threshold=-4.0,
            pit_veto_max_filing_age=180,
            pit_veto_new_only=True,
            pit_veto_regimes=("neutral", "risk_off"),
        ),
        replace(
            champion,
            name="weekly_veto_recentq_newonly_neutral_soft_bonus_ro2",
            top_k_risk_off=2,
            pit_bonus=0.05,
            pit_veto_threshold=-4.0,
            pit_veto_max_filing_age=180,
            pit_veto_new_only=True,
            pit_veto_regimes=("neutral",),
        ),
        replace(
            champion,
            name="weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto35",
            top_k_risk_off=2,
            pit_bonus=0.05,
            pit_veto_threshold=-3.5,
            pit_veto_max_filing_age=180,
            pit_veto_new_only=True,
            pit_veto_regimes=("neutral",),
        ),
        replace(
            champion,
            name="weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto325",
            top_k_risk_off=2,
            pit_bonus=0.05,
            pit_veto_threshold=-3.25,
            pit_veto_max_filing_age=180,
            pit_veto_new_only=True,
            pit_veto_regimes=("neutral",),
        ),
        replace(
            champion,
            name="weekly_veto_recentq_newonly_nrisk_soft_bonus_ro2",
            top_k_risk_off=2,
            pit_bonus=0.05,
            pit_veto_threshold=-4.0,
            pit_veto_max_filing_age=180,
            pit_veto_new_only=True,
            pit_veto_regimes=("neutral", "risk_off"),
        ),
    ]


def _champion_from_research_module(research_module: Any, champion_name: str) -> Any:
    for hypothesis in research_module._hypotheses():
        if str(getattr(hypothesis, "name", "")) == champion_name:
            return hypothesis
    raise ValueError(f"Champion hypothesis not found: {champion_name}")


def _evaluate_sensitivity_suite(
    research_module: Any,
    champion_name: str,
    oos_start_year: int,
) -> dict[str, Any]:
    champion = _champion_from_research_module(research_module, champion_name)
    bt = research_module._load_bt_module(champion.freq)
    records, meta = research_module._build_records(bt)
    if not records:
        raise RuntimeError("No records available for champion/challenger evaluation")

    periods_per_year = int(bt._periods_per_year(champion.freq))
    oos_start_year = int(oos_start_year)
    rows: list[dict[str, Any]] = []
    suite = _build_sensitivity_suite(champion)
    for hypothesis in suite:
        df = research_module._evaluate(bt, records, hypothesis)
        full = research_module._summarize(df, periods_per_year)
        mask = pd.to_datetime(df["entry_day"], errors="coerce").dt.year >= oos_start_year
        df_oos = df.loc[mask].copy()
        oos = research_module._summarize(df_oos, periods_per_year) if not df_oos.empty else {}
        rows.append(
            {
                "name": hypothesis.name,
                "config": asdict(hypothesis),
                "full": full,
                "oos": oos,
            }
        )

    ranked = sorted(rows, key=_ranking_key, reverse=True)
    champion_row = next((row for row in ranked if row["name"] == champion_name), None)
    best_row = ranked[0] if ranked else {}
    challenger_rows = [row for row in ranked if row["name"] != champion_name]
    better_challengers = [row for row in challenger_rows if _ranking_key(row) > _ranking_key(champion_row or {})]

    return {
        "champion": champion,
        "ranked_results": ranked,
        "champion_result": champion_row or {},
        "best_result": best_row,
        "better_challengers": better_challengers,
        "records_meta": meta,
    }


def _write_sensitivity_csv(path: Path, rows: list[dict[str, Any]]) -> str:
    flat_rows = [_flatten_result_row(row) for row in rows]
    pd.DataFrame(flat_rows).to_csv(path, index=False)
    return str(path)


def _build_markdown_report(summary: dict[str, Any], sensitivity_csv_path: Path) -> str:
    decision = summary.get("decision") or {}
    champion = summary.get("champion") or {}
    champion_oos = champion.get("oos") or {}
    wf = summary.get("annual_rollforward") or {}
    wf_static = wf.get("oos_static_baseline") or {}
    best = summary.get("best_result") or {}
    best_oos = best.get("oos") or {}
    table = pd.DataFrame([_flatten_result_row(row) for row in summary.get("sensitivity_ranking") or []])
    if not table.empty:
        table = table[
            [
                "name",
                "max_per_sector",
                "min_overlap",
                "oos_cagr_diff_pct",
                "oos_mdd_diff_pct",
                "oos_sharpe_diff",
                "oos_avg_turnover",
                "oos_nw_p_two",
            ]
        ]

    lines = [
        "# Champion/Challenger Validation",
        "",
        f"- champion: `{summary.get('champion_name', '')}`",
        f"- fixed OOS start year: `{summary.get('inputs', {}).get('fixed_oos_start_year', '')}`",
        f"- sensitivity grid: **{len(summary.get('sensitivity_ranking') or [])}** variants",
        "",
        "## Decision",
        "",
        f"- champion retained: **{decision.get('champion_retained', False)}**",
        f"- best candidate: `{best.get('name', '')}`",
        f"- reason: {decision.get('reason', '')}",
        "",
        "## Champion Fixed OOS",
        "",
        (
            f"- CAGR diff {float(champion_oos.get('cagr_diff_pct', 0.0)):+.2f}pp "
            f"| Sharpe diff {float(champion_oos.get('sharpe', 0.0)) - float(champion_oos.get('benchmark_sharpe', 0.0)):+.3f} "
            f"| MDD diff {float(champion_oos.get('mdd_diff_pct', 0.0)):+.2f}pp "
            f"| turnover {float(champion_oos.get('avg_turnover', 0.0)):.3f}"
        ),
        "",
        "## Annual Roll-Forward",
        "",
        (
            f"- static baseline CAGR diff {float(wf_static.get('cagr_diff_pct', 0.0)):+.2f}pp "
            f"| Sharpe diff {float(wf_static.get('sharpe', 0.0)) - float(wf_static.get('benchmark_sharpe', 0.0)):+.3f} "
            f"| MDD diff {float(wf_static.get('mdd_diff_pct', 0.0)):+.2f}pp "
            f"| turnover {float(wf_static.get('avg_turnover', 0.0)):.3f}"
        ),
        (
            f"- selected config CAGR diff {float((wf.get('oos_selected') or {}).get('cagr_diff_pct', 0.0)):+.2f}pp "
            f"| fold count **{len(wf.get('folds') or [])}**"
        ),
        "",
        "## Best Challenger",
        "",
        (
            f"- `{best.get('name', '')}` | OOS CAGR diff {float(best_oos.get('cagr_diff_pct', 0.0)):+.2f}pp "
            f"| OOS MDD diff {float(best_oos.get('mdd_diff_pct', 0.0)):+.2f}pp "
            f"| OOS turnover {float(best_oos.get('avg_turnover', 0.0)):.3f}"
        ),
        "",
        "## Sensitivity Table",
        "",
        f"- csv: `{sensitivity_csv_path.relative_to(ROOT_DIR)}`",
        "",
    ]
    if not table.empty:
        lines.append("```text")
        lines.append(table.to_string(index=False, float_format=lambda x: f"{float(x):.3f}"))
        lines.append("```")
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def run_champion_challenger_pipeline(run_verify: bool = True) -> dict[str, Any]:
    RUNS_DIR.mkdir(parents=True, exist_ok=True)

    run_tag = (os.getenv("CCP_RUN_TAG") or "").strip() or "champion_challenger"
    champion_name = (os.getenv("CCP_CHAMPION") or "weekly_baseline_v4").strip()
    fixed_oos_start_year = int(float(os.getenv("CCP_FIXED_OOS_START_YEAR") or "2016"))

    baseline_tag = f"{run_tag}_baseline"
    wf_tag = f"{run_tag}_walkforward"

    baseline_run = _run_python_script(RUNNER_SCRIPT, extra_env={"AI_RUN_TAG": baseline_tag})
    baseline_summary_path = RUNS_DIR / f"champion_backtest_summary_{run_tag}.json"
    baseline_results_path = RUNS_DIR / f"champion_backtest_results_{run_tag}.csv"
    _copy_required(BASELINE_SUMMARY_JSON, baseline_summary_path)
    _copy_required(BASELINE_RESULTS_CSV, baseline_results_path)

    verify_run: dict[str, Any] | None = None
    verify_json_path = RUNS_DIR / f"champion_backtest_verification_{run_tag}.json"
    verify_md_path = RUNS_DIR / f"champion_backtest_verification_{run_tag}.md"
    if run_verify:
        verify_run = _run_python_script(
            VERIFY_SCRIPT,
            extra_env={
                "AI_PORTFOLIO_RESULTS_CSV": str(baseline_results_path),
                "AI_PORTFOLIO_SUMMARY_JSON": str(baseline_summary_path),
                "AI_PORTFOLIO_VERIFY_JSON": str(verify_json_path),
                "AI_PORTFOLIO_VERIFY_MD": str(verify_md_path),
            },
        )

    walkforward_run = _run_python_script(WALKFORWARD_SCRIPT, extra_env={"WF_RUN_TAG": wf_tag})
    walkforward_summary_path = RUNS_DIR / f"walkforward_stock_momentum_summary_{wf_tag}.json"
    walkforward_summary = _read_json(walkforward_summary_path)

    research_module = _load_script_module(RESEARCH_SCRIPT, f"research_stock_hypotheses_{run_tag}")
    sensitivity = _evaluate_sensitivity_suite(
        research_module=research_module,
        champion_name=champion_name,
        oos_start_year=fixed_oos_start_year,
    )
    champion = sensitivity["champion"]
    ranked_results = sensitivity["ranked_results"]
    champion_result = sensitivity["champion_result"]
    best_result = sensitivity["best_result"]
    better_challengers = sensitivity["better_challengers"]

    sensitivity_csv_path = RUNS_DIR / f"champion_challenger_sensitivity_{run_tag}.csv"
    _write_sensitivity_csv(sensitivity_csv_path, ranked_results)

    decision = {
        "champion_retained": bool(best_result.get("name") == champion_name),
        "best_candidate_name": best_result.get("name"),
        "better_challenger_count": int(len(better_challengers)),
        "reason": (
            "No challenger beat the frozen champion on fixed OOS ranking."
            if best_result.get("name") == champion_name
            else "A challenger outranked the frozen champion on fixed OOS ranking."
        ),
    }

    summary = {
        "run_tag": run_tag,
        "inputs": {
            "champion": champion_name,
            "fixed_oos_start_year": int(fixed_oos_start_year),
            "hypothesis_start_date": (os.getenv("HYP_START_DATE") or "").strip(),
            "hypothesis_end_date": (os.getenv("HYP_END_DATE") or "").strip(),
            "walkforward_start_date": (os.getenv("WF_START_DATE") or "").strip(),
            "walkforward_end_date": (os.getenv("WF_END_DATE") or "").strip(),
        },
        "champion_name": champion_name,
        "champion_config": asdict(champion),
        "champion": champion_result,
        "best_result": best_result,
        "decision": decision,
        "annual_rollforward": walkforward_summary,
        "records_meta": sensitivity["records_meta"],
        "sensitivity_ranking": ranked_results,
        "baseline_run": baseline_run,
        "verification_run": verify_run,
        "walkforward_run": walkforward_run,
        "paths": {
            "baseline_summary_json": str(baseline_summary_path),
            "baseline_results_csv": str(baseline_results_path),
            "verification_json": str(verify_json_path) if run_verify else "",
            "verification_md": str(verify_md_path) if run_verify else "",
            "walkforward_summary_json": str(walkforward_summary_path),
            "sensitivity_csv": str(sensitivity_csv_path),
        },
    }

    if baseline_summary_path.exists():
        summary["baseline_summary"] = _read_json(baseline_summary_path)
    if run_verify and verify_json_path.exists():
        summary["baseline_verification"] = _read_json(verify_json_path)

    summary_json_path = RUNS_DIR / f"champion_challenger_summary_{run_tag}.json"
    summary_md_path = RUNS_DIR / f"champion_challenger_summary_{run_tag}.md"
    summary_json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    summary_md_path.write_text(_build_markdown_report(summary, sensitivity_csv_path), encoding="utf-8")

    return {
        "run_tag": run_tag,
        "summary_json_path": str(summary_json_path),
        "summary_md_path": str(summary_md_path),
        "sensitivity_csv_path": str(sensitivity_csv_path),
        "champion_retained": bool(decision["champion_retained"]),
        "best_candidate_name": str(decision["best_candidate_name"] or ""),
    }
