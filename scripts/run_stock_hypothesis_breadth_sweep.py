from __future__ import annotations

import importlib.util
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import product
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
RUNS_DIR = ROOT / "data" / "runs"
SELECTOR_SCRIPT = ROOT / "scripts" / "backtest_ai_portfolio_selector.py"
EVAL_RUNNER_SCRIPT = ROOT / "scripts" / "run_stock_hypothesis_eval.py"


def _load_module(path: Path, name: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _slug(text: str) -> str:
    clean = "".join(ch if ch.isalnum() else "_" for ch in str(text).strip().lower()).strip("_")
    return clean or "run"


def _parse_list(raw: str, default: list[str]) -> list[str]:
    items = [part.strip() for part in str(raw).split(",") if part.strip()]
    return items or list(default)


def _bool_text(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class SweepProfile:
    name: str
    env: dict[str, str]


@dataclass(frozen=True)
class CellArtifacts:
    hypothesis_name: str
    profile_name: str
    run_tag: str
    results_csv: Path
    summary_json: Path


def _load_eval_runner() -> Any:
    return _load_module(
        EVAL_RUNNER_SCRIPT,
        f"run_stock_hypothesis_eval_breadth_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
    )


def _default_hypothesis_names() -> list[str]:
    return _parse_list(
        os.getenv("STOCK_BREADTH_SWEEP_NAMES", ""),
        [
            "weekly_veto_recentq_newonly_neutral_soft_bonus_ro2",
            "weekly_veto_recentq_newonly_nrisk_soft_bonus_ro2",
        ],
    )


def _build_profiles() -> list[SweepProfile]:
    breadth_sources = _parse_list(os.getenv("STOCK_BREADTH_SOURCE_GRID", ""), ["universe", "safe"])
    regime_modes = _parse_list(os.getenv("STOCK_BREADTH_REGIME_GRID", ""), ["off", "protective"])

    neutral_on_pct = str(os.getenv("STOCK_BREADTH_NEUTRAL_EXPOSURE_PCT", "95"))
    recovery_on_pct = str(os.getenv("STOCK_BREADTH_RECOVERY_EXPOSURE_PCT", neutral_on_pct))
    risk_off_pct = str(os.getenv("STOCK_BREADTH_RISK_OFF_EXPOSURE_PCT", "35"))
    crash_pct = str(os.getenv("STOCK_BREADTH_CRASH_EXPOSURE_PCT", "20"))
    risk_off_vix = str(os.getenv("STOCK_BREADTH_RISK_OFF_VIX", "30"))
    risk_off_vix_hard_pct = str(os.getenv("STOCK_BREADTH_RISK_OFF_VIX_HARD_EXPOSURE_PCT", "25"))
    risk_off_vix_extreme = str(os.getenv("STOCK_BREADTH_RISK_OFF_VIX_EXTREME", "34"))
    risk_off_vix_extreme_pct = str(os.getenv("STOCK_BREADTH_RISK_OFF_VIX_EXTREME_EXPOSURE_PCT", "10"))

    profiles: list[SweepProfile] = []
    for breadth_source, regime_mode in product(breadth_sources, regime_modes):
        env: dict[str, str] = {
            "AI_BREADTH_SOURCE": str(breadth_source),
        }
        if regime_mode == "protective":
            env.update(
                {
                    "AI_REGIME_EXPOSURE": "1",
                    "AI_REGIME_ON_EXPOSURE_PCT": "100",
                    "AI_REGIME_RISK_ON_ALT_EXPOSURE_PCT": "100",
                    "AI_REGIME_NEUTRAL_EXPOSURE_PCT": neutral_on_pct,
                    "AI_REGIME_RECOVERY_EXPOSURE_PCT": recovery_on_pct,
                    "AI_REGIME_RISK_OFF_EXPOSURE_PCT": risk_off_pct,
                    "AI_REGIME_CRASH_EXPOSURE_PCT": crash_pct,
                    "AI_REGIME_RISK_OFF_VIX": risk_off_vix,
                    "AI_REGIME_RISK_OFF_VIX_HARD_EXPOSURE_PCT": risk_off_vix_hard_pct,
                    "AI_REGIME_RISK_OFF_VIX_EXTREME": risk_off_vix_extreme,
                    "AI_REGIME_RISK_OFF_VIX_EXTREME_EXPOSURE_PCT": risk_off_vix_extreme_pct,
                }
            )
        else:
            env["AI_REGIME_EXPOSURE"] = "0"

        profiles.append(
            SweepProfile(
                name=f"breadth_{_slug(breadth_source)}__regime_{_slug(regime_mode)}",
                env=env,
            )
        )

    return profiles


def _cell_artifacts(run_tag: str, hypothesis_name: str, profile_name: str) -> CellArtifacts:
    cell_tag = f"{run_tag}__{_slug(hypothesis_name)}__{_slug(profile_name)}"
    return CellArtifacts(
        hypothesis_name=hypothesis_name,
        profile_name=profile_name,
        run_tag=cell_tag,
        results_csv=RUNS_DIR / f"ai_portfolio_backtest_results_{cell_tag}.csv",
        summary_json=RUNS_DIR / f"ai_portfolio_backtest_summary_{cell_tag}.json",
    )


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _run_selector(eval_runner: Any, extra_env: dict[str, str]) -> None:
    eval_runner._run_python_script(SELECTOR_SCRIPT, extra_env)


def _run_cell(
    eval_runner: Any,
    hypothesis_name: str,
    profile: SweepProfile,
    run_tag: str,
    start_date: str,
    end_date: str,
) -> tuple[CellArtifacts, dict[str, Any], Any]:
    hypothesis = eval_runner._load_hypothesis(hypothesis_name)
    env = eval_runner._env_for_hypothesis(hypothesis)
    env.update(
        {
            "AI_START_DATE": str(start_date),
            "AI_END_DATE": str(end_date),
            "AI_RUN_TAG": f"{run_tag}__{_slug(hypothesis_name)}__{_slug(profile.name)}",
        }
    )
    env.update(profile.env)

    artifacts = _cell_artifacts(run_tag, hypothesis_name, profile.name)
    _run_selector(eval_runner, env)
    summary = _load_json(artifacts.summary_json)
    if not summary:
        raise RuntimeError(f"Missing selector summary: {artifacts.summary_json.relative_to(ROOT)}")
    return artifacts, summary, hypothesis


def _metrics(summary: dict[str, Any]) -> dict[str, float]:
    portfolio_metrics = summary.get("portfolio_metrics") or {}
    ai = portfolio_metrics.get("ai_portfolio") or {}
    bench = portfolio_metrics.get("benchmark") or {}
    return {
        "periods": float(summary.get("periods", 0.0)),
        "cagr_pct": float(ai.get("cagr_pct", 0.0)),
        "benchmark_cagr_pct": float(bench.get("cagr_pct", 0.0)),
        "cagr_diff_pct": float(ai.get("cagr_pct", 0.0)) - float(bench.get("cagr_pct", 0.0)),
        "sharpe": float(ai.get("sharpe", 0.0)),
        "benchmark_sharpe": float(bench.get("sharpe", 0.0)),
        "mdd_pct": float(ai.get("max_drawdown_pct", 0.0)),
        "benchmark_mdd_pct": float(bench.get("max_drawdown_pct", 0.0)),
        "mdd_diff_pct": float(ai.get("max_drawdown_pct", 0.0)) - float(bench.get("max_drawdown_pct", 0.0)),
        "avg_turnover": float(summary.get("avg_turnover", 0.0)),
        "ai_calls": float(summary.get("ai_calls", 0.0)),
        "sit_out_rate_pct": float(summary.get("sit_out_rate_pct", 0.0)),
    }


def _result_row(
    artifacts: CellArtifacts,
    hypothesis: Any,
    profile: SweepProfile,
    summary: dict[str, Any],
) -> dict[str, Any]:
    metrics = _metrics(summary)
    return {
        "hypothesis_name": str(getattr(hypothesis, "name", artifacts.hypothesis_name)),
        "engine": str(getattr(hypothesis, "engine", "")),
        "freq": str(getattr(hypothesis, "freq", "")),
        "top_k": int(getattr(hypothesis, "top_k", 0) or 0),
        "top_k_neutral": int(getattr(hypothesis, "top_k_neutral", 0) or 0),
        "top_k_risk_off": int(getattr(hypothesis, "top_k_risk_off", 0) or 0),
        "pit_bonus": float(getattr(hypothesis, "pit_bonus", 0.0) or 0.0),
        "pit_veto_threshold": float(getattr(hypothesis, "pit_veto_threshold", 0.0) or 0.0),
        "pit_veto_regimes": ",".join(str(x) for x in getattr(hypothesis, "pit_veto_regimes", ()) or ()),
        "profile_name": profile.name,
        "breadth_source": str(profile.env.get("AI_BREADTH_SOURCE", "")),
        "regime_exposure_enabled": _bool_text(profile.env.get("AI_REGIME_EXPOSURE", "0")),
        "regime_neutral_exposure_pct": float(profile.env.get("AI_REGIME_NEUTRAL_EXPOSURE_PCT", 0.0) or 0.0),
        "regime_recovery_exposure_pct": float(profile.env.get("AI_REGIME_RECOVERY_EXPOSURE_PCT", 0.0) or 0.0),
        "regime_risk_off_exposure_pct": float(profile.env.get("AI_REGIME_RISK_OFF_EXPOSURE_PCT", 0.0) or 0.0),
        "regime_crash_exposure_pct": float(profile.env.get("AI_REGIME_CRASH_EXPOSURE_PCT", 0.0) or 0.0),
        "regime_risk_off_vix": float(profile.env.get("AI_REGIME_RISK_OFF_VIX", 0.0) or 0.0),
        "regime_risk_off_vix_hard_exposure_pct": float(profile.env.get("AI_REGIME_RISK_OFF_VIX_HARD_EXPOSURE_PCT", 0.0) or 0.0),
        "regime_risk_off_vix_extreme": float(profile.env.get("AI_REGIME_RISK_OFF_VIX_EXTREME", 0.0) or 0.0),
        "regime_risk_off_vix_extreme_exposure_pct": float(
            profile.env.get("AI_REGIME_RISK_OFF_VIX_EXTREME_EXPOSURE_PCT", 0.0) or 0.0
        ),
        "selector_run_tag": artifacts.run_tag,
        "selector_results_csv": str(artifacts.results_csv.relative_to(ROOT)),
        "selector_summary_json": str(artifacts.summary_json.relative_to(ROOT)),
        "config_hash": str(summary.get("config_hash", "")),
        "breadth_source_mode": str(summary.get("breadth_source_mode", profile.env.get("AI_BREADTH_SOURCE", ""))),
        "regime_exposure_enabled_summary": bool(summary.get("regime_exposure_enabled", _bool_text(profile.env.get("AI_REGIME_EXPOSURE", "0")))),
        **metrics,
    }


def _ranking_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        1 if float(row.get("cagr_diff_pct", -999.0)) > 0 else 0,
        1 if float(row.get("mdd_diff_pct", -999.0)) >= 0 else 0,
        float(row.get("cagr_diff_pct", -999.0)),
        float(row.get("mdd_diff_pct", -999.0)),
        float(row.get("sharpe", 0.0)) - float(row.get("benchmark_sharpe", 0.0)),
        -float(row.get("avg_turnover", 999.0)),
    )


def _build_markdown(summary: dict[str, Any], csv_rel: str) -> str:
    lines = [
        "# Stock Hypothesis Breadth Sweep",
        "",
        f"- hypotheses: **{len(summary.get('inputs', {}).get('hypotheses', []))}**",
        f"- profiles: **{len(summary.get('inputs', {}).get('profiles', []))}**",
        f"- start: `{summary['inputs']['start_date']}`",
        f"- end: `{summary['inputs']['end_date']}`",
        f"- csv: `{csv_rel}`",
        "",
        "## Top Runs",
        "",
    ]
    for row in summary.get("ranking") or []:
        lines.append(
            f"- `{row['hypothesis_name']}` / `{row['profile_name']}` | "
            f"CAGR diff {float(row.get('cagr_diff_pct', 0.0)):+.2f}pp | "
            f"Sharpe {float(row.get('sharpe', 0.0)):.2f} | "
            f"MDD diff {float(row.get('mdd_diff_pct', 0.0)):+.2f}pp | "
            f"turnover {float(row.get('avg_turnover', 0.0)):.3f}"
        )
    lines.append("")
    return "\n".join(lines).strip() + "\n"


def _run_sweep(
    eval_runner: Any,
    hypothesis_names: list[str],
    profiles: list[SweepProfile],
    run_tag: str,
    start_date: str,
    end_date: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    cells: list[dict[str, Any]] = []
    for hypothesis_name in hypothesis_names:
        for profile in profiles:
            artifacts, summary, hypothesis = _run_cell(
                eval_runner=eval_runner,
                hypothesis_name=hypothesis_name,
                profile=profile,
                run_tag=run_tag,
                start_date=start_date,
                end_date=end_date,
            )
            row = _result_row(artifacts, hypothesis, profile, summary)
            rows.append(row)
            cells.append(
                {
                    "hypothesis_name": row["hypothesis_name"],
                    "profile_name": row["profile_name"],
                    "selector_run_tag": row["selector_run_tag"],
                    "selector_results_csv": row["selector_results_csv"],
                    "selector_summary_json": row["selector_summary_json"],
                    "cagr_diff_pct": row["cagr_diff_pct"],
                    "mdd_diff_pct": row["mdd_diff_pct"],
                    "avg_turnover": row["avg_turnover"],
                }
            )
    rows.sort(key=_ranking_key, reverse=True)
    cells.sort(key=lambda row: _ranking_key(row), reverse=True)
    return rows, cells


def main() -> None:
    run_tag = (
        os.getenv("STOCK_BREADTH_SWEEP_TAG")
        or f"stock_hypothesis_breadth_sweep_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    ).strip()
    start_date = (os.getenv("STOCK_BREADTH_START_DATE") or "2011-03-01").strip()
    end_date = (os.getenv("STOCK_BREADTH_END_DATE") or "2026-03-01").strip()
    hypothesis_names = _default_hypothesis_names()
    profiles = _build_profiles()
    if not hypothesis_names:
        raise RuntimeError("No hypothesis names provided")
    if not profiles:
        raise RuntimeError("No breadth/regime profiles available")

    eval_runner = _load_eval_runner()
    rows, cells = _run_sweep(
        eval_runner=eval_runner,
        hypothesis_names=hypothesis_names,
        profiles=profiles,
        run_tag=run_tag,
        start_date=start_date,
        end_date=end_date,
    )

    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    json_path = RUNS_DIR / f"{run_tag}.json"
    csv_path = RUNS_DIR / f"{run_tag}.csv"
    md_path = RUNS_DIR / f"{run_tag}.md"

    summary = {
        "run_tag": run_tag,
        "inputs": {
            "start_date": start_date,
            "end_date": end_date,
            "hypotheses": list(hypothesis_names),
            "profiles": [profile.name for profile in profiles],
            "profile_envs": {profile.name: dict(profile.env) for profile in profiles},
        },
        "results": rows,
        "cells": cells,
        "ranking": rows,
        "paths": {
            "json": str(json_path.relative_to(ROOT)),
            "csv": str(csv_path.relative_to(ROOT)),
            "md": str(md_path.relative_to(ROOT)),
        },
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
            "Best breadth sweep run -> "
            f"{best['hypothesis_name']} / {best['profile_name']} | "
            f"CAGR diff {float(best.get('cagr_diff_pct', 0.0)):+.2f}pp | "
            f"MDD diff {float(best.get('mdd_diff_pct', 0.0)):+.2f}pp"
        )


if __name__ == "__main__":
    main()
