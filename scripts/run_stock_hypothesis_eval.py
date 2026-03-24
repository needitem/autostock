from __future__ import annotations

import importlib.util
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
RUNS_DIR = ROOT / "data" / "runs"
SELECTOR_SCRIPT = ROOT / "scripts" / "backtest_ai_portfolio_selector.py"
VERIFY_SCRIPT = ROOT / "scripts" / "verify_ai_portfolio_backtest.py"
RESEARCH_SCRIPT = ROOT / "scripts" / "research_stock_hypotheses.py"


DEFAULT_ENV: dict[str, str] = {
    "AI_UNIVERSE": "nasdaq100",
    "AI_UNIVERSE_MODE": "by_date",
    "AI_UNIVERSE_BY_DATE_FILE": "data/universe/nasdaq100_by_date_weekly_2006_2026.json",
    "AI_HORIZON_MODE": "next_snapshot",
    "AI_EXECUTION_TIMING": "next_open",
    "AI_PORTFOLIO_MAX_WEIGHT_PCT": "20",
    "AI_PROMPT_MAX_SYMBOLS": "40",
    "AI_TRADE_COST_BPS": "20",
    "AI_SLIPPAGE_BPS": "0",
    "AI_SPREAD_BPS": "0",
    "AI_TAX_BPS": "0",
    "AI_START_DATE": "2011-03-01",
    "AI_END_DATE": "2026-03-01",
    "AI_SAFE_MODE": "1",
    "AI_SAFE_REQUIRE_RISK_ON": "0",
    "AI_ALGO_USE_BENCHMARK_FEATURES": "1",
    "AI_SKIP_LATEST_WRITE": "1",
    "AI_MOMENTUM_BLEND_PCT": "0",
}


@dataclass(frozen=True)
class RunArtifacts:
    hypothesis_name: str
    run_tag: str
    results_csv: Path
    summary_json: Path
    verification_json: Path
    verification_md: Path


def _slug(text: str) -> str:
    clean = re.sub(r"[^A-Za-z0-9]+", "_", str(text).strip()).strip("_").lower()
    return clean or "run"


def _load_module(path: Path, name: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _load_hypothesis(name: str) -> Any:
    research = _load_module(RESEARCH_SCRIPT, f"research_stock_hypotheses_eval_{_slug(name)}")
    for hypothesis in research._hypotheses():
        if str(getattr(hypothesis, "name", "")) == name:
            return hypothesis
    raise ValueError(f"Hypothesis not found: {name}")


def _bool_flag(value: bool) -> str:
    return "1" if bool(value) else "0"


def _env_for_hypothesis(hypothesis: Any) -> dict[str, str]:
    env = dict(DEFAULT_ENV)
    env.update(
        {
            "AI_DECISION_ENGINE": str(hypothesis.engine),
            "AI_SNAPSHOT_FREQ": str(hypothesis.freq),
            "AI_PORTFOLIO_TOP_K": str(int(hypothesis.top_k)),
            "AI_PORTFOLIO_MIN_OVERLAP": str(int(hypothesis.min_overlap)),
            "AI_SAFE_USE_TREND_TEMPLATE": _bool_flag(bool(hypothesis.safe_use_trend_template)),
            "AI_SAFE_MIN_VOLUME_RATIO": str(float(hypothesis.safe_min_volume_ratio)),
        }
    )
    if str(hypothesis.engine) == "stock_momentum":
        env.update(
            {
                "AI_STOCK_MOMO_WEIGHT_MODE": str(hypothesis.weight_mode),
                "AI_STOCK_MOMO_TOP_K_NEUTRAL": str(int(hypothesis.top_k_neutral)),
                "AI_STOCK_MOMO_TOP_K_RISK_OFF": str(int(hypothesis.top_k_risk_off)),
                "AI_STOCK_MOMO_MIN_POSITIONS_FOR_INVEST": str(int(hypothesis.min_positions_for_invest)),
                "AI_STOCK_MOMO_MAX_PER_SECTOR": str(int(hypothesis.max_per_sector)),
                "AI_STOCK_MOMO_SECTOR_BONUS": str(float(hypothesis.sector_bonus)),
                "AI_STOCK_MOMO_PIT_BONUS": str(float(hypothesis.pit_bonus)),
                "AI_STOCK_MOMO_PIT_MAX_FILING_AGE": str(int(hypothesis.pit_max_filing_age)),
                "AI_STOCK_MOMO_PIT_VETO_THRESHOLD": str(float(hypothesis.pit_veto_threshold)),
                "AI_STOCK_MOMO_PIT_VETO_MAX_FILING_AGE": str(int(hypothesis.pit_veto_max_filing_age)),
                "AI_STOCK_MOMO_PIT_VETO_NEW_ONLY": _bool_flag(bool(hypothesis.pit_veto_new_only)),
                "AI_STOCK_MOMO_PIT_VETO_REGIMES": ",".join(str(x) for x in hypothesis.pit_veto_regimes),
                "AI_STOCK_MOMO_NEUTRAL_ENTRY_MIN_BREADTH_UP200": str(float(hypothesis.neutral_entry_min_breadth_up200)),
                "AI_STOCK_MOMO_NEUTRAL_ENTRY_MIN_BREADTH_POS63": str(float(hypothesis.neutral_entry_min_breadth_positive63)),
                "AI_STOCK_MOMO_NEUTRAL_MAX_NEW_WHEN_WEAK": str(int(hypothesis.neutral_max_new_names_when_weak)),
            }
        )
    elif str(hypothesis.engine) == "quality_momentum":
        env.update(
            {
                "AI_QM_WEIGHT_MODE": str(hypothesis.weight_mode),
                "AI_QM_TOP_K_NEUTRAL": str(int(hypothesis.top_k_neutral)),
                "AI_QM_TOP_K_RISK_OFF": str(int(hypothesis.top_k_risk_off)),
                "AI_QM_MIN_POSITIONS_FOR_INVEST": str(int(hypothesis.min_positions_for_invest)),
                "AI_QM_MAX_PER_SECTOR": str(int(hypothesis.max_per_sector)),
                "AI_QM_SECTOR_BONUS": str(float(hypothesis.sector_bonus)),
                "AI_QM_MAX_FILING_AGE": str(int(hypothesis.pit_max_filing_age)),
                "AI_QM_QUALITY_WEIGHT": str(float(hypothesis.quality_weight)),
                "AI_QM_MOMENTUM_WEIGHT": str(float(hypothesis.momentum_weight)),
                "AI_QM_MIN_QUALITY_SCORE": str(float(hypothesis.min_quality_score)),
                "AI_QM_MIN_MOMENTUM_SCORE": str(float(hypothesis.min_momentum_score)),
                "AI_QM_REQUIRE_TREND_TEMPLATE": _bool_flag(bool(hypothesis.require_trend_template)),
            }
        )
    else:
        raise ValueError(f"Unsupported hypothesis engine for standard runner: {hypothesis.engine}")
    return env


def _run_python_script(script_path: Path, extra_env: dict[str, str]) -> dict[str, Any]:
    env = os.environ.copy()
    env.update({k: str(v) for k, v in extra_env.items()})
    proc = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(ROOT),
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


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def _run_hypothesis(hypothesis_name: str, session_tag: str) -> tuple[RunArtifacts, dict[str, Any], dict[str, Any]]:
    hypothesis = _load_hypothesis(hypothesis_name)
    run_tag = f"stock_hypothesis_{_slug(hypothesis_name)}_{session_tag}"
    run_env = _env_for_hypothesis(hypothesis)
    run_env["AI_RUN_TAG"] = run_tag

    print(f"Running hypothesis: {hypothesis_name}")
    backtest_run = _run_python_script(SELECTOR_SCRIPT, run_env)

    artifacts = RunArtifacts(
        hypothesis_name=hypothesis_name,
        run_tag=run_tag,
        results_csv=RUNS_DIR / f"ai_portfolio_backtest_results_{run_tag}.csv",
        summary_json=RUNS_DIR / f"ai_portfolio_backtest_summary_{run_tag}.json",
        verification_json=RUNS_DIR / f"ai_portfolio_backtest_verification_{run_tag}.json",
        verification_md=RUNS_DIR / f"ai_portfolio_backtest_verification_{run_tag}.md",
    )
    verify_run = _run_python_script(
        VERIFY_SCRIPT,
        {
            "AI_PORTFOLIO_RESULTS_CSV": str(artifacts.results_csv),
            "AI_PORTFOLIO_SUMMARY_JSON": str(artifacts.summary_json),
            "AI_PORTFOLIO_VERIFY_JSON": str(artifacts.verification_json),
            "AI_PORTFOLIO_VERIFY_MD": str(artifacts.verification_md),
        },
    )
    return artifacts, backtest_run, verify_run


def _metric(d: dict[str, Any], *keys: str, default: float = 0.0) -> float:
    cur: Any = d
    for key in keys:
        if not isinstance(cur, dict):
            return float(default)
        cur = cur.get(key)
    try:
        return float(cur)
    except Exception:
        return float(default)


def _yearly_compare(primary_verify: dict[str, Any], compare_verify: dict[str, Any]) -> pd.DataFrame:
    primary_df = pd.DataFrame(primary_verify.get("yearly") or [])
    compare_df = pd.DataFrame(compare_verify.get("yearly") or [])
    if primary_df.empty and compare_df.empty:
        return pd.DataFrame()

    primary_df = primary_df.rename(
        columns={
            "ai_ret_pct": "primary_ret_pct",
            "qqq_ret_pct": "primary_qqq_ret_pct",
            "mom_ret_pct": "primary_mom_ret_pct",
            "ai_minus_qqq_pct": "primary_minus_qqq_pct",
            "n": "primary_n",
        }
    )
    compare_df = compare_df.rename(
        columns={
            "ai_ret_pct": "compare_ret_pct",
            "qqq_ret_pct": "compare_qqq_ret_pct",
            "mom_ret_pct": "compare_mom_ret_pct",
            "ai_minus_qqq_pct": "compare_minus_qqq_pct",
            "n": "compare_n",
        }
    )
    merged = primary_df.merge(compare_df, on="year", how="outer").sort_values("year").reset_index(drop=True)
    if "primary_ret_pct" in merged.columns and "compare_ret_pct" in merged.columns:
        merged["primary_minus_compare_pct"] = merged["primary_ret_pct"] - merged["compare_ret_pct"]
    return merged


def _build_compare_report(
    primary: RunArtifacts,
    compare: RunArtifacts,
    primary_verify: dict[str, Any],
    compare_verify: dict[str, Any],
) -> tuple[dict[str, Any], str]:
    primary_metrics = primary_verify.get("metrics") or {}
    compare_metrics = compare_verify.get("metrics") or {}
    primary_ai = primary_metrics.get("ai_portfolio") or {}
    primary_bench = primary_metrics.get("benchmark") or {}
    compare_ai = compare_metrics.get("ai_portfolio") or {}
    compare_bench = compare_metrics.get("benchmark") or {}
    yearly = _yearly_compare(primary_verify, compare_verify)

    headline = {
        "primary_cagr_pct": _metric(primary_ai, "cagr_pct"),
        "compare_cagr_pct": _metric(compare_ai, "cagr_pct"),
        "primary_minus_compare_cagr_pct": _metric(primary_ai, "cagr_pct") - _metric(compare_ai, "cagr_pct"),
        "primary_sharpe": _metric(primary_ai, "sharpe"),
        "compare_sharpe": _metric(compare_ai, "sharpe"),
        "primary_minus_compare_sharpe": _metric(primary_ai, "sharpe") - _metric(compare_ai, "sharpe"),
        "primary_mdd_pct": _metric(primary_ai, "max_drawdown_pct"),
        "compare_mdd_pct": _metric(compare_ai, "max_drawdown_pct"),
        "primary_minus_compare_mdd_pct": _metric(primary_ai, "max_drawdown_pct") - _metric(compare_ai, "max_drawdown_pct"),
        "primary_minus_qqq_cagr_pct": _metric(primary_ai, "cagr_pct") - _metric(primary_bench, "cagr_pct"),
        "compare_minus_qqq_cagr_pct": _metric(compare_ai, "cagr_pct") - _metric(compare_bench, "cagr_pct"),
        "primary_turnover_mean": _metric(primary_verify, "turnover", "ai", "mean"),
        "compare_turnover_mean": _metric(compare_verify, "turnover", "ai", "mean"),
    }

    report = {
        "primary": {
            "hypothesis": primary.hypothesis_name,
            "run_tag": primary.run_tag,
            "results_csv": str(primary.results_csv),
            "summary_json": str(primary.summary_json),
            "verification_json": str(primary.verification_json),
            "verification_md": str(primary.verification_md),
        },
        "compare": {
            "hypothesis": compare.hypothesis_name,
            "run_tag": compare.run_tag,
            "results_csv": str(compare.results_csv),
            "summary_json": str(compare.summary_json),
            "verification_json": str(compare.verification_json),
            "verification_md": str(compare.verification_md),
        },
        "headline": headline,
        "primary_alpha": primary_verify.get("alpha") or {},
        "compare_alpha": compare_verify.get("alpha") or {},
        "primary_bootstrap": primary_verify.get("bootstrap") or {},
        "compare_bootstrap": compare_verify.get("bootstrap") or {},
        "yearly": yearly.to_dict(orient="records"),
    }

    lines = [
        "# Stock Hypothesis Comparison",
        "",
        f"- primary: `{primary.hypothesis_name}`",
        f"- compare: `{compare.hypothesis_name}`",
        f"- primary verification: `{primary.verification_json.relative_to(ROOT)}`",
        f"- compare verification: `{compare.verification_json.relative_to(ROOT)}`",
        "",
        "## Headline",
        "",
        (
            f"- CAGR {headline['primary_cagr_pct']:.2f}% vs {headline['compare_cagr_pct']:.2f}% "
            f"| delta {headline['primary_minus_compare_cagr_pct']:+.2f}pp"
        ),
        (
            f"- Sharpe {headline['primary_sharpe']:.3f} vs {headline['compare_sharpe']:.3f} "
            f"| delta {headline['primary_minus_compare_sharpe']:+.3f}"
        ),
        (
            f"- MDD {headline['primary_mdd_pct']:.2f}% vs {headline['compare_mdd_pct']:.2f}% "
            f"| delta {headline['primary_minus_compare_mdd_pct']:+.2f}pp"
        ),
        (
            f"- turnover mean {headline['primary_turnover_mean']:.3f} vs {headline['compare_turnover_mean']:.3f}"
        ),
        (
            f"- vs QQQ CAGR diff {headline['primary_minus_qqq_cagr_pct']:+.2f}pp "
            f"vs {headline['compare_minus_qqq_cagr_pct']:+.2f}pp"
        ),
        "",
        "## Alpha Checks",
        "",
        (
            f"- primary NW p(two-sided) {_metric(primary_verify, 'alpha', 'nw_p_two_sided', default=1.0):.3f} "
            f"| bootstrap P(diff>0) {_metric(primary_verify, 'bootstrap', 'p_cagr_diff_gt0', default=0.0):.3f}"
        ),
        (
            f"- compare NW p(two-sided) {_metric(compare_verify, 'alpha', 'nw_p_two_sided', default=1.0):.3f} "
            f"| bootstrap P(diff>0) {_metric(compare_verify, 'bootstrap', 'p_cagr_diff_gt0', default=0.0):.3f}"
        ),
        "",
        "## Yearly",
        "",
    ]
    if not yearly.empty:
        lines.append("```text")
        lines.append(yearly.to_string(index=False, float_format=lambda x: f"{float(x):.2f}"))
        lines.append("```")
        lines.append("")
    md = "\n".join(lines).strip() + "\n"
    return report, md


def _save_compare_report(primary: RunArtifacts, compare: RunArtifacts, session_tag: str, report: dict[str, Any], md: str) -> tuple[Path, Path]:
    prefix = f"stock_hypothesis_compare_{_slug(primary.hypothesis_name)}_vs_{_slug(compare.hypothesis_name)}_{session_tag}"
    json_path = RUNS_DIR / f"{prefix}.json"
    md_path = RUNS_DIR / f"{prefix}.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(md, encoding="utf-8")
    return json_path, md_path


def main() -> None:
    primary_name = (os.getenv("STOCK_HYPOTHESIS_NAME") or "weekly_veto_recentq_newonly_nrisk_soft_bonus").strip()
    compare_name = (os.getenv("STOCK_COMPARE_HYPOTHESIS") or "weekly_baseline_v4").strip()
    session_tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    RUNS_DIR.mkdir(parents=True, exist_ok=True)

    primary_artifacts, primary_backtest_run, primary_verify_run = _run_hypothesis(primary_name, session_tag)
    primary_verify = _load_json(primary_artifacts.verification_json)

    compare_artifacts: RunArtifacts | None = None
    compare_backtest_run: dict[str, Any] | None = None
    compare_verify_run: dict[str, Any] | None = None
    compare_verify: dict[str, Any] | None = None
    compare_json_path = Path()
    compare_md_path = Path()

    if compare_name and compare_name != primary_name:
        compare_artifacts, compare_backtest_run, compare_verify_run = _run_hypothesis(compare_name, session_tag)
        compare_verify = _load_json(compare_artifacts.verification_json)
        compare_report, compare_md = _build_compare_report(
            primary_artifacts,
            compare_artifacts,
            primary_verify,
            compare_verify,
        )
        compare_json_path, compare_md_path = _save_compare_report(
            primary_artifacts,
            compare_artifacts,
            session_tag,
            compare_report,
            compare_md,
        )

    print(f"Saved: {primary_artifacts.summary_json.relative_to(ROOT)}")
    print(f"Saved: {primary_artifacts.verification_json.relative_to(ROOT)}")
    print(f"Saved: {primary_artifacts.verification_md.relative_to(ROOT)}")
    if compare_artifacts is not None:
        print(f"Saved: {compare_artifacts.summary_json.relative_to(ROOT)}")
        print(f"Saved: {compare_artifacts.verification_json.relative_to(ROOT)}")
        print(f"Saved: {compare_artifacts.verification_md.relative_to(ROOT)}")
        print(f"Saved: {compare_json_path.relative_to(ROOT)}")
        print(f"Saved: {compare_md_path.relative_to(ROOT)}")

    summary = {
        "primary": {
            "hypothesis": primary_name,
            "run_tag": primary_artifacts.run_tag,
            "backtest_run": primary_backtest_run,
            "verify_run": primary_verify_run,
            "artifacts": {
                "results_csv": str(primary_artifacts.results_csv),
                "summary_json": str(primary_artifacts.summary_json),
                "verification_json": str(primary_artifacts.verification_json),
                "verification_md": str(primary_artifacts.verification_md),
            },
        },
        "compare": {
            "hypothesis": compare_name,
            "run_tag": compare_artifacts.run_tag if compare_artifacts is not None else "",
            "backtest_run": compare_backtest_run,
            "verify_run": compare_verify_run,
            "artifacts": {
                "results_csv": str(compare_artifacts.results_csv) if compare_artifacts is not None else "",
                "summary_json": str(compare_artifacts.summary_json) if compare_artifacts is not None else "",
                "verification_json": str(compare_artifacts.verification_json) if compare_artifacts is not None else "",
                "verification_md": str(compare_artifacts.verification_md) if compare_artifacts is not None else "",
                "comparison_json": str(compare_json_path) if compare_json_path else "",
                "comparison_md": str(compare_md_path) if compare_md_path else "",
            },
        },
    }
    summary_path = RUNS_DIR / f"stock_hypothesis_eval_session_{_slug(primary_name)}_{session_tag}.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Saved: {summary_path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
