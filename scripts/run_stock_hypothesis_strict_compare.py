from __future__ import annotations

import importlib.util
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
RUNS_DIR = ROOT / "data" / "runs"
RESEARCH_SCRIPT = ROOT / "scripts" / "research_stock_hypotheses.py"


def _slug(text: str) -> str:
    out = "".join(ch if ch.isalnum() else "_" for ch in str(text).strip().lower()).strip("_")
    return out or "run"


def _load_module(path: Path, name: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _load_research_module() -> Any:
    return _load_module(RESEARCH_SCRIPT, f"research_stock_hypotheses_strict_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}")


def _parse_names(raw: str) -> list[str]:
    names = [part.strip() for part in str(raw).split(",") if part.strip()]
    if not names:
        return ["weekly_baseline_v4", "weekly_veto_recentq_newonly_nrisk_soft_bonus"]
    return names


def _load_hypotheses(research: Any, names: list[str]) -> list[Any]:
    found = {str(h.name): h for h in research._hypotheses()}
    missing = [name for name in names if name not in found]
    if missing:
        raise ValueError(f"Unknown hypotheses: {', '.join(missing)}")
    return [found[name] for name in names]


def _walkforward_folds(df_records: pd.DataFrame, train_years: int, test_years: int, min_test_weeks: int) -> list[tuple[int, int, int, int]]:
    years = sorted(df_records["entry_year"].dropna().astype(int).unique().tolist())
    folds: list[tuple[int, int, int, int]] = []
    for test_year in years:
        train_start = test_year - int(train_years)
        train_end = test_year - 1
        test_end = test_year + int(test_years) - 1
        train_mask = df_records["entry_year"].between(train_start, train_end)
        test_mask = df_records["entry_year"].between(test_year, test_end)
        if int(train_mask.sum()) < int(train_years) * 40:
            continue
        if int(test_mask.sum()) < int(min_test_weeks):
            continue
        folds.append((train_start, train_end, test_year, test_end))
    return folds


def _ranking_key(result: dict[str, Any], section: str) -> tuple[Any, ...]:
    metrics = result.get(section) or {}
    return (
        1 if float(metrics.get("cagr_diff_pct", -999.0)) > 0 else 0,
        1 if float(metrics.get("mdd_diff_pct", -999.0)) >= 0 else 0,
        float(metrics.get("cagr_diff_pct", -999.0)),
        float(metrics.get("mdd_diff_pct", -999.0)),
        float(metrics.get("sharpe", 0.0)) - float(metrics.get("benchmark_sharpe", 0.0)),
        -float(metrics.get("avg_turnover", 999.0)),
    )


def _evaluate_hypothesis(research: Any, bt: Any, records: list[dict[str, Any]], hypothesis: Any, fixed_oos_start_year: int) -> dict[str, Any]:
    df = research._evaluate(bt, records, hypothesis)
    periods_per_year = int(bt._periods_per_year(hypothesis.freq))
    full = research._summarize(df, periods_per_year)
    oos_mask = pd.to_datetime(df["entry_day"], errors="coerce").dt.year >= int(fixed_oos_start_year)
    df_oos = df.loc[oos_mask].copy()
    fixed_oos = research._summarize(df_oos, periods_per_year) if not df_oos.empty else {}
    return {
        "df": df,
        "full": full,
        "fixed_oos_2016_plus": fixed_oos,
    }


def _annual_rollforward_static(research: Any, bt: Any, records: list[dict[str, Any]], hypothesis: Any, folds: list[tuple[int, int, int, int]]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    parts: list[pd.DataFrame] = []
    fold_rows: list[dict[str, Any]] = []
    periods_per_year = int(bt._periods_per_year(hypothesis.freq))
    for train_start, train_end, test_year, test_end in folds:
        test_records = [r for r in records if test_year <= pd.Timestamp(r["entry_day"]).year <= test_end]
        if not test_records:
            continue
        df = research._evaluate(bt, test_records, hypothesis)
        summary = research._summarize(df, periods_per_year)
        parts.append(df)
        fold_rows.append(
            {
                "test_year": int(test_year),
                "train_start": int(train_start),
                "train_end": int(train_end),
                "test_end": int(test_end),
                "cagr_diff_pct": float(summary.get("cagr_diff_pct", 0.0)),
                "mdd_diff_pct": float(summary.get("mdd_diff_pct", 0.0)),
                "avg_turnover": float(summary.get("avg_turnover", 0.0)),
            }
        )
    if not parts:
        return {}, fold_rows
    annual_df = pd.concat(parts, ignore_index=True)
    summary = research._summarize(annual_df, periods_per_year)
    return summary, fold_rows


def _build_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Stock Hypothesis Strict Compare",
        "",
        f"- start: `{summary['inputs']['start_date']}`",
        f"- end: `{summary['inputs']['end_date']}`",
        f"- fixed OOS start year: `{summary['inputs']['fixed_oos_start_year']}`",
        f"- train/test: **{summary['inputs']['train_years']}y / {summary['inputs']['test_years']}y**",
        f"- folds: **{summary['inputs']['folds']}**",
        "",
        "## Fixed OOS Ranking",
        "",
    ]
    for row in summary.get("rankings", {}).get("fixed_oos_2016_plus", []):
        metrics = (summary.get("results") or {}).get(row, {}).get("fixed_oos_2016_plus") or {}
        lines.append(
            f"- `{row}` | CAGR diff {float(metrics.get('cagr_diff_pct', 0.0)):+.2f}pp "
            f"| Sharpe diff {float(metrics.get('sharpe', 0.0)) - float(metrics.get('benchmark_sharpe', 0.0)):+.3f} "
            f"| MDD diff {float(metrics.get('mdd_diff_pct', 0.0)):+.2f}pp "
            f"| turnover {float(metrics.get('avg_turnover', 0.0)):.3f}"
        )
    lines.extend(["", "## Annual Roll-Forward Static", ""])
    for row in summary.get("rankings", {}).get("annual_rollforward_static", []):
        metrics = (summary.get("results") or {}).get(row, {}).get("annual_rollforward_static") or {}
        lines.append(
            f"- `{row}` | CAGR diff {float(metrics.get('cagr_diff_pct', 0.0)):+.2f}pp "
            f"| Sharpe diff {float(metrics.get('sharpe', 0.0)) - float(metrics.get('benchmark_sharpe', 0.0)):+.3f} "
            f"| MDD diff {float(metrics.get('mdd_diff_pct', 0.0)):+.2f}pp "
            f"| turnover {float(metrics.get('avg_turnover', 0.0)):.3f} "
            f"| NW p2 {float(metrics.get('nw_p_two', 1.0)):.3f}"
        )
    lines.append("")
    return "\n".join(lines).strip() + "\n"


def main() -> None:
    names = _parse_names(os.getenv("STOCK_STRICT_COMPARE_NAMES", ""))
    tag = (os.getenv("STOCK_STRICT_COMPARE_TAG") or "").strip()
    strict_start_date = (os.getenv("STOCK_STRICT_START_DATE") or "2006-03-01").strip()
    strict_end_date = (os.getenv("STOCK_STRICT_END_DATE") or "2026-03-11").strip()
    fixed_oos_start_year = int(float(os.getenv("STOCK_STRICT_FIXED_OOS_START_YEAR") or "2016"))
    train_years = int(float(os.getenv("WF_TRAIN_YEARS") or "5"))
    test_years = int(float(os.getenv("WF_TEST_YEARS") or "1"))
    min_test_weeks = int(float(os.getenv("WF_MIN_TEST_WEEKS") or "40"))
    os.environ["HYP_START_DATE"] = strict_start_date
    os.environ["HYP_END_DATE"] = strict_end_date
    os.environ["HYP_OOS_START_YEAR"] = str(int(fixed_oos_start_year))
    research = _load_research_module()
    hypotheses = _load_hypotheses(research, names)
    freq_set = {str(h.freq) for h in hypotheses}
    if freq_set != {"weekly"}:
        raise ValueError("Strict compare currently supports weekly hypotheses only")

    bt = research._load_bt_module("weekly")
    records, meta = research._build_records(bt)
    if not records:
        raise RuntimeError("No records available")

    df_records = pd.DataFrame({"entry_day": pd.to_datetime([r["entry_day"] for r in records], errors="coerce")})
    df_records["entry_year"] = df_records["entry_day"].dt.year.astype(int)
    folds = _walkforward_folds(df_records, train_years=train_years, test_years=test_years, min_test_weeks=min_test_weeks)
    if not folds:
        raise RuntimeError("No walk-forward folds available")

    results: dict[str, Any] = {}
    for hypothesis in hypotheses:
        evaluated = _evaluate_hypothesis(
            research=research,
            bt=bt,
            records=records,
            hypothesis=hypothesis,
            fixed_oos_start_year=fixed_oos_start_year,
        )
        annual_summary, fold_rows = _annual_rollforward_static(
            research=research,
            bt=bt,
            records=records,
            hypothesis=hypothesis,
            folds=folds,
        )
        results[hypothesis.name] = {
            "config": research.asdict(hypothesis),
            "full": evaluated["full"],
            "fixed_oos_2016_plus": evaluated["fixed_oos_2016_plus"],
            "annual_rollforward_static": annual_summary,
            "folds": fold_rows,
        }

    rankings = {
        "fixed_oos_2016_plus": sorted(results, key=lambda name: _ranking_key(results[name], "fixed_oos_2016_plus"), reverse=True),
        "annual_rollforward_static": sorted(results, key=lambda name: _ranking_key(results[name], "annual_rollforward_static"), reverse=True),
    }
    run_tag = tag or f"stock_hypothesis_strict_compare_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    summary = {
        "run_tag": run_tag,
        "inputs": {
            "start_date": str(research.START),
            "end_date": str(research.END),
            "fixed_oos_start_year": int(fixed_oos_start_year),
            "train_years": int(train_years),
            "test_years": int(test_years),
            "min_test_weeks": int(min_test_weeks),
            "folds": int(len(folds)),
            "meta": meta,
        },
        "results": results,
        "rankings": rankings,
    }
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    json_path = RUNS_DIR / f"{run_tag}.json"
    md_path = RUNS_DIR / f"{run_tag}.md"
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_build_markdown(summary), encoding="utf-8")

    print(f"Saved: {json_path.relative_to(ROOT)}")
    print(f"Saved: {md_path.relative_to(ROOT)}")
    print(
        "Best annual static -> "
        f"{rankings['annual_rollforward_static'][0]} | "
        f"CAGR diff {float((results[rankings['annual_rollforward_static'][0]].get('annual_rollforward_static') or {}).get('cagr_diff_pct', 0.0)):+.2f}pp"
    )


if __name__ == "__main__":
    main()
