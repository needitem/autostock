from __future__ import annotations

import importlib.util
import json
import os
import sys
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
RUNS_DIR = ROOT / "data" / "runs"
RESEARCH_SCRIPT = ROOT / "scripts" / "research_stock_hypotheses.py"


def _load_module(path: Path, name: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _parse_float_grid(raw: str, default: list[float]) -> list[float]:
    items = [part.strip() for part in str(raw).split(",") if part.strip()]
    if not items:
        return list(default)
    out: list[float] = []
    for item in items:
        out.append(float(item))
    return out


def _slug_num(value: float) -> str:
    text = f"{float(value):+.2f}".replace("+", "").replace("-", "m").replace(".", "p")
    return text


def _variant_name(base_name: str, pit_bonus: float, pit_veto_threshold: float) -> str:
    return f"{base_name}__pb{_slug_num(pit_bonus)}__vt{_slug_num(pit_veto_threshold)}"


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


def _ranking_key(row: dict[str, Any]) -> tuple[Any, ...]:
    annual = row.get("annual_rollforward_static") or {}
    fixed = row.get("fixed_oos_2016_plus") or {}
    return (
        1 if float(annual.get("cagr_diff_pct", -999.0)) > 0 else 0,
        1 if float(annual.get("mdd_diff_pct", -999.0)) >= 0 else 0,
        float(annual.get("cagr_diff_pct", -999.0)),
        float(annual.get("mdd_diff_pct", -999.0)),
        float(annual.get("sharpe", 0.0)) - float(annual.get("benchmark_sharpe", 0.0)),
        float(fixed.get("cagr_diff_pct", -999.0)),
        -float(annual.get("avg_turnover", 999.0)),
    )


def _evaluate_annual_static(research: Any, bt: Any, records: list[dict[str, Any]], hypothesis: Any, folds: list[tuple[int, int, int, int]]) -> dict[str, Any]:
    parts: list[pd.DataFrame] = []
    for _, _, test_year, test_end in folds:
        test_records = [r for r in records if test_year <= pd.Timestamp(r["entry_day"]).year <= test_end]
        if not test_records:
            continue
        parts.append(research._evaluate(bt, test_records, hypothesis))
    if not parts:
        return {}
    annual_df = pd.concat(parts, ignore_index=True)
    return research._summarize(annual_df, int(bt._periods_per_year(hypothesis.freq)))


def _build_markdown(summary: dict[str, Any], csv_rel: str) -> str:
    lines = [
        "# Stock Hypothesis Param Sweep",
        "",
        f"- base: `{summary['base_hypothesis']}`",
        f"- start: `{summary['inputs']['start_date']}`",
        f"- end: `{summary['inputs']['end_date']}`",
        f"- fixed OOS start year: `{summary['inputs']['fixed_oos_start_year']}`",
        f"- variants: **{len(summary['results'])}**",
        f"- csv: `{csv_rel}`",
        "",
        "## Ranking",
        "",
    ]
    for row in summary.get("ranking") or []:
        annual = row.get("annual_rollforward_static") or {}
        fixed = row.get("fixed_oos_2016_plus") or {}
        lines.append(
            f"- `{row['name']}` | annual diff {float(annual.get('cagr_diff_pct', 0.0)):+.2f}pp "
            f"| annual MDD diff {float(annual.get('mdd_diff_pct', 0.0)):+.2f}pp "
            f"| fixed OOS diff {float(fixed.get('cagr_diff_pct', 0.0)):+.2f}pp "
            f"| turnover {float(annual.get('avg_turnover', 0.0)):.3f}"
        )
    lines.append("")
    return "\n".join(lines).strip() + "\n"


def main() -> None:
    base_name = (os.getenv("STOCK_SWEEP_BASE") or "weekly_veto_recentq_newonly_neutral_soft_bonus_ro2").strip()
    run_tag = (os.getenv("STOCK_SWEEP_TAG") or f"stock_hypothesis_sweep_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}").strip()
    start_date = (os.getenv("STOCK_SWEEP_START_DATE") or "2006-03-01").strip()
    end_date = (os.getenv("STOCK_SWEEP_END_DATE") or "2026-03-11").strip()
    fixed_oos_start_year = int(float(os.getenv("STOCK_SWEEP_FIXED_OOS_START_YEAR") or "2016"))
    train_years = int(float(os.getenv("WF_TRAIN_YEARS") or "5"))
    test_years = int(float(os.getenv("WF_TEST_YEARS") or "1"))
    min_test_weeks = int(float(os.getenv("WF_MIN_TEST_WEEKS") or "40"))
    bonus_grid = _parse_float_grid(os.getenv("STOCK_SWEEP_BONUSES", ""), [0.03, 0.05, 0.07])
    threshold_grid = _parse_float_grid(os.getenv("STOCK_SWEEP_THRESHOLDS", ""), [-4.5, -4.0, -3.5])

    os.environ["HYP_START_DATE"] = start_date
    os.environ["HYP_END_DATE"] = end_date
    os.environ["HYP_OOS_START_YEAR"] = str(int(fixed_oos_start_year))

    research = _load_module(RESEARCH_SCRIPT, f"research_stock_hypotheses_sweep_{run_tag}")
    base = next((h for h in research._hypotheses() if h.name == base_name), None)
    if base is None:
        raise ValueError(f"Unknown base hypothesis: {base_name}")
    if str(base.freq) != "weekly":
        raise ValueError("Param sweep currently supports weekly hypotheses only")

    bt = research._load_bt_module("weekly")
    records, meta = research._build_records(bt)
    df_records = pd.DataFrame({"entry_day": pd.to_datetime([r["entry_day"] for r in records], errors="coerce")})
    df_records["entry_year"] = df_records["entry_day"].dt.year.astype(int)
    folds = _walkforward_folds(df_records, train_years=train_years, test_years=test_years, min_test_weeks=min_test_weeks)

    variants: list[Any] = []
    seen: set[str] = set()
    for bonus in bonus_grid:
        for threshold in threshold_grid:
            name = _variant_name(base_name, bonus, threshold)
            if name in seen:
                continue
            seen.add(name)
            variants.append(
                replace(
                    base,
                    name=name,
                    pit_bonus=float(bonus),
                    pit_veto_threshold=float(threshold),
                )
            )

    results: list[dict[str, Any]] = []
    for hypothesis in variants:
        df = research._evaluate(bt, records, hypothesis)
        periods_per_year = int(bt._periods_per_year(hypothesis.freq))
        full = research._summarize(df, periods_per_year)
        oos_mask = pd.to_datetime(df["entry_day"], errors="coerce").dt.year >= int(fixed_oos_start_year)
        df_oos = df.loc[oos_mask].copy()
        fixed_oos = research._summarize(df_oos, periods_per_year) if not df_oos.empty else {}
        annual = _evaluate_annual_static(research, bt, records, hypothesis, folds)
        results.append(
            {
                "name": hypothesis.name,
                "pit_bonus": float(hypothesis.pit_bonus),
                "pit_veto_threshold": float(hypothesis.pit_veto_threshold),
                "fixed_oos_2016_plus": fixed_oos,
                "annual_rollforward_static": annual,
                "full": full,
            }
        )

    results.sort(key=_ranking_key, reverse=True)
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    json_path = RUNS_DIR / f"{run_tag}.json"
    csv_path = RUNS_DIR / f"{run_tag}.csv"
    md_path = RUNS_DIR / f"{run_tag}.md"

    flat = []
    for row in results:
        fixed = row.get("fixed_oos_2016_plus") or {}
        annual = row.get("annual_rollforward_static") or {}
        flat.append(
            {
                "name": row["name"],
                "pit_bonus": row["pit_bonus"],
                "pit_veto_threshold": row["pit_veto_threshold"],
                "fixed_oos_cagr_diff_pct": fixed.get("cagr_diff_pct"),
                "fixed_oos_mdd_diff_pct": fixed.get("mdd_diff_pct"),
                "fixed_oos_sharpe_diff": float(fixed.get("sharpe", 0.0)) - float(fixed.get("benchmark_sharpe", 0.0)),
                "fixed_oos_avg_turnover": fixed.get("avg_turnover"),
                "annual_cagr_diff_pct": annual.get("cagr_diff_pct"),
                "annual_mdd_diff_pct": annual.get("mdd_diff_pct"),
                "annual_sharpe_diff": float(annual.get("sharpe", 0.0)) - float(annual.get("benchmark_sharpe", 0.0)),
                "annual_avg_turnover": annual.get("avg_turnover"),
                "annual_nw_p_two": annual.get("nw_p_two"),
            }
        )
    pd.DataFrame(flat).to_csv(csv_path, index=False)

    summary = {
        "run_tag": run_tag,
        "base_hypothesis": base_name,
        "inputs": {
            "start_date": start_date,
            "end_date": end_date,
            "fixed_oos_start_year": int(fixed_oos_start_year),
            "train_years": int(train_years),
            "test_years": int(test_years),
            "min_test_weeks": int(min_test_weeks),
            "bonus_grid": bonus_grid,
            "threshold_grid": threshold_grid,
            "meta": meta,
        },
        "results": results,
        "ranking": results,
        "paths": {
            "csv": str(csv_path.relative_to(ROOT)),
        },
    }
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_build_markdown(summary, str(csv_path.relative_to(ROOT))), encoding="utf-8")

    print(f"Saved: {json_path.relative_to(ROOT)}")
    print(f"Saved: {csv_path.relative_to(ROOT)}")
    print(f"Saved: {md_path.relative_to(ROOT)}")
    if results:
        best = results[0]
        annual = best.get("annual_rollforward_static") or {}
        print(
            "Best sweep variant -> "
            f"{best['name']} | annual diff {float(annual.get('cagr_diff_pct', 0.0)):+.2f}pp "
            f"| fixed OOS diff {float((best.get('fixed_oos_2016_plus') or {}).get('cagr_diff_pct', 0.0)):+.2f}pp"
        )


if __name__ == "__main__":
    main()
