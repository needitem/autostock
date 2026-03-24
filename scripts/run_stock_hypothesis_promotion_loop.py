from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import sys
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
RUNS_DIR = ROOT / "data" / "runs"
RESEARCH_SCRIPT = ROOT / "scripts" / "research_stock_hypotheses.py"
PROMOTION_SCRIPT = ROOT / "scripts" / "run_strategy_promotion_check.py"


@dataclass(frozen=True)
class LoopConfig:
    run_tag: str
    start_date: str
    end_date: str
    oos_start_year: int
    trade_cost_bps: float
    max_rounds: int
    frontier_size: int
    top_candidates_per_round: int
    save_round_leaders: int
    seed_names: tuple[str, ...]
    bonus_override: tuple[float, ...]
    threshold_override: tuple[float, ...]
    risk_off_override: tuple[int, ...]
    entry_freeze_override: tuple[int, ...]
    sector_cap_override: tuple[int, ...]
    min_overlap_override: tuple[int, ...]
    weight_mode_override: tuple[str, ...]
    promotion_min_cost_bps: float
    promotion_max_mdd_worse_pctp: float
    promotion_min_p_alpha_gt0: float


def _load_module(path: Path, name: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _parse_list(raw: str, default: list[str]) -> tuple[str, ...]:
    items = [part.strip() for part in str(raw).split(",") if part.strip()]
    return tuple(items or list(default))


def _parse_float_list(raw: str) -> tuple[float, ...]:
    items = [part.strip() for part in str(raw).split(",") if part.strip()]
    return tuple(float(item) for item in items)


def _parse_int_list(raw: str) -> tuple[int, ...]:
    items = [part.strip() for part in str(raw).split(",") if part.strip()]
    return tuple(int(float(item)) for item in items)


def _parse_str_list(raw: str) -> tuple[str, ...]:
    items = [part.strip().lower() for part in str(raw).split(",") if part.strip()]
    return tuple(items)


def _load_config() -> LoopConfig:
    run_tag = (
        os.getenv("STOCK_PROMO_LOOP_TAG")
        or f"stock_hypothesis_promotion_loop_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    ).strip()
    return LoopConfig(
        run_tag=run_tag,
        start_date=(os.getenv("STOCK_PROMO_LOOP_START_DATE") or "2011-03-01").strip(),
        end_date=(os.getenv("STOCK_PROMO_LOOP_END_DATE") or "2026-03-01").strip(),
        oos_start_year=int(float(os.getenv("STOCK_PROMO_LOOP_OOS_START_YEAR") or "2016")),
        trade_cost_bps=float(os.getenv("STOCK_PROMO_LOOP_TRADE_COST_BPS") or "20"),
        max_rounds=max(1, int(float(os.getenv("STOCK_PROMO_LOOP_MAX_ROUNDS") or "3"))),
        frontier_size=max(1, int(float(os.getenv("STOCK_PROMO_LOOP_FRONTIER_SIZE") or "2"))),
        top_candidates_per_round=max(1, int(float(os.getenv("STOCK_PROMO_LOOP_TOP_N") or "5"))),
        save_round_leaders=max(0, int(float(os.getenv("STOCK_PROMO_LOOP_SAVE_ROUND_LEADERS") or "1"))),
        seed_names=_parse_list(
            os.getenv("STOCK_PROMO_LOOP_SEEDS", ""),
            [
                "weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto325_entryfreeze1_pb007",
                "weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto35",
            ],
        ),
        bonus_override=_parse_float_list(os.getenv("STOCK_PROMO_LOOP_BONUSES", "")),
        threshold_override=_parse_float_list(os.getenv("STOCK_PROMO_LOOP_THRESHOLDS", "")),
        risk_off_override=_parse_int_list(os.getenv("STOCK_PROMO_LOOP_RISK_OFF_GRID", "")),
        entry_freeze_override=_parse_int_list(os.getenv("STOCK_PROMO_LOOP_ENTRY_FREEZE_GRID", "")),
        sector_cap_override=_parse_int_list(os.getenv("STOCK_PROMO_LOOP_SECTOR_CAP_GRID", "")),
        min_overlap_override=_parse_int_list(os.getenv("STOCK_PROMO_LOOP_MIN_OVERLAP_GRID", "")),
        weight_mode_override=_parse_str_list(os.getenv("STOCK_PROMO_LOOP_WEIGHT_MODE_GRID", "")),
        promotion_min_cost_bps=float(os.getenv("PROMOTION_MIN_COST_BPS") or "20"),
        promotion_max_mdd_worse_pctp=float(os.getenv("PROMOTION_MAX_MDD_WORSE_PCTP") or "10"),
        promotion_min_p_alpha_gt0=float(os.getenv("PROMOTION_MIN_P_ALPHA_GT0") or "0.90"),
    )


def _unique_floats(values: list[float], ndigits: int = 6) -> tuple[float, ...]:
    seen: set[float] = set()
    out: list[float] = []
    for value in values:
        rounded = round(float(value), ndigits)
        if rounded in seen:
            continue
        seen.add(rounded)
        out.append(float(rounded))
    return tuple(out)


def _unique_ints(values: list[int]) -> tuple[int, ...]:
    seen: set[int] = set()
    out: list[int] = []
    for value in values:
        item = int(value)
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return tuple(out)


def _bounded_triplet(
    base_value: float,
    step: float,
    round_index: int,
    lower: float,
    upper: float,
    override: tuple[float, ...],
) -> tuple[float, ...]:
    if override:
        return _unique_floats([min(upper, max(lower, item)) for item in override])
    width = float(step) * max(1, int(round_index))
    raw = [base_value - width, base_value, base_value + width]
    clipped = [min(upper, max(lower, item)) for item in raw]
    return _unique_floats(clipped)


def _candidate_signature(hypothesis: Any) -> str:
    return json.dumps(asdict(hypothesis), ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _candidate_hash(hypothesis: Any) -> str:
    return hashlib.sha1(_candidate_signature(hypothesis).encode("utf-8")).hexdigest()[:10]


def _short_label(hypothesis: Any) -> str:
    return f"{hypothesis.name}#{_candidate_hash(hypothesis)}"


def _brief_config(hypothesis: Any) -> dict[str, Any]:
    return {
        "pit_bonus": float(getattr(hypothesis, "pit_bonus", 0.0) or 0.0),
        "pit_veto_threshold": float(getattr(hypothesis, "pit_veto_threshold", -999.0) or -999.0),
        "top_k_risk_off": int(getattr(hypothesis, "top_k_risk_off", 0) or 0),
        "pit_veto_regimes": list(getattr(hypothesis, "pit_veto_regimes", ()) or ()),
        "neutral_max_new_names_when_weak": int(getattr(hypothesis, "neutral_max_new_names_when_weak", -1) or -1),
        "neutral_entry_min_breadth_up200": float(getattr(hypothesis, "neutral_entry_min_breadth_up200", -1.0) or -1.0),
        "neutral_entry_min_breadth_positive63": float(
            getattr(hypothesis, "neutral_entry_min_breadth_positive63", -1.0) or -1.0
        ),
        "neutral_max_positions_when_weak": int(getattr(hypothesis, "neutral_max_positions_when_weak", -1) or -1),
        "neutral_min_breadth_up200": float(getattr(hypothesis, "neutral_min_breadth_up200", -1.0) or -1.0),
        "neutral_min_breadth_positive63": float(getattr(hypothesis, "neutral_min_breadth_positive63", -1.0) or -1.0),
    }


def _load_seed_hypotheses(research: Any, names: tuple[str, ...]) -> list[Any]:
    available = {str(h.name): h for h in research._hypotheses()}
    missing = [name for name in names if name not in available]
    if missing:
        raise ValueError(f"Unknown stock hypotheses: {', '.join(missing)}")
    return [available[name] for name in names]


def _base_entry_gate(value: float, fallback: float) -> float:
    current = float(value)
    return current if current >= 0 else float(fallback)


def _base_entry_limit(value: int, fallback: int) -> int:
    current = int(value)
    return current if current >= 0 else int(fallback)


def _param_mutations(base: Any, round_index: int, config: LoopConfig) -> list[Any]:
    bonuses = _bounded_triplet(
        float(getattr(base, "pit_bonus", 0.0) or 0.0),
        step=0.02,
        round_index=round_index,
        lower=0.0,
        upper=0.12,
        override=config.bonus_override,
    )
    thresholds = _bounded_triplet(
        float(getattr(base, "pit_veto_threshold", -4.0) or -4.0),
        step=0.25,
        round_index=round_index,
        lower=-5.5,
        upper=-2.5,
        override=config.threshold_override,
    )
    return [replace(base, pit_bonus=bonus, pit_veto_threshold=threshold) for bonus in bonuses for threshold in thresholds]


def _risk_off_mutations(base: Any, round_index: int, config: LoopConfig) -> list[Any]:
    if config.risk_off_override:
        risk_off_values = _unique_ints(list(config.risk_off_override))
    else:
        current = int(getattr(base, "top_k_risk_off", 0) or 0)
        risk_off_values = _unique_ints([0, current, min(3, current + round_index)])
    out: list[Any] = []
    for top_k_risk_off in risk_off_values:
        out.append(replace(base, top_k_risk_off=top_k_risk_off, pit_veto_regimes=("neutral",)))
        if top_k_risk_off > 0:
            out.append(replace(base, top_k_risk_off=top_k_risk_off, pit_veto_regimes=("neutral", "risk_off")))
    return out


def _entry_freeze_mutations(base: Any, round_index: int, config: LoopConfig) -> list[Any]:
    if config.entry_freeze_override:
        entry_limits = _unique_ints(list(config.entry_freeze_override))
    else:
        current = _base_entry_limit(getattr(base, "neutral_max_new_names_when_weak", -1), 1)
        entry_limits = _unique_ints([0, current, min(3, current + round_index)])

    up200_base = _base_entry_gate(getattr(base, "neutral_entry_min_breadth_up200", -1.0), 0.50)
    pos63_base = _base_entry_gate(getattr(base, "neutral_entry_min_breadth_positive63", -1.0), 0.45)
    up200_values = _bounded_triplet(up200_base, step=0.05, round_index=round_index, lower=0.35, upper=0.70, override=())
    pos63_values = _bounded_triplet(pos63_base, step=0.05, round_index=round_index, lower=0.30, upper=0.65, override=())

    out: list[Any] = []
    for entry_limit in entry_limits:
        for up200 in up200_values:
            for pos63 in pos63_values:
                out.append(
                    replace(
                        base,
                        breadth_entry_source_mode="universe",
                        neutral_entry_min_breadth_up200=up200,
                        neutral_entry_min_breadth_positive63=pos63,
                        neutral_max_new_names_when_weak=entry_limit,
                    )
                )
    return out


def _breadth_cap_mutations(base: Any, round_index: int) -> list[Any]:
    current_cap = int(getattr(base, "neutral_max_positions_when_weak", -1) or -1)
    cap_values = _unique_ints([2, 3, current_cap, -1])
    up200_base = _base_entry_gate(getattr(base, "neutral_min_breadth_up200", -1.0), 0.50)
    pos63_base = _base_entry_gate(getattr(base, "neutral_min_breadth_positive63", -1.0), 0.45)
    up200_values = _bounded_triplet(up200_base, step=0.05, round_index=round_index, lower=0.35, upper=0.70, override=())
    pos63_values = _bounded_triplet(pos63_base, step=0.05, round_index=round_index, lower=0.30, upper=0.65, override=())

    out: list[Any] = []
    for cap in cap_values:
        for up200 in up200_values:
            for pos63 in pos63_values:
                out.append(
                    replace(
                        base,
                        breadth_source_mode="universe",
                        neutral_min_breadth_up200=up200,
                        neutral_min_breadth_positive63=pos63,
                        neutral_max_positions_when_weak=cap,
                    )
                )
    return out


def _portfolio_structure_mutations(base: Any, config: LoopConfig) -> list[Any]:
    if config.sector_cap_override:
        sector_caps = _unique_ints(list(config.sector_cap_override))
    else:
        current_cap = int(getattr(base, "max_per_sector", 2) or 2)
        sector_caps = _unique_ints([max(0, current_cap - 1), current_cap, current_cap + 1])

    if config.min_overlap_override:
        overlap_values = _unique_ints(list(config.min_overlap_override))
    else:
        current_overlap = int(getattr(base, "min_overlap", 4) or 4)
        overlap_values = _unique_ints([max(1, current_overlap - 1), current_overlap, current_overlap + 1])

    if config.weight_mode_override:
        weight_modes = tuple(
            mode for mode in config.weight_mode_override if mode in {"equal", "score", "inv_vol"}
        )
    else:
        current_mode = str(getattr(base, "weight_mode", "equal") or "equal").strip().lower() or "equal"
        weight_modes = tuple(
            mode for mode in (current_mode, "equal", "score", "inv_vol") if mode in {"equal", "score", "inv_vol"}
        )

    out: list[Any] = []
    for sector_cap in sector_caps:
        out.append(replace(base, max_per_sector=sector_cap))
    for min_overlap in overlap_values:
        out.append(replace(base, min_overlap=min_overlap))
    for weight_mode in weight_modes:
        out.append(replace(base, weight_mode=weight_mode))
    for weight_mode in weight_modes:
        for sector_cap in sector_caps:
            out.append(replace(base, weight_mode=weight_mode, max_per_sector=sector_cap))
    for weight_mode in weight_modes:
        for min_overlap in overlap_values:
            out.append(replace(base, weight_mode=weight_mode, min_overlap=min_overlap))
    return out


def _expand_frontier(frontier: list[Any], round_index: int, config: LoopConfig) -> list[Any]:
    pool: list[Any] = []
    for base in frontier:
        pool.append(base)
        pool.extend(_param_mutations(base, round_index, config))
        pool.extend(_entry_freeze_mutations(base, round_index, config))
        if round_index >= 2:
            pool.extend(_risk_off_mutations(base, round_index, config))
            pool.extend(_breadth_cap_mutations(base, round_index))
            pool.extend(_portfolio_structure_mutations(base, config))
    deduped: list[Any] = []
    seen: set[str] = set()
    for candidate in pool:
        signature = _candidate_signature(candidate)
        if signature in seen:
            continue
        seen.add(signature)
        deduped.append(candidate)
    return deduped


def _promotion_report_from_df(
    promotion_runner: Any,
    df: pd.DataFrame,
    full_summary: dict[str, Any],
    periods_per_year: int,
    config: LoopConfig,
) -> dict[str, Any]:
    horizon_checks = [
        promotion_runner._horizon_check(df, years=years, periods_per_year=periods_per_year) for years in (3, 5, 7)
    ]
    cagr_diff = float(full_summary.get("cagr_diff_pct", 0.0))
    p_alpha_gt0 = float(full_summary.get("nw_p_gt0", 0.0))
    mdd_diff = float(full_summary.get("mdd_diff_pct", 0.0))
    turnover_mean = float(full_summary.get("avg_turnover", 999.0))

    criterion_cost = bool(float(config.promotion_min_cost_bps) <= float(config.trade_cost_bps))
    criterion_full = bool(cagr_diff > 0.0 and p_alpha_gt0 >= float(config.promotion_min_p_alpha_gt0))
    criterion_mdd = bool(mdd_diff >= -float(config.promotion_max_mdd_worse_pctp))
    criterion_turnover = bool(turnover_mean <= 0.30)

    criteria = [
        {
            "name": "cost_at_least_20bps",
            "passes": criterion_cost,
            "detail": f"base_cost_bps={float(config.trade_cost_bps):.1f}",
        },
        {
            "name": "horizon_3y",
            "passes": bool(horizon_checks[0]["passes"]),
            "detail": f"cagr_diff={float(horizon_checks[0]['cagr_diff_pct']):+.2f}pp nw_p2={float(horizon_checks[0]['nw_p_two']):.3f}",
        },
        {
            "name": "horizon_5y",
            "passes": bool(horizon_checks[1]["passes"]),
            "detail": f"cagr_diff={float(horizon_checks[1]['cagr_diff_pct']):+.2f}pp nw_p2={float(horizon_checks[1]['nw_p_two']):.3f}",
        },
        {
            "name": "horizon_7y",
            "passes": bool(horizon_checks[2]["passes"]),
            "detail": f"cagr_diff={float(horizon_checks[2]['cagr_diff_pct']):+.2f}pp nw_p2={float(horizon_checks[2]['nw_p_two']):.3f}",
        },
        {
            "name": "full_window_alpha",
            "passes": criterion_full,
            "detail": f"cagr_diff={cagr_diff:+.2f}pp p_alpha_gt0={p_alpha_gt0:.3f}",
        },
        {
            "name": "drawdown_guardrail",
            "passes": criterion_mdd,
            "detail": f"mdd_diff={mdd_diff:+.2f}pp",
        },
        {
            "name": "turnover_guardrail",
            "passes": criterion_turnover,
            "detail": f"turnover_mean={turnover_mean:.3f}",
        },
    ]
    criteria_map = {item["name"]: bool(item["passes"]) for item in criteria}
    return {
        "headline": {
            "strategy_cagr_pct": float(full_summary.get("cagr_pct", 0.0)),
            "benchmark_cagr_pct": float(full_summary.get("benchmark_cagr_pct", 0.0)),
            "strategy_sharpe": float(full_summary.get("sharpe", 0.0)),
            "benchmark_sharpe": float(full_summary.get("benchmark_sharpe", 0.0)),
            "strategy_mdd_pct": float(full_summary.get("mdd_pct", 0.0)),
            "benchmark_mdd_pct": float(full_summary.get("benchmark_mdd_pct", 0.0)),
            "nw_p_two_sided": float(full_summary.get("nw_p_two", 1.0)),
            "p_alpha_gt0": p_alpha_gt0,
            "turnover_mean": turnover_mean,
        },
        "horizon_checks": horizon_checks,
        "criteria": criteria,
        "criteria_map": criteria_map,
        "criteria_pass_count": int(sum(1 for item in criteria if item["passes"])),
        "overall_pass": bool(all(item["passes"] for item in criteria)),
    }


def _evaluate_candidate(
    research: Any,
    promotion_runner: Any,
    bt: Any,
    records: list[dict[str, Any]],
    hypothesis: Any,
    config: LoopConfig,
) -> dict[str, Any]:
    df = research._evaluate(bt, records, hypothesis)
    periods_per_year = int(bt._periods_per_year(hypothesis.freq))
    full_summary = research._summarize(df, periods_per_year)
    oos_mask = pd.to_datetime(df["entry_day"], errors="coerce").dt.year >= int(config.oos_start_year)
    oos_df = df.loc[oos_mask].copy()
    fixed_oos = research._summarize(oos_df, periods_per_year) if not oos_df.empty else {}
    promotion = _promotion_report_from_df(
        promotion_runner=promotion_runner,
        df=df,
        full_summary=full_summary,
        periods_per_year=periods_per_year,
        config=config,
    )
    return {
        "hypothesis": hypothesis,
        "candidate_hash": _candidate_hash(hypothesis),
        "short_label": _short_label(hypothesis),
        "config": asdict(hypothesis),
        "brief_config": _brief_config(hypothesis),
        "results_df": df,
        "full": full_summary,
        "fixed_oos": fixed_oos,
        "promotion": promotion,
    }


def _ranking_key(row: dict[str, Any]) -> tuple[Any, ...]:
    promotion = row.get("promotion") or {}
    full = row.get("full") or {}
    fixed_oos = row.get("fixed_oos") or {}
    horizons = promotion.get("horizon_checks") or []
    worst_horizon_p = max(float(item.get("nw_p_two", 1.0)) for item in horizons) if horizons else 1.0
    worst_horizon_cagr = min(float(item.get("cagr_diff_pct", -999.0)) for item in horizons) if horizons else -999.0
    return (
        1 if bool(promotion.get("overall_pass")) else 0,
        int(promotion.get("criteria_pass_count", 0)),
        1 if bool((promotion.get("criteria_map") or {}).get("full_window_alpha")) else 0,
        1 if worst_horizon_cagr > 0 else 0,
        float(worst_horizon_cagr),
        -float(worst_horizon_p),
        float(fixed_oos.get("cagr_diff_pct", -999.0)),
        float(full.get("cagr_diff_pct", -999.0)),
        float(fixed_oos.get("mdd_diff_pct", -999.0)),
        float(full.get("mdd_diff_pct", -999.0)),
        float(full.get("sharpe", 0.0)) - float(full.get("benchmark_sharpe", 0.0)),
        -float(full.get("avg_turnover", 999.0)),
    )


def _serialize_row(row: dict[str, Any] | None) -> dict[str, Any]:
    if not row:
        return {}
    return {
        "candidate_hash": row.get("candidate_hash"),
        "short_label": row.get("short_label"),
        "hypothesis_name": str((row.get("hypothesis") or {}).name if row.get("hypothesis") is not None else ""),
        "config": row.get("config") or {},
        "brief_config": row.get("brief_config") or {},
        "full": row.get("full") or {},
        "fixed_oos": row.get("fixed_oos") or {},
        "promotion": row.get("promotion") or {},
    }


def _run_loop(
    research: Any,
    promotion_runner: Any,
    bt: Any,
    records: list[dict[str, Any]],
    frontier: list[Any],
    config: LoopConfig,
) -> dict[str, Any]:
    rounds: list[dict[str, Any]] = []
    all_rows: list[dict[str, Any]] = []
    round_lookup: dict[str, int] = {}
    seen: set[str] = set()
    winner: dict[str, Any] | None = None

    for round_index in range(1, int(config.max_rounds) + 1):
        pool = _expand_frontier(frontier, round_index, config)
        round_rows: list[dict[str, Any]] = []
        for candidate in pool:
            signature = _candidate_signature(candidate)
            if signature in seen:
                continue
            seen.add(signature)
            row = _evaluate_candidate(
                research=research,
                promotion_runner=promotion_runner,
                bt=bt,
                records=records,
                hypothesis=candidate,
                config=config,
            )
            round_lookup[str(row["candidate_hash"])] = int(round_index)
            round_rows.append(row)
        if not round_rows:
            break

        round_rows.sort(key=_ranking_key, reverse=True)
        leader = round_rows[0]
        top_rows = round_rows[: int(config.top_candidates_per_round)]
        rounds.append(
            {
                "round": int(round_index),
                "candidate_count": int(len(round_rows)),
                "leader": _serialize_row(leader),
                "top_candidates": [_serialize_row(item) for item in top_rows],
                "artifacts": _write_round_leader_artifacts(
                    config.run_tag,
                    round_index,
                    round_rows,
                    config.save_round_leaders,
                ),
            }
        )
        all_rows.extend(round_rows)
        if bool((leader.get("promotion") or {}).get("overall_pass")):
            winner = leader
            break
        frontier = [item["hypothesis"] for item in round_rows[: int(config.frontier_size)]]

    all_rows.sort(key=_ranking_key, reverse=True)
    return {
        "rounds": rounds,
        "all_rows": all_rows,
        "round_lookup": round_lookup,
        "winner": winner,
        "best": all_rows[0] if all_rows else None,
    }


def _leader_markdown(round_index: int, row: dict[str, Any]) -> str:
    full = row.get("full") or {}
    fixed_oos = row.get("fixed_oos") or {}
    promotion = row.get("promotion") or {}
    horizons = promotion.get("horizon_checks") or []
    lines = [
        "# Stock Hypothesis Promotion Loop Leader",
        "",
        f"- round: `{round_index}`",
        f"- candidate: `{row.get('short_label', '')}`",
        f"- overall pass: **{promotion.get('overall_pass', False)}**",
        f"- criteria passed: **{promotion.get('criteria_pass_count', 0)} / {len(promotion.get('criteria', []))}**",
        "",
        "## Headline",
        "",
        f"- full CAGR diff: {float(full.get('cagr_diff_pct', 0.0)):+.2f}pp",
        f"- fixed OOS CAGR diff: {float(fixed_oos.get('cagr_diff_pct', 0.0)):+.2f}pp",
        f"- full MDD diff: {float(full.get('mdd_diff_pct', 0.0)):+.2f}pp",
        f"- turnover: {float(full.get('avg_turnover', 0.0)):.3f}",
        "",
        "## Horizon Checks",
        "",
    ]
    for item in horizons:
        lines.append(
            f"- {int(item.get('years', 0))}y | pass={bool(item.get('passes', False))} "
            f"| cagr diff {float(item.get('cagr_diff_pct', 0.0)):+.2f}pp "
            f"| nw p2 {float(item.get('nw_p_two', 1.0)):.3f}"
        )
    lines.append("")
    return "\n".join(lines)


def _write_round_leader_artifacts(run_tag: str, round_index: int, rows: list[dict[str, Any]], limit: int) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    for row in rows[: max(0, int(limit))]:
        candidate_hash = str(row.get("candidate_hash", "candidate"))
        prefix = f"{run_tag}__round{int(round_index):02d}__{candidate_hash}"
        csv_path = RUNS_DIR / f"{prefix}.csv"
        json_path = RUNS_DIR / f"{prefix}.json"
        md_path = RUNS_DIR / f"{prefix}.md"
        row["results_df"].to_csv(csv_path, index=False)
        payload = {
            "round": int(round_index),
            "candidate_hash": candidate_hash,
            "short_label": row.get("short_label", ""),
            "config": row.get("config") or {},
            "full": row.get("full") or {},
            "fixed_oos": row.get("fixed_oos") or {},
            "promotion": row.get("promotion") or {},
        }
        json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        md_path.write_text(_leader_markdown(round_index, row), encoding="utf-8")
        out.append(
            {
                "csv": str(csv_path.relative_to(ROOT)),
                "json": str(json_path.relative_to(ROOT)),
                "md": str(md_path.relative_to(ROOT)),
            }
        )
    return out


def _flatten_row(round_index: int, row: dict[str, Any]) -> dict[str, Any]:
    full = row.get("full") or {}
    fixed_oos = row.get("fixed_oos") or {}
    promotion = row.get("promotion") or {}
    horizons = {int(item.get("years", 0)): item for item in promotion.get("horizon_checks") or []}
    brief = row.get("brief_config") or {}
    return {
        "round": int(round_index),
        "candidate_hash": row.get("candidate_hash"),
        "short_label": row.get("short_label"),
        "hypothesis_name": (row.get("hypothesis") or {}).name if row.get("hypothesis") is not None else "",
        "criteria_pass_count": int(promotion.get("criteria_pass_count", 0)),
        "overall_pass": bool(promotion.get("overall_pass", False)),
        "full_cagr_diff_pct": float(full.get("cagr_diff_pct", 0.0)),
        "full_mdd_diff_pct": float(full.get("mdd_diff_pct", 0.0)),
        "full_sharpe_diff": float(full.get("sharpe", 0.0)) - float(full.get("benchmark_sharpe", 0.0)),
        "full_turnover": float(full.get("avg_turnover", 0.0)),
        "fixed_oos_cagr_diff_pct": float(fixed_oos.get("cagr_diff_pct", 0.0)),
        "fixed_oos_mdd_diff_pct": float(fixed_oos.get("mdd_diff_pct", 0.0)),
        "fixed_oos_sharpe_diff": float(fixed_oos.get("sharpe", 0.0)) - float(fixed_oos.get("benchmark_sharpe", 0.0)),
        "full_p_alpha_gt0": float((promotion.get("headline") or {}).get("p_alpha_gt0", 0.0)),
        "full_nw_p_two_sided": float((promotion.get("headline") or {}).get("nw_p_two_sided", 1.0)),
        "pit_bonus": float(brief.get("pit_bonus", 0.0)),
        "pit_veto_threshold": float(brief.get("pit_veto_threshold", 0.0)),
        "top_k_risk_off": int(brief.get("top_k_risk_off", 0)),
        "pit_veto_regimes": ",".join(str(x) for x in brief.get("pit_veto_regimes", [])),
        "neutral_max_new_names_when_weak": int(brief.get("neutral_max_new_names_when_weak", -1)),
        "neutral_entry_min_breadth_up200": float(brief.get("neutral_entry_min_breadth_up200", -1.0)),
        "neutral_entry_min_breadth_positive63": float(brief.get("neutral_entry_min_breadth_positive63", -1.0)),
        "neutral_max_positions_when_weak": int(brief.get("neutral_max_positions_when_weak", -1)),
        "neutral_min_breadth_up200": float(brief.get("neutral_min_breadth_up200", -1.0)),
        "neutral_min_breadth_positive63": float(brief.get("neutral_min_breadth_positive63", -1.0)),
        "horizon_3y_pass": bool((horizons.get(3) or {}).get("passes", False)),
        "horizon_3y_cagr_diff_pct": float((horizons.get(3) or {}).get("cagr_diff_pct", 0.0)),
        "horizon_3y_nw_p_two": float((horizons.get(3) or {}).get("nw_p_two", 1.0)),
        "horizon_5y_pass": bool((horizons.get(5) or {}).get("passes", False)),
        "horizon_5y_cagr_diff_pct": float((horizons.get(5) or {}).get("cagr_diff_pct", 0.0)),
        "horizon_5y_nw_p_two": float((horizons.get(5) or {}).get("nw_p_two", 1.0)),
        "horizon_7y_pass": bool((horizons.get(7) or {}).get("passes", False)),
        "horizon_7y_cagr_diff_pct": float((horizons.get(7) or {}).get("cagr_diff_pct", 0.0)),
        "horizon_7y_nw_p_two": float((horizons.get(7) or {}).get("nw_p_two", 1.0)),
    }


def _build_markdown(summary: dict[str, Any], csv_rel: str) -> str:
    winner = summary.get("winner") or {}
    best = summary.get("best") or {}
    lines = [
        "# Stock Hypothesis Promotion Loop",
        "",
        f"- run tag: `{summary['run_tag']}`",
        f"- seeds: `{', '.join(summary['inputs']['seed_names'])}`",
        f"- rounds run: **{len(summary.get('rounds') or [])}**",
        f"- winner found: **{summary.get('winner_found', False)}**",
        f"- csv: `{csv_rel}`",
        "",
    ]
    if winner:
        promotion = winner.get("promotion") or {}
        full = winner.get("full") or {}
        lines.extend(
            [
                "## Winner",
                "",
                f"- `{winner.get('short_label', '')}`",
                f"- criteria passed: **{promotion.get('criteria_pass_count', 0)} / {len(promotion.get('criteria', []))}**",
                f"- full CAGR diff {float(full.get('cagr_diff_pct', 0.0)):+.2f}pp | full MDD diff {float(full.get('mdd_diff_pct', 0.0)):+.2f}pp",
                "",
            ]
        )
    elif best:
        promotion = best.get("promotion") or {}
        full = best.get("full") or {}
        lines.extend(
            [
                "## Best So Far",
                "",
                f"- `{best.get('short_label', '')}`",
                f"- criteria passed: **{promotion.get('criteria_pass_count', 0)} / {len(promotion.get('criteria', []))}**",
                f"- full CAGR diff {float(full.get('cagr_diff_pct', 0.0)):+.2f}pp | full MDD diff {float(full.get('mdd_diff_pct', 0.0)):+.2f}pp",
                "",
            ]
        )

    lines.extend(["## Round Leaders", ""])
    for round_summary in summary.get("rounds") or []:
        leader = round_summary.get("leader") or {}
        full = leader.get("full") or {}
        promotion = leader.get("promotion") or {}
        lines.append(
            f"- round {int(round_summary.get('round', 0))}: `{leader.get('short_label', '')}` "
            f"| pass {promotion.get('criteria_pass_count', 0)}/{len(promotion.get('criteria', []))} "
            f"| overall {promotion.get('overall_pass', False)} "
            f"| full diff {float(full.get('cagr_diff_pct', 0.0)):+.2f}pp"
        )

    flat_rows = pd.DataFrame(summary.get("ranking_rows") or [])
    if not flat_rows.empty:
        cols = [
            "round",
            "short_label",
            "criteria_pass_count",
            "overall_pass",
            "full_cagr_diff_pct",
            "fixed_oos_cagr_diff_pct",
            "full_mdd_diff_pct",
            "horizon_3y_nw_p_two",
            "horizon_5y_nw_p_two",
            "horizon_7y_nw_p_two",
        ]
        flat_rows = flat_rows[cols].head(10)
        lines.extend(["", "## Final Ranking", "", "```text", flat_rows.to_string(index=False), "```", ""])
    return "\n".join(lines).strip() + "\n"


def main() -> None:
    config = _load_config()
    os.environ["HYP_START_DATE"] = config.start_date
    os.environ["HYP_END_DATE"] = config.end_date
    os.environ["HYP_OOS_START_YEAR"] = str(int(config.oos_start_year))
    os.environ["HYP_TRADE_COST_BPS"] = str(float(config.trade_cost_bps))

    research = _load_module(RESEARCH_SCRIPT, f"research_stock_hypotheses_promo_loop_{config.run_tag}")
    promotion_runner = _load_module(PROMOTION_SCRIPT, f"run_strategy_promotion_check_{config.run_tag}")
    frontier = _load_seed_hypotheses(research, config.seed_names)
    if {str(h.freq) for h in frontier} != {"weekly"}:
        raise ValueError("Promotion loop currently supports weekly stock hypotheses only")

    bt = research._load_bt_module("weekly")
    records, meta = research._build_records(bt)
    if not records:
        raise RuntimeError("No weekly stock-hypothesis records available")

    print("Running stock hypothesis promotion loop...")
    print(f"  STOCK_PROMO_LOOP_TAG={config.run_tag}")
    print(f"  STOCK_PROMO_LOOP_SEEDS={','.join(config.seed_names)}")
    print(f"  STOCK_PROMO_LOOP_MAX_ROUNDS={config.max_rounds}")
    print(f"  STOCK_PROMO_LOOP_FRONTIER_SIZE={config.frontier_size}")
    print(f"  HYP_START_DATE={config.start_date}")
    print(f"  HYP_END_DATE={config.end_date}")

    loop_out = _run_loop(
        research=research,
        promotion_runner=promotion_runner,
        bt=bt,
        records=records,
        frontier=frontier,
        config=config,
    )
    round_lookup = loop_out.get("round_lookup") or {}
    all_rows = loop_out.get("all_rows") or []
    winner = loop_out.get("winner")
    best = loop_out.get("best")

    ranking_rows = [_flatten_row(int(round_lookup.get(str(row.get("candidate_hash", "")), 0)), row) for row in all_rows]
    summary = {
        "run_tag": config.run_tag,
        "inputs": {
            "seed_names": list(config.seed_names),
            "start_date": config.start_date,
            "end_date": config.end_date,
            "oos_start_year": int(config.oos_start_year),
            "trade_cost_bps": float(config.trade_cost_bps),
            "max_rounds": int(config.max_rounds),
            "frontier_size": int(config.frontier_size),
            "top_candidates_per_round": int(config.top_candidates_per_round),
            "promotion_min_cost_bps": float(config.promotion_min_cost_bps),
            "promotion_max_mdd_worse_pctp": float(config.promotion_max_mdd_worse_pctp),
            "promotion_min_p_alpha_gt0": float(config.promotion_min_p_alpha_gt0),
            "records_meta": meta,
        },
        "rounds": loop_out.get("rounds") or [],
        "winner_found": bool(winner),
        "winner": _serialize_row(winner),
        "best": _serialize_row(best),
        "ranking_rows": ranking_rows,
    }

    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    json_path = RUNS_DIR / f"{config.run_tag}.json"
    csv_path = RUNS_DIR / f"{config.run_tag}.csv"
    md_path = RUNS_DIR / f"{config.run_tag}.md"
    pd.DataFrame(ranking_rows).to_csv(csv_path, index=False)
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_build_markdown(summary, str(csv_path.relative_to(ROOT))), encoding="utf-8")

    print(f"Saved: {json_path.relative_to(ROOT)}")
    print(f"Saved: {csv_path.relative_to(ROOT)}")
    print(f"Saved: {md_path.relative_to(ROOT)}")
    if winner:
        print(
            "Winner -> "
            f"{winner['short_label']} | criteria_passed={winner['promotion']['criteria_pass_count']} "
            f"| full_diff={float((winner.get('full') or {}).get('cagr_diff_pct', 0.0)):+.2f}pp"
        )
    elif best:
        print(
            "Best so far -> "
            f"{best['short_label']} | criteria_passed={best['promotion']['criteria_pass_count']} "
            f"| full_diff={float((best.get('full') or {}).get('cagr_diff_pct', 0.0)):+.2f}pp"
        )


if __name__ == "__main__":
    main()
