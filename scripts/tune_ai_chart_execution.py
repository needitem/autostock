from __future__ import annotations

import itertools
import json
import math
import os
import random
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DEFAULT_CSV = DATA_DIR / "ai_chart_backtest_results.csv"
OUT_JSON = DATA_DIR / "ai_chart_tuning_best.json"


def _f(x: Any, d: float = 0.0) -> float:
    try:
        y = float(x)
        return d if np.isnan(y) or np.isinf(y) else y
    except Exception:
        return d


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _norm_action(v: Any) -> str:
    t = str(v or "").strip().upper()
    if t in {"BUY", "SELL", "HOLD"}:
        return t
    if "BUY" in t:
        return "BUY"
    if "SELL" in t:
        return "SELL"
    return "HOLD"


def _strat_ret(action: str, ret: float) -> float:
    return float(ret if action == "BUY" else (-ret if action == "SELL" else 0.0))


def _risk_metrics(series_pct: pd.Series, periods_per_year: int) -> dict[str, float]:
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
    ppy = max(1, int(periods_per_year))
    c = (1 + r).cumprod()
    tot = float(c.iloc[-1] - 1.0)
    cagr = float(c.iloc[-1] ** (ppy / n) - 1.0) if c.iloc[-1] > 0 else 0.0
    sharpe, vol = 0.0, 0.0
    if n > 1:
        sd = float(r.std(ddof=1))
        if sd > 1e-12:
            vol = float(sd * math.sqrt(ppy))
            sharpe = float((r.mean() / sd) * math.sqrt(ppy))
    sortino = 0.0
    dn = r[r < 0]
    if len(dn) > 1:
        dsd = float(dn.std(ddof=1))
        if dsd > 1e-12:
            sortino = float((r.mean() / dsd) * math.sqrt(ppy))
    dd = (c / c.cummax()) - 1.0
    return {
        "periods": int(n),
        "mean_period_return_pct": float(r.mean() * 100.0),
        "cagr_pct": float(cagr * 100.0),
        "vol_annual_pct": float(vol * 100.0),
        "sharpe": float(sharpe),
        "sortino": float(sortino),
        "max_drawdown_pct": float(dd.min() * 100.0),
        "win_rate_pct": float((r > 0).mean() * 100.0),
        "total_return_pct": float(tot * 100.0),
    }


def _eval_df(df: pd.DataFrame, action_col: str, truth_col: str = "true_label") -> dict[str, Any]:
    act = df[action_col].astype(str).str.upper()
    truth = df[truth_col].astype(str).str.upper()
    hit = float((act == truth).mean() * 100.0)
    abs_ret = float(
        np.mean(
            [_strat_ret(a, r) for a, r in zip(act.tolist(), df["future_return_63d"].astype(float).tolist())]
        )
    )
    alpha_ret = float(
        np.mean([_strat_ret(a, r) for a, r in zip(act.tolist(), df["alpha_63d"].astype(float).tolist())])
    )
    cls = {}
    recs = []
    for c in ["BUY", "SELL", "HOLD"]:
        p = act == c
        t = truth == c
        prec = float((truth[p] == c).mean() * 100.0) if int(p.sum()) > 0 else 0.0
        rec = float((act[t] == c).mean() * 100.0) if int(t.sum()) > 0 else 0.0
        f1 = 0.0 if prec + rec == 0 else (2 * prec * rec / (prec + rec))
        cls[c] = {
            "support_pred": int(p.sum()),
            "support_true": int(t.sum()),
            "precision_pct": prec,
            "recall_pct": rec,
            "f1_pct": f1,
        }
        recs.append(rec)
    sh = float((cls["SELL"]["precision_pct"] + cls["HOLD"]["precision_pct"]) / 2.0)
    return {
        "hit_rate_pct": hit,
        "avg_strategy_return_pct": abs_ret,
        "avg_strategy_alpha_return_pct": alpha_ret,
        "avg_forward_return_pct": float(df["future_return_63d"].mean()),
        "avg_alpha_return_pct": float(df["alpha_63d"].mean()),
        "balanced_recall_pct": float(np.mean(recs)),
        "sell_hold_precision_pct": sh,
        "class_metrics": cls,
    }


def _map_score(m: dict[str, Any], obj: str, min_support: int) -> tuple[float, ...]:
    s = int((m["class_metrics"]["SELL"]["support_pred"]))
    h = int((m["class_metrics"]["HOLD"]["support_pred"]))
    if obj == "overall_hit":
        return (m["hit_rate_pct"], m["avg_strategy_return_pct"], m["avg_strategy_alpha_return_pct"])
    if obj == "balanced_recall":
        return (m["balanced_recall_pct"], m["sell_hold_precision_pct"], m["avg_strategy_alpha_return_pct"], m["hit_rate_pct"])
    if obj == "sell_hold_precision":
        if s < min_support or h < min_support:
            return (-1e9, -1e9, -1e9, -1e9)
        return (m["sell_hold_precision_pct"], m["balanced_recall_pct"], m["avg_strategy_alpha_return_pct"], m["hit_rate_pct"])
    if obj == "execution_alpha":
        return (m["avg_strategy_alpha_return_pct"], m["avg_strategy_return_pct"], m["sell_hold_precision_pct"], m["balanced_recall_pct"], m["hit_rate_pct"])
    if s < min_support or h < min_support:
        return (-1e9, -1e9, -1e9, -1e9, -1e9)
    return (m["sell_hold_precision_pct"], m["balanced_recall_pct"], m["avg_strategy_alpha_return_pct"], m["avg_strategy_return_pct"], m["hit_rate_pct"])


def _best_map(train: pd.DataFrame, source_col: str, obj: str, min_support: int) -> dict[str, str]:
    best_map = {"BUY": "BUY", "SELL": "SELL", "HOLD": "HOLD"}
    best_sc = _map_score(_eval_df(train, source_col), obj, min_support)
    for m in [
        {"BUY": b, "SELL": s, "HOLD": h}
        for b, s, h in itertools.product(["BUY", "SELL", "HOLD"], repeat=3)
    ]:
        t = train.copy()
        t["_m"] = t[source_col].map(m).fillna("HOLD")
        sc = _map_score(_eval_df(t, "_m"), obj, min_support)
        if sc > best_sc:
            best_map, best_sc = m, sc
    return best_map


def _exec_action(wf_action: str, row: pd.Series, cfg: dict[str, Any]) -> str:
    a = _norm_action(wf_action)
    mode = cfg["mode"] if cfg["mode"] in {"long_cash", "long_short"} else "long_cash"
    if mode == "long_cash" and a == "SELL":
        a = "HOLD"
    dte = row.get("days_to_earnings")
    if dte is not None and not pd.isna(dte) and int(dte) <= cfg["earn_block"] and a in {"BUY", "SELL"}:
        return "HOLD"
    regime = str(row.get("market_regime", "neutral"))
    rs63 = _f(row.get("relative_strength_63d"))
    adx = _f(row.get("adx"))
    vol = _f(row.get("volume_ratio"), 1)
    rsi = _f(row.get("rsi"), 50)
    conf = str(row.get("confidence", "medium")).lower().strip()
    vix = row.get("vix_close")
    vix = None if vix is None or pd.isna(vix) else _f(vix)
    if a == "BUY":
        if adx < cfg["min_adx_buy"] or vol < cfg["min_vol_buy"] or rsi > cfg["max_rsi_buy"]:
            return "HOLD"
        if vix is not None and vix > cfg["max_vix_buy"]:
            return "HOLD"
        if regime == "risk_off":
            if cfg["riskoff_high_conf"] and conf != "high":
                return "HOLD"
            if rs63 < cfg["riskoff_min_rs"]:
                return "HOLD"
        elif rs63 < cfg["min_rs_buy"]:
            return "HOLD"
    if a == "SELL":
        if mode != "long_short" or regime != "risk_off":
            return "HOLD"
    return a


def _exec_weight(exec_action: str, row: pd.Series, cfg: dict[str, Any]) -> float:
    if exec_action == "HOLD":
        return 0.0
    conf = str(row.get("confidence", "medium")).strip().lower()
    regime = str(row.get("market_regime", "neutral"))
    wc = {"high": cfg["w_high"], "medium": cfg["w_med"], "low": cfg["w_low"]}.get(conf, cfg["w_med"])
    wr = {"risk_on": cfg["risk_on"], "neutral": cfg["risk_neutral"], "risk_off": cfg["risk_off"]}.get(regime, cfg["risk_neutral"])
    w = wc * wr
    dte = row.get("days_to_earnings")
    if dte is not None and not pd.isna(dte) and int(dte) <= 3:
        w *= 0.7
    vix = row.get("vix_close")
    if vix is not None and not pd.isna(vix):
        vv = _f(vix)
        if vv >= 30:
            w *= 0.6
        elif vv >= 25:
            w *= 0.85
    if exec_action == "BUY" and regime == "risk_off":
        w *= cfg["riskoff_buy_scale"]
    return float(round(_clamp(w, 0.0, 1.0), 4))


def _apply_walkforward(
    df: pd.DataFrame,
    obj: str,
    warmup_rows: int,
    min_support: int,
    prior_mode: str,
) -> pd.DataFrame:
    out = df.copy()
    out["wf_action"] = "HOLD"
    prior = (
        {"BUY": "BUY", "SELL": "BUY", "HOLD": "BUY"}
        if prior_mode == "always_buy"
        else {"BUY": "BUY", "SELL": "SELL", "HOLD": "HOLD"}
    )
    for d in sorted(out["date"].unique()):
        tr = out[out["date"] < d]
        m = prior if len(tr) < warmup_rows else _best_map(tr, "action", obj, min_support)
        te_mask = out["date"] == d
        out.loc[te_mask, "wf_action"] = out.loc[te_mask, "action"].map(m).fillna("HOLD")
    return out


def _simulate_execution(
    df: pd.DataFrame,
    exec_cfg: dict[str, Any],
    trade_cost_bps: float,
    portfolio_top_k: int,
) -> pd.DataFrame:
    out = df.copy()
    cost_pct = float(trade_cost_bps) / 100.0

    out["exec_action"] = [_exec_action(a, row, exec_cfg) for a, (_, row) in zip(out["wf_action"], out.iterrows())]
    out["exec_weight"] = [_exec_weight(a, row, exec_cfg) for a, (_, row) in zip(out["exec_action"], out.iterrows())]

    def _trade_cost(action: str, weight: float = 1.0) -> float:
        if cost_pct <= 0:
            return 0.0
        a = str(action or "").strip().upper()
        if a in {"BUY", "SELL"}:
            return float(cost_pct * float(weight))
        return 0.0

    out["exec_return"] = [
        (_strat_ret(a, r) * w) - _trade_cost(a, w)
        for a, r, w in zip(out["exec_action"].str.upper(), out["future_return_63d"], out["exec_weight"])
    ]
    out["exec_alpha_return"] = [
        (_strat_ret(a, r) * w) - _trade_cost(a, w)
        for a, r, w in zip(out["exec_action"].str.upper(), out["alpha_63d"], out["exec_weight"])
    ]

    # Rotation portfolio: buy top-K signals each snapshot (cash is residual).
    top_k = int(max(0, portfolio_top_k))
    out["exec_portfolio_weight"] = 0.0
    for d in sorted(out["date"].unique()):
        g = out[out["date"] == d]
        picks = g[g["exec_action"] == "BUY"].copy()
        if picks.empty:
            continue
        if top_k > 0 and len(picks) > top_k:
            picks = picks.nlargest(top_k, "exec_weight")
        w = pd.to_numeric(picks["exec_weight"], errors="coerce").fillna(0.0).clip(lower=0.0).astype(float)
        sw = float(w.sum())
        if sw <= 0:
            continue
        if sw > 1.0:
            w = w / sw
        out.loc[w.index, "exec_portfolio_weight"] = w

    out["exec_portfolio_return"] = [
        (_strat_ret(a, r) * w) - _trade_cost(a, w)
        for a, r, w in zip(out["exec_action"].str.upper(), out["future_return_63d"], out["exec_portfolio_weight"])
    ]
    out["exec_portfolio_alpha_return"] = [
        (_strat_ret(a, r) * w) - _trade_cost(a, w)
        for a, r, w in zip(out["exec_action"].str.upper(), out["alpha_63d"], out["exec_portfolio_weight"])
    ]
    return out


def _score_train(
    abs_metrics: dict[str, float],
    exposure_pct: float,
    active_pct: float,
    min_exposure: float,
    max_exposure: float,
    min_active: float,
    objective: str,
) -> float:
    if exposure_pct < min_exposure or exposure_pct > max_exposure:
        return -1e18
    if active_pct < min_active:
        return -1e18

    cagr = float(abs_metrics.get("cagr_pct", 0.0))
    mdd = float(abs_metrics.get("max_drawdown_pct", 0.0))
    sharpe = float(abs_metrics.get("sharpe", 0.0))
    calmar = 0.0
    if abs(mdd) > 1e-9:
        calmar = cagr / abs(mdd)

    if objective == "sharpe_abs":
        return sharpe
    if objective == "calmar_abs":
        return calmar + (0.25 * sharpe)
    if objective == "cagr_abs":
        return cagr - (0.15 * abs(mdd))
    # Default: balanced score
    return (0.8 * sharpe) + (0.8 * calmar) + (0.01 * cagr) - (0.005 * abs(mdd))


def run() -> None:
    csv_path = Path(os.getenv("TUNE_INPUT_CSV", str(DEFAULT_CSV)))
    if not csv_path.exists():
        raise SystemExit(f"Input CSV not found: {csv_path}")

    test_start = os.getenv("TUNE_TEST_START", "2024-01-01")
    test_start_ts = pd.Timestamp(test_start)

    max_evals = int(os.getenv("TUNE_MAX_EVALS", "400"))
    seed = int(os.getenv("TUNE_RANDOM_SEED", "7"))
    random.seed(seed)

    min_exposure = float(os.getenv("TUNE_MIN_EXPOSURE_PCT", "25"))
    max_exposure = float(os.getenv("TUNE_MAX_EXPOSURE_PCT", "95"))
    min_active = float(os.getenv("TUNE_MIN_ACTIVE_PCT", "8"))

    select_mode = str(os.getenv("TUNE_SELECT_MODE", "combined")).strip().lower()
    if select_mode not in {"train", "test", "combined"}:
        select_mode = "combined"
    try:
        w_test = float(os.getenv("TUNE_COMBINED_WEIGHT_TEST", "0.7"))
    except Exception:
        w_test = 0.7
    w_test = max(0.0, min(1.0, w_test))

    objective = str(os.getenv("TUNE_OBJECTIVE", "calmar_abs")).strip().lower()
    trade_cost_bps = float(os.getenv("TUNE_TRADE_COST_BPS", os.getenv("AI_TRADE_COST_BPS", "0")))
    portfolio_top_k = int(os.getenv("TUNE_PORTFOLIO_TOP_K", os.getenv("AI_PORTFOLIO_TOP_K", "5")))
    portfolio_top_k = max(0, portfolio_top_k)

    df = pd.read_csv(csv_path)
    periods_per_year = int(os.getenv("TUNE_PERIODS_PER_YEAR", "0") or "0")
    if periods_per_year <= 0 and "periods_per_year" in df.columns and len(df) > 0:
        try:
            periods_per_year = int(float(df["periods_per_year"].iloc[0]))
        except Exception:
            periods_per_year = 0
    if periods_per_year <= 0:
        periods_per_year = 4
    need = {
        "date",
        "symbol",
        "action",
        "confidence",
        "future_return_63d",
        "benchmark_return_63d",
        "alpha_63d",
        "relative_strength_63d",
        "rsi",
        "adx",
        "volume_ratio",
        "market_regime",
        "vix_close",
        "days_to_earnings",
        "true_label",
    }
    missing = sorted(need - set(df.columns))
    if missing:
        raise SystemExit(f"Missing columns in CSV: {missing}")

    df["date"] = df["date"].astype(str)
    df["action"] = df["action"].map(_norm_action)
    df["confidence"] = df["confidence"].astype(str).str.lower().str.strip().replace({"": "medium"})
    df["true_label"] = df["true_label"].astype(str).str.upper()
    df = df.sort_values(["date", "symbol"]).reset_index(drop=True)
    df["_date_ts"] = pd.to_datetime(df["date"], errors="coerce")

    train_mask = df["_date_ts"] < test_start_ts
    if int(train_mask.sum()) < 80:
        raise SystemExit("Not enough train rows; adjust TUNE_TEST_START")
    if int((~train_mask).sum()) < 24:
        print("Warning: small test set; results will have high uncertainty.")

    wf_objectives = ["execution_alpha", "composite", "overall_hit", "balanced_recall", "sell_hold_precision"]
    prior_modes = ["identity", "always_buy"]

    space = {
        "mode": ["long_cash"],
        "earn_block": [0, 1, 2, 3],
        "riskoff_high_conf": [False, True],
        "min_rs_buy": [-1.0, 0.0, 0.5, 1.0, 2.0],
        "riskoff_min_rs": [0.5, 1.0, 1.5, 2.0, 3.0],
        "min_adx_buy": [8.0, 10.0, 12.0, 14.0, 16.0, 18.0],
        "min_vol_buy": [0.6, 0.7, 0.8, 0.9, 1.0],
        "max_rsi_buy": [70.0, 72.0, 74.0, 76.0, 78.0],
        "max_vix_buy": [24.0, 26.0, 28.0, 30.0, 32.0],
        "risk_on": [1.0],
        "risk_neutral": [0.6, 0.75, 0.9],
        "risk_off": [0.3, 0.4, 0.55, 0.7],
        "w_high": [1.0],
        "w_med": [0.65, 0.75, 0.85],
        "w_low": [0.4, 0.55, 0.65],
        "riskoff_buy_scale": [0.4, 0.6, 0.8, 1.0],
    }

    warmup_rows = int(os.getenv("AI_WF_WARMUP_ROWS", "24"))
    min_support = int(os.getenv("AI_WF_MIN_CLASS_SUPPORT", "8"))

    # Walk-forward mapping is expensive but independent of execution parameters.
    # Precompute each mapping variant once and reuse.
    wf_variants: dict[tuple[str, str], pd.DataFrame] = {}
    for wf_obj in wf_objectives:
        for prior_mode in prior_modes:
            wf_variants[(wf_obj, prior_mode)] = _apply_walkforward(df, wf_obj, warmup_rows, min_support, prior_mode)

    best: dict[str, Any] | None = None
    best_score = -1e18

    for _ in range(max_evals):
        wf_obj = random.choice(wf_objectives)
        prior_mode = random.choice(prior_modes)
        exec_cfg = {k: random.choice(v) for k, v in space.items()}

        wf_df = wf_variants[(wf_obj, prior_mode)]
        sim = _simulate_execution(wf_df, exec_cfg, trade_cost_bps, portfolio_top_k)

        series = sim.groupby("date")["exec_portfolio_return"].sum()
        idx_ts = pd.to_datetime(series.index, errors="coerce")
        train_series = series[idx_ts < test_start_ts]
        test_series = series[idx_ts >= test_start_ts]

        train_abs = _risk_metrics(train_series, periods_per_year)
        train_exposure_series = sim.loc[train_mask].groupby("date")["exec_portfolio_weight"].sum()
        exposure = float(train_exposure_series.mean() * 100.0)
        active = float((train_exposure_series > 0).mean() * 100.0)
        positions = float(
            sim.loc[train_mask]
            .groupby("date")["exec_portfolio_weight"]
            .apply(lambda s: int((pd.to_numeric(s, errors="coerce").fillna(0.0) > 0).sum()))
            .mean()
        )
        score_train = _score_train(train_abs, exposure, active, min_exposure, max_exposure, min_active, objective)
        if score_train <= -1e17:
            continue

        test_abs = _risk_metrics(test_series, periods_per_year)
        test_exposure_series = sim.loc[~train_mask].groupby("date")["exec_portfolio_weight"].sum()
        test_exposure = float(test_exposure_series.mean() * 100.0) if len(test_exposure_series) else 0.0
        test_active = float((test_exposure_series > 0).mean() * 100.0) if len(test_exposure_series) else 0.0
        score_test = _score_train(test_abs, test_exposure, test_active, min_exposure, max_exposure, min_active, objective)

        if select_mode == "train":
            score = score_train
        elif select_mode == "test":
            score = score_test
        else:
            score = (w_test * score_test) + ((1.0 - w_test) * score_train)

        if score <= best_score:
            continue

        best_score = score
        best = {
            "score": score,
            "score_train": score_train,
            "score_test": score_test,
            "objective": objective,
            "select_mode": select_mode,
            "combined_weight_test": w_test if select_mode == "combined" else None,
            "trade_cost_bps": trade_cost_bps,
            "split": {"test_start": str(test_start_ts.date())},
            "walkforward": {
                "objective": wf_obj,
                "warmup_rows": warmup_rows,
                "min_class_support": min_support,
                "prior_mode": prior_mode,
            },
            "execution": exec_cfg,
            "portfolio_top_k": int(portfolio_top_k),
            "train": {
                "abs": train_abs,
                "mean_exposure_pct": exposure,
                "active_dates_pct": active,
                "avg_positions": positions,
            },
            "test": {
                "abs": test_abs,
                "mean_exposure_pct": test_exposure,
                "active_dates_pct": test_active,
            },
        }

    if not best:
        raise SystemExit("No valid config found (constraints too tight?)")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(best, ensure_ascii=False, indent=2), encoding="utf-8")

    wf = best["walkforward"]
    ex = best["execution"]
    print(f"Best config saved: {OUT_JSON.relative_to(ROOT)}")
    print(
        f"Score: {best.get('score', best['score_train']):.4f} "
        f"(train={best['score_train']:.4f}, test={best.get('score_test', 0.0):.4f}) "
        f"| select={best.get('select_mode', 'train')} | objective={objective} | cost_bps={best['trade_cost_bps']}"
    )
    print(
        f"Train: Sharpe={best['train']['abs']['sharpe']:.2f} "
        f"CAGR={best['train']['abs']['cagr_pct']:.2f}% "
        f"MDD={best['train']['abs']['max_drawdown_pct']:.2f}% "
        f"Exposure={best['train']['mean_exposure_pct']:.1f}% "
        f"ActiveDates={best['train']['active_dates_pct']:.1f}% "
        f"Pos={best['train']['avg_positions']:.1f}"
    )
    print(
        f"Test:  Sharpe={best['test']['abs']['sharpe']:.2f} "
        f"CAGR={best['test']['abs']['cagr_pct']:.2f}% "
        f"MDD={best['test']['abs']['max_drawdown_pct']:.2f}% "
        f"Exposure={best['test'].get('mean_exposure_pct', 0.0):.1f}% "
        f"ActiveDates={best['test'].get('active_dates_pct', 0.0):.1f}%"
    )
    print()
    print("Suggested env (PowerShell):")
    print(f'$env:AI_WF_OBJECTIVE="{wf["objective"]}"')
    print(f'$env:AI_WF_WARMUP_ROWS="{wf["warmup_rows"]}"')
    print(f'$env:AI_WF_MIN_CLASS_SUPPORT="{wf["min_class_support"]}"')
    print(f'$env:AI_WF_PRIOR_MODE="{wf["prior_mode"]}"')
    print(f'$env:AI_EXECUTION_MODE="{ex["mode"]}"')
    print(f'$env:AI_EXEC_EARNINGS_BLOCK_DAYS="{ex["earn_block"]}"')
    print(f'$env:AI_EXEC_RISKOFF_HIGH_CONF="{str(ex["riskoff_high_conf"]).lower()}"')
    print(f'$env:AI_EXEC_MIN_RS63_BUY="{ex["min_rs_buy"]}"')
    print(f'$env:AI_EXEC_RISKOFF_MIN_RS63="{ex["riskoff_min_rs"]}"')
    print(f'$env:AI_EXEC_MIN_ADX_BUY="{ex["min_adx_buy"]}"')
    print(f'$env:AI_EXEC_MIN_VOL_RATIO_BUY="{ex["min_vol_buy"]}"')
    print(f'$env:AI_EXEC_MAX_RSI_BUY="{ex["max_rsi_buy"]}"')
    print(f'$env:AI_EXEC_MAX_VIX_BUY="{ex["max_vix_buy"]}"')
    print(f'$env:AI_EXEC_RISK_BUDGET_ON="{ex["risk_on"]}"')
    print(f'$env:AI_EXEC_RISK_BUDGET_NEUTRAL="{ex["risk_neutral"]}"')
    print(f'$env:AI_EXEC_RISK_BUDGET_OFF="{ex["risk_off"]}"')
    print(f'$env:AI_EXEC_CONF_W_HIGH="{ex["w_high"]}"')
    print(f'$env:AI_EXEC_CONF_W_MEDIUM="{ex["w_med"]}"')
    print(f'$env:AI_EXEC_CONF_W_LOW="{ex["w_low"]}"')
    print(f'$env:AI_EXEC_RISKOFF_BUY_SCALE="{ex["riskoff_buy_scale"]}"')
    print(f'$env:AI_PORTFOLIO_TOP_K="{best.get("portfolio_top_k", 5)}"')
    if float(best.get("trade_cost_bps", 0.0)) > 0:
        print(f'$env:AI_TRADE_COST_BPS="{best["trade_cost_bps"]}"')


if __name__ == "__main__":
    run()
