from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np

from ai.analyzer import AIAnalyzer
from config import load_all_us_stocks, load_nasdaq_100, load_sp500
from core.indicators import calculate_indicators
from core.stock_data import get_market_condition, get_stock_data, get_stock_info


ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = ROOT / "data" / "rebalance"


def _f(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
        if np.isnan(v) or np.isinf(v):
            return d
        return v
    except Exception:
        return d


def _env_bool(key: str, default: bool = False) -> bool:
    raw = str(os.getenv(key, "1" if default else "0")).strip().lower()
    return raw in {"1", "true", "yes", "on", "y"}


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return int(default)


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return float(default)


def _selection_score(rs63: float, rs21: float) -> float:
    # Momentum-first ranking: longer trend has slightly higher weight.
    return round(0.6 * float(rs63) + 0.4 * float(rs21), 2)


def _cross_conviction_score(crosses: list[dict[str, Any]]) -> float:
    score = 0.0
    for c in crosses or []:
        if not isinstance(c, dict):
            continue
        sig = str(c.get("signal", "")).strip().lower()
        typ = str(c.get("type", "")).strip().lower()
        if not sig and not typ:
            continue
        is_buy = ("매수" in sig) or ("buy" in sig) or ("골든" in typ) or ("golden" in typ)
        is_sell = ("매도" in sig) or ("sell" in sig) or ("데드" in typ) or ("dead" in typ)
        if is_buy and not is_sell:
            score += 0.5
        elif is_sell and not is_buy:
            score -= 0.5
    return score


def _parse_symbols(raw: str) -> list[str]:
    text = (raw or "").replace("\n", ",").replace(";", ",").replace("|", ",")
    parts = [p.strip().upper() for p in text.split(",") if p.strip()]
    out: list[str] = []
    seen: set[str] = set()
    for p in parts:
        sym = p.replace(".", "-")
        if not sym or sym in seen:
            continue
        seen.add(sym)
        out.append(sym)
    return out


def _latest_report_path(base: Path) -> Path | None:
    if not base.exists():
        return None
    candidates = [p for p in base.glob("report_*.json") if p.is_file()]
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def _load_report(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _load_current_portfolio(path: Path) -> dict[str, float]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if isinstance(payload, dict):
        if "positions" in payload:
            out = {}
            for row in payload.get("positions", []):
                if not isinstance(row, dict):
                    continue
                sym = str(row.get("symbol", "")).strip().upper()
                if not sym:
                    continue
                out[sym] = _f(row.get("weight_pct"), 0.0) / 100.0
            cash = _f(payload.get("cash_pct"), 0.0) / 100.0
            if cash > 0:
                out["__CASH__"] = cash
            return out
        return {k: _f(v, 0.0) / 100.0 for k, v in payload.items() if isinstance(k, str)}
    return {}


def _load_universe() -> tuple[str, list[str]]:
    raw_symbols = (os.getenv("AI_SYMBOLS") or "").strip()
    if raw_symbols:
        return ("custom", _parse_symbols(raw_symbols))

    raw = (os.getenv("AI_UNIVERSE") or "all_us").strip().lower() or "all_us"
    if raw in {"nasdaq100", "nasdaq-100", "ndx"}:
        return ("nasdaq100", load_nasdaq_100())
    if raw in {"sp500", "s&p500", "snp500"}:
        return ("sp500", load_sp500())
    if raw in {"all_us", "all-us", "all"}:
        return ("all_us", load_all_us_stocks())
    return ("nasdaq100", load_nasdaq_100())


def _select_candidates(
    features: list[dict[str, Any]],
    max_symbols: int,
    include_symbols: list[str] | None,
) -> list[dict[str, Any]]:
    if not features:
        return []
    df = sorted(
        features,
        key=lambda x: (
            float(
                x.get(
                    "selection_score",
                    _selection_score(_f(x.get("relative_strength_63d", 0.0)), _f(x.get("relative_strength_21d", 0.0))),
                )
            ),
            float(x.get("relative_strength_63d", 0.0)),
            float(x.get("relative_strength_21d", 0.0)),
            float(x.get("entry_conviction", 0.0)),
        ),
        reverse=True,
    )
    nmax = max(1, int(max_symbols))
    base = df[:nmax]

    include = [s for s in (include_symbols or []) if isinstance(s, str) and s.strip()]
    if not include:
        return base

    feat_by_sym = {str(x.get("symbol")): x for x in features if isinstance(x, dict) and x.get("symbol")}
    chosen: dict[str, dict[str, Any]] = {}
    for x in base:
        sym = str(x.get("symbol", ""))
        if sym:
            chosen[sym] = x
    for sym in include:
        fx = feat_by_sym.get(sym)
        if fx is not None:
            chosen[sym] = fx
    if len(chosen) <= nmax:
        return list(chosen.values())
    keep = sorted(
        chosen.values(),
        key=lambda x: (
            float(
                x.get(
                    "selection_score",
                    _selection_score(_f(x.get("relative_strength_63d", 0.0)), _f(x.get("relative_strength_21d", 0.0))),
                )
            ),
            float(x.get("relative_strength_63d", 0.0)),
            float(x.get("relative_strength_21d", 0.0)),
            float(x.get("entry_conviction", 0.0)),
        ),
        reverse=True,
    )
    return keep[:nmax]


def _portfolio_prompt(
    report: dict[str, Any],
    market_ctx: dict[str, Any],
    candidates: list[dict[str, Any]],
    top_k: int,
    max_weight_pct: float,
    turnover_target_pct: float,
    prev_portfolio_pct: dict[str, float] | None,
    exposure_target_pct: float,
    regime_note: str,
) -> str:
    risk = (report.get("module1_liquidity") or {}).get("risk_on_off", {})
    macro = report.get("module6_macro_scenarios", {})
    valuation = report.get("module2_valuation_distortion", {})
    flow = report.get("module3_flow_reversal", {})
    short_interest = report.get("module3b_short_interest", {})
    narrative = report.get("module5_narrative_vs_numbers", {})

    def _fmt_prev(prev: dict[str, float] | None) -> str:
        if not prev:
            return "Current portfolio: none\n"
        lines = ["Current portfolio (weights %):"]
        for sym, w in sorted(prev.items(), key=lambda kv: -float(kv[1])):
            if sym == "__CASH__":
                lines.append(f"- CASH: {float(w) * 100:.1f}%")
                continue
            lines.append(f"- {sym}: {float(w) * 100:.1f}%")
        return "\n".join(lines) + "\n"

    cand_lines = []
    for x in candidates:
        warn = ",".join(x.get("warnings", [])) if isinstance(x.get("warnings"), list) else ""
        cand_lines.append(
            f"{x['symbol']} price={x.get('price', 0):.2f} "
            f"score={x.get('selection_score', 0):.2f} "
            f"rs63={x.get('relative_strength_63d', 0):.1f} rs21={x.get('relative_strength_21d', 0):.1f} "
            f"rsi={x.get('rsi', 50):.1f} adx={x.get('adx', 0):.1f} "
            f"ma50_gap={x.get('ma50_gap', 0):.1f}% ma200_gap={x.get('ma200_gap', 0):.1f}% "
            f"bb_pos={x.get('bb_position', 50):.0f} vol_ratio={x.get('volume_ratio', 1):.2f} "
            f"atr_pct={x.get('atr_pct', 0):.2f} sector={x.get('sector', 'N/A')} "
            f"sleeve={x.get('sleeve', 'momentum')} "
            f"conv={x.get('entry_conviction', 0):.1f} "
            f"warn={warn or 'none'}"
        )

    return (
        "You are a disciplined US equity portfolio allocator.\n"
        "Write in Korean. Use only the provided research report + chart indicators.\n"
        "Goal: build a LONG-ONLY portfolio for the next 1-3 months.\n"
        "Momentum (rs63/rs21) is primary. selection_score=0.6*rs63+0.4*rs21.\n"
        "Use risk filters (rsi/ma gaps/adx/vol) and size overheat names conservatively.\n"
        "Avoid names with weak chart structure or extreme overextension.\n\n"
        "If rebound_sleeve appears in candidates, keep it as a minor sleeve only.\n\n"
        "Macro research summary inputs:\n"
        f"- Risk-on/off: {risk}\n"
        f"- Macro scenarios: {macro}\n"
        f"- Valuation distortion: {valuation}\n"
        f"- Flow reversal: {flow}\n"
        f"- Short interest: {short_interest}\n"
        f"- Narrative vs numbers: {narrative}\n\n"
        f"Market regime: {market_ctx.get('message', 'N/A')} | "
        f"bench 21d={market_ctx.get('benchmark_return_21d', 0):.1f}% "
        f"bench 63d={market_ctx.get('benchmark_return_63d', 0):.1f}%\n\n"
        f"{_fmt_prev(prev_portfolio_pct)}\n"
        "Constraints:\n"
        f"- Choose 1 to {int(top_k)} positions from the provided list.\n"
        f"- Max weight per position: {float(max_weight_pct):.0f}%.\n"
        f"- Target turnover (soft): <= {float(turnover_target_pct):.0f}% per rebalance.\n"
        f"- Target total exposure (approx): {float(exposure_target_pct):.0f}%.\n"
        f"- Regime note: {regime_note}\n"
        "- Weights must sum to 100% including cash.\n"
        "- Cash is allowed; increase cash if risk-off or charts are weak.\n\n"
        "Output STRICT JSON only (no markdown):\n"
        '{"cash_pct":0-100,"positions":[{"symbol":"AAPL","weight_pct":0-100},...]}\n\n'
        "Candidates (chart summary):\n"
        + "\n".join(cand_lines)
    )


def _extract_json(text: str) -> dict[str, Any] | None:
    if not text:
        return None
    text = text.strip().replace("```json", "").replace("```", "")
    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else None
    except Exception:
        pass
    decoder = json.JSONDecoder()
    for i, ch in enumerate(text):
        if ch != "{":
            continue
        try:
            obj, _ = decoder.raw_decode(text[i:])
            if isinstance(obj, dict):
                return obj
        except Exception:
            continue
    return None


def _fallback_portfolio(
    candidates: list[dict[str, Any]],
    top_k: int,
    max_weight_pct: float,
    exposure_target_pct: float,
) -> dict[str, Any]:
    if not candidates:
        return {"cash_pct": 100.0, "positions": []}
    ordered = sorted(
        candidates,
        key=lambda x: (
            float(
                x.get(
                    "selection_score",
                    _selection_score(_f(x.get("relative_strength_63d", 0.0)), _f(x.get("relative_strength_21d", 0.0))),
                )
            ),
            float(x.get("relative_strength_63d", 0.0)),
            float(x.get("relative_strength_21d", 0.0)),
            float(x.get("entry_conviction", 0.0)),
        ),
        reverse=True,
    )
    picked = ordered[: max(1, int(top_k))]
    n = max(1, len(picked))
    exposure = max(0.0, min(100.0, float(exposure_target_pct)))
    w = min(exposure / n, float(max_weight_pct))
    positions = [{"symbol": x["symbol"], "weight_pct": w} for x in picked]
    cash = max(0.0, 100.0 - w * n)
    return {"cash_pct": cash, "positions": positions, "_fallback": True}


def _portfolio_from_ai(
    obj: dict[str, Any],
    allowed: set[str],
    top_k: int,
    max_weight_pct: float,
    exposure_target_pct: float,
) -> tuple[dict[str, float], float]:
    pos = obj.get("positions")
    if not isinstance(pos, list):
        raise ValueError("positions missing")

    weights: dict[str, float] = {}
    for row in pos:
        if not isinstance(row, dict):
            continue
        sym = str(row.get("symbol", "")).strip().upper()
        if not sym or sym not in allowed:
            continue
        w = _f(row.get("weight_pct"), 0.0)
        if w <= 0:
            continue
        weights[sym] = weights.get(sym, 0.0) + float(w)

    if not weights:
        # Allow a fully defensive output (cash-only) when no tradable symbol survives.
        return {}, 100.0

    items = sorted(weights.items(), key=lambda x: x[1], reverse=True)
    if top_k > 0 and len(items) > top_k:
        items = items[:top_k]
    weights = {k: float(v) for k, v in items}

    max_w = max(1.0, min(100.0, float(max_weight_pct)))
    clamped: dict[str, float] = {}
    for k, v in weights.items():
        clamped[k] = float(min(float(v), max_w))
    weights = clamped

    total = float(sum(weights.values()))
    if total <= 0:
        return {}, 100.0
    if total > 100.0:
        scale = 100.0 / total
        weights = {k: float(v) * scale for k, v in weights.items()}
        total = 100.0

    cash = float(100.0 - total)
    if cash < 0:
        cash = 0.0
    exposure = max(0.0, min(100.0, float(exposure_target_pct)))
    if exposure < 100.0:
        scale = exposure / max(1e-9, total)
        weights = {k: float(v) * scale for k, v in weights.items()}
        total = float(sum(weights.values()))
        cash = max(0.0, 100.0 - total)
    return weights, cash


def _regime_controls(report: dict[str, Any]) -> dict[str, Any]:
    risk = (report.get("module1_liquidity") or {}).get("risk_on_off", {})
    label = str(risk.get("label", "neutral"))
    score = _f(risk.get("score", 0.0))
    flows_conf = (report.get("module1_liquidity") or {}).get("confidence_flows", "medium")
    if label == "risk_on":
        exposure = 85.0 if score >= 1.5 else 75.0
        max_weight = 25.0
        turnover = 35.0
        min_adx = 20.0
        min_vol_ratio = 1.0
        note = "Risk-on: allow higher exposure; trend sleeve favored."
    elif label == "risk_off":
        exposure = 40.0 if score <= -1.5 else 55.0
        max_weight = 18.0
        turnover = 20.0
        min_adx = 25.0
        min_vol_ratio = 1.3
        note = "Risk-off: reduce exposure; require stronger trend/volume."
    else:
        exposure = 65.0
        max_weight = 22.0
        turnover = 28.0
        min_adx = 20.0
        min_vol_ratio = 1.0
        note = "Neutral: balanced exposure; keep core trend bias."
    flow_conf_key = str(flows_conf).strip().lower()
    flow_conf_multiplier = {"high": 1.0, "medium": 0.9, "low": 0.8}.get(flow_conf_key, 0.9)
    exposure = round(exposure * flow_conf_multiplier, 2)
    if flow_conf_key in {"low", "medium"}:
        note += f" Flows confidence={flow_conf_key}; scale exposure x{flow_conf_multiplier:.2f}."
    return {
        "label": label,
        "score": score,
        "exposure_target_pct": exposure,
        "max_weight_pct": max_weight,
        "turnover_target_pct": turnover,
        "min_adx": min_adx,
        # Soft volume threshold used for warnings/sizing bias.
        "volume_warn_threshold": min_vol_ratio,
        # Backward-compat key kept for downstream readers.
        "min_volume_ratio": min_vol_ratio,
        "flow_confidence": flow_conf_key,
        "flow_confidence_multiplier": flow_conf_multiplier,
        "note": note,
    }




def _market_exposure_filter(market_ctx: dict[str, Any]) -> dict[str, Any]:
    price = _f(market_ctx.get("price"), 0.0)
    ma50 = _f(market_ctx.get("ma50"), 0.0)
    ma200 = _f(market_ctx.get("ma200"), 0.0)
    if price <= 0 or ma50 <= 0 or ma200 <= 0:
        return {"multiplier": 1.0, "reason": "benchmark_trend_data_missing"}
    if price < ma200:
        return {"multiplier": 0.7, "reason": "benchmark_below_ma200"}
    if price < ma50:
        return {"multiplier": 0.85, "reason": "benchmark_below_ma50"}
    return {"multiplier": 1.0, "reason": "benchmark_above_ma50"}


def _rebound_settings() -> dict[str, Any]:
    enabled = _env_bool("AI_ENABLE_REBOUND_SLEEVE", False)
    max_weight_pct = max(1.0, min(10.0, _env_float("AI_REBOUND_MAX_WEIGHT_PCT", 4.0)))
    max_count = max(0, _env_int("AI_REBOUND_MAX_COUNT", 2))
    max_ma200_drawdown_pct = min(-1.0, _env_float("AI_REBOUND_MAX_MA200_DRAWDOWN_PCT", -15.0))
    min_volume_ratio = max(0.5, _env_float("AI_REBOUND_MIN_VOLUME_RATIO", 1.0))
    min_adx = max(5.0, _env_float("AI_REBOUND_MIN_ADX", 10.0))
    return {
        "enabled": enabled,
        "max_weight_pct": max_weight_pct,
        "max_count": max_count,
        "max_ma200_drawdown_pct": max_ma200_drawdown_pct,
        "min_volume_ratio": min_volume_ratio,
        "min_adx": min_adx,
    }


def _is_rebound_candidate(
    *,
    rsi: float,
    bb_pos: float,
    entry_conviction: float,
    volume_ratio: float,
    ma200_gap: float,
    rebound_cfg: dict[str, Any],
) -> bool:
    if not rebound_cfg.get("enabled"):
        return False
    if volume_ratio < _f(rebound_cfg.get("min_volume_ratio", 1.0), 1.0):
        return False
    if ma200_gap < _f(rebound_cfg.get("max_ma200_drawdown_pct", -15.0), -15.0):
        return False
    if entry_conviction <= 0:
        return False
    # Rebound sleeve is for oversold + mean-reversion setups.
    return (rsi <= 35.0 or bb_pos <= 20.0) and rsi < 55.0


def _candidate_warnings(row: dict[str, Any]) -> list[str]:
    flags: list[str] = []
    rsi = _f(row.get("rsi", 50.0))
    bb_pos = _f(row.get("bb_position", 50.0))
    conv = _f(row.get("entry_conviction", 0.0))

    if rsi >= 80 or bb_pos >= 95:
        flags.append("overheat_extreme")
    elif rsi >= 70 and bb_pos >= 80:
        flags.append("overheat_dual")
    elif rsi >= 70 or bb_pos >= 80:
        flags.append("overheat_warning")

    if conv < 0:
        flags.append("entry_negative")
    return flags


def _position_multiplier(row: dict[str, Any]) -> tuple[float, list[str]]:
    mult = 1.0
    flags: list[str] = []
    rsi = _f(row.get("rsi", 50.0))
    bb_pos = _f(row.get("bb_position", 50.0))
    vol_ratio = _f(row.get("volume_ratio", 1.0))
    conv = _f(row.get("entry_conviction", 0.0))
    sleeve = str(row.get("sleeve", "momentum") or "momentum").strip().lower()

    if rsi >= 80 or bb_pos >= 95:
        mult *= 0.25
        flags.append("overheat_extreme")
    elif rsi >= 70 and bb_pos >= 80:
        mult *= 0.4
        flags.append("overheat_dual")
    elif rsi >= 70 or bb_pos >= 80:
        mult *= 0.6
        flags.append("overheat_warning")

    if conv < 0:
        mult *= 0.5
        flags.append("entry_negative")

    if sleeve == "rebound":
        # Keep rebound sleeve intentionally small vs momentum sleeve.
        mult *= 0.35
        flags.append("rebound_sleeve")

    if vol_ratio >= 1.5:
        mult *= 1.2
        flags.append("volume_strong")
    elif vol_ratio < 1.0:
        mult *= 0.7
        flags.append("volume_weak")
    elif vol_ratio < 1.2:
        mult *= 0.8
        flags.append("volume_soft")

    return max(0.0, mult), flags


def _apply_weight_multipliers(
    target_weights_pct: dict[str, float],
    feature_by_symbol: dict[str, dict[str, Any]],
    max_weight_pct: float,
) -> tuple[dict[str, float], dict[str, Any]]:
    out: dict[str, float] = {}
    audit: dict[str, Any] = {}
    max_w = max(1.0, min(100.0, float(max_weight_pct)))

    for sym, base_w in target_weights_pct.items():
        row = feature_by_symbol.get(sym, {})
        mult, flags = _position_multiplier(row)
        adj = min(float(base_w) * mult, max_w)
        if adj <= 0:
            continue
        out[sym] = float(adj)
        audit[sym] = {
            "base_weight_pct": round(float(base_w), 2),
            "multiplier": round(float(mult), 3),
            "adjusted_weight_pct": round(float(adj), 2),
            "flags": flags,
        }

    total = float(sum(out.values()))
    if total > 100.0 and total > 0:
        scale = 100.0 / total
        out = {k: float(v) * scale for k, v in out.items()}
        for sym in list(audit.keys()):
            if sym in out:
                audit[sym]["adjusted_weight_pct"] = round(float(out[sym]), 2)
        audit["_scaled_to_100"] = True
    else:
        audit["_scaled_to_100"] = False
    audit["_total_after_pct"] = round(float(sum(out.values())), 2)
    return out, audit


def _apply_rebound_limits(
    target_weights_pct: dict[str, float],
    feature_by_symbol: dict[str, dict[str, Any]],
    rebound_max_weight_pct: float,
    rebound_max_count: int,
) -> tuple[dict[str, float], dict[str, Any]]:
    out = {k: float(v) for k, v in target_weights_pct.items() if float(v) > 1e-9}
    max_w = max(0.0, float(rebound_max_weight_pct))
    max_n = max(0, int(rebound_max_count))
    removed: list[str] = []
    if not out or max_n <= 0:
        # Rebound sleeve disabled or not allowed.
        if max_n <= 0:
            for sym in list(out.keys()):
                row = feature_by_symbol.get(sym, {})
                if str(row.get("sleeve", "")).lower() == "rebound":
                    removed.append(sym)
                    out.pop(sym, None)
        return out, {
            "enabled": max_n > 0,
            "rebound_max_weight_pct": round(max_w, 2),
            "rebound_max_count": int(max_n),
            "kept_symbols": [],
            "removed_symbols": removed,
            "capped_symbols": [],
        }

    rebound_rows = []
    for sym, w in out.items():
        row = feature_by_symbol.get(sym, {})
        if str(row.get("sleeve", "momentum")).strip().lower() != "rebound":
            continue
        rebound_rows.append(
            (
                sym,
                float(w),
                _f(row.get("selection_score", 0.0)),
                _f(row.get("entry_conviction", 0.0)),
            )
        )
    rebound_rows.sort(key=lambda x: (x[2], x[3], x[1]), reverse=True)

    keep_set = {sym for sym, *_ in rebound_rows[:max_n]}
    kept_symbols: list[str] = []
    removed_symbols: list[str] = []
    capped_symbols: list[str] = []
    for sym, cur_w, *_ in rebound_rows:
        if sym not in keep_set:
            out.pop(sym, None)
            removed_symbols.append(sym)
            continue
        kept_symbols.append(sym)
        if cur_w > max_w > 0:
            out[sym] = max_w
            capped_symbols.append(sym)

    return out, {
        "enabled": True,
        "rebound_max_weight_pct": round(max_w, 2),
        "rebound_max_count": int(max_n),
        "kept_symbols": kept_symbols,
        "removed_symbols": removed_symbols,
        "capped_symbols": capped_symbols,
    }


def _apply_sector_cap(
    target_weights_pct: dict[str, float],
    feature_by_symbol: dict[str, dict[str, Any]],
    sector_cap_pct: float,
) -> tuple[dict[str, float], dict[str, Any]]:
    cap = max(0.0, min(100.0, float(sector_cap_pct)))
    out = {k: float(v) for k, v in target_weights_pct.items() if float(v) > 1e-9}
    if cap >= 100.0 or not out:
        return out, {"enabled": False, "sector_cap_pct": round(cap, 2), "capped_sectors": []}

    by_sector: dict[str, list[tuple[str, float]]] = {}
    for sym, w in out.items():
        sector = str((feature_by_symbol.get(sym, {}) or {}).get("sector", "Unknown")) or "Unknown"
        by_sector.setdefault(sector, []).append((sym, float(w)))

    capped_sectors: list[dict[str, Any]] = []
    for sector, rows in by_sector.items():
        total = float(sum(w for _, w in rows))
        if total <= cap + 1e-9:
            continue
        scale = cap / total if total > 0 else 0.0
        before = total
        after = 0.0
        for sym, w in rows:
            nw = float(w) * scale
            out[sym] = nw
            after += nw
        capped_sectors.append(
            {
                "sector": sector,
                "before_pct": round(before, 2),
                "after_pct": round(after, 2),
                "scale": round(scale, 4),
            }
        )

    return out, {
        "enabled": True,
        "sector_cap_pct": round(cap, 2),
        "capped_sectors": capped_sectors,
        "total_after_pct": round(float(sum(out.values())), 2),
    }


def _sector_totals_pct(weights_pct: dict[str, float], feature_by_symbol: dict[str, dict[str, Any]]) -> dict[str, float]:
    totals: dict[str, float] = {}
    for sym, w in weights_pct.items():
        if float(w) <= 1e-9:
            continue
        sector = str((feature_by_symbol.get(sym, {}) or {}).get("sector", "Unknown")) or "Unknown"
        totals[sector] = float(totals.get(sector, 0.0)) + float(w)
    return totals


def _sector_room_pct(
    symbol: str,
    weights_pct: dict[str, float],
    feature_by_symbol: dict[str, dict[str, Any]],
    sector_cap_pct: float,
) -> float:
    cap = max(0.0, min(100.0, float(sector_cap_pct)))
    if cap >= 100.0:
        return 100.0
    row = feature_by_symbol.get(symbol, {}) or {}
    sector = str(row.get("sector", "Unknown")) or "Unknown"
    totals = _sector_totals_pct(weights_pct, feature_by_symbol)
    total_sector = float(totals.get(sector, 0.0))
    return max(0.0, cap - total_sector)


def _sector_cap_violations(
    weights_pct: dict[str, float],
    feature_by_symbol: dict[str, dict[str, Any]],
    sector_cap_pct: float,
) -> list[dict[str, Any]]:
    cap = max(0.0, min(100.0, float(sector_cap_pct)))
    if cap >= 100.0:
        return []
    totals = _sector_totals_pct(weights_pct, feature_by_symbol)
    out = []
    for sector, total in totals.items():
        if float(total) > cap + 1e-9:
            out.append(
                {
                    "sector": sector,
                    "total_pct": round(float(total), 2),
                    "cap_pct": round(cap, 2),
                    "excess_pct": round(float(total) - cap, 2),
                }
            )
    out.sort(key=lambda x: float(x.get("excess_pct", 0.0)), reverse=True)
    return out


def _is_fill_eligible(row: dict[str, Any]) -> bool:
    warnings = row.get("warnings", [])
    if not isinstance(warnings, list):
        warnings = []
    blocked = {"overheat_warning", "overheat_dual", "overheat_extreme", "entry_negative"}
    if any(str(x) in blocked for x in warnings):
        return False
    if _f(row.get("ma50_gap", 0.0)) <= 0 or _f(row.get("ma200_gap", 0.0)) <= 0:
        return False
    return True


def _resolve_fill_max_positions(
    fill_style: str,
    top_k: int,
    max_positions: int | None,
    candidate_count: int,
    current_positions: int = 0,
) -> tuple[int, str]:
    style = str(fill_style or "balanced").strip().lower()
    if style not in {"concentrated", "diversified", "balanced"}:
        style = "balanced"
    if max_positions is None or int(max_positions) <= 0:
        if style == "concentrated":
            base = max(1, int(top_k))
        elif style == "diversified":
            base = max(1, int(top_k) + 4)
        else:
            base = max(1, int(top_k) + 2)
    else:
        base = int(max_positions)
    resolved = max(int(current_positions), int(base))
    resolved = min(resolved, max(0, int(candidate_count)))
    return max(0, int(resolved)), style


def _cap_desired_exposure_by_constraints(
    desired_exposure_pct: float,
    max_weight_pct: float,
    max_positions: int,
) -> tuple[float, dict[str, Any]]:
    desired = max(0.0, min(100.0, float(desired_exposure_pct)))
    max_w = max(0.0, min(100.0, float(max_weight_pct)))
    slots = max(0, int(max_positions))
    feasible = max(0.0, min(100.0, max_w * float(slots)))
    effective = min(desired, feasible)
    return effective, {
        "raw_desired_exposure_pct": round(desired, 2),
        "effective_desired_exposure_pct": round(effective, 2),
        "feasible_max_exposure_pct": round(feasible, 2),
        "max_weight_pct": round(max_w, 2),
        "max_positions": int(slots),
        "capped_by_constraints": bool(effective + 1e-9 < desired),
    }


def _fill_to_target_exposure(
    target_weights_pct: dict[str, float],
    desired_exposure_pct: float,
    max_weight_pct: float,
    top_k: int,
    ordered_candidates: list[dict[str, Any]],
    feature_by_symbol: dict[str, dict[str, Any]],
    fill_style: str = "balanced",
    max_positions: int | None = None,
    sector_cap_pct: float = 100.0,
) -> tuple[dict[str, float], dict[str, Any]]:
    out = {k: float(v) for k, v in target_weights_pct.items() if float(v) > 0}
    desired = max(0.0, min(100.0, float(desired_exposure_pct)))
    max_w = max(1.0, min(100.0, float(max_weight_pct)))
    before = float(sum(out.values()))
    gap = max(0.0, desired - before)
    max_positions, style = _resolve_fill_max_positions(
        fill_style=fill_style,
        top_k=top_k,
        max_positions=max_positions,
        candidate_count=len(ordered_candidates),
        current_positions=len(out),
    )

    boosted_symbols: list[str] = []
    added_symbols: list[str] = []
    blocked_sample: list[dict[str, Any]] = []

    if gap <= 1e-9:
        return out, {
            "fill_applied": False,
            "fill_style": style,
            "max_positions": int(max_positions),
            "desired_exposure_pct": round(desired, 2),
            "achieved_before_pct": round(before, 2),
            "achieved_after_pct": round(before, 2),
            "gap_before_pct": round(max(0.0, desired - before), 2),
            "gap_after_pct": 0.0,
            "boosted_symbols": boosted_symbols,
            "added_symbols": added_symbols,
            "blocked_candidates_sample": blocked_sample,
        }

    elig_existing = []
    for sym in out.keys():
        row = feature_by_symbol.get(sym, {})
        if _is_fill_eligible(row):
            elig_existing.append(sym)

    for row in ordered_candidates:
        if len(blocked_sample) >= 8:
            break
        sym = str(row.get("symbol", ""))
        if not sym:
            continue
        if sym in elig_existing:
            continue
        if _is_fill_eligible(row):
            continue
        blocked_sample.append(
            {
                "symbol": sym,
                "warnings": row.get("warnings", []),
                "selection_score": _f(row.get("selection_score", 0.0)),
            }
        )

    def _add_new(budget: float) -> float:
        used = 0.0
        if budget <= 1e-9:
            return used
        for row in ordered_candidates:
            if used >= budget - 1e-9:
                break
            sym = str(row.get("symbol", ""))
            if not sym or sym in out:
                continue
            if not _is_fill_eligible(row):
                continue
            if len(out) >= int(max_positions):
                break
            sector_room = _sector_room_pct(sym, out, feature_by_symbol, sector_cap_pct)
            if sector_room <= 1e-9:
                continue
            add = min(max_w, budget - used, sector_room)
            if add <= 1e-9:
                continue
            out[sym] = float(add)
            used += float(add)
            added_symbols.append(sym)
        return used

    def _boost_existing(budget: float) -> float:
        used = 0.0
        if budget <= 1e-9:
            return used
        loops = 0
        while used < budget - 1e-9 and elig_existing and loops < 10:
            rooms = {}
            for sym in elig_existing:
                indiv_room = max(0.0, max_w - float(out.get(sym, 0.0)))
                sector_room = _sector_room_pct(sym, out, feature_by_symbol, sector_cap_pct)
                rooms[sym] = max(0.0, min(indiv_room, sector_room))
            active = [sym for sym in elig_existing if rooms[sym] > 1e-9]
            if not active:
                break
            scores = {sym: max(0.1, _f(feature_by_symbol.get(sym, {}).get("selection_score", 0.0))) for sym in active}
            denom = float(sum(scores.values()))
            if denom <= 0:
                break
            progressed = False
            rem_budget = budget - used
            for sym in active:
                share = rem_budget * (scores[sym] / denom)
                live_sector_room = _sector_room_pct(sym, out, feature_by_symbol, sector_cap_pct)
                add = min(rooms[sym], share, live_sector_room)
                if add <= 1e-9:
                    continue
                out[sym] = float(out.get(sym, 0.0)) + float(add)
                used += float(add)
                progressed = True
                if sym not in boosted_symbols:
                    boosted_symbols.append(sym)
                if used >= budget - 1e-9:
                    break
            if not progressed:
                break
            loops += 1
        return used

    if style == "concentrated":
        gap -= _boost_existing(gap)
        gap -= _add_new(gap)
    elif style == "diversified":
        gap -= _add_new(gap)
        gap -= _boost_existing(gap)
    else:
        first_budget = gap * 0.5
        gap -= _add_new(first_budget)
        gap -= _boost_existing(gap)
        gap -= _add_new(gap)
        gap -= _boost_existing(gap)

    after = float(sum(out.values()))
    return out, {
        "fill_applied": bool(boosted_symbols or added_symbols),
        "fill_style": style,
        "max_positions": int(max_positions),
        "desired_exposure_pct": round(desired, 2),
        "achieved_before_pct": round(before, 2),
        "achieved_after_pct": round(after, 2),
        "gap_before_pct": round(max(0.0, desired - before), 2),
        "gap_after_pct": round(max(0.0, desired - after), 2),
        "boosted_symbols": boosted_symbols,
        "added_symbols": added_symbols,
        "blocked_candidates_sample": blocked_sample,
    }


def _turnover_l1_pct(prev_port: dict[str, float], target_weights_pct: dict[str, float]) -> float:
    prev_pct = {k: float(v) * 100.0 for k, v in prev_port.items() if k != "__CASH__"}
    syms = set(prev_pct.keys()) | set(target_weights_pct.keys())
    return float(sum(abs(float(target_weights_pct.get(s, 0.0)) - float(prev_pct.get(s, 0.0))) for s in syms))


def _turnover_pct(prev_port: dict[str, float], target_weights_pct: dict[str, float], definition: str = "half_l1") -> float:
    mode = str(definition or "half_l1").strip().lower()
    l1 = _turnover_l1_pct(prev_port, target_weights_pct)
    if mode in {"l1", "sum_abs"}:
        return float(l1)
    # Industry-standard portfolio turnover proxy: 0.5 * sum(|delta weight|).
    return float(l1 * 0.5)


def _has_invested_positions(prev_port: dict[str, float]) -> bool:
    return any(sym != "__CASH__" and float(w) > 1e-6 for sym, w in prev_port.items())


def _apply_turnover_cap(
    prev_port: dict[str, float],
    target_weights_pct: dict[str, float],
    turnover_target_pct: float,
    feature_by_symbol: dict[str, dict[str, Any]],
) -> tuple[dict[str, float], dict[str, Any]]:
    turnover_definition = str(os.getenv("AI_TURNOVER_DEFINITION", "half_l1")).strip().lower() or "half_l1"
    if turnover_definition not in {"half_l1", "l1", "sum_abs"}:
        turnover_definition = "half_l1"
    cap = max(0.0, float(turnover_target_pct))
    before = _turnover_pct(prev_port, target_weights_pct, definition=turnover_definition)
    before_l1 = _turnover_l1_pct(prev_port, target_weights_pct)
    l1_cap_budget = cap if turnover_definition in {"l1", "sum_abs"} else cap * 2.0
    if cap <= 0:
        return target_weights_pct, {
            "mode": "turnover_cap_disabled",
            "asset_scope": "equity_only_ex_cash",
            "turnover_definition": turnover_definition,
            "before_pct": round(before, 2),
            "after_pct": round(before, 2),
            "before_l1_pct": round(before_l1, 2),
            "after_l1_pct": round(before_l1, 2),
            "cap_pct": cap,
            "l1_cap_budget_pct": round(l1_cap_budget, 2),
            "cap_applied": False,
        }
    if not _has_invested_positions(prev_port):
        return target_weights_pct, {
            "mode": "initial_build_no_constraint",
            "asset_scope": "equity_only_ex_cash",
            "turnover_definition": turnover_definition,
            "before_pct": round(before, 2),
            "after_pct": round(before, 2),
            "before_l1_pct": round(before_l1, 2),
            "after_l1_pct": round(before_l1, 2),
            "cap_pct": cap,
            "l1_cap_budget_pct": round(l1_cap_budget, 2),
            "cap_applied": False,
        }
    if before <= cap + 1e-9:
        return target_weights_pct, {
            "mode": "rebalancing",
            "asset_scope": "equity_only_ex_cash",
            "turnover_definition": turnover_definition,
            "before_pct": round(before, 2),
            "after_pct": round(before, 2),
            "before_l1_pct": round(before_l1, 2),
            "after_l1_pct": round(before_l1, 2),
            "cap_pct": cap,
            "l1_cap_budget_pct": round(l1_cap_budget, 2),
            "cap_applied": False,
        }

    prev_pct = {k: float(v) * 100.0 for k, v in prev_port.items() if k != "__CASH__"}
    syms = sorted(set(prev_pct.keys()) | set(target_weights_pct.keys()))
    rows: list[dict[str, Any]] = []
    for sym in syms:
        cur = float(prev_pct.get(sym, 0.0))
        tgt = float(target_weights_pct.get(sym, 0.0))
        delta = tgt - cur
        if abs(delta) <= 1e-9:
            continue
        meta = feature_by_symbol.get(sym, {})
        score = _f(meta.get("selection_score", 0.0))
        conv = _f(meta.get("entry_conviction", 0.0))
        if delta > 0:
            priority = score + max(0.0, conv) * 5.0
        else:
            priority = (100.0 - score) + max(0.0, -conv) * 5.0
        rows.append({"symbol": sym, "cur": cur, "delta": delta, "priority": priority})

    buys = [r for r in rows if float(r["delta"]) > 0]
    sells = [r for r in rows if float(r["delta"]) < 0]
    buys.sort(key=lambda x: float(x["priority"]), reverse=True)
    sells.sort(key=lambda x: float(x["priority"]), reverse=True)

    applied: dict[str, float] = {}

    def _consume(pool: list[dict[str, Any]], budget: float) -> float:
        used = 0.0
        if budget <= 0:
            return used
        for row in pool:
            d = float(row["delta"])
            already = abs(float(applied.get(row["symbol"], 0.0)))
            remain = max(0.0, abs(d) - already)
            use = min(remain, budget - used)
            if use <= 0:
                continue
            signed = use if d > 0 else -use
            applied[row["symbol"]] = float(applied.get(row["symbol"], 0.0)) + signed
            used += use
            if used >= budget - 1e-9:
                break
        return used

    if buys and sells:
        used_buy = _consume(buys, l1_cap_budget * 0.5)
        used_sell = _consume(sells, l1_cap_budget * 0.5)
    elif buys:
        used_buy = _consume(buys, l1_cap_budget)
        used_sell = 0.0
    else:
        used_buy = 0.0
        used_sell = _consume(sells, l1_cap_budget)

    rem = max(0.0, l1_cap_budget - (used_buy + used_sell))
    if rem > 1e-9:
        rows.sort(key=lambda x: float(x["priority"]), reverse=True)
        _consume(rows, rem)
        rem = max(0.0, l1_cap_budget - sum(abs(float(v)) for v in applied.values()))

    new_target: dict[str, float] = {}
    for sym in syms:
        cur = float(prev_pct.get(sym, 0.0))
        nxt = cur + float(applied.get(sym, 0.0))
        if nxt > 1e-6:
            new_target[sym] = nxt

    after_l1 = _turnover_l1_pct(prev_port, new_target)
    after = _turnover_pct(prev_port, new_target, definition=turnover_definition)
    return new_target, {
        "mode": "rebalancing",
        "asset_scope": "equity_only_ex_cash",
        "turnover_definition": turnover_definition,
        "before_pct": round(before, 2),
        "after_pct": round(after, 2),
        "before_l1_pct": round(before_l1, 2),
        "after_l1_pct": round(after_l1, 2),
        "cap_pct": cap,
        "l1_cap_budget_pct": round(l1_cap_budget, 2),
        "cap_applied": True,
        "remaining_unused_pct": round(max(0.0, rem), 4),
    }


def _chart_rationale(row: dict[str, Any]) -> list[str]:
    notes: list[str] = []
    rsi = _f(row.get("rsi", 50.0))
    adx = _f(row.get("adx", 0.0))
    rs63 = _f(row.get("relative_strength_63d", 0.0))
    ma50_gap = _f(row.get("ma50_gap", 0.0))
    ma200_gap = _f(row.get("ma200_gap", 0.0))
    vol_ratio = _f(row.get("volume_ratio", 1.0))
    bb_pos = _f(row.get("bb_position", 50.0))

    if rs63 >= 5:
        notes.append(f"RS63 strong (+{rs63:.1f}pp)")
    elif rs63 <= -5:
        notes.append(f"RS63 weak ({rs63:.1f}pp)")

    if adx >= 25:
        notes.append(f"Trend strong (ADX {adx:.0f})")
    elif adx <= 15:
        notes.append(f"Trend weak (ADX {adx:.0f})")

    if rsi >= 70:
        notes.append(f"Overbought (RSI {rsi:.0f})")
    elif rsi <= 30:
        notes.append(f"Oversold (RSI {rsi:.0f})")
    else:
        notes.append(f"RSI neutral ({rsi:.0f})")

    if ma50_gap >= 3 and ma200_gap >= 3:
        notes.append("Price above MA50/200")
    elif ma50_gap <= -3 or ma200_gap <= -3:
        notes.append("Price below MA50/200")

    if vol_ratio >= 1.8:
        notes.append(f"Volume surge ({vol_ratio:.1f}x)")

    if bb_pos >= 80:
        notes.append("BB upper zone")
    elif bb_pos <= 20:
        notes.append("BB lower zone")

    return notes[:5]


def _reconcile_target_with_min_trade(
    prev_port: dict[str, float],
    target_weights_pct: dict[str, float],
    min_trade_pct: float,
    desired_exposure_pct: float,
    max_weight_pct: float,
    feature_by_symbol: dict[str, dict[str, Any]],
    allow_refill: bool,
) -> tuple[dict[str, float], dict[str, Any]]:
    prev_pct = {k: float(v) * 100.0 for k, v in prev_port.items() if k != "__CASH__"}
    out = {k: float(v) for k, v in target_weights_pct.items() if float(v) > 1e-9}
    min_trade = max(0.0, float(min_trade_pct))
    desired = max(0.0, min(100.0, float(desired_exposure_pct)))
    max_w = max(1.0, min(100.0, float(max_weight_pct)))

    adjusted: list[dict[str, Any]] = []
    if min_trade > 0:
        for sym in sorted(set(prev_pct.keys()) | set(out.keys())):
            cur = float(prev_pct.get(sym, 0.0))
            tgt = float(out.get(sym, 0.0))
            delta = tgt - cur
            if abs(delta) < min_trade and abs(delta) > 1e-9:
                adjusted.append(
                    {
                        "symbol": sym,
                        "current_weight_pct": round(cur, 2),
                        "target_weight_pct_before_reconcile": round(tgt, 2),
                        "delta_pct": round(delta, 2),
                        "skip_reason": "below_min_trade",
                        "min_trade_pct": round(min_trade, 2),
                    }
                )
                if cur > 1e-9:
                    out[sym] = cur
                else:
                    out.pop(sym, None)

    out = {k: float(v) for k, v in out.items() if float(v) > 1e-9}
    before_refill = float(sum(out.values()))

    refill_symbols: list[str] = []
    refill_blocked: list[dict[str, Any]] = []
    remaining = max(0.0, desired - before_refill)
    refill_used = 0.0

    if allow_refill and remaining > 1e-9:
        tradeable = []
        for sym, tgt in out.items():
            row = feature_by_symbol.get(sym, {})
            cur = float(prev_pct.get(sym, 0.0))
            if abs(tgt - cur) < max(min_trade, 1e-9):
                continue
            if not _is_fill_eligible(row):
                if len(refill_blocked) < 8:
                    refill_blocked.append(
                        {
                            "symbol": sym,
                            "warnings": row.get("warnings", []),
                            "reason": "refill_blocked_by_risk_filter",
                        }
                    )
                continue
            room = max(0.0, max_w - float(tgt))
            if room <= 1e-9:
                continue
            tradeable.append(sym)

        tradeable.sort(
            key=lambda s: (
                1 if _is_fill_eligible(feature_by_symbol.get(s, {})) else 0,
                _f(feature_by_symbol.get(s, {}).get("selection_score", 0.0)),
                _f(feature_by_symbol.get(s, {}).get("relative_strength_63d", 0.0)),
                _f(feature_by_symbol.get(s, {}).get("relative_strength_21d", 0.0)),
            ),
            reverse=True,
        )

        loops = 0
        while remaining > 1e-9 and tradeable and loops < 10:
            rooms = {sym: max(0.0, max_w - float(out.get(sym, 0.0))) for sym in tradeable}
            active = [sym for sym in tradeable if rooms[sym] > 1e-9]
            if not active:
                break
            scores = {
                sym: max(0.1, _f(feature_by_symbol.get(sym, {}).get("selection_score", 0.0)))
                for sym in active
            }
            denom = float(sum(scores.values()))
            if denom <= 0:
                break
            progressed = False
            for sym in active:
                share = remaining * (scores[sym] / denom)
                add = min(rooms[sym], share)
                if add <= 1e-9:
                    continue
                out[sym] = float(out.get(sym, 0.0)) + float(add)
                refill_used += float(add)
                remaining -= float(add)
                progressed = True
                if sym not in refill_symbols:
                    refill_symbols.append(sym)
                if remaining <= 1e-9:
                    break
            if not progressed:
                break
            loops += 1

    after_refill = float(sum(out.values()))
    return out, {
        "applied": len(adjusted) > 0,
        "allow_refill": bool(allow_refill),
        "refill_policy": "clean_only",
        "min_trade_pct": round(min_trade, 2),
        "desired_exposure_pct": round(desired, 2),
        "achieved_before_refill_pct": round(before_refill, 2),
        "achieved_after_refill_pct": round(after_refill, 2),
        "gap_after_refill_pct": round(max(0.0, desired - after_refill), 2),
        "refill_used_pct": round(refill_used, 2),
        "refill_symbols": refill_symbols,
        "refill_blocked_symbols": refill_blocked,
        "adjusted_targets": adjusted,
    }


def _build_orders(
    prev_port: dict[str, float],
    target_weights_pct: dict[str, float],
    min_trade_pct: float,
) -> list[dict[str, Any]]:
    orders: list[dict[str, Any]] = []
    prev_pct = {k: float(v) * 100.0 for k, v in prev_port.items() if k != "__CASH__"}
    next_pct = {k: float(v) for k, v in target_weights_pct.items()}
    for sym in sorted(set(prev_pct) | set(next_pct)):
        cur = prev_pct.get(sym, 0.0)
        tgt = next_pct.get(sym, 0.0)
        delta = tgt - cur
        if abs(delta) < min_trade_pct:
            continue
        action = "BUY" if delta > 0 else "SELL"
        orders.append(
            {
                "symbol": sym,
                "action": action,
                "current_weight_pct": round(cur, 2),
                "target_weight_pct": round(tgt, 2),
                "delta_pct": round(delta, 2),
            }
        )
    return orders


def _build_orders_with_skips(
    prev_port: dict[str, float],
    target_weights_pct: dict[str, float],
    min_trade_pct: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    orders: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    prev_pct = {k: float(v) * 100.0 for k, v in prev_port.items() if k != "__CASH__"}
    next_pct = {k: float(v) for k, v in target_weights_pct.items()}
    for sym in sorted(set(prev_pct) | set(next_pct)):
        cur = prev_pct.get(sym, 0.0)
        tgt = next_pct.get(sym, 0.0)
        delta = tgt - cur
        action = "BUY" if delta > 0 else "SELL"
        if abs(delta) < min_trade_pct:
            if abs(delta) > 1e-9:
                skipped.append(
                    {
                        "symbol": sym,
                        "action": action,
                        "current_weight_pct": round(cur, 2),
                        "target_weight_pct": round(tgt, 2),
                        "delta_pct": round(delta, 2),
                        "skip_reason": "below_min_trade",
                        "min_trade_pct": float(min_trade_pct),
                    }
                )
            continue
        orders.append(
            {
                "symbol": sym,
                "action": action,
                "current_weight_pct": round(cur, 2),
                "target_weight_pct": round(tgt, 2),
                "delta_pct": round(delta, 2),
            }
        )
    return orders, skipped


def _executed_portfolio_from_orders(
    prev_port: dict[str, float],
    executed_orders: list[dict[str, Any]],
) -> tuple[dict[str, float], float, float]:
    prev_pct = {k: float(v) * 100.0 for k, v in prev_port.items() if k != "__CASH__"}
    out = {k: float(v) for k, v in prev_pct.items() if float(v) > 1e-9}

    for row in executed_orders:
        if not isinstance(row, dict):
            continue
        sym = str(row.get("symbol", "")).strip().upper()
        if not sym:
            continue
        tgt = _f(row.get("target_weight_pct"), out.get(sym, 0.0))
        if tgt <= 1e-9:
            out.pop(sym, None)
        else:
            out[sym] = float(tgt)

    exposure = float(sum(out.values()))
    cash = max(0.0, 100.0 - exposure)
    return out, cash, exposure


def run_us_rebalance(report_dir: str | None = None) -> dict[str, Any]:
    analyzer = AIAnalyzer()
    allow_fallback = str(os.getenv("AI_ALLOW_FALLBACK_NO_API", "0")).strip().lower() in {"1", "true", "yes", "on"}
    if not analyzer.has_api_access and not allow_fallback:
        raise RuntimeError("Codex login required. Run: codex login")

    if report_dir:
        cand = Path(report_dir)
        if cand.is_dir():
            legacy = cand / "report.json"
            if legacy.exists():
                report_path = legacy
            else:
                report_path = _latest_report_path(cand)
        else:
            report_path = cand
    else:
        report_path = _latest_report_path(ROOT / "outputs")
    if report_path is None:
        raise RuntimeError("No report JSON found under outputs/")

    report = _load_report(report_path)
    universe_name, symbols = _load_universe()
    top_k = max(1, int(os.getenv("AI_PORTFOLIO_TOP_K", "8")))
    max_weight_pct = float(os.getenv("AI_PORTFOLIO_MAX_WEIGHT_PCT", "25"))
    prompt_max_symbols = max(10, int(os.getenv("AI_PROMPT_MAX_SYMBOLS", "35")))
    base_turnover_target_pct = float(os.getenv("AI_PORTFOLIO_TURNOVER_TARGET_PCT", "30"))
    regime = _regime_controls(report)
    max_weight_pct = min(max_weight_pct, regime["max_weight_pct"])
    regime_turnover_target_pct = float(regime.get("turnover_target_pct", base_turnover_target_pct))
    turnover_target_pct = min(base_turnover_target_pct, regime_turnover_target_pct)
    effective_turnover_target_pct = turnover_target_pct
    regime["base_turnover_cap_pct"] = base_turnover_target_pct
    regime["regime_turnover_cap_pct"] = regime_turnover_target_pct
    regime["selected_turnover_cap_pct"] = turnover_target_pct
    regime["effective_turnover_target_pct"] = effective_turnover_target_pct

    current_path = Path(os.getenv("AI_CURRENT_PORTFOLIO_JSON", str(OUTPUT_DIR / "current_portfolio.json")))
    if not current_path.exists():
        current_path.parent.mkdir(parents=True, exist_ok=True)
        current_path.write_text(json.dumps({"cash_pct": 100.0, "positions": []}, indent=2), encoding="utf-8")
    prev_port = _load_current_portfolio(current_path)
    prev_syms = [s for s in prev_port.keys() if s and s != "__CASH__"]

    market_ctx = get_market_condition()
    market_filter = _market_exposure_filter(market_ctx)
    regime_exposure_target_pct = float(regime.get("exposure_target_pct", 65.0))
    final_exposure_target_pct = max(
        0.0,
        min(100.0, round(regime_exposure_target_pct * _f(market_filter.get("multiplier", 1.0), 1.0), 2)),
    )
    regime["regime_exposure_target_pct"] = regime_exposure_target_pct
    regime["market_filter"] = market_filter
    regime["exposure_target_pre_feasibility_pct"] = final_exposure_target_pct

    features: list[dict[str, Any]] = []
    excluded_candidates: list[dict[str, Any]] = []
    missing: list[str] = []
    missing_diagnostics: list[dict[str, Any]] = []
    max_symbols = int(os.getenv("AI_REBALANCE_MAX_SYMBOLS", "0") or "0")
    volume_hard_floor = max(0.05, min(1.0, _f(os.getenv("AI_MIN_VOLUME_HARD_FLOOR", "0.10"), 0.10)))
    rebound_cfg = _rebound_settings()
    regime["volume_hard_floor"] = round(volume_hard_floor, 3)
    regime["rebound_sleeve"] = rebound_cfg
    if max_symbols > 0:
        symbols = symbols[:max_symbols]

    for sym in symbols:
        df = get_stock_data(sym)
        if df is None:
            missing.append(sym)
            if len(missing_diagnostics) < 200:
                missing_diagnostics.append({"symbol": sym, "reason": "no_ohlcv"})
            continue
        if len(df) < 200:
            missing.append(sym)
            if len(missing_diagnostics) < 200:
                missing_diagnostics.append({"symbol": sym, "reason": "insufficient_history", "rows": int(len(df))})
            continue
        ind = calculate_indicators(df)
        if not ind:
            missing.append(sym)
            if len(missing_diagnostics) < 200:
                missing_diagnostics.append({"symbol": sym, "reason": "indicator_calc_failed"})
            continue
        info = get_stock_info(sym)
        crosses = ind.get("crosses", []) or []
        cross_score = _cross_conviction_score(crosses)
        rsi = _f(ind.get("rsi", 50.0))
        bb_pos = _f(ind.get("bb_position", 50.0))
        vol_ratio = _f(ind.get("volume_ratio", 1.0))
        adx = _f(ind.get("adx", 0.0))
        ma50_gap = _f(ind.get("ma50_gap", 0.0))
        ma200_gap = _f(ind.get("ma200_gap", 0.0))
        rs63 = _f(ind.get("return_63d", 0.0)) - _f(market_ctx.get("benchmark_return_63d", 0.0))
        rs21 = _f(ind.get("return_21d", 0.0)) - _f(market_ctx.get("benchmark_return_21d", 0.0))
        sel_score = _selection_score(rs63, rs21)
        entry_conviction = cross_score
        if vol_ratio >= 1.8:
            entry_conviction += 0.5
        pos_52w = _f(ind.get("position_52w", 50.0))
        if pos_52w >= 90:
            entry_conviction += 0.3
        if rsi >= 70 or bb_pos >= 85:
            entry_conviction -= 0.5
        if rsi <= 30:
            entry_conviction += 0.2
        rebound_candidate = _is_rebound_candidate(
            rsi=rsi,
            bb_pos=bb_pos,
            entry_conviction=entry_conviction,
            volume_ratio=vol_ratio,
            ma200_gap=ma200_gap,
            rebound_cfg=rebound_cfg,
        )
        sleeve = "rebound" if rebound_candidate else "momentum"
        failed: list[str] = []
        rebound_min_adx = _f(rebound_cfg.get("min_adx", 10.0), 10.0)
        allow_rebound_adx = rebound_candidate and adx >= rebound_min_adx
        if adx < regime["min_adx"] and not allow_rebound_adx:
            failed.append("adx_below_min")
        if vol_ratio < volume_hard_floor:
            failed.append("volume_ratio_hard_floor")
        if ma50_gap <= 0 or ma200_gap <= 0:
            if rebound_candidate:
                # keep it in rebound sleeve with small sizing cap later
                pass
            else:
                failed.append("trend_below_ma")
        if sleeve == "rebound" and entry_conviction <= 0:
            failed.append("rebound_conviction_weak")
        if failed:
            excluded_candidates.append(
                {
                    "symbol": sym,
                    "selection_score": sel_score,
                    "relative_strength_63d": rs63,
                    "relative_strength_21d": rs21,
                    "rsi": rsi,
                    "adx": adx,
                    "ma50_gap": ma50_gap,
                    "ma200_gap": ma200_gap,
                    "bb_position": bb_pos,
                    "volume_ratio": vol_ratio,
                    "entry_conviction": round(entry_conviction, 2),
                    "sleeve": sleeve,
                    "filters_failed": failed,
                }
            )
            continue
        row = {
            "symbol": sym,
            "price": _f(ind.get("price", info.get("price"))),
            "sector": info.get("sector", "N/A"),
            "selection_score": sel_score,
            "relative_strength_63d": rs63,
            "relative_strength_21d": rs21,
            "rsi": rsi,
            "adx": adx,
            "ma50_gap": ma50_gap,
            "ma200_gap": ma200_gap,
            "bb_position": bb_pos,
            "atr_pct": _f(ind.get("atr_pct", 0.0)),
            "volume_ratio": vol_ratio,
            "support": ind.get("support", []),
            "resistance": ind.get("resistance", []),
            "entry_conviction": round(entry_conviction, 2),
            "sleeve": sleeve,
            "filters_failed": [],
        }
        row["warnings"] = _candidate_warnings(row)
        if sleeve == "rebound":
            row["warnings"].append("rebound_sleeve")
        if vol_ratio < _f(regime.get("volume_warn_threshold", 1.0), 1.0):
            row["warnings"].append("volume_below_warn_threshold")
        # backward-compat alias for existing consumers
        if "volume_below_warn_threshold" in row["warnings"]:
            row["warnings"].append("volume_below_regime_min")
        features.append(row)

    requested_symbols = max(1, len(symbols))
    fetched_symbols = requested_symbols - len(missing)
    coverage_pct = round(100.0 * float(fetched_symbols) / float(requested_symbols), 2)
    min_coverage_pct = max(0.0, min(100.0, _f(os.getenv("AI_REBALANCE_MIN_COVERAGE_PCT", "20"), 20.0)))
    if coverage_pct < min_coverage_pct:
        raise RuntimeError(
            f"Insufficient market data coverage: {coverage_pct:.2f}% "
            f"(required >= {min_coverage_pct:.2f}%). "
            f"Fetched {fetched_symbols}/{requested_symbols} symbols."
        )

    candidates = _select_candidates(features, prompt_max_symbols, include_symbols=prev_syms)
    if not candidates:
        raise RuntimeError("No candidates after filters. Check data source connectivity and filter thresholds.")
    fill_style = str(os.getenv("AI_FILL_TO_TARGET_STYLE", "balanced")).strip().lower()
    fill_max_positions_raw = int(os.getenv("AI_FILL_TO_TARGET_MAX_POSITIONS", "0") or "0")
    inferred_current_positions = min(int(top_k), len(candidates))
    resolved_max_positions, resolved_fill_style = _resolve_fill_max_positions(
        fill_style=fill_style,
        top_k=top_k,
        max_positions=fill_max_positions_raw,
        candidate_count=len(candidates),
        current_positions=inferred_current_positions,
    )
    effective_exposure_target_pct, exposure_feasibility = _cap_desired_exposure_by_constraints(
        desired_exposure_pct=final_exposure_target_pct,
        max_weight_pct=max_weight_pct,
        max_positions=resolved_max_positions,
    )
    exposure_feasibility["candidate_count"] = int(len(candidates))
    exposure_feasibility["top_k"] = int(top_k)
    exposure_feasibility["fill_style"] = resolved_fill_style
    exposure_feasibility["configured_max_positions"] = (
        None if fill_max_positions_raw <= 0 else int(fill_max_positions_raw)
    )
    exposure_feasibility["candidate_shortfall"] = max(0, int(top_k) - int(len(candidates)))
    regime["exposure_feasible_max_pct"] = exposure_feasibility["feasible_max_exposure_pct"]
    regime["exposure_target_pct"] = effective_exposure_target_pct

    allowed = {x["symbol"] for x in candidates}
    prompt = _portfolio_prompt(
        report=report,
        market_ctx=market_ctx,
        candidates=candidates,
        top_k=top_k,
        max_weight_pct=max_weight_pct,
        turnover_target_pct=turnover_target_pct,
        prev_portfolio_pct=prev_port,
        exposure_target_pct=effective_exposure_target_pct,
        regime_note=regime["note"],
    )

    ai_error_reason: str | None = None
    raw = analyzer._call(prompt, max_tokens=2000) if analyzer.has_api_access else None
    parsed = _extract_json(raw or "")
    if not parsed:
        if analyzer.has_api_access:
            if raw:
                ai_error_reason = "AI response was not valid JSON; used fallback portfolio"
            else:
                ai_error_reason = "AI call returned empty output; used fallback portfolio"
        else:
            ai_error_reason = "AI unavailable; used fallback portfolio"
        if not analyzer.has_api_access and not allow_fallback:
            raise RuntimeError("AI call failed and fallback disabled.")
        parsed = _fallback_portfolio(
            candidates,
            top_k=top_k,
            max_weight_pct=max_weight_pct,
            exposure_target_pct=effective_exposure_target_pct,
        )

    weights_pct, cash_pct = _portfolio_from_ai(
        parsed,
        allowed=allowed,
        top_k=top_k,
        max_weight_pct=max_weight_pct,
        exposure_target_pct=effective_exposure_target_pct,
    )
    cand_by_symbol = {x.get("symbol"): x for x in candidates if isinstance(x, dict)}
    ordered_candidates = sorted(
        candidates,
        key=lambda x: (
            _f(x.get("selection_score", 0.0)),
            _f(x.get("relative_strength_63d", 0.0)),
            _f(x.get("relative_strength_21d", 0.0)),
        ),
        reverse=True,
    )
    weights_pct, sizing_audit = _apply_weight_multipliers(weights_pct, cand_by_symbol, max_weight_pct)
    rebound_max_weight_pct = _f(rebound_cfg.get("max_weight_pct"), 4.0)
    rebound_max_count = int(rebound_cfg.get("max_count", 2) or 2)
    weights_pct, rebound_audit = _apply_rebound_limits(
        weights_pct,
        cand_by_symbol,
        rebound_max_weight_pct=rebound_max_weight_pct,
        rebound_max_count=rebound_max_count,
    )
    sector_cap_pct = max(0.0, min(100.0, _f(os.getenv("AI_SECTOR_CAP_PCT", "100"), 100.0)))
    weights_pct, sector_cap_audit_prefill = _apply_sector_cap(weights_pct, cand_by_symbol, sector_cap_pct)
    weights_pct, fill_audit = _fill_to_target_exposure(
        target_weights_pct=weights_pct,
        desired_exposure_pct=effective_exposure_target_pct,
        max_weight_pct=max_weight_pct,
        top_k=top_k,
        ordered_candidates=ordered_candidates,
        feature_by_symbol=cand_by_symbol,
        fill_style=resolved_fill_style,
        max_positions=resolved_max_positions,
        sector_cap_pct=sector_cap_pct,
    )
    weights_pct, sector_cap_audit_postfill = _apply_sector_cap(weights_pct, cand_by_symbol, sector_cap_pct)
    weights_pct, turnover_audit = _apply_turnover_cap(prev_port, weights_pct, turnover_target_pct, cand_by_symbol)
    turnover_audit["base_turnover_cap_pct"] = round(base_turnover_target_pct, 2)
    turnover_audit["regime_turnover_cap_pct"] = round(regime_turnover_target_pct, 2)
    turnover_audit["selected_turnover_cap_pct"] = round(turnover_target_pct, 2)
    turnover_audit["applied_cap_pct"] = None if turnover_audit.get("mode") == "initial_build_no_constraint" else round(turnover_target_pct, 2)

    min_trade_pct = float(os.getenv("AI_REBALANCE_MIN_TRADE_PCT", "1.0"))
    allow_min_trade_refill = turnover_audit.get("mode") == "initial_build_no_constraint"
    weights_pct, min_trade_reconcile_audit = _reconcile_target_with_min_trade(
        prev_port=prev_port,
        target_weights_pct=weights_pct,
        min_trade_pct=min_trade_pct,
        desired_exposure_pct=effective_exposure_target_pct,
        max_weight_pct=max_weight_pct,
        feature_by_symbol=cand_by_symbol,
        allow_refill=allow_min_trade_refill,
    )
    before_final_sector_cap = float(sum(weights_pct.values()))
    weights_pct, sector_cap_audit_final = _apply_sector_cap(weights_pct, cand_by_symbol, sector_cap_pct)
    after_final_sector_cap = float(sum(weights_pct.values()))
    target_sector_cap_violations = _sector_cap_violations(weights_pct, cand_by_symbol, sector_cap_pct)
    min_trade_reconcile_audit["post_final_sector_cap_adjustment_pct"] = round(
        max(0.0, before_final_sector_cap - after_final_sector_cap),
        2,
    )
    min_trade_reconcile_audit["target_sector_cap_violation_count"] = len(target_sector_cap_violations)

    achieved_exposure_pct = float(sum(weights_pct.values()))
    cash_pct = max(0.0, 100.0 - achieved_exposure_pct)
    exposure_gap_pct = max(0.0, float(effective_exposure_target_pct) - achieved_exposure_pct)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    orders, orders_skipped = _build_orders_with_skips(prev_port, weights_pct, min_trade_pct)
    if orders_skipped:
        # Final safety: align displayed target with executable portfolio if a residual small-delta skip remains.
        aligned_weights, aligned_cash, aligned_exposure = _executed_portfolio_from_orders(prev_port, orders)
        weights_pct = {k: float(v) for k, v in aligned_weights.items() if float(v) > 1e-9}
        cash_pct = max(0.0, float(aligned_cash))
        achieved_exposure_pct = float(aligned_exposure)
        exposure_gap_pct = max(0.0, float(effective_exposure_target_pct) - achieved_exposure_pct)
        min_trade_reconcile_audit["forced_execution_alignment"] = True
        min_trade_reconcile_audit["residual_orders_skipped"] = orders_skipped
        orders, orders_skipped = _build_orders_with_skips(prev_port, weights_pct, min_trade_pct)
    else:
        min_trade_reconcile_audit["forced_execution_alignment"] = False

    executed_weights_pct, executed_cash_pct, achieved_exposure_after_execution_pct = _executed_portfolio_from_orders(
        prev_port,
        orders,
    )
    executed_sector_cap_violations = _sector_cap_violations(executed_weights_pct, cand_by_symbol, sector_cap_pct)
    turnover_definition = str(turnover_audit.get("turnover_definition", "half_l1") or "half_l1")
    final_target_turnover_pct = _turnover_pct(prev_port, weights_pct, definition=turnover_definition)
    final_executed_turnover_pct = _turnover_pct(prev_port, executed_weights_pct, definition=turnover_definition)
    final_target_turnover_l1_pct = _turnover_l1_pct(prev_port, weights_pct)
    final_executed_turnover_l1_pct = _turnover_l1_pct(prev_port, executed_weights_pct)
    turnover_audit["final_target_pct"] = round(final_target_turnover_pct, 2)
    turnover_audit["final_executed_pct"] = round(final_executed_turnover_pct, 2)
    turnover_audit["final_target_l1_pct"] = round(final_target_turnover_l1_pct, 2)
    turnover_audit["final_executed_l1_pct"] = round(final_executed_turnover_l1_pct, 2)
    execution_gap_pct = max(0.0, float(effective_exposure_target_pct) - achieved_exposure_after_execution_pct)
    execution_matches_target_weights = len(orders_skipped) == 0 and abs(
        achieved_exposure_after_execution_pct - achieved_exposure_pct
    ) <= 1e-6
    min_trade_reconcile_audit["executed_sector_cap_violation_count"] = len(executed_sector_cap_violations)

    chart_rationale = {
        sym: _chart_rationale(cand_by_symbol.get(sym, {}))
        for sym in weights_pct.keys()
        if sym in cand_by_symbol
    }

    initial_selected_symbols = [str(k) for k in list(weights_pct.keys())]
    final_selected_symbols = [str(k) for k in list(executed_weights_pct.keys())]
    final_positions_n = len(final_selected_symbols)
    reason_not_filled_to_top_k = None
    if final_positions_n < int(top_k):
        reason_not_filled_to_top_k = (
            "insufficient_clean_candidates" if len(candidates) < int(top_k) else "post_filters_and_constraints"
        )

    result = {
        "generated_at": datetime.now().isoformat(),
        "report_path": str(report_path),
        "universe": universe_name,
        "top_k": top_k,
        "final_positions_n": final_positions_n,
        "initial_selected_symbols": initial_selected_symbols,
        "final_selected_symbols": final_selected_symbols,
        "reason_not_filled_to_top_k": reason_not_filled_to_top_k,
        "max_weight_pct": max_weight_pct,
        "turnover_target_pct": turnover_target_pct,
        "turnover_definition": turnover_audit.get("turnover_definition", "half_l1"),
        "final_target_turnover_pct": round(final_target_turnover_pct, 2),
        "final_executed_turnover_pct": round(final_executed_turnover_pct, 2),
        "effective_turnover_target_pct": effective_turnover_target_pct,
        "effective_turnover_source": "min(user,regime)",
        "turnover_audit": turnover_audit,
        "desired_exposure_raw_pct": round(final_exposure_target_pct, 2),
        "desired_exposure_pct": round(effective_exposure_target_pct, 2),
        "achieved_exposure_pct": round(achieved_exposure_pct, 2),
        "exposure_gap_pct": round(exposure_gap_pct, 2),
        "exposure_feasibility": exposure_feasibility,
        "exposure_fill_audit": fill_audit,
        "min_trade_reconcile_audit": min_trade_reconcile_audit,
        "executed_weights_pct": executed_weights_pct,
        "executed_cash_pct": round(executed_cash_pct, 2),
        "achieved_exposure_after_execution_pct": round(achieved_exposure_after_execution_pct, 2),
        "execution_gap_pct": round(execution_gap_pct, 2),
        "execution_matches_target_weights": execution_matches_target_weights,
        "regime_controls": regime,
        "market_ctx": market_ctx,
        "risk_on_off": (report.get("module1_liquidity") or {}).get("risk_on_off", {}),
        "decision_basis": {
            "macro_metrics": (report.get("module1_liquidity") or {}).get("metrics", {}),
            "risk_on_components": ((report.get("module1_liquidity") or {}).get("risk_on_off", {}) or {}).get(
                "components", []
            ),
            "etf_flows_latest": {
                "latest": (report.get("module1_liquidity") or {}).get("etf_flows", {}).get(
                    "latest_net_inflow_month_usd_b"
                ),
                "latest_scope": (report.get("module1_liquidity") or {}).get("etf_flows", {}).get("latest_scope"),
                "latest_date": (report.get("module1_liquidity") or {}).get("etf_flows", {}).get("latest_date"),
                "latest_kind": (report.get("module1_liquidity") or {}).get("etf_flows", {}).get("latest_kind"),
                "latest_month_label": (report.get("module1_liquidity") or {}).get("etf_flows", {}).get(
                    "latest_month_label"
                ),
                "latest_by_published_date": (report.get("module1_liquidity") or {}).get("etf_flows", {}).get(
                    "latest_by_published_date"
                ),
            },
            "confidence": {
                "macro": (report.get("module1_liquidity") or {}).get("confidence_macro"),
                "flows": (report.get("module1_liquidity") or {}).get("confidence_flows"),
                "module1": (report.get("module1_liquidity") or {}).get("confidence"),
            },
            "data_gaps": report.get("data_gaps", []),
        },
        "weights_pct": weights_pct,
        "cash_pct": cash_pct,
        "candidates": candidates,
        "excluded_candidates": excluded_candidates[:500],
        "position_sizing_audit": sizing_audit,
        "rebound_audit": rebound_audit,
        "sector_cap_audit": {
            "prefill": sector_cap_audit_prefill,
            "postfill": sector_cap_audit_postfill,
            "final": sector_cap_audit_final,
            "sector_cap_pct": round(sector_cap_pct, 2),
        },
        "sector_cap_violations_target": target_sector_cap_violations,
        "sector_cap_violations_executed": executed_sector_cap_violations,
        "missing_symbols": missing[:200],
        "missing_diagnostics": missing_diagnostics,
        "orders": orders,
        "orders_skipped": orders_skipped,
        "chart_rationale": chart_rationale,
        "ai_raw": raw if isinstance(raw, str) else "",
        "ai_error": ai_error_reason,
        "ai_fallback": bool(parsed.get("_fallback", False)),
    }

    date_tag = datetime.now().strftime("%Y-%m-%d")
    out_md = None
    out_orders_csv = OUTPUT_DIR / f"rebalance_orders_{date_tag}.csv"
    out_result_json = OUTPUT_DIR / f"rebalance_recommendation_{date_tag}.json"
    out_orders_csv.parent.mkdir(parents=True, exist_ok=True)
    if orders:
        header = "symbol,action,current_weight_pct,target_weight_pct,delta_pct\n"
        rows = [
            f"{o['symbol']},{o['action']},{o['current_weight_pct']},{o['target_weight_pct']},{o['delta_pct']}"
            for o in orders
        ]
        out_orders_csv.write_text(header + "\n".join(rows), encoding="utf-8")
    else:
        out_orders_csv.write_text("symbol,action,current_weight_pct,target_weight_pct,delta_pct\n", encoding="utf-8")

    out_result_json.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "md_path": None,
        "orders_csv": str(out_orders_csv),
        "result_json": str(out_result_json),
        "result": result,
    }


if __name__ == "__main__":
    run_us_rebalance()
