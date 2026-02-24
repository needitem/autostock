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
            float(x.get("relative_strength_63d", 0.0)),
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
    keep = sorted(chosen.values(), key=lambda x: float(x.get("relative_strength_63d", 0.0)), reverse=True)
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
        cand_lines.append(
            f"{x['symbol']} price={x.get('price', 0):.2f} "
            f"rs63={x.get('relative_strength_63d', 0):.1f} rs21={x.get('relative_strength_21d', 0):.1f} "
            f"rsi={x.get('rsi', 50):.1f} adx={x.get('adx', 0):.1f} "
            f"ma50_gap={x.get('ma50_gap', 0):.1f}% ma200_gap={x.get('ma200_gap', 0):.1f}% "
            f"bb_pos={x.get('bb_position', 50):.0f} vol_ratio={x.get('volume_ratio', 1):.2f} "
            f"atr_pct={x.get('atr_pct', 0):.2f} sector={x.get('sector', 'N/A')} "
            f"conv={x.get('entry_conviction', 0):.1f}"
        )

    return (
        "You are a disciplined US equity portfolio allocator.\n"
        "Write in Korean. Use only the provided research report + chart indicators.\n"
        "Goal: build a LONG-ONLY portfolio for the next 1-3 months.\n"
        "Momentum (rs63/rs21) is primary, but use risk filters (rsi/ma gaps/adx/vol).\n"
        "Avoid names with weak chart structure or extreme overextension.\n\n"
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
            float(x.get("relative_strength_63d", 0.0)),
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
        raise ValueError("no valid weights")

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
        raise ValueError("sum weights <= 0")
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
    if str(flows_conf).lower() in {"low", "medium"}:
        note += " Flows confidence not high; keep macro tilt modest."
    return {
        "label": label,
        "score": score,
        "exposure_target_pct": exposure,
        "max_weight_pct": max_weight,
        "turnover_target_pct": turnover,
        "min_adx": min_adx,
        "min_volume_ratio": min_vol_ratio,
        "note": note,
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
    turnover_target_pct = float(os.getenv("AI_PORTFOLIO_TURNOVER_TARGET_PCT", "30"))
    regime = _regime_controls(report)
    max_weight_pct = min(max_weight_pct, regime["max_weight_pct"])
    turnover_target_pct = min(turnover_target_pct, regime["turnover_target_pct"])

    current_path = Path(os.getenv("AI_CURRENT_PORTFOLIO_JSON", str(OUTPUT_DIR / "current_portfolio.json")))
    if not current_path.exists():
        current_path.parent.mkdir(parents=True, exist_ok=True)
        current_path.write_text(json.dumps({"cash_pct": 100.0, "positions": []}, indent=2), encoding="utf-8")
    prev_port = _load_current_portfolio(current_path)
    prev_syms = [s for s in prev_port.keys() if s and s != "__CASH__"]

    market_ctx = get_market_condition()

    features: list[dict[str, Any]] = []
    missing: list[str] = []
    max_symbols = int(os.getenv("AI_REBALANCE_MAX_SYMBOLS", "0") or "0")
    if max_symbols > 0:
        symbols = symbols[:max_symbols]

    for sym in symbols:
        df = get_stock_data(sym)
        if df is None or len(df) < 200:
            missing.append(sym)
            continue
        ind = calculate_indicators(df)
        if not ind:
            missing.append(sym)
            continue
        info = get_stock_info(sym)
        crosses = ind.get("crosses", []) or []
        cross_score = 0.0
        for c in crosses:
            sig = str(c.get("signal", "")).lower()
            typ = str(c.get("type", "")).lower()
            if "매수" in sig or "golden" in typ or "macd" in typ:
                cross_score += 0.5
        rsi = _f(ind.get("rsi", 50.0))
        bb_pos = _f(ind.get("bb_position", 50.0))
        vol_ratio = _f(ind.get("volume_ratio", 1.0))
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
        if _f(ind.get("adx", 0.0)) < regime["min_adx"]:
            continue
        if vol_ratio < regime["min_volume_ratio"]:
            continue
        features.append(
            {
                "symbol": sym,
                "price": _f(ind.get("price", info.get("price"))),
                "sector": info.get("sector", "N/A"),
                "relative_strength_63d": _f(ind.get("return_63d", 0.0)) - _f(market_ctx.get("benchmark_return_63d", 0.0)),
                "relative_strength_21d": _f(ind.get("return_21d", 0.0)) - _f(market_ctx.get("benchmark_return_21d", 0.0)),
                "rsi": _f(ind.get("rsi", 50.0)),
                "adx": _f(ind.get("adx", 0.0)),
                "ma50_gap": _f(ind.get("ma50_gap", 0.0)),
                "ma200_gap": _f(ind.get("ma200_gap", 0.0)),
                "bb_position": _f(ind.get("bb_position", 50.0)),
                "atr_pct": _f(ind.get("atr_pct", 0.0)),
                "volume_ratio": _f(ind.get("volume_ratio", 1.0)),
                "support": ind.get("support", []),
                "resistance": ind.get("resistance", []),
                "entry_conviction": round(entry_conviction, 2),
            }
        )

    candidates = _select_candidates(features, prompt_max_symbols, include_symbols=prev_syms)
    allowed = {x["symbol"] for x in candidates}
    prompt = _portfolio_prompt(
        report=report,
        market_ctx=market_ctx,
        candidates=candidates,
        top_k=top_k,
        max_weight_pct=max_weight_pct,
        turnover_target_pct=turnover_target_pct,
        prev_portfolio_pct=prev_port,
        exposure_target_pct=regime["exposure_target_pct"],
        regime_note=regime["note"],
    )

    raw = analyzer._call(prompt, max_tokens=2000) if analyzer.has_api_access else None
    parsed = _extract_json(raw or "")
    if not parsed:
        if not analyzer.has_api_access:
            raise RuntimeError("AI call failed and fallback disabled.")
        parsed = _fallback_portfolio(
            candidates,
            top_k=top_k,
            max_weight_pct=max_weight_pct,
            exposure_target_pct=regime["exposure_target_pct"],
        )

    weights_pct, cash_pct = _portfolio_from_ai(
        parsed,
        allowed=allowed,
        top_k=top_k,
        max_weight_pct=max_weight_pct,
        exposure_target_pct=regime["exposure_target_pct"],
    )
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    min_trade_pct = float(os.getenv("AI_REBALANCE_MIN_TRADE_PCT", "1.0"))
    orders = _build_orders(prev_port, weights_pct, min_trade_pct)

    cand_by_symbol = {x.get("symbol"): x for x in candidates if isinstance(x, dict)}
    chart_rationale = {
        sym: _chart_rationale(cand_by_symbol.get(sym, {}))
        for sym in weights_pct.keys()
        if sym in cand_by_symbol
    }

    result = {
        "generated_at": datetime.now().isoformat(),
        "report_path": str(report_path),
        "universe": universe_name,
        "top_k": top_k,
        "max_weight_pct": max_weight_pct,
        "turnover_target_pct": turnover_target_pct,
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
        "missing_symbols": missing[:200],
        "orders": orders,
        "chart_rationale": chart_rationale,
        "ai_raw": raw if isinstance(raw, str) else "",
        "ai_error": None if analyzer.has_api_access else "AI unavailable; used fallback portfolio",
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
