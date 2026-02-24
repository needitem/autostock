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


def _latest_run_dir(base: Path) -> Path | None:
    if not base.exists():
        return None
    runs = [p for p in base.iterdir() if p.is_dir() and p.name.startswith("run_")]
    if not runs:
        return None
    runs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return runs[0]


def _load_report(report_dir: Path) -> dict[str, Any]:
    path = report_dir / "report.json"
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
    df = sorted(features, key=lambda x: float(x.get("relative_strength_63d", 0.0)), reverse=True)
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
            f"atr_pct={x.get('atr_pct', 0):.2f} sector={x.get('sector', 'N/A')}"
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


def _fallback_portfolio(candidates: list[dict[str, Any]], top_k: int, max_weight_pct: float) -> dict[str, Any]:
    if not candidates:
        return {"cash_pct": 100.0, "positions": []}
    ordered = sorted(candidates, key=lambda x: float(x.get("relative_strength_63d", 0.0)), reverse=True)
    picked = ordered[: max(1, int(top_k))]
    n = max(1, len(picked))
    w = min(100.0 / n, float(max_weight_pct))
    positions = [{"symbol": x["symbol"], "weight_pct": w} for x in picked]
    cash = max(0.0, 100.0 - w * n)
    return {"cash_pct": cash, "positions": positions, "_fallback": True}


def _portfolio_from_ai(
    obj: dict[str, Any],
    allowed: set[str],
    top_k: int,
    max_weight_pct: float,
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
    return weights, cash




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
        notes.append(f"RS63 ??(+{rs63:.1f}%p)")
    elif rs63 <= -5:
        notes.append(f"RS63 ??({rs63:.1f}%p)")

    if adx >= 25:
        notes.append(f"?? ??(ADX {adx:.0f})")
    elif adx <= 15:
        notes.append(f"?? ??(ADX {adx:.0f})")

    if rsi >= 70:
        notes.append(f"??(RSI {rsi:.0f})")
    elif rsi <= 30:
        notes.append(f"???(RSI {rsi:.0f})")
    else:
        notes.append(f"RSI ??({rsi:.0f})")

    if ma50_gap >= 3 and ma200_gap >= 3:
        notes.append("??>MA50/200 (?? ??)")
    elif ma50_gap <= -3 or ma200_gap <= -3:
        notes.append("??<MA50/200 (?? ??)")

    if vol_ratio >= 1.8:
        notes.append(f"??? ??({vol_ratio:.1f}x)")

    if bb_pos >= 80:
        notes.append("?? ??(?? ??)")
    elif bb_pos <= 20:
        notes.append("?? ??(?? ??)")

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
    if not analyzer.has_api_access:
        raise RuntimeError("Codex login required. Run: codex login")

    report_base = Path(report_dir) if report_dir else _latest_run_dir(ROOT / "outputs")
    if report_base is None:
        raise RuntimeError("No report directory found under outputs/")

    report = _load_report(report_base)
    universe_name, symbols = _load_universe()
    top_k = max(1, int(os.getenv("AI_PORTFOLIO_TOP_K", "8")))
    max_weight_pct = float(os.getenv("AI_PORTFOLIO_MAX_WEIGHT_PCT", "25"))
    prompt_max_symbols = max(10, int(os.getenv("AI_PROMPT_MAX_SYMBOLS", "35")))
    turnover_target_pct = float(os.getenv("AI_PORTFOLIO_TURNOVER_TARGET_PCT", "30"))

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
    )

    raw = analyzer._call(prompt, max_tokens=2000)
    parsed = _extract_json(raw or "")
    if not parsed:
        parsed = _fallback_portfolio(candidates, top_k=top_k, max_weight_pct=max_weight_pct)

    weights_pct, cash_pct = _portfolio_from_ai(parsed, allowed=allowed, top_k=top_k, max_weight_pct=max_weight_pct)
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
        "report_dir": str(report_base),
        "universe": universe_name,
        "top_k": top_k,
        "max_weight_pct": max_weight_pct,
        "turnover_target_pct": turnover_target_pct,
        "market_ctx": market_ctx,
        "risk_on_off": (report.get("module1_liquidity") or {}).get("risk_on_off", {}),
        "weights_pct": weights_pct,
        "cash_pct": cash_pct,
        "candidates": candidates,
        "missing_symbols": missing[:200],
        "orders": orders,
        "chart_rationale": chart_rationale,
        "ai_raw": raw if isinstance(raw, str) else "",
        "ai_fallback": bool(parsed.get("_fallback", False)),
    }

    date_tag = datetime.now().strftime("%Y-%m-%d")
    out_md = None
    out_orders_csv = OUTPUT_DIR / f"rebalance_orders_{date_tag}.csv"
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



    return {"md_path": None, "orders_csv": str(out_orders_csv), "result": result}


if __name__ == "__main__":
    run_us_rebalance()
