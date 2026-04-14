from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
TARGET_TS = ROOT / "cloudflare" / "telegram-bot" / "src" / "snapshot-data.ts"
TARGET_JSON = ROOT / "cloudflare" / "telegram-bot" / "live" / "snapshot.json"
FALLBACK_STRATEGY_METRICS: dict[str, dict[str, Any]] = {
    "v4": {
        "label": "Strategy V4",
        "cagr": 54.16,
        "benchmarkCagr": 18.96,
        "drawdown": -33.81,
        "turnover": 0.197,
        "pAlphaGt0": 0.999,
    },
}
def _latest_json(pattern: str) -> Path | None:
    files = sorted((ROOT / "data" / "runs").glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        return None
    return files[0]


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_bool(raw: str) -> bool:
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _latest_rebalance_json() -> Path | None:
    files = sorted((ROOT / "data" / "rebalance").glob("rebalance_recommendation_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        return None
    return files[0]


def _positions_from_weights(weights: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for symbol, weight in (weights or {}).items():
        try:
            weight_value = float(weight)
        except Exception:
            continue
        if weight_value <= 0:
            continue
        rows.append({"symbol": str(symbol).upper(), "weight_pct": round(weight_value, 4)})
    rows.sort(key=lambda row: -float(row["weight_pct"]))
    return rows


def _pct_gap(price: Any, ma_value: Any) -> float | None:
    try:
        price_value = float(price)
        ma = float(ma_value)
    except Exception:
        return None
    if ma == 0:
        return None
    return (price_value / ma - 1.0) * 100.0


def _build_current_signal() -> dict[str, Any]:
    path = _latest_rebalance_json()
    if path is None:
        return {
            "latestMarketDay": "",
            "signalDay": "",
            "entryDay": "",
            "regimeState": "unknown",
            "regimeReason": "",
            "positions": [],
            "positionPriceRefs": [],
            "liveSignalDay": "",
            "liveRegimeState": "unknown",
            "liveRegimeReason": "",
            "livePositions": [],
            "livePositionPriceRefs": [],
            "signalQqqClose": None,
            "signalQqqMa200Gap": None,
            "signalQqqReturn21d": None,
            "signalQqqReturn63d": None,
            "signalVixClose": None,
            "latestQqqClose": None,
            "latestQqqMa200Gap": None,
            "latestQqqReturn21d": None,
            "latestQqqReturn63d": None,
            "latestVixClose": None,
        }

    payload = _load_json(path)
    generated_at = str(payload.get("generated_at", "") or "")
    generated_day = str(pd.Timestamp(generated_at).date()) if generated_at else ""
    executed_weights = payload.get("executed_weights_pct") or payload.get("weights_pct") or {}
    positions = _positions_from_weights(executed_weights)
    current_positions = _positions_from_weights(payload.get("weights_pct") or executed_weights)
    executed_cash_pct = payload.get("executed_cash_pct")
    current_cash_pct = payload.get("cash_pct")

    candidates = {
        str(item.get("symbol", "")).upper(): item
        for item in (payload.get("candidates") or [])
        if isinstance(item, dict) and str(item.get("symbol", "")).strip()
    }
    orders = {
        str(item.get("symbol", "")).upper(): item
        for item in (payload.get("orders") or [])
        if isinstance(item, dict) and str(item.get("symbol", "")).strip()
    }

    refs: list[dict[str, Any]] = []
    live_refs: list[dict[str, Any]] = []
    for position in positions:
        symbol = str(position.get("symbol", "")).upper()
        candidate = candidates.get(symbol, {})
        order = orders.get(symbol, {})
        entry_plan = order.get("entry_plan") if isinstance(order, dict) else {}
        average_entry = None
        if isinstance(entry_plan, dict):
            try:
                average_entry = float(entry_plan.get("average_entry_price"))
            except Exception:
                average_entry = None
        latest_close = None
        try:
            latest_close = float(candidate.get("price"))
        except Exception:
            latest_close = None
        refs.append(
            {
                "symbol": symbol,
                "weightPct": round(float(position.get("weight_pct", 0.0) or 0.0), 4),
                "entryDay": generated_day,
                "entryDayOpen": round(average_entry, 2) if average_entry is not None else None,
                "latestMarketDay": generated_day,
                "latestClose": round(latest_close, 2) if latest_close is not None else None,
            }
        )
        live_refs.append(
            {
                "symbol": symbol,
                "weightPct": round(float(position.get("weight_pct", 0.0) or 0.0), 4),
                "latestMarketDay": generated_day,
                "latestClose": round(latest_close, 2) if latest_close is not None else None,
            }
        )

    market_ctx = payload.get("market_ctx") if isinstance(payload.get("market_ctx"), dict) else {}
    risk = payload.get("risk_on_off") if isinstance(payload.get("risk_on_off"), dict) else {}
    regime_controls = payload.get("regime_controls") if isinstance(payload.get("regime_controls"), dict) else {}
    qqq_close = market_ctx.get("price")
    qqq_ma200_gap = _pct_gap(market_ctx.get("price"), market_ctx.get("ma200"))
    qqq_return_21d = market_ctx.get("benchmark_return_21d")
    qqq_return_63d = market_ctx.get("benchmark_return_63d")

    return {
        "latestMarketDay": generated_day,
        "signalDay": generated_day,
        "entryDay": generated_day,
        "regimeState": str(risk.get("label", "unknown") or "unknown"),
        "regimeReason": str(regime_controls.get("note", "") or ""),
        "cashPct": round(float(current_cash_pct), 2) if current_cash_pct is not None else round(100.0 - sum(float(row["weight_pct"]) for row in current_positions), 2),
        "positions": current_positions,
        "positionPriceRefs": refs,
        "liveSignalDay": generated_day,
        "liveRegimeState": str(risk.get("label", "unknown") or "unknown"),
        "liveRegimeReason": str(regime_controls.get("note", "") or ""),
        "liveCashPct": round(float(executed_cash_pct), 2) if executed_cash_pct is not None else round(100.0 - sum(float(row["weight_pct"]) for row in positions), 2),
        "livePositions": positions,
        "livePositionPriceRefs": live_refs,
        "signalQqqClose": round(float(qqq_close), 2) if qqq_close is not None else None,
        "signalQqqMa200Gap": round(float(qqq_ma200_gap), 4) if qqq_ma200_gap is not None else None,
        "signalQqqReturn21d": round(float(qqq_return_21d), 4) if qqq_return_21d is not None else None,
        "signalQqqReturn63d": round(float(qqq_return_63d), 4) if qqq_return_63d is not None else None,
        "signalVixClose": None,
        "latestQqqClose": round(float(qqq_close), 2) if qqq_close is not None else None,
        "latestQqqMa200Gap": round(float(qqq_ma200_gap), 4) if qqq_ma200_gap is not None else None,
        "latestQqqReturn21d": round(float(qqq_return_21d), 4) if qqq_return_21d is not None else None,
        "latestQqqReturn63d": round(float(qqq_return_63d), 4) if qqq_return_63d is not None else None,
        "latestVixClose": None,
    }


def _strategy_metrics(pattern: str, strategy_key: str) -> dict[str, Any]:
    fallback = dict(FALLBACK_STRATEGY_METRICS[strategy_key])
    path = _latest_json(pattern)
    if path is None:
        return fallback
    payload = _load_json(path)
    metrics = payload.get("metrics") or {}
    alpha = payload.get("alpha") or {}
    turnover = payload.get("turnover") or {}
    ai = (metrics.get("ai_portfolio") or {}) if isinstance(metrics, dict) else {}
    benchmark = (metrics.get("benchmark") or {}) if isinstance(metrics, dict) else {}
    turnover_ai = (turnover.get("ai") or {}) if isinstance(turnover, dict) else {}
    return {
        "label": fallback["label"],
        "cagr": round(float(ai.get("cagr_pct", fallback["cagr"]) or fallback["cagr"]), 2),
        "benchmarkCagr": round(
            float(benchmark.get("cagr_pct", fallback["benchmarkCagr"]) or fallback["benchmarkCagr"]),
            2,
        ),
        "drawdown": round(float(ai.get("max_drawdown_pct", fallback["drawdown"]) or fallback["drawdown"]), 2),
        "turnover": round(float(turnover_ai.get("mean", fallback["turnover"]) or fallback["turnover"]), 3),
        "pAlphaGt0": round(float(alpha.get("nw_p_gt0", fallback["pAlphaGt0"]) or fallback["pAlphaGt0"]), 3),
    }


def _build_payload() -> dict[str, Any]:
    return {
        "generatedAt": str(pd.Timestamp.utcnow()),
        "strategies": {
            "v4": _strategy_metrics("ai_portfolio_backtest_verification_strategy_v4*.json", "v4"),
        },
        "rebalance": {
            "v4": _build_current_signal(),
        },
    }


def main() -> None:
    payload = _build_payload()
    TARGET_TS.parent.mkdir(parents=True, exist_ok=True)
    TARGET_JSON.parent.mkdir(parents=True, exist_ok=True)
    TARGET_TS.write_text(
        "export const SNAPSHOT = " + json.dumps(payload, ensure_ascii=False, indent=2) + " as const;\n",
        encoding="utf-8",
    )
    TARGET_JSON.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"Saved: {TARGET_TS.relative_to(ROOT)}")
    print(f"Saved: {TARGET_JSON.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
