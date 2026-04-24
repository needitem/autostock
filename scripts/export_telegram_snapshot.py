from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from local_telegram_trade import analyze_rebalance_universe

OUTPUT_ROOT = ROOT / "outputs" / "telegram"
TARGET_JSON = OUTPUT_ROOT / "snapshot.json"
EVENT_RUNTIME_DIR = ROOT / "data" / "event_runtime"
NAUTILUS_ROOT = ROOT / "data" / "nautilus_v2"


def _s(value: Any) -> str:
    return str(value or "").strip()


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _latest_dir(root: Path) -> Path | None:
    if not root.exists():
        return None
    candidates = [item for item in root.iterdir() if item.is_dir()]
    if not candidates:
        return None
    candidates.sort(key=lambda item: item.stat().st_mtime, reverse=True)
    return candidates[0]


def _f(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _latest_runtime_profile_dir() -> Path | None:
    return _latest_dir(EVENT_RUNTIME_DIR)


def _latest_nautilus_bundle_dir(profile_name: str) -> Path | None:
    return _latest_dir(NAUTILUS_ROOT / profile_name)


def _build_runtime_snapshot() -> dict[str, Any]:
    profile_dir = _latest_runtime_profile_dir()
    if profile_dir is None:
        return {}

    payload_path = profile_dir / "latest_payload.json"
    state_path = profile_dir / "state.json"
    if not payload_path.exists():
        return {}

    payload = _load_json(payload_path)
    state = _load_json(state_path) if state_path.exists() else {}
    recommendations = payload.get("recommendations") if isinstance(payload.get("recommendations"), list) else []
    top = recommendations[0] if recommendations and isinstance(recommendations[0], dict) else {}
    chart_gate = top.get("chart_gate") if isinstance(top.get("chart_gate"), dict) else {}

    return {
        "profile": profile_dir.name,
        "generatedAt": _s(payload.get("generated_at")),
        "symbol": _s(top.get("symbol") or payload.get("watchlist", [""])[0] if isinstance(payload.get("watchlist"), list) else ""),
        "action": _s(top.get("action")),
        "confidence": round(float(top.get("confidence", 0.0) or 0.0), 2),
        "eventSignal": _s(top.get("event_signal")),
        "eventStrength": _s(top.get("event_strength")),
        "price": round(float(top.get("price", 0.0) or 0.0), 2) if top.get("price") is not None else None,
        "chartState": _s(chart_gate.get("state")),
        "volumeRatio": round(float(chart_gate.get("volume_ratio", 0.0) or 0.0), 2) if chart_gate.get("volume_ratio") is not None else None,
        "reasons": [str(item) for item in (top.get("reason_lines") or [])[:5]],
        "nextKnownEvents": (payload.get("next_known_events") or [])[:5],
        "rawEvents": (top.get("raw_events") or [])[:5],
        "state": {
            "lastRunAt": _s(state.get("last_run_at")),
            "lastAction": _s(((state.get("last_actions") or {}).get(_s(top.get("symbol")), ""))),
            "seenEventCount": len(state.get("seen_event_keys") or []),
        },
    }


def _build_nautilus_snapshot(profile_name: str) -> dict[str, Any]:
    bundle_dir = _latest_nautilus_bundle_dir(profile_name)
    if bundle_dir is None:
        return {}

    signal_files = sorted(bundle_dir.glob("*_signal_snapshot.json"), key=lambda item: item.stat().st_mtime, reverse=True)
    backtest_files = sorted(bundle_dir.glob("*_nautilus_backtest_summary.json"), key=lambda item: item.stat().st_mtime, reverse=True)

    signal_snapshot = _load_json(signal_files[0]) if signal_files else {}
    backtest_payload = _load_json(backtest_files[0]) if backtest_files else {}
    backtest_summary = backtest_payload.get("backtest_summary", backtest_payload) if isinstance(backtest_payload, dict) else {}

    return {
        "profile": profile_name,
        "bundleDir": str(bundle_dir),
        "generatedAt": _s(signal_snapshot.get("generated_at")) or pd.Timestamp.utcnow().isoformat(),
        "signalSnapshot": signal_snapshot,
        "backtestSummary": backtest_summary,
        "catalogImport": backtest_payload.get("catalog_import", {}) if isinstance(backtest_payload, dict) else {},
    }


def _build_payload() -> dict[str, Any]:
    runtime = _build_runtime_snapshot()
    profile_name = _s(runtime.get("profile") or "tsla")
    return {
        "generatedAt": pd.Timestamp.utcnow().isoformat(),
        "app": {
            "name": "autostock-nautilus",
            "mode": "local-telegram",
            "profile": profile_name,
            "symbol": _s(runtime.get("symbol") or "TSLA"),
        },
        "runtime": runtime,
        "nautilus": _build_nautilus_snapshot(profile_name),
        "trade": analyze_rebalance_universe(force_refresh=False),
    }


def main() -> None:
    payload = _build_payload()
    TARGET_JSON.parent.mkdir(parents=True, exist_ok=True)
    TARGET_JSON.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"Saved: {TARGET_JSON.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
