"""Inventory reconcile helpers (internal SSOT vs channel snapshot)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _f(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        if out != out:  # NaN
            return default
        return out
    except Exception:
        return default


def _s(value: Any) -> str:
    return str(value or "").strip()


def _normalize_snapshot_row(row: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(row, dict):
        return None
    sku = _s(row.get("sku") or row.get("variant_id") or row.get("symbol")).upper()
    location = _s(row.get("location") or row.get("location_id") or "default").lower()
    available = _f(row.get("available"), None)
    if available is None:
        # fallback aliases
        if "available_qty" in row:
            available = _f(row.get("available_qty"), None)
        elif "qty" in row:
            available = _f(row.get("qty"), None)
    if not sku or available is None:
        return None
    return {"sku": sku, "location": location, "available": float(available)}


def load_channel_snapshot_json(path: str | Path) -> list[dict[str, Any]]:
    src = Path(path)
    if not src.exists():
        return []
    try:
        obj = json.loads(src.read_text(encoding="utf-8"))
    except Exception:
        return []
    rows: list[dict[str, Any]] = []
    raw = obj.get("stocks") if isinstance(obj, dict) and isinstance(obj.get("stocks"), list) else obj
    if not isinstance(raw, list):
        return rows
    for item in raw:
        normalized = _normalize_snapshot_row(item if isinstance(item, dict) else {})
        if normalized:
            rows.append(normalized)
    return rows


def reconcile_available(
    internal_balances: list[dict[str, Any]],
    channel_snapshot: list[dict[str, Any]],
    mismatch_threshold: float = 0.0001,
) -> dict[str, Any]:
    threshold = max(0.0, float(_f(mismatch_threshold, 0.0001)))
    internal_map: dict[str, float] = {}
    channel_map: dict[str, float] = {}

    for row in internal_balances or []:
        if not isinstance(row, dict):
            continue
        sku = _s(row.get("sku")).upper()
        location = _s(row.get("location") or "default").lower()
        if not sku:
            continue
        key = f"{sku}@{location}"
        internal_map[key] = float(_f(row.get("available"), 0.0))

    for row in channel_snapshot or []:
        if not isinstance(row, dict):
            continue
        sku = _s(row.get("sku")).upper()
        location = _s(row.get("location") or "default").lower()
        if not sku:
            continue
        key = f"{sku}@{location}"
        channel_map[key] = float(_f(row.get("available"), 0.0))

    mismatches: list[dict[str, Any]] = []
    matched = 0
    keys = sorted(set(internal_map) | set(channel_map))
    for key in keys:
        sku, location = key.split("@", 1)
        in_has = key in internal_map
        ch_has = key in channel_map
        in_qty = float(internal_map.get(key, 0.0))
        ch_qty = float(channel_map.get(key, 0.0))

        if in_has and ch_has:
            diff = in_qty - ch_qty
            if abs(diff) <= threshold:
                matched += 1
                continue
            status = "mismatch"
        elif in_has:
            diff = in_qty
            status = "missing_in_channel"
        else:
            diff = -ch_qty
            status = "missing_in_internal"

        mismatches.append(
            {
                "sku": sku,
                "location": location,
                "internal_available": round(in_qty, 4),
                "channel_available": round(ch_qty, 4),
                "diff_qty": round(diff, 4),
                "status": status,
            }
        )

    missing_internal = sum(1 for x in mismatches if x["status"] == "missing_in_internal")
    missing_channel = sum(1 for x in mismatches if x["status"] == "missing_in_channel")
    pure_mismatch = sum(1 for x in mismatches if x["status"] == "mismatch")

    return {
        "threshold": threshold,
        "internal_count": len(internal_map),
        "channel_count": len(channel_map),
        "matched_count": matched,
        "mismatch_count": len(mismatches),
        "mismatch_by_status": {
            "mismatch": pure_mismatch,
            "missing_in_internal": missing_internal,
            "missing_in_channel": missing_channel,
        },
        "mismatches": mismatches,
    }


__all__ = [
    "load_channel_snapshot_json",
    "reconcile_available",
]

