"""Inventory ledger core utilities.

The goal is to provide a minimal, deterministic SSOT scaffold that can be
expanded later with channel connectors and a persistent DB.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        if out != out:  # NaN
            return default
        return out
    except Exception:
        return default


def _safe_str(value: Any) -> str:
    return str(value or "").strip()


def _parse_ts(value: Any) -> str:
    raw = _safe_str(value)
    if not raw:
        return datetime.now(timezone.utc).isoformat()
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        return datetime.fromisoformat(raw).astimezone(timezone.utc).isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()


def _normalize_movement(row: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(row, dict):
        return None

    movement_id = _safe_str(row.get("movement_id") or row.get("id"))
    sku = _safe_str(row.get("sku") or row.get("variant_id") or row.get("symbol"))
    location = _safe_str(row.get("location") or row.get("location_id") or "default")
    movement_type = _safe_str(row.get("type") or "ADJUST").upper()

    qty_delta = _safe_float(row.get("qty_delta"), 0.0)
    allocated_delta = _safe_float(row.get("allocated_delta"), 0.0)

    # Backward compatibility: support qty + direction style payload.
    if qty_delta == 0 and "qty" in row:
        qty = _safe_float(row.get("qty"), 0.0)
        direction = _safe_str(row.get("direction") or "").lower()
        if direction in {"out", "decrease", "minus", "-"}:
            qty = -abs(qty)
        elif direction in {"in", "increase", "plus", "+"}:
            qty = abs(qty)
        qty_delta = qty

    if not sku:
        return None
    if qty_delta == 0 and allocated_delta == 0:
        return None

    return {
        "movement_id": movement_id,
        "sku": sku.upper(),
        "location": location.lower(),
        "type": movement_type,
        "qty_delta": float(qty_delta),
        "allocated_delta": float(allocated_delta),
        "ts": _parse_ts(row.get("ts") or row.get("timestamp") or row.get("created_at")),
        "reason": _safe_str(row.get("reason")),
        "ref_type": _safe_str(row.get("ref_type")),
        "ref_id": _safe_str(row.get("ref_id")),
    }


def compute_inventory_balances(movements: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Aggregate movement events into balance snapshots.

    Returns a mapping keyed by `SKU@location`.
    """
    balances: dict[str, dict[str, Any]] = {}
    seen_ids: set[str] = set()

    for row in movements or []:
        item = _normalize_movement(row)
        if not item:
            continue

        movement_id = item["movement_id"]
        if movement_id and movement_id in seen_ids:
            continue
        if movement_id:
            seen_ids.add(movement_id)

        key = f"{item['sku']}@{item['location']}"
        slot = balances.setdefault(
            key,
            {
                "sku": item["sku"],
                "location": item["location"],
                "on_hand": 0.0,
                "allocated": 0.0,
                "available": 0.0,
                "movement_count": 0,
                "last_ts": item["ts"],
            },
        )
        slot["on_hand"] = float(slot["on_hand"]) + float(item["qty_delta"])
        slot["allocated"] = max(0.0, float(slot["allocated"]) + float(item["allocated_delta"]))
        slot["available"] = float(slot["on_hand"]) - float(slot["allocated"])
        slot["movement_count"] = int(slot["movement_count"]) + 1
        slot["last_ts"] = max(str(slot["last_ts"]), str(item["ts"]))

    return balances


def balances_to_rows(balances: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for key in sorted((balances or {}).keys()):
        row = dict(balances[key])
        row["on_hand"] = round(float(row.get("on_hand", 0.0)), 4)
        row["allocated"] = round(float(row.get("allocated", 0.0)), 4)
        row["available"] = round(float(row.get("available", 0.0)), 4)
        rows.append(row)
    return rows


def load_movements_json(path: str | Path) -> list[dict[str, Any]]:
    src = Path(path)
    if not src.exists():
        return []
    try:
        obj = json.loads(src.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(obj, list):
        return [row for row in obj if isinstance(row, dict)]
    if isinstance(obj, dict) and isinstance(obj.get("movements"), list):
        return [row for row in obj["movements"] if isinstance(row, dict)]
    return []


__all__ = [
    "compute_inventory_balances",
    "balances_to_rows",
    "load_movements_json",
]

