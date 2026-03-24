"""Replenishment policy helpers (ROP + safety stock)."""

from __future__ import annotations

import math
from typing import Any


def _f(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        if out != out or math.isinf(out):  # NaN/inf
            return default
        return out
    except Exception:
        return default


def reorder_point_basic(avg_daily_demand: float, lead_time_days: float) -> float:
    demand = max(0.0, _f(avg_daily_demand, 0.0))
    lt = max(0.0, _f(lead_time_days, 0.0))
    return demand * lt


def safety_stock(service_level_z: float, demand_stddev: float, lead_time_days: float) -> float:
    z = max(0.0, _f(service_level_z, 0.0))
    sigma_d = max(0.0, _f(demand_stddev, 0.0))
    lt = max(0.0, _f(lead_time_days, 0.0))
    return z * sigma_d * math.sqrt(lt)


def reorder_point_with_safety(
    avg_daily_demand: float,
    lead_time_days: float,
    demand_stddev: float = 0.0,
    service_level_z: float = 0.0,
) -> float:
    return reorder_point_basic(avg_daily_demand, lead_time_days) + safety_stock(
        service_level_z=service_level_z,
        demand_stddev=demand_stddev,
        lead_time_days=lead_time_days,
    )


def recommended_order_qty(
    available_qty: float,
    reorder_point_qty: float,
    avg_daily_demand: float,
    target_days_of_supply: float,
    moq: float = 0.0,
    pack_size: float = 1.0,
) -> int:
    available = _f(available_qty, 0.0)
    rop = max(0.0, _f(reorder_point_qty, 0.0))
    demand = max(0.0, _f(avg_daily_demand, 0.0))
    target_days = max(0.0, _f(target_days_of_supply, 0.0))
    min_order = max(0.0, _f(moq, 0.0))
    pack = max(1.0, _f(pack_size, 1.0))

    target_stock = max(rop, demand * target_days)
    need = max(0.0, target_stock - available)
    if need <= 0:
        return 0
    if need < min_order:
        need = min_order
    need = math.ceil(need / pack) * pack
    return int(round(need))


def build_replenishment_candidates(
    balances: list[dict[str, Any]],
    policy_by_sku: dict[str, dict[str, Any]] | None = None,
    default_target_days: float = 21.0,
) -> list[dict[str, Any]]:
    policy_by_sku = policy_by_sku or {}
    out: list[dict[str, Any]] = []
    for row in balances or []:
        if not isinstance(row, dict):
            continue
        sku = str(row.get("sku", "")).strip().upper()
        if not sku:
            continue
        policy = policy_by_sku.get(sku, {}) if isinstance(policy_by_sku.get(sku), dict) else {}

        available = _f(row.get("available"), 0.0)
        avg_daily = max(0.0, _f(policy.get("avg_daily_demand"), 0.0))
        lead_time = max(0.0, _f(policy.get("lead_time_days"), 0.0))
        sigma_d = max(0.0, _f(policy.get("demand_stddev"), 0.0))
        z = max(0.0, _f(policy.get("service_level_z"), 0.0))
        moq = max(0.0, _f(policy.get("moq"), 0.0))
        pack_size = max(1.0, _f(policy.get("pack_size"), 1.0))
        target_days = max(1.0, _f(policy.get("target_days_of_supply"), default_target_days))

        rop = reorder_point_with_safety(
            avg_daily_demand=avg_daily,
            lead_time_days=lead_time,
            demand_stddev=sigma_d,
            service_level_z=z,
        )
        qty = recommended_order_qty(
            available_qty=available,
            reorder_point_qty=rop,
            avg_daily_demand=avg_daily,
            target_days_of_supply=target_days,
            moq=moq,
            pack_size=pack_size,
        )
        if qty <= 0:
            continue
        out.append(
            {
                "sku": sku,
                "location": str(row.get("location", "default")),
                "available": round(available, 4),
                "reorder_point": round(rop, 4),
                "shortfall_qty": round(max(0.0, rop - available), 4),
                "recommended_order_qty": int(qty),
                "policy": {
                    "avg_daily_demand": round(avg_daily, 4),
                    "lead_time_days": round(lead_time, 4),
                    "demand_stddev": round(sigma_d, 4),
                    "service_level_z": round(z, 4),
                    "target_days_of_supply": round(target_days, 4),
                    "moq": round(moq, 4),
                    "pack_size": round(pack_size, 4),
                },
            }
        )

    out.sort(key=lambda x: (x["shortfall_qty"], x["recommended_order_qty"]), reverse=True)
    return out


__all__ = [
    "reorder_point_basic",
    "safety_stock",
    "reorder_point_with_safety",
    "recommended_order_qty",
    "build_replenishment_candidates",
]

