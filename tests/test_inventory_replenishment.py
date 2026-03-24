from __future__ import annotations

from inventory.replenishment import (
    build_replenishment_candidates,
    recommended_order_qty,
    reorder_point_basic,
    reorder_point_with_safety,
    safety_stock,
)


def test_reorder_point_formulas() -> None:
    assert reorder_point_basic(10, 3) == 30
    assert round(safety_stock(1.65, 4, 9), 4) == 19.8
    assert round(reorder_point_with_safety(10, 3, demand_stddev=4, service_level_z=1.65), 4) == 41.4315


def test_recommended_order_qty_respects_moq_and_pack_size() -> None:
    qty = recommended_order_qty(
        available_qty=6,
        reorder_point_qty=18,
        avg_daily_demand=3,
        target_days_of_supply=7,
        moq=10,
        pack_size=4,
    )
    # need is 15 -> rounded to pack size 4 => 16
    assert qty == 16


def test_build_replenishment_candidates_filters_only_actionable_rows() -> None:
    balances = [
        {"sku": "AAA", "location": "main", "available": 3},
        {"sku": "BBB", "location": "main", "available": 50},
    ]
    policy = {
        "AAA": {"avg_daily_demand": 2, "lead_time_days": 5, "target_days_of_supply": 14},
        "BBB": {"avg_daily_demand": 1, "lead_time_days": 5, "target_days_of_supply": 14},
    }
    out = build_replenishment_candidates(balances, policy_by_sku=policy)
    assert len(out) == 1
    assert out[0]["sku"] == "AAA"
    assert out[0]["recommended_order_qty"] > 0

