from __future__ import annotations

from inventory.ledger import balances_to_rows, compute_inventory_balances


def test_compute_inventory_balances_dedupes_by_movement_id() -> None:
    movements = [
        {"movement_id": "m1", "sku": "sku-1", "location": "main", "qty_delta": 10},
        {"movement_id": "m2", "sku": "sku-1", "location": "main", "qty_delta": -2, "allocated_delta": 2},
        {"movement_id": "m2", "sku": "sku-1", "location": "main", "qty_delta": -2, "allocated_delta": 2},  # duplicate
    ]
    balances = compute_inventory_balances(movements)
    row = balances["SKU-1@main"]
    assert row["on_hand"] == 8
    assert row["allocated"] == 2
    assert row["available"] == 6
    assert row["movement_count"] == 2


def test_compute_inventory_balances_supports_qty_direction_payload() -> None:
    movements = [
        {"id": "a1", "variant_id": "abc", "location_id": "wh1", "qty": 5, "direction": "in"},
        {"id": "a2", "variant_id": "abc", "location_id": "wh1", "qty": 2, "direction": "out"},
    ]
    balances = compute_inventory_balances(movements)
    rows = balances_to_rows(balances)
    assert len(rows) == 1
    assert rows[0]["sku"] == "ABC"
    assert rows[0]["on_hand"] == 3
    assert rows[0]["available"] == 3

