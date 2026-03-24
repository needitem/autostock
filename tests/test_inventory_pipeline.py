from __future__ import annotations

import json

from pipelines import inventory_report as inv_pipeline


def test_run_inventory_report_writes_outputs(tmp_path, monkeypatch) -> None:
    movements_path = tmp_path / "movements.json"
    policy_path = tmp_path / "policy.json"
    snapshot_path = tmp_path / "channel_snapshot.json"
    out_dir = tmp_path / "out"

    movements_path.write_text(
        json.dumps(
            [
                {"movement_id": "m1", "sku": "SKU1", "location": "main", "qty_delta": 12},
                {"movement_id": "m2", "sku": "SKU1", "location": "main", "qty_delta": -8},
            ]
        ),
        encoding="utf-8",
    )
    policy_path.write_text(
        json.dumps({"SKU1": {"avg_daily_demand": 3, "lead_time_days": 4, "target_days_of_supply": 10}}),
        encoding="utf-8",
    )
    snapshot_path.write_text(
        json.dumps(
            [
                {"sku": "SKU1", "location": "main", "available": 3},
                {"sku": "SKU2", "location": "main", "available": 5},
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(inv_pipeline, "OUTPUT_DIR", out_dir)
    result = inv_pipeline.run_inventory_report(
        str(movements_path),
        str(policy_path),
        str(snapshot_path),
        mismatch_threshold=0.0001,
    )
    summary = result.get("summary", {})

    assert int(summary.get("movement_count", 0)) == 2
    assert int(summary.get("balance_count", 0)) == 1
    assert int(summary.get("low_stock_count", 0)) == 1
    assert int(summary.get("channel_snapshot_count", 0)) == 2
    assert int(summary.get("mismatch_count", 0)) == 2
    assert out_dir.exists()
    assert out_dir.joinpath(next(p.name for p in out_dir.glob("inventory_report_*.json"))).exists()
    assert out_dir.joinpath(next(p.name for p in out_dir.glob("inventory_report_*.md"))).exists()
