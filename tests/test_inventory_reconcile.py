from __future__ import annotations

import json

from inventory.reconcile import load_channel_snapshot_json, reconcile_available


def test_load_channel_snapshot_json_supports_plain_list(tmp_path) -> None:
    path = tmp_path / "snapshot.json"
    path.write_text(
        json.dumps(
            [
                {"sku": "a", "location": "main", "available": 3},
                {"variant_id": "b", "location_id": "wh1", "available_qty": 5},
            ]
        ),
        encoding="utf-8",
    )
    rows = load_channel_snapshot_json(path)
    assert len(rows) == 2
    assert rows[0]["sku"] == "A"
    assert rows[1]["sku"] == "B"


def test_reconcile_available_detects_mismatch_and_missing() -> None:
    internal = [
        {"sku": "AAA", "location": "main", "available": 10},
        {"sku": "BBB", "location": "main", "available": 7},
    ]
    channel = [
        {"sku": "AAA", "location": "main", "available": 8},  # mismatch
        {"sku": "CCC", "location": "main", "available": 2},  # missing internal
    ]
    out = reconcile_available(internal, channel, mismatch_threshold=0.0001)
    assert out["mismatch_count"] == 3
    by_status = out["mismatch_by_status"]
    assert by_status["mismatch"] == 1
    assert by_status["missing_in_internal"] == 1
    assert by_status["missing_in_channel"] == 1

