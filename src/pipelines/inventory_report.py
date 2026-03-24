"""Inventory daily report pipeline (migration bootstrap)."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from inventory.ledger import balances_to_rows, compute_inventory_balances, load_movements_json
from inventory.reconcile import load_channel_snapshot_json, reconcile_available
from inventory.replenishment import build_replenishment_candidates


ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data" / "inventory"
OUTPUT_DIR = DATA_DIR

DEFAULT_MOVEMENTS_JSON = DATA_DIR / "movements.json"
DEFAULT_POLICY_JSON = DATA_DIR / "reorder_policy.json"
DEFAULT_CHANNEL_SNAPSHOT_JSON = DATA_DIR / "channel_snapshot.json"


def _load_policy(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(obj, dict):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for k, v in obj.items():
        sku = str(k or "").strip().upper()
        if not sku or not isinstance(v, dict):
            continue
        out[sku] = dict(v)
    return out


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Inventory Daily Report",
        "",
        f"- generated_at_utc: `{report.get('generated_at_utc', '-')}`",
        f"- movement_count: **{report.get('movement_count', 0)}**",
        f"- balance_count: **{report.get('balance_count', 0)}**",
        f"- low_stock_count: **{report.get('low_stock_count', 0)}**",
        "",
        "## Top Low-Stock Candidates",
        "",
    ]
    candidates = report.get("low_stock_candidates", [])
    if not isinstance(candidates, list) or not candidates:
        lines.append("_No low-stock candidates based on current policy._")
    else:
        lines.extend(
            [
                "| SKU | Location | Available | ROP | Recommended Order |",
                "|---|---:|---:|---:|---:|",
            ]
        )
        for row in candidates[:30]:
            if not isinstance(row, dict):
                continue
            lines.append(
                "| {sku} | {location} | {available:.2f} | {rop:.2f} | {order} |".format(
                    sku=str(row.get("sku", "")),
                    location=str(row.get("location", "")),
                    available=float(row.get("available", 0.0) or 0.0),
                    rop=float(row.get("reorder_point", 0.0) or 0.0),
                    order=int(row.get("recommended_order_qty", 0) or 0),
                )
            )

    reconcile = report.get("reconcile") if isinstance(report.get("reconcile"), dict) else {}
    lines.extend(
        [
            "",
            "## Internal vs Channel Reconcile",
            "",
            f"- compared_rows(internal/channel): **{reconcile.get('internal_count', 0)} / {reconcile.get('channel_count', 0)}**",
            f"- mismatch_count: **{reconcile.get('mismatch_count', 0)}**",
        ]
    )
    mismatches = reconcile.get("mismatches") if isinstance(reconcile.get("mismatches"), list) else []
    if not mismatches:
        lines.append("- _No mismatch (or channel snapshot missing)._")
    else:
        lines.extend(
            [
                "",
                "| SKU | Location | Internal | Channel | Diff | Status |",
                "|---|---:|---:|---:|---:|---|",
            ]
        )
        for row in mismatches[:30]:
            if not isinstance(row, dict):
                continue
            lines.append(
                "| {sku} | {location} | {internal:.2f} | {channel:.2f} | {diff:.2f} | {status} |".format(
                    sku=str(row.get("sku", "")),
                    location=str(row.get("location", "")),
                    internal=float(row.get("internal_available", 0.0) or 0.0),
                    channel=float(row.get("channel_available", 0.0) or 0.0),
                    diff=float(row.get("diff_qty", 0.0) or 0.0),
                    status=str(row.get("status", "")),
                )
            )
    return "\n".join(lines)


def run_inventory_report(
    movements_path: str | None = None,
    policy_path: str | None = None,
    channel_snapshot_path: str | None = None,
    mismatch_threshold: float | None = None,
) -> dict[str, Any]:
    """Build inventory report from local movement/policy JSON files."""
    mov_src = Path(movements_path or os.getenv("INVENTORY_MOVEMENTS_JSON") or DEFAULT_MOVEMENTS_JSON)
    pol_src = Path(policy_path or os.getenv("INVENTORY_POLICY_JSON") or DEFAULT_POLICY_JSON)
    ch_src = Path(channel_snapshot_path or os.getenv("INVENTORY_CHANNEL_SNAPSHOT_JSON") or DEFAULT_CHANNEL_SNAPSHOT_JSON)
    if mismatch_threshold is None:
        try:
            mismatch_threshold = float(os.getenv("INVENTORY_MISMATCH_THRESHOLD", "0.0001"))
        except Exception:
            mismatch_threshold = 0.0001

    movements = load_movements_json(mov_src)
    policy = _load_policy(pol_src)
    balances = balances_to_rows(compute_inventory_balances(movements))
    low_stock = build_replenishment_candidates(balances, policy_by_sku=policy)
    channel_snapshot = load_channel_snapshot_json(ch_src) if ch_src.exists() else []
    reconcile = reconcile_available(balances, channel_snapshot, mismatch_threshold=mismatch_threshold)

    generated_at = datetime.now(timezone.utc)
    report = {
        "generated_at_utc": generated_at.isoformat(),
        "source_movements_json": str(mov_src),
        "source_policy_json": str(pol_src),
        "source_channel_snapshot_json": str(ch_src),
        "movement_count": int(len(movements)),
        "balance_count": int(len(balances)),
        "low_stock_count": int(len(low_stock)),
        "channel_snapshot_count": int(len(channel_snapshot)),
        "mismatch_count": int(reconcile.get("mismatch_count", 0)),
        "balances": balances,
        "low_stock_candidates": low_stock,
        "reconcile": reconcile,
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    tag = generated_at.strftime("%Y%m%d_%H%M%S")
    report_json = OUTPUT_DIR / f"inventory_report_{tag}.json"
    report_md = OUTPUT_DIR / f"inventory_report_{tag}.md"

    report_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    report_md.write_text(_render_markdown(report), encoding="utf-8")

    summary = {
        "generated_at_utc": report["generated_at_utc"],
        "movement_count": report["movement_count"],
        "balance_count": report["balance_count"],
        "low_stock_count": report["low_stock_count"],
        "channel_snapshot_count": report["channel_snapshot_count"],
        "mismatch_count": report["mismatch_count"],
    }
    return {
        "report_path": str(report_json),
        "md_path": str(report_md),
        "summary": summary,
        "report": report,
    }


if __name__ == "__main__":
    out = run_inventory_report()
    print(f"report_json: {out['report_path']}")
    print(f"report_md: {out['md_path']}")
