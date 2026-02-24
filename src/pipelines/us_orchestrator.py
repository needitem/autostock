from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any

from dotenv import load_dotenv
import numpy as np

from pipelines.us_free_pipeline import run_us_free_pipeline


def _make_run_dir(base_dir: str = "outputs") -> str:
    os.makedirs(base_dir, exist_ok=True)
    return base_dir


def _json_default(obj: Any) -> Any:
    if isinstance(obj, (np.integer, np.floating, np.bool_)):
        return obj.item()
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if hasattr(obj, "isoformat"):
        try:
            return obj.isoformat()
        except Exception:
            pass
    return str(obj)


def _write_json(path: str, payload: Any) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2, default=_json_default)


def run_all_us_engines(out_dir: str | None = None) -> dict[str, Any]:
    load_dotenv()
    run_dir = out_dir or _make_run_dir()
    date_tag = datetime.utcnow().strftime("%Y-%m-%d")
    report_path = os.path.join(run_dir, f"report_{date_tag}.json")

    result = run_us_free_pipeline(output_dir=run_dir, write_outputs=False)
    report = result.get("report") or {}

    _write_json(report_path, report)

    return {"report_path": report_path, "report": report}


if __name__ == "__main__":
    run_all_us_engines()
