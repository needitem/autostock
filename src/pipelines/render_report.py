from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any

from dotenv import load_dotenv

from ai.analyzer import ai


def _read_json(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def render_report(in_dir: str, out_path: str | None = None) -> str:
    load_dotenv()
    report_path = os.path.join(in_dir, "report.json")
    report = _read_json(report_path)

    ai_summary = ai.analyze_research_report(report)
    report["ai_summary"] = ai_summary

    date_tag = datetime.utcnow().strftime("%Y-%m-%d")
    return ""


if __name__ == "__main__":
    import sys

    in_dir = sys.argv[1] if len(sys.argv) > 1 else "outputs"
    render_report(in_dir)
