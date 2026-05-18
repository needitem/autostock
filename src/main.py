from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from event_profile import load_event_profile, symbol_slug
from event_runtime.engine import run_runtime_cycle, run_runtime_loop
from nautilus_v2.bridge import export_symbol_bundle
from pipelines.autostock_v2_pipeline import run_autostock_v2
from core.data_collector import DataCollector


DATA_ROOT = ROOT / "data" / "nautilus_v2"
_DATA_COLLECTOR = DataCollector(root=ROOT)


def _configure_console_output() -> None:
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream and hasattr(stream, "reconfigure"):
            try:
                stream.reconfigure(errors="replace")
            except Exception:
                pass


def _s(value: Any) -> str:
    return str(value or "").strip()


def _profile(profile_name: str | None) -> dict[str, Any]:
    return load_event_profile(profile_name)


def _bundle_dir(profile: dict[str, Any], date_tag: str | None = None) -> Path:
    tag = date_tag or datetime.now().strftime("%Y-%m-%d")
    return DATA_ROOT / _s(profile.get("name") or symbol_slug(profile)) / tag


def _latest_bundle_dir(profile: dict[str, Any]) -> Path:
    base_dir = DATA_ROOT / _s(profile.get("name") or symbol_slug(profile))
    candidates = [item for item in base_dir.glob("*") if item.is_dir()]
    if not candidates:
        raise FileNotFoundError(f"No Nautilus bundles found under {base_dir}")
    candidates.sort(key=lambda item: item.stat().st_mtime, reverse=True)
    return candidates[0]


def run_signal_once(profile_name: str | None) -> dict[str, Any]:
    profile = _profile(profile_name)
    print(f"[{datetime.now()}] signal analysis started...")
    result = run_autostock_v2(profile=profile, watchlist_override=list(profile.get("symbols", [])))
    payload = result.get("payload", {}) if isinstance(result, dict) else {}
    recommendations = payload.get("recommendations", []) if isinstance(payload, dict) else []
    top_rows = [row for row in recommendations if isinstance(row, dict)][:5]
    print(f"signal_json: {result.get('report_path')}")
    print(f"signal_md: {result.get('md_path')}")
    if top_rows:
        print("top_actions:")
        for row in top_rows:
            print(
                f"  {row.get('symbol', '-'):5} "
                f"{row.get('action', '-'):5} "
                f"conf={float(row.get('confidence', 0.0)):.2f} "
                f"event={_s(row.get('event_signal'))}/{_s(row.get('event_strength'))} "
                f"chart={_s((row.get('chart_gate') or {}).get('state'))}"
            )
    return result


def run_runtime_once(profile_name: str | None, loop: bool, interval_seconds: int) -> dict[str, Any]:
    profile = _profile(profile_name)
    print(f"[{datetime.now()}] event runtime started...")
    result = (
        run_runtime_loop(profile_name=_s(profile.get("name")), interval_seconds=max(5, int(interval_seconds)))
        if loop
        else run_runtime_cycle(profile_name=_s(profile.get("name")))
    )
    print(f"runtime_dir: {result.get('runtime_dir')}")
    print(f"payload_json: {result.get('payload_path')}")
    print(f"state_json: {result.get('state_path')}")
    print(f"outbox_jsonl: {result.get('outbox_path')}")
    print(f"notifications: {result.get('notification_count', 0)}")
    return result


def export_nautilus_bundle(profile_name: str | None) -> dict[str, Any]:
    profile = _profile(profile_name)
    symbol = _s(profile.get("primary_symbol", "TSLA")).upper()
    print(f"[{datetime.now()}] nautilus bundle export started...")
    result = run_autostock_v2(profile=profile, watchlist_override=list(profile.get("symbols", [symbol])))
    payload = result.get("payload", {}) if isinstance(result, dict) else {}
    bars_df = _DATA_COLLECTOR.get_stock_data(
        symbol,
        period=str(os.getenv("AI_EVENT_BARS_PERIOD", "15mo") or "15mo"),
        auto_adjust=False,
    )
    out_dir = _bundle_dir(profile)
    paths = export_symbol_bundle(
        payload=payload if isinstance(payload, dict) else {},
        bars_df=bars_df,
        output_dir=out_dir,
        symbol=symbol,
    )
    print(f"bundle_dir: {out_dir}")
    for key, value in paths.items():
        print(f"{key}: {value}")
    return {
        "profile": _s(profile.get("name")),
        "bundle_dir": str(out_dir),
        "paths": paths,
        "signal_report": result.get("report_path"),
    }


def run_nautilus_backtest(profile_name: str | None) -> dict[str, Any]:
    profile = _profile(profile_name)
    print(f"[{datetime.now()}] nautilus backtest started...")
    try:
        from nautilus_v2.backtest import import_tsla_bundle_to_catalog, run_tsla_backtest_in_memory
    except ModuleNotFoundError as exc:
        if "nautilus_trader" not in str(exc):
            raise
        venv_python = ROOT / "nautilus_v2" / ".venv" / "Scripts" / "python.exe"
        if not venv_python.exists():
            raise
        proc = subprocess.run(
            [str(venv_python), str(ROOT / "scripts" / "run_nautilus_tsla_backtest.py")],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
            check=False,
            cwd=str(ROOT),
        )
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr or proc.stdout or "nautilus backtest failed")
        print(proc.stdout.strip())
        bundle_dir = _latest_bundle_dir(profile)
        output_path = bundle_dir / f"{symbol_slug(profile)}_nautilus_backtest_summary.json"
        return json.loads(output_path.read_text(encoding="utf-8"))

    bundle_dir = _latest_bundle_dir(profile)
    catalog_dir = bundle_dir / "catalog"
    import_info = import_tsla_bundle_to_catalog(bundle_dir, catalog_dir)
    summary = run_tsla_backtest_in_memory(bundle_dir)
    output_path = bundle_dir / f"{symbol_slug(profile)}_nautilus_backtest_summary.json"
    payload = {
        "profile": _s(profile.get("name")),
        "bundle_dir": str(bundle_dir),
        "catalog_import": import_info,
        "backtest_summary": summary,
    }
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"summary_json: {output_path}")
    return payload


def export_telegram_snapshot() -> None:
    print(f"[{datetime.now()}] telegram snapshot export started...")
    proc = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "export_telegram_snapshot.py")],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="ignore",
        check=False,
        cwd=str(ROOT),
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr or proc.stdout or "telegram export failed")
    if proc.stdout.strip():
        print(proc.stdout.strip())


def run_local_telegram_bot() -> None:
    from local_telegram_bot import run_local_telegram_bot as _run_local_telegram_bot

    print(f"[{datetime.now()}] local telegram bot started...")
    _run_local_telegram_bot()


def run_all(profile_name: str | None) -> None:
    profile = _profile(profile_name)
    run_runtime_once(_s(profile.get("name")), loop=False, interval_seconds=60)
    export_nautilus_bundle(_s(profile.get("name")))
    run_nautilus_backtest(_s(profile.get("name")))
    export_telegram_snapshot()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Autostock Nautilus + Telegram entrypoint")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--signal", action="store_true", help="Run one-shot event signal analysis")
    mode.add_argument("--runtime", action="store_true", help="Run event runtime")
    mode.add_argument("--nautilus-bundle", action="store_true", help="Export the latest signal as a Nautilus bundle")
    mode.add_argument("--nautilus-backtest", action="store_true", help="Run Nautilus backtest from the latest bundle")
    mode.add_argument("--telegram-export", action="store_true", help="Export local Telegram snapshot data")
    mode.add_argument("--telegram-bot", action="store_true", help="Run local Telegram polling bot")
    mode.add_argument("--all", action="store_true", help="Run runtime -> Nautilus bundle -> backtest -> Telegram export")
    parser.add_argument("--profile", default="tsla", help="Event profile name or JSON path")
    parser.add_argument("--loop", action="store_true", help="Run runtime continuously")
    parser.add_argument("--interval-sec", type=int, default=60, help="Runtime polling interval in seconds")
    return parser


def main(argv: list[str] | None = None) -> None:
    _configure_console_output()
    raw_argv = argv or sys.argv[1:]
    parser = _build_parser()
    args = parser.parse_args(raw_argv)

    if args.signal:
        run_signal_once(args.profile)
        return
    if args.runtime:
        run_runtime_once(args.profile, loop=bool(args.loop), interval_seconds=max(5, int(args.interval_sec)))
        return
    if args.nautilus_bundle:
        export_nautilus_bundle(args.profile)
        return
    if args.nautilus_backtest:
        run_nautilus_backtest(args.profile)
        return
    if args.telegram_export:
        export_telegram_snapshot()
        return
    if args.telegram_bot:
        run_local_telegram_bot()
        return

    if args.all:
        run_all(args.profile)
        return

    parser.print_help()


if __name__ == "__main__":
    main()
