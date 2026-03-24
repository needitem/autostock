from __future__ import annotations

import json
import math
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yfinance as yf


ROOT = Path(__file__).resolve().parents[2]
CACHE_DIR = ROOT / "data" / "earnings_cache"


def _to_float(value: Any, default: float = float("nan")) -> float:
    try:
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return default
        return out
    except Exception:
        return default


def _cache_path(symbol: str) -> Path:
    return CACHE_DIR / f"{str(symbol).upper()}.json"


def _serialize_earnings_df(df: pd.DataFrame) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if df is None or df.empty:
        return out
    view = df.copy()
    for idx, row in view.iterrows():
        dt = pd.Timestamp(idx)
        out.append(
            {
                "earnings_date": dt.tz_convert("UTC").isoformat() if dt.tzinfo else dt.tz_localize("UTC").isoformat(),
                "eps_estimate": _to_float(row.get("EPS Estimate")),
                "reported_eps": _to_float(row.get("Reported EPS")),
                "surprise_pct": _to_float(row.get("Surprise(%)")),
            }
        )
    return out


def _load_cached(symbol: str) -> list[dict[str, Any]] | None:
    path = _cache_path(symbol)
    if not path.exists():
        return None
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(obj, dict):
        return None
    rows = obj.get("rows")
    return rows if isinstance(rows, list) else None


def _save_cached(symbol: str, rows: list[dict[str, Any]]) -> None:
    path = _cache_path(symbol)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "symbol": str(symbol).upper(),
        "updated_at": datetime.utcnow().isoformat() + "Z",
        "rows": rows,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def fetch_earnings_dates(symbol: str, limit: int = 120, refresh: bool = False) -> list[dict[str, Any]]:
    symbol = str(symbol).upper()
    if not refresh:
        cached = _load_cached(symbol)
        if cached is not None and any(math.isfinite(_to_float(row.get("reported_eps"))) for row in cached if isinstance(row, dict)):
            return cached
    try:
        df = yf.Ticker(symbol).get_earnings_dates(limit=int(limit))
    except Exception:
        df = None
    rows = _serialize_earnings_df(df) if df is not None else []
    _save_cached(symbol, rows)
    return rows


class EarningsEventStore:
    def __init__(self, symbols: list[str], limit: int = 120, max_workers: int = 8):
        self._rows: dict[str, list[dict[str, Any]]] = {}
        syms = sorted({str(s).upper() for s in symbols if isinstance(s, str) and s.strip()})
        if not syms:
            return

        workers = min(max(2, int(max_workers)), 12)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(fetch_earnings_dates, sym, limit, False): sym for sym in syms}
            for future in as_completed(futures):
                sym = futures[future]
                try:
                    self._rows[sym] = future.result()
                except Exception:
                    self._rows[sym] = []

    def latest_event_asof(self, symbol: str, asof: pd.Timestamp) -> dict[str, Any]:
        rows = self._rows.get(str(symbol).upper(), [])
        if not rows:
            return {"earnings_has_data": False}

        asof_ts = pd.Timestamp(asof)
        if asof_ts.tzinfo is None:
            asof_ts = asof_ts.tz_localize("UTC")
        else:
            asof_ts = asof_ts.tz_convert("UTC")

        eligible: list[dict[str, Any]] = []
        for row in rows:
            try:
                event_ts = pd.Timestamp(row.get("earnings_date"))
            except Exception:
                continue
            if event_ts.tzinfo is None:
                event_ts = event_ts.tz_localize("UTC")
            else:
                event_ts = event_ts.tz_convert("UTC")
            if event_ts <= asof_ts and math.isfinite(_to_float(row.get("reported_eps"))):
                eligible.append({"event_ts": event_ts, **row})

        if not eligible:
            return {"earnings_has_data": False}

        latest = max(eligible, key=lambda x: x["event_ts"])
        days_since = max(0, int((asof_ts.date() - latest["event_ts"].date()).days))
        return {
            "earnings_has_data": True,
            "earnings_days_since": int(days_since),
            "earnings_surprise_pct": _to_float(latest.get("surprise_pct")),
            "earnings_eps_estimate": _to_float(latest.get("eps_estimate")),
            "earnings_reported_eps": _to_float(latest.get("reported_eps")),
            "earnings_event_date": latest["event_ts"].date().isoformat(),
        }
