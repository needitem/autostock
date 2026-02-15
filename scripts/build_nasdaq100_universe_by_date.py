from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd
import requests


ROOT = Path(__file__).resolve().parents[1]


def _normalize_symbol(value: Any) -> str | None:
    if value is None:
        return None
    symbol = str(value).strip().upper()
    if not symbol or symbol.lower() in {"nan", "none"}:
        return None

    # Drop common footnote/annotation tails like "AAPL[1]" or "AAPL (Class A)".
    symbol = re.sub(r"\s*\[.*?\]\s*$", "", symbol).strip()
    symbol = re.sub(r"\s*\(.*?\)\s*$", "", symbol).strip()

    # yfinance compatibility.
    symbol = symbol.replace(".", "-")

    # Keep a conservative set of characters.
    symbol = re.sub(r"[^A-Z0-9-]", "", symbol)
    if not symbol or len(symbol) > 10:
        return None
    return symbol


def _snapshot_dates(start: str, end: str, freq: str, max_snapshots: int) -> list[pd.Timestamp]:
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    if end_ts < start_ts:
        start_ts, end_ts = end_ts, start_ts

    f = (freq or "").strip().lower() or "quarterly"
    if f in {"q", "quarter", "quarters", "quarterly"}:
        dates = list(pd.date_range(start=start_ts, end=end_ts, freq="QE-DEC"))
    elif f in {"m", "month", "months", "monthly"}:
        dates = list(pd.date_range(start=start_ts, end=end_ts, freq="ME"))
    elif f in {"w", "week", "weeks", "weekly"}:
        dates = list(pd.date_range(start=start_ts, end=end_ts, freq="W-FRI"))
    else:
        dates = list(pd.date_range(start=start_ts, end=end_ts, freq="QE-DEC"))

    if max_snapshots > 0:
        dates = dates[-max_snapshots:]
    return [pd.Timestamp(d).normalize() for d in dates]


def _wiki_url(title: str) -> str:
    safe = (title or "Nasdaq-100").strip().replace(" ", "_")
    return f"https://en.wikipedia.org/wiki/{safe}"


def _download_html(session: requests.Session, title: str) -> str:
    url = _wiki_url(title)
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    return resp.text


def _flatten_columns(frame: pd.DataFrame) -> pd.DataFrame:
    df = frame.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            " ".join(str(x) for x in col if x is not None and str(x).strip() and str(x).lower() != "nan").strip()
            for col in df.columns.to_list()
        ]
    else:
        df.columns = [str(c).strip() for c in df.columns.to_list()]
    return df


def _extract_current_constituents(tables: list[pd.DataFrame]) -> list[str]:
    candidate_cols = {"Ticker", "Ticker symbol", "Symbol", "Ticker Symbol"}
    best: list[str] = []
    for t in tables:
        if not isinstance(t, pd.DataFrame) or t.empty:
            continue
        cols = [str(c) for c in t.columns]
        col = next((c for c in cols if c in candidate_cols), None)
        if not col:
            continue
        out: list[str] = []
        seen: set[str] = set()
        for raw in t[col].tolist():
            sym = _normalize_symbol(raw)
            if not sym or sym in seen:
                continue
            seen.add(sym)
            out.append(sym)
        if len(out) > len(best):
            best = out
    return sorted(set(best))


def _extract_change_events(tables: list[pd.DataFrame]) -> list[dict[str, Any]]:
    # The Nasdaq-100 page includes a "Changes" table with columns like:
    # Date | Added (Ticker/Security) | Removed (Ticker/Security) | Reason
    best: pd.DataFrame | None = None
    for t in tables:
        if not isinstance(t, pd.DataFrame) or t.empty:
            continue
        df = _flatten_columns(t)
        cols = {c.lower(): c for c in df.columns}
        has_date = any("date" == c.lower() or c.lower().startswith("date ") for c in df.columns)
        has_added = any("added ticker" in c.lower() for c in df.columns)
        has_removed = any("removed ticker" in c.lower() for c in df.columns)
        if has_date and has_added and has_removed and len(df) >= 10:
            best = df
            break

    if best is None:
        return []

    # Identify columns
    date_col = next((c for c in best.columns if c.lower() == "date" or c.lower().startswith("date ")), None)
    add_col = next((c for c in best.columns if "added ticker" in c.lower()), None)
    rem_col = next((c for c in best.columns if "removed ticker" in c.lower()), None)
    if not date_col or not add_col or not rem_col:
        return []

    out: list[dict[str, Any]] = []
    for _, row in best.iterrows():
        raw_date = row.get(date_col)
        dt = pd.to_datetime(raw_date, errors="coerce", utc=False)
        if pd.isna(dt):
            continue
        added = _normalize_symbol(row.get(add_col))
        removed = _normalize_symbol(row.get(rem_col))
        out.append({"date": pd.Timestamp(dt).normalize(), "added": added, "removed": removed})

    # Sort newest -> oldest for backward reconstruction.
    out.sort(key=lambda x: x["date"], reverse=True)
    return out


def _reconstruct_universe_by_date(
    current_universe: list[str],
    change_events_desc: list[dict[str, Any]],
    snapshots: list[pd.Timestamp],
) -> dict[str, list[str]]:
    cur: set[str] = set(current_universe)
    events = change_events_desc
    snaps_desc = sorted(snapshots, reverse=True)

    j = 0
    out: dict[str, list[str]] = {}
    for snap in snaps_desc:
        # Roll back all changes strictly after this snapshot date.
        while j < len(events) and pd.Timestamp(events[j]["date"]) > snap:
            e = events[j]
            added = e.get("added")
            removed = e.get("removed")

            # Reverse the forward change:
            # - if it was added on that date, it was NOT present before -> remove it.
            # - if it was removed on that date, it WAS present before -> add it back.
            if added:
                cur.discard(str(added))
            if removed:
                cur.add(str(removed))
            j += 1

        out[str(pd.Timestamp(snap).date())] = sorted(cur)
    return {k: out[k] for k in sorted(out.keys())}


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build Nasdaq-100 universe by date.\n\n"
            "Method: download the current Wikipedia page once, parse:\n"
            "1) current constituents table (tickers)\n"
            "2) change-log table (date, added ticker, removed ticker)\n"
            "Then reconstruct historical constituents by rolling changes backward.\n"
        )
    )
    parser.add_argument("--title", default=os.getenv("WIKI_NASDAQ100_TITLE", "Nasdaq-100"))
    parser.add_argument("--start", default=os.getenv("AI_START_DATE", "2016-01-01"))
    parser.add_argument("--end", default=os.getenv("AI_END_DATE", "2025-12-31"))
    parser.add_argument("--freq", default=os.getenv("AI_SNAPSHOT_FREQ", "quarterly"))
    parser.add_argument("--max-snapshots", type=int, default=int(os.getenv("AI_MAX_SNAPSHOTS", "0") or "0"))
    parser.add_argument(
        "--out",
        default=os.getenv("AI_UNIVERSE_BY_DATE_FILE", "data/universe/nasdaq100_by_date.json"),
    )
    args = parser.parse_args()

    out_path = Path(args.out)
    if not out_path.is_absolute():
        out_path = ROOT / out_path

    snaps = _snapshot_dates(args.start, args.end, args.freq, max(0, int(args.max_snapshots)))
    if not snaps:
        raise SystemExit("No snapshot dates produced (check start/end/freq).")

    session = requests.Session()
    session.headers.update({"User-Agent": "autostock/1.0 (historical universe builder)"})

    html = _download_html(session, args.title)
    tables = pd.read_html(StringIO(html))

    current = _extract_current_constituents(tables)
    if len(current) < 50:
        raise SystemExit("Failed to parse current constituents table (too few tickers).")

    events = _extract_change_events(tables)
    if not events:
        raise SystemExit("Failed to parse change-log table from Wikipedia page.")

    dates = _reconstruct_universe_by_date(current, events, snaps)

    payload = {
        "meta": {
            "source": "wikipedia",
            "method": "current_constituents_plus_change_log",
            "page_title": args.title,
            "page_url": _wiki_url(args.title),
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "start_date": args.start,
            "end_date": args.end,
            "snapshot_freq": args.freq,
            "max_snapshots": max(0, int(args.max_snapshots)),
            "snapshot_dates": [str(pd.Timestamp(s).date()) for s in snaps],
            "current_tickers": len(current),
            "change_events": len(events),
            "notes": [
                "Universe is reconstructed by reversing the Nasdaq-100 change-log table.",
                "Symbols are normalized for yfinance: '.' -> '-' and non-alphanumerics removed.",
            ],
        },
        "dates": dates,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    # Keep console output ASCII-only (Windows codepages can break on emojis).
    first = next(iter(dates.keys()))
    last = next(reversed(dates.keys()))
    print(f"Wrote: {out_path} (dates={len(dates)}; range={first}..{last})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

