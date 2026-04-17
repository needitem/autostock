# Nautilus V2 Lane

This directory is the separate `NautilusTrader`-oriented strategy lane for Autostock.

It is intentionally **not** a full repo rewrite. The current Autostock codebase still owns:

- news/public-filing collection
- Telegram/Cloudflare delivery
- watchlist scoring and macro overlay generation

This lane owns the **event-driven trading inputs** for a future Nautilus-based strategy engine.

## Current scope

The current bridge exports TSLA-specific artifacts:

- `news events` JSONL
- `macro event` JSONL
- `signal snapshot` JSON
- `daily bars` CSV

These files are generated from the existing `autostock_v2` TSLA event engine.

## Why this split

`NautilusTrader` is strongest as an event-driven strategy/runtime and backtest/live parity engine.
It does **not** solve the news ingestion problem for us, so we keep ingestion in Autostock and
export normalized artifacts into a Nautilus-ready lane.

## Command

From the repo root:

```bash
python scripts/export_nautilus_tsla_inputs.py
```

Optional environment variables:

- `AI_V2_EVENT_FILE` - manual event JSON to merge
- `AI_V2_RSS_URLS` - comma-separated RSS feeds
- `AI_V2_TSLA_BARS_PERIOD` - price history period, default `15mo`

## Output

Artifacts are written under:

```text
data/nautilus_v2/<YYYY-MM-DD>/
```

## Next step

The next implementation step is a real Nautilus strategy package which reads these artifacts as:

- `NewsEvent`
- `MacroEvent`
- `Bar`

and applies the TSLA event logic inside the Nautilus event loop.

## Current runner status

There is now a working TSLA-only runner:

```bash
nautilus_v2\.venv\Scripts\python scripts/run_nautilus_tsla_backtest.py
```

Important:

- The artifacts are imported into a Nautilus catalog for inspection/reuse.
- The actual backtest currently runs with the in-memory `BacktestEngine`.
- This is deliberate: the Rust parquet backtest path currently rejects our Python custom news/macro data classes directly.

So the current lane already provides:

- real TSLA bar import
- real TSLA custom event import
- real Nautilus strategy execution

but custom event playback is still using the Python/in-memory path rather than a pure Rust catalog streaming path.
