# Strategy V3: Stock-First Selector

Status: experimental redesign as of 2026-03-11

## Goal

V3 moves back to the original product goal:

- use `QQQ` and `VIX` as market filters
- buy individual stocks from the Nasdaq-100 universe
- stop treating `QQQ`, `QLD`, `TQQQ`, and `GLD` as the actual production holdings

## Core Thesis

The market filter and the execution target should be separated.

- Market filter: `QQQ` trend and `VIX`
- Candidate universe: time-varying Nasdaq-100 constituents
- Position selection: top-ranked individual names only
- Risk control: reduce count or go to cash in weaker regimes

This matches the original objective better than the ETF-rotation baseline, even if the stock-first path still needs more work before it can replace that baseline.

## Baseline Shape

The checked-in stock-first runner uses:

```bash
AI_DECISION_ENGINE=selector
AI_UNIVERSE=nasdaq100
AI_UNIVERSE_MODE=by_date
AI_UNIVERSE_BY_DATE_FILE=data/universe/nasdaq100_by_date_weekly_2006_2026.json

AI_SNAPSHOT_FREQ=weekly
AI_HORIZON_MODE=next_snapshot
AI_EXECUTION_TIMING=next_open

AI_PORTFOLIO_TOP_K=5
AI_PORTFOLIO_MAX_WEIGHT_PCT=25

AI_TRADE_COST_BPS=20
AI_SLIPPAGE_BPS=0
AI_SPREAD_BPS=0
AI_TAX_BPS=0

AI_START_DATE=2016-03-01
AI_END_DATE=2026-03-01

AI_SAFE_MODE=1
AI_SAFE_REQUIRE_RISK_ON=0
AI_SAFE_USE_TREND_TEMPLATE=1
AI_SAFE_MIN_VOLUME_RATIO=0.8

AI_ALGO_USE_BENCHMARK_FEATURES=1
AI_SELECTOR_USE_UNIVERSE_REGIME=0
AI_SELECTOR_SCORING_MODE=balanced
AI_SELECTOR_WEIGHT_MODE=inv_vol
AI_SELECTOR_TOP_K_NEUTRAL=2
AI_SELECTOR_TOP_K_RISK_OFF=0
AI_SELECTOR_MIN_POSITIONS_FOR_INVEST=2
```

## Interpretation

- `risk_on`: allow up to 5 individual stock positions
- `neutral`: cut the stock count to 2
- `risk_off`: hold cash
- execution holdings remain individual names only

## Promotion Rule

Do not replace the ETF baseline until the stock-first path shows:

1. acceptable turnover
2. better long-window robustness than the older stock-picking path
3. a clear case for using single-name execution despite higher complexity
