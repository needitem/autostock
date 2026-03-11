# Strategy V4: Stock Momentum Core

Status: fresh baseline candidate as of 2026-03-11

## Goal

Start over from the original product goal with the simplest defensible structure:

- market filter from `QQQ` and `VIX`
- actual holdings are individual Nasdaq-100 names
- no ETF rotation in production holdings

## Thesis

The first stock-first strategy should be:

1. simple enough to validate honestly
2. free of current-fundamental lookahead bias
3. based on price-relative strength plus market gating

That means:

- cross-sectional momentum for stock ranking
- trend/liquidity safety filters
- fewer names in neutral markets
- cash in risk-off markets

## Default Runtime Profile

```bash
AI_DECISION_ENGINE=stock_momentum
AI_UNIVERSE=nasdaq100
AI_UNIVERSE_MODE=by_date
AI_UNIVERSE_BY_DATE_FILE=data/universe/nasdaq100_by_date_weekly_2006_2026.json

AI_SNAPSHOT_FREQ=weekly
AI_HORIZON_MODE=next_snapshot
AI_EXECUTION_TIMING=next_open

AI_PORTFOLIO_TOP_K=5
AI_PORTFOLIO_MAX_WEIGHT_PCT=20
AI_PORTFOLIO_MIN_OVERLAP=4

AI_TRADE_COST_BPS=20
AI_SLIPPAGE_BPS=0
AI_SPREAD_BPS=0
AI_TAX_BPS=0

AI_START_DATE=2016-03-01
AI_END_DATE=2026-03-01

AI_SAFE_MODE=1
AI_SAFE_REQUIRE_RISK_ON=0
AI_SAFE_USE_TREND_TEMPLATE=1
AI_SAFE_MIN_VOLUME_RATIO=0.0

AI_ALGO_USE_BENCHMARK_FEATURES=1
AI_STOCK_MOMO_WEIGHT_MODE=equal
AI_STOCK_MOMO_TOP_K_NEUTRAL=5
AI_STOCK_MOMO_TOP_K_RISK_OFF=0
AI_STOCK_MOMO_MIN_POSITIONS_FOR_INVEST=2
AI_STOCK_MOMO_MAX_PER_SECTOR=2
AI_STOCK_MOMO_SECTOR_BONUS=0.15
```

## Interpretation

- `risk_on`: buy the top 5 individual names by relative strength
- `neutral`: still allow the same 5-name stock sleeve
- `risk_off`: hold cash
- ranking uses price-relative strength only, not ETFs as holdings
- rebalance is intentionally sticky: keep at least 4 of the existing 5 names when possible
- sector overlay is active: at most 2 names per sector, with a small bonus for stronger sectors

## Why This Version

This version is intentionally narrower than the previous selector:

- no complex chart score mixing
- no current-fundamental lookahead
- no ETF execution

The current checked-in baseline also keeps at least 4 of 5 holdings when possible to reduce churn.

It is the cleanest stock-first baseline to beat before adding another alpha source.
