# AI Weekly 20Y Dynamic Blend Tuning (2026-03-06)

## Goal
- Weekly rebalance only
- QQQ is comparison only, not model input
- Improve both CAGR and max drawdown versus the previous fixed-blend best

## Previous best
- Run: `ai20y_ndx100_bydate_weekly_ai_ro1_blend88p76_20260305`
- CAGR: **18.288%**
- MDD: **-51.847%**
- Sharpe: **0.771**

## New best
- Run: `ai20y_ndx100_bydate_weekly_ai_ro1_dynblend_v2_20260306`
- Logic:
  - default momentum blend = **88.76%**
  - raise blend to **100%** only when:
    - `breadth_up200 >= 0.45`
    - `breadth_up50 >= 0.55`
    - `breadth_positive_63d >= 0.40`
    - `VIX <= 22`
- Result:
  - CAGR: **18.643%**
  - MDD: **-51.840%**
  - Sharpe: **0.770**

## Versus QQQ
- Strategy CAGR **18.643%** vs QQQ **14.229%** → **+4.414%p**
- Strategy MDD **-51.840%** vs QQQ **-51.855%** → **+0.015%p**
- Strategy Sharpe **0.770** vs QQQ **0.744**

## Artifacts
- Summary: `data/runs/ai_portfolio_backtest_summary_ai20y_ndx100_bydate_weekly_ai_ro1_dynblend_v2_20260306.json`
- Verification: `data/runs/verify_ai20y_ndx100_bydate_weekly_ai_ro1_dynblend_v2_20260306.md`
- Trade journal: `docs/reports/trade_journal_ai20y_ndx100_bydate_weekly_ai_ro1_dynblend_v2_20260306.md`
