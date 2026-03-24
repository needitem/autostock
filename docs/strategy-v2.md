# Strategy V2: Regime-First Core

Status: baseline updated with recovery state, mixed risk-on sleeve, and a levered-state MA filter as of 2026-03-11

## Summary

The primary thesis for Autostock V2 is:

- alpha should come mainly from exposure selection and drawdown control
- the mainline system should use a small ETF universe with weekly rebalancing
- AI stock picking should move out of the critical execution path and become a research or commentary layer

This is a strategy reset, not a full rewrite. The existing data pipeline, backtest engine, verification scripts, and Telegram workflow remain useful.

## Why The Thesis Changes

The stored research already shows that the current "weekly AI stock selection as primary alpha" path is not robust enough to be the main strategy.

- The 10-year AI validation beat QQQ on CAGR, but did so with materially worse drawdown and weak significance. See `docs/reports/validation_full10y_ai10y_codex_weekly_report_20260305.md`.
- The strongest 1-year tuned runs did not hold up when expanded across 2-year to 10-year horizons and calendar years. See `docs/reports/codex_weekly_tuning_ladder_20260304.md`.
- The 20-year AI dynamic blend still depended on a small edge, weak statistical support, and low assumed trading costs. See `data/runs/verify_ai20y_ndx100_bydate_weekly_ai_ro1_dynblend_v2_20260306.md`.
- Broader systematic searches found stronger persistence from simple trend and regime rules than from prompt-tuned stock selection. See `docs/reports/weekly_broad_strategy_search_20260304.md` and `data/runs/regime_rotation_best_rr_v5_grid_20260305.json`.

## V2 Hypothesis

V2 assumes:

1. The largest repeatable edge in this repo is regime handling, not single-name intelligence.
2. Simpler tradable building blocks are easier to validate across long samples and cost assumptions.
3. AI is more defensible as a support layer than as a direct portfolio constructor.

## Design Principles

- Weekly only.
- Next-open execution only.
- Long-only only for the baseline.
- Small ETF universe only for the production baseline.
- Costs included in every headline result.
- Robustness ranks above peak CAGR.
- AI must not change live weights in the default production path.

## Baseline Portfolio Ladder

The first V2 production baseline should target a simple exposure ladder:

- Risk-on: `TQQQ 80% + QQQ 20%`
- Risk-on alt: `QLD`
- Neutral: `QLD`
- Recovery below the slow trend line: `QQQ`
- Risk-off: `GLD`
- Cash-like fallback if needed: `BIL`

The default state machine should prefer reducing exposure instead of searching for short-term stock alpha.
It should also distinguish between true risk-off conditions and shallow below-`MA200` rebounds, so the system can step down from leverage without immediately hiding in `GLD`.
It should also downshift levered states when `QLD` loses its `MA50`, even if the broader `QQQ` regime is still constructive.

## Default Runtime Profile

The checked-in V2 baseline runner should use these defaults unless overridden by env vars:

```bash
AI_DECISION_ENGINE=regime
AI_SNAPSHOT_FREQ=weekly
AI_HORIZON_MODE=next_snapshot
AI_EXECUTION_TIMING=next_open

AI_SYMBOLS=TQQQ,QLD,QQQ,GLD,BIL
AI_PORTFOLIO_TOP_K=1
AI_PORTFOLIO_MAX_WEIGHT_PCT=100

AI_TRADE_COST_BPS=20
AI_SLIPPAGE_BPS=0
AI_SPREAD_BPS=0
AI_TAX_BPS=0

AI_START_DATE=2016-03-01
AI_END_DATE=2026-03-01

AI_SAFE_MODE=1
AI_SAFE_USE_TREND_TEMPLATE=0
AI_SAFE_MIN_VOLUME_RATIO=0

AI_REGIME_SOURCE=QQQ
AI_REGIME_MA_FAST=100
AI_REGIME_MA_SLOW=200
AI_REGIME_MOM_LB=21
AI_REGIME_MOM_THR=0.0
AI_REGIME_RISK_ON=TQQQ:80,QQQ:20
AI_REGIME_RISK_ON_ALT=QLD
AI_REGIME_NEUTRAL=QLD
AI_REGIME_RECOVERY=QQQ
AI_REGIME_RISK_OFF=GLD
AI_REGIME_CRASH=GLD
AI_REGIME_VOL_CAP=0.05
AI_REGIME_VOL_LOW=0.035
AI_REGIME_VOL_MID=0.04
AI_REGIME_MOM_STRONG=0.06
AI_REGIME_CRASH_VOL=0.06
AI_REGIME_CRASH_DD=-0.2
AI_REGIME_HYSTERESIS=0.0
AI_REGIME_RECOVERY_SLOW_BUFFER=0.03
AI_REGIME_RECOVERY_MIN_MOM=0.015
AI_REGIME_RECOVERY_MAX_VOL=0.045
AI_REGIME_RECOVERY_DD_FLOOR=-0.12
AI_REGIME_FILTER_ASSET=QLD
AI_REGIME_FILTER_MA=50
AI_REGIME_FILTER_SAFE=QQQ

AI_REGIME_EXPOSURE=0
AI_MOMENTUM_BLEND_PCT=0
AI_ALGO_USE_BENCHMARK_FEATURES=0
```

This does not claim to be the final best profile. It is the baseline that matches the new thesis, adds a dedicated recovery state for shallow rebounds, trims the pure `TQQQ` attack sleeve with a small `QQQ` ballast, and uses a simple `QLD` `MA50` downshift rule to step from levered exposure into `QQQ` before broader regime damage appears.

## Keep Vs Demote

| Area | Decision | Why |
| --- | --- | --- |
| `scripts/backtest_ai_portfolio_selector.py` | Keep | It now supports `regime`, `trend`, `chart`, and `ai` engines in one backtest loop. |
| `scripts/research_weekly_regime_allocator.py` | Keep | It is already the strongest research harness for robustness-first regime search. |
| `scripts/verify_ai_portfolio_backtest.py` | Keep | It provides claim-grade verification, turnover, and cost checks. |
| `scripts/build_trade_journal_report.py` | Keep | It remains useful for trade-level diagnostics. |
| `src/bot/*` and `src/main.py` | Keep | Delivery and scheduling still matter after the strategy reset. |
| Weekly AI stock picking (`AI_DECISION_ENGINE=ai`) | Demote | Research only, not the default production path. |
| Broad-stock chart-only universe as primary strategy | Demote | It has not shown enough robustness against QQQ after full validation. |
| Prompt tuning as the main optimization loop | Demote | It adds complexity faster than it adds durable edge. |

## Acceptance Criteria For V2

The baseline is good enough to promote only if all of the following are true:

1. Every headline report uses total cost of at least `20 bps` per 100 percent turnover.
2. The 3-year, 5-year, and 7-year horizon windows each show `CAGR diff > 0` and `Newey-West p(two-sided) < 0.10`.
3. The full 10-year window shows `CAGR diff > 0` and `P(alpha > 0) >= 0.90`.
4. Full-sample max drawdown is not worse than QQQ by more than `10 percentage points`.
5. Average turnover stays at or below `0.30`.
6. The default bot and CLI workflow can run the V2 baseline without any LLM dependency.
7. Any AI layer that remains in production can explain positions, but cannot override weights in the default path.

## Implementation Shape

### Phase 1: Freeze the baseline

- Add a checked-in runner that launches the V2 regime baseline with explicit env defaults.
- Document the thesis and the success criteria in the repo.
- Stop treating AI stock selection as the primary benchmark to beat.

### Phase 2: Separate execution from commentary

- Keep the execution engine systematic and deterministic.
- Keep AI for report writing, regime commentary, and post-trade review.
- Do not let the commentary layer mutate portfolio weights.

### Phase 3: Promote V2 to the default user path

- Add a stable CLI entry point for the V2 baseline.
- Make bot labels and help text point to the regime-first baseline instead of the AI picker.
- Keep AI experiments behind an explicit research switch.

### Phase 4: Extend only after robustness holds

- Only then evaluate optional additions such as `QQQ` mid-state, `TLT` fallback, or limited dynamic exposure overlays.
- Any new branch must beat the frozen baseline on robustness, not just headline CAGR.

## What V2 Does Not Try To Promise

V2 does not assume one system can maximize upside and minimize downside in every regime at once.

Instead it aims to:

- outperform QQQ over long windows often enough to matter
- avoid large avoidable drawdowns
- preserve a simple enough structure that failed assumptions are visible

## Next Build Target

The next implementation target is not "make AI smarter."

It is:

- make the regime-first baseline easy to run
- make verification the default
- keep AI in a support role until it proves durable incremental value
