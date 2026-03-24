# Strategy V3: Balance Profile

Status: experimental as of 2026-03-11

## Goal

This profile is a rewrite of the sizing logic, not the whole stack.

It keeps the V2 regime-first state machine, but changes the objective:

- accept lower peak CAGR than the growth baseline
- reduce leverage in weaker bullish states
- improve drawdown behavior without giving up the long-only weekly workflow

## Core Idea

The main change is state-aware exposure:

- `risk_on`: full participation is allowed
- `risk_on_alt`: leverage is trimmed
- `neutral` and `recovery`: stay invested, but do not add more leverage than needed
- `risk_off` and `crash`: remain fully defensive in this first balance profile

This is a strategy-layer rewrite because V2 previously treated `risk_on` and `risk_on_alt` as the same exposure bucket.

## Default Runtime Profile

The runner in `scripts/run_strategy_v3_balance.py` uses:

```bash
AI_DECISION_ENGINE=regime
AI_SYMBOLS=TQQQ,QLD,QQQ,GLD,BIL
AI_TRADE_COST_BPS=20

AI_REGIME_RISK_ON=TQQQ
AI_REGIME_RISK_ON_ALT=QLD
AI_REGIME_NEUTRAL=QLD
AI_REGIME_RECOVERY=QQQ
AI_REGIME_RISK_OFF=GLD
AI_REGIME_CRASH=GLD

AI_REGIME_EXPOSURE=1
AI_REGIME_ON_EXPOSURE_PCT=95
AI_REGIME_RISK_ON_ALT_EXPOSURE_PCT=90
AI_REGIME_NEUTRAL_EXPOSURE_PCT=100
AI_REGIME_RECOVERY_EXPOSURE_PCT=100
AI_REGIME_RISK_OFF_EXPOSURE_PCT=100
AI_REGIME_CRASH_EXPOSURE_PCT=100
```

## Promotion Rule

Do not replace the V2 baseline unless this balance profile shows a clearly better return-drawdown tradeoff after full verification.
