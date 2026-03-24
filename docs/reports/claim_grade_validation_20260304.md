# Claim-Grade Validation (2026-03-04)

## Protocol (locked)
- Engine: `chart` (non-AI)
- Benchmark features in model: `OFF` (`AI_ALGO_USE_BENCHMARK_FEATURES=0`)
- Execution timing: `next_open` (signal at t close, fill at t+1 open)
- Universe: `nasdaq100_by_date` (`AI_UNIVERSE_MODE=by_date`)
- Rebalance: weekly
- Transaction cost: 20 bps (turnover-based)
- Fallback: OFF (`AI_FALLBACK_ON_AI_FAIL=0`, `AI_FORCE_FALLBACK=0`)

## Files
- 2025:
  - `data/runs/ai_portfolio_backtest_summary_claimgrade_2025_weekly_chart_bydate_nextopen_20260304.json`
  - `data/runs/verify_claimgrade_2025_weekly_chart_bydate_nextopen_20260304.json`
- Last 12M:
  - `data/runs/ai_portfolio_backtest_summary_claimgrade_last12m_weekly_chart_bydate_nextopen_20260304.json`
  - `data/runs/verify_claimgrade_last12m_weekly_chart_bydate_nextopen_20260304.json`
- 2018-2025 OOS:
  - `data/runs/ai_portfolio_backtest_summary_claimgrade_2018_2025_weekly_chart_bydate_nextopen_20260304.json`
  - `data/runs/verify_claimgrade_2018_2025_weekly_chart_bydate_nextopen_20260304.json`

## Results

### 2025-01-01 ~ 2025-12-31 (51 weeks)
- CAGR diff (Algo - QQQ): **-2.72%p**
- Total return diff: **-2.66%p**
- Alpha mean/week: **-0.0489%p**
- Newey-West t-stat: **-0.148** (two-sided p=**0.882**)
- Bootstrap CAGR diff CI95: **[-47.10%, +47.63%]**

### 2025-03-01 ~ 2026-03-04 (51 weeks)
- CAGR diff (Algo - QQQ): **+16.06%p**
- Total return diff: **+15.66%p**
- Alpha mean/week: **+0.2129%p**
- Newey-West t-stat: **+0.570** (two-sided p=**0.569**)
- Bootstrap CAGR diff CI95: **[-36.21%, +77.56%]**

### 2018-01-01 ~ 2025-12-31 (416 weeks)
- CAGR diff (Algo - QQQ): **-22.47%p**
- Total return diff: **-311.99%p**
- Alpha mean/week: **-0.4111%p**
- Newey-West t-stat: **-3.966** (two-sided p=**0.0001**)
- Bootstrap CAGR diff CI95: **[-36.43%, -9.16%]**

## Verdict
- **Claim-grade outperformance vs QQQ: FAIL**
- 이유:
  1. 단기(51주) 구간은 신뢰구간이 매우 넓고 유의하지 않음.
  2. 장기 OOS(2018-2025)에서는 통계적으로 유의하게 QQQ 하회.
  3. 따라서 현재 로직은 “일부 최근 구간에서 우연히 강함” 가능성이 크며, 일반화된 우위 전략으로 보기 어려움.

## Next
- 이 상태에서 성능 주장 대신, 규칙 재설계 후 동일 프로토콜로 재검증 필요.
- 특히 2018~2023 구간의 underperformance 원인 분해(진입/청산/리스크 축소 규칙) 우선.
