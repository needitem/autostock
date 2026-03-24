# Tuning Round v6 (Risk↓ + Return↑ 집중) - 2026-03-05

## 1) 기존 Regime v5 상태
- best CAGR diff: **+9.80%p**
- best MDD diff: **-6.15%p**
- full-year(2017~2025) outperformance: **8/9**
- 해석: 연도 강건성은 유지되지만, **리스크/수익 동시 개선(둘 다 +)** 조합은 이 계열에서 미발견.

## 2) 전략군 교체: Levered Trend(자산 자체 MA 기반)
- 새 탐색 스크립트: `scripts/research_weekly_levered_trend.py`
- 탐색 수: **270 configs**
- best config: `{'risk': 'QLD', 'safe': 'GLD', 'ma_window': 125, 'qqq_confirm': False, 'vix_max': 0.0}`
- best CAGR diff: **+13.58%p**
- best MDD diff: **+2.13%p**
- full-year(2017~2025) outperformance: **7/9**
- NW p(two): **0.026**

### Dominant(수익+, MDD+) 상위 10
```text
risk safe  ma_window  qqq_confirm  vix_max  full_year_pos  cagr_diff_pctp  mdd_diff_pctp  nw_p_two
 QLD  GLD        125        False      0.0              7       13.577325       2.125208  0.025977
 QLD  GLD        125         True      0.0              7       13.125638       2.125208  0.030374
 QLD  GLD        100        False      0.0              7        9.291162       2.125208  0.100361
 QLD  GLD        100         True      0.0              7        8.908573       2.125208  0.124275
 QLD  GLD        125        False     32.0              6        7.766872       2.125208  0.163526
 QLD  GLD        125         True     32.0              6        7.334986       2.125208  0.183406
 QLD  GLD        125        False     28.0              6        6.033102       2.125208  0.257117
 QLD  GLD        100        False     32.0              6        5.978443       2.125208  0.237374
 QLD  GLD        125         True     28.0              6        5.607124       2.125208  0.284532
 QLD  GLD        100         True     32.0              6        5.605736       2.125208  0.278822
```

### Best config 연도별 총수익(%)
```text
 year  strategy_total_pct  benchmark_total_pct  total_diff_pctp
 2016           -5.115686            13.450014       -18.565700
 2017           69.726138            31.265198        38.460940
 2018           16.075074             0.038322        16.036752
 2019           23.527575            35.678717       -12.151142
 2020           94.519129            48.287052        46.232077
 2021           54.276373            26.638319        27.638055
 2022          -14.134901           -32.677609        18.542708
 2023           71.204737            51.066445        20.138292
 2024           10.645896            29.119856       -18.473960
 2025           47.402374            18.186326        29.216048
 2026            1.246373            -3.303627         4.550000
```

## 3) 백테스트 엔진 반영
- `scripts/backtest_ai_portfolio_selector.py`에 `AI_DECISION_ENGINE=trend` 추가
- 신규 파라미터:
  - `AI_TREND_RISK_SYMBOL`, `AI_TREND_SAFE_SYMBOL`, `AI_TREND_ALT_SYMBOL`
  - `AI_TREND_MA_WINDOW` (50/100/125/150/175/200)
  - `AI_TREND_REQUIRE_RISK_ON`, `AI_TREND_VIX_MAX`, `AI_TREND_USE_ALT` 등
- indicator 확장: `ma100/125/175` 및 gap 필드 추가

## 4) 반영 후 실측 (backtest_ai_portfolio_selector)
- run tag: `trend10y_qld_gld125_20260305_v2`
- AI portfolio CAGR: **33.67%** vs QQQ **18.96%**
- AI portfolio MDD: **-32.08%** vs QQQ **-34.47%**
- CAGR diff: **+14.71%p**, MDD diff: **+2.39%p**
- 총수익률: **1731.51%** vs QQQ **469.64%**

## 5) 결론
- 이번 라운드에서 **리스크↓ + 수익↑ 동시 달성** 조합을 확보함.
- 다만 연도별로는 아직 **7/9** 수준(2019, 2024 약세)이라, 다음 라운드는 해당 2개년도 실패구간 타겟 보완이 필요.