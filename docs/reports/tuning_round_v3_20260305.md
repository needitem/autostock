# Tuning Round v3 (Adaptive Leverage Buckets) - 2026-03-05

- 제약: **주간 리밸런싱 고정**
- 변경: risk_on을 단일 토큰에서 **2단계(강세/중립 강세)**로 분리하고, 변동성 버킷(`vol_low`, `vol_mid`) + 강모멘텀 임계(`mom_strong`)를 추가
- 탐색 수: **3,456 configs**

## v3 Best-by-objective

- config: `{'ma_fast': 100, 'ma_slow': 200, 'mom_lb': 21, 'mom_thr': 0.0, 'risk_on': 'TQQQ', 'risk_on_alt': 'QLD', 'neutral': 'QLD', 'risk_off': 'GLD', 'crash': 'GLD', 'vol_cap': 0.05, 'vol_low': 0.035, 'vol_mid': 0.04, 'mom_strong': 0.06, 'crash_vol': 0.06, 'crash_dd': -0.2, 'hysteresis': 0.0}`
- CAGR diff: **+9.80%p**
- MDD diff: **-6.15%p**
- NW p(two): **0.166**, P(alpha>0): **0.917**
- Full-year(2017~2025) pos/loose/strict: **8/2/1**
- Horizon loose/strict: **9/10**, **7/10**

## v2 vs v3 (best config 비교)

| metric | v2 | v3 | delta(v3-v2) |
|---|---:|---:|---:|
| cagr_diff_pctp | 20.562 | 9.799 | -10.764 |
| mdd_diff_pctp | -11.031 | -6.154 | +4.876 |
| nw_t | 2.222 | 1.385 | -0.836 |
| nw_p_two | 0.026 | 0.166 | +0.140 |
| nw_p_gt0 | 0.987 | 0.917 | -0.070 |
| full_year_pos | 8 | 8 | +0 |
| full_year_strict | 2 | 1 | -1 |

## v3 Annual robustness ceiling

- max full_year_pos: **8/9**
- max full_year_strict: **1/9**

Top 10 configs (annual robustness 우선):

```text
 ma_fast risk_on risk_on_alt neutral risk_off crash  vol_low  vol_mid  mom_strong  crash_dd  hysteresis  full_year_pos  full_year_strict  cagr_diff_pctp  mdd_diff_pctp  nw_p_two
     100    TQQQ         QLD     QLD      GLD   GLD    0.035     0.04        0.06     -0.20         0.0              8                 1        9.798548      -6.154311  0.166009
     100    TQQQ         QLD     QLD      GLD   GLD    0.035     0.04        0.06     -0.28         0.0              8                 1        9.798548      -6.154311  0.166009
     100    TQQQ         QLD     QLD      GLD   GLD    0.035     0.05        0.06     -0.20         0.0              8                 1        9.798548      -6.154311  0.166009
     100    TQQQ         QLD     QLD      GLD   GLD    0.035     0.05        0.06     -0.28         0.0              8                 1        9.798548      -6.154311  0.166009
     100    TQQQ         QLD     QLD      GLD   GLD    0.030     0.04        0.06     -0.20         0.0              8                 1        9.718766      -6.154311  0.167996
     100    TQQQ         QLD     QLD      GLD   GLD    0.030     0.04        0.06     -0.28         0.0              8                 1        9.718766      -6.154311  0.167996
     100    TQQQ         QLD     QLD      GLD   GLD    0.030     0.05        0.06     -0.20         0.0              8                 1        9.718766      -6.154311  0.167996
     100    TQQQ         QLD     QLD      GLD   GLD    0.030     0.05        0.06     -0.28         0.0              8                 1        9.718766      -6.154311  0.167996
     100    TQQQ         QLD     QLD      GLD   GLD    0.025     0.04        0.06     -0.20         0.0              8                 1        9.379441      -6.154311  0.178934
     100    TQQQ         QLD     QLD      GLD   GLD    0.025     0.04        0.06     -0.28         0.0              8                 1        9.379441      -6.154311  0.178934
```

## 결론

- v3는 **MDD를 v2 대비 완화**(손실 완충)했지만, CAGR/유의성은 하락.
- 현재 탐색 범위에서는 여전히 **매년 전부 유의미(9/9) 미달**.
- 다음 라운드는 신호군 확장(벤치마크 외 breadth/credit/term-spread, 자산군 확대) 또는 모델 자체 교체가 필요.