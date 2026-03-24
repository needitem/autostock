# Tuning Round v2 (Weekly Constraint) - 2026-03-05

- 제약: **주간 리밸런싱 고정**
- 변경: crash regime + volatility cap + drawdown gate + hysteresis 추가
- 탐색 수: **7,776 configs**

## Best-by-objective (연도 강건성 우선)

- config: `{'ma_fast': 100, 'ma_slow': 200, 'mom_lb': 21, 'mom_thr': 0.0, 'risk_on': 'TQQQ', 'neutral': 'QLD', 'risk_off': 'GLD', 'crash': 'GLD', 'vol_cap': 0.04, 'crash_vol': 0.06, 'crash_dd': -0.2, 'hysteresis': 0.0}`
- CAGR diff: **+20.56%p**
- MDD diff: **-11.03%p**
- NW p(two): **0.026**, P(alpha>0): **0.987**
- Full-year(2017~2025) pos/loose/strict: **8/2/2**
- Horizon loose/strict: **10/10**, **9/10**

## Max annual robustness reached

- max full_year_pos: **8/9**
- max full_year_strict: **2/9**

Top configs among max full_year_pos:

```text
 ma_fast  ma_slow  mom_lb  mom_thr risk_on neutral risk_off crash  vol_cap  crash_vol  crash_dd  hysteresis  full_year_pos  full_year_loose  full_year_strict  loose10  strict10  cagr_diff_pctp  mdd_diff_pctp  nw_p_two
     100      200      21    0.000    TQQQ     QLD      GLD   GLD    0.040      0.060    -0.200       0.000              8                2                 2       10         9          20.562        -11.031     0.026
     100      200      21    0.000    TQQQ     QLD      GLD   GLD    0.040      0.060    -0.280       0.000              8                2                 2       10         9          20.562        -11.031     0.026
     100      200      21    0.000    TQQQ     QLD      GLD   GLD    0.050      0.060    -0.200       0.000              8                2                 2       10         9          20.562        -11.031     0.026
     100      200      21    0.000    TQQQ     QLD      GLD   GLD    0.050      0.060    -0.280       0.000              8                2                 2       10         9          20.562        -11.031     0.026
     100      200      21    0.000    TQQQ     QQQ      GLD   GLD    0.040      0.060    -0.200       0.000              8                2                 2       10         9          19.115        -11.031     0.034
     100      200      21    0.000    TQQQ     QQQ      GLD   GLD    0.040      0.060    -0.280       0.000              8                2                 2       10         9          19.115        -11.031     0.034
     100      200      21    0.000    TQQQ     QQQ      GLD   GLD    0.050      0.060    -0.200       0.000              8                2                 2       10         9          19.115        -11.031     0.034
     100      200      21    0.000    TQQQ     QQQ      GLD   GLD    0.050      0.060    -0.280       0.000              8                2                 2       10         9          19.115        -11.031     0.034
     100      200      21    0.000    TQQQ     QLD      GLD   GLD    0.040      0.050    -0.200       0.000              8                2                 2       10         9          18.795        -11.031     0.036
     100      200      21    0.000    TQQQ     QLD      GLD   GLD    0.040      0.050    -0.280       0.000              8                2                 2       10         9          18.795        -11.031     0.036
```

## Top10 overall (objective sorted)

```text
 ma_fast  ma_slow  mom_lb  mom_thr risk_on neutral risk_off crash  vol_cap  crash_vol  crash_dd  hysteresis  full_year_pos  full_year_loose  full_year_strict  loose10  strict10  cagr_diff_pctp  mdd_diff_pctp  nw_p_two
     100      200      21    0.000    TQQQ     QLD      GLD   GLD    0.040      0.060    -0.200       0.000              8                2                 2       10         9          20.562        -11.031     0.026
     100      200      21    0.000    TQQQ     QLD      GLD   GLD    0.040      0.060    -0.280       0.000              8                2                 2       10         9          20.562        -11.031     0.026
     100      200      21    0.000    TQQQ     QLD      GLD   GLD    0.050      0.060    -0.200       0.000              8                2                 2       10         9          20.562        -11.031     0.026
     100      200      21    0.000    TQQQ     QLD      GLD   GLD    0.050      0.060    -0.280       0.000              8                2                 2       10         9          20.562        -11.031     0.026
     100      200      21    0.000    TQQQ     QQQ      GLD   GLD    0.040      0.060    -0.200       0.000              8                2                 2       10         9          19.115        -11.031     0.034
     100      200      21    0.000    TQQQ     QQQ      GLD   GLD    0.040      0.060    -0.280       0.000              8                2                 2       10         9          19.115        -11.031     0.034
     100      200      21    0.000    TQQQ     QQQ      GLD   GLD    0.050      0.060    -0.200       0.000              8                2                 2       10         9          19.115        -11.031     0.034
     100      200      21    0.000    TQQQ     QQQ      GLD   GLD    0.050      0.060    -0.280       0.000              8                2                 2       10         9          19.115        -11.031     0.034
     100      200      21    0.000    TQQQ     QLD      GLD   GLD    0.040      0.050    -0.200       0.000              8                2                 2       10         9          18.795        -11.031     0.036
     100      200      21    0.000    TQQQ     QLD      GLD   GLD    0.040      0.050    -0.280       0.000              8                2                 2       10         9          18.795        -11.031     0.036
```

## 결론

- v1 대비 성능/유의성은 개선됨 (특히 horizon strict 9/10).
- 하지만 **매년 전부 유의미(9/9)** 는 이번 라운드에서도 미달 (max full_year_pos 8/9).
- 다음 라운드: 자산군/전략군 확장(섹터 로테이션, pair/hedge 강도 동적화, 연도별 실패구간 타겟 페널티) 필요.
