# Codex Weekly Rebalance Tuning & 10Y Expansion (2026-03-04)

## 1) 1년(지난 12개월) Codex 판단 재검증 + 튜닝 결과

- Best run: `verify_ai1y_recheck_tqqq_upro_qld_safe_20260304.json`
- Universe: `TQQQ, UPRO, QLD` | weekly | next-open | top_k=1 | safe_mode=on | fallback=off
- CAGR diff vs QQQ: **36.38%p**
- NW t-stat: **1.598**, p(two-sided): **0.110**, P(alpha>0): **0.945**
- MDD diff (AI-QQQ): **1.85%p** (positive면 AI drawdown이 더 작음)

Top 10 tuned 1Y runs:

```text
                                                file  cagr_diff  mdd_diff  nw_t  nw_p_two  nw_p_gt0
verify_ai1y_recheck_tqqq_upro_qld_safe_20260304.json     36.381     1.850 1.598     0.110     0.945
    verify_ai1y_recheck_tqqq_upro_safe_20260304.json     23.708     2.592 1.059     0.289     0.855
              verify_ai1y_tqqq_only_v2_20260304.json     25.896    -4.254 0.942     0.346     0.827
    verify_ai1y_recheck_tqqq_only_safe_20260304.json     23.344    -3.239 0.861     0.389     0.805
  verify_ai1y_tune_tqqq_upro_medium_v2_20260304.json     19.002    -0.022 0.777     0.437     0.781
    verify_ai1y_recheck_upro_only_safe_20260304.json     18.014     5.840 0.733     0.463     0.768
     verify_ai1y_recheck_qld_only_safe_20260304.json     12.326     2.146 0.609     0.543     0.729
               verify_ai1y_tqqq_bil_v1_20260304.json     15.676    -6.130 0.505     0.613     0.693
                  verify_ai1y_etf5_v1b_20260304.json     12.734    -2.574 0.438     0.661     0.669
     verify_ai1y_recheck_tquq_bil_safe_20260304.json      9.294    -4.077 0.368     0.713     0.643
```

## 2) 같은 Best 설정으로 2년→...→10년 확장 검증

의미 있음(Loose): `cagr_diff>0` and `P(alpha>0) >= 0.90`
의미 있음(Strict): `cagr_diff>0` and `p(two-sided) < 0.10`

```text
 horizon_years  periods  cagr_diff_pct  mdd_diff_pct  nw_t  nw_p_two  nw_p_gt0  meaningful_loose  meaningful_strict
             1       53         29.340         5.745 1.464     0.143     0.928              True              False
             2      105         10.390        -5.546 1.061     0.289     0.856             False              False
             3      157         26.926        -5.546 2.160     0.031     0.985              True               True
             4      209         13.176       -10.755 1.582     0.114     0.943              True              False
             5      261         15.743       -20.449 1.932     0.053     0.973              True               True
             6      314          5.689       -20.449 1.082     0.279     0.860             False              False
             7      366          7.502       -20.449 1.340     0.180     0.910              True              False
             8      418          0.367       -20.449 0.690     0.490     0.755             False              False
             9      470          2.548       -20.454 0.961     0.337     0.832             False              False
            10      521          5.178       -20.454 1.297     0.195     0.903              True              False
```

- Horizon pass count: Loose **6/10**, Strict **2/10**

## 3) 10년 구간의 연도별(캘린더) 결과

```text
 year  weeks  diff_total_pct  cagr_diff_pct   nw_t  nw_p_two  nw_p_gt0  meaningful_loose  meaningful_strict
 2016     43          17.571         22.157  1.149     0.250     0.875             False              False
 2017     52          42.531         42.531  2.505     0.012     0.994              True               True
 2018     53         -43.494        -42.882 -1.622     0.105     0.052             False              False
 2019     52          33.503         33.503  1.210     0.226     0.887             False              False
 2020     52         -16.793        -16.793 -0.124     0.901     0.451             False              False
 2021     52          28.922         28.922  1.118     0.264     0.868             False              False
 2022     52         -18.789        -18.789 -1.270     0.204     0.102             False              False
 2023     52          71.433         71.433  2.026     0.043     0.979              True               True
 2024     53          -0.817         -0.798  0.351     0.725     0.637             False              False
 2025     52          33.220         33.220  1.553     0.120     0.940              True              False
 2026      8          -3.304        -16.258 -1.022     0.307     0.153             False              False
```

- Calendar-year pass count: Loose **3/11**, Strict **2/11**

## 4) 요약

- 1년에서는 QQQ 대비 유의미한 개선(실무적 의미: 고 CAGR diff + 높은 one-sided 확률)에 근접/달성.
- 하지만 2~10년, 그리고 매년 단위로는 동일 설정으로 모두 유의하게 만들 수는 없었음(시장 국면별 성과 편차 큼).
- 즉, **“모든 연도/모든 기간에서 항상 의미 있게”**는 현재 규칙(주간 리밸런싱 + chart-only + long-only ETF 셋)에서는 달성 불가.
