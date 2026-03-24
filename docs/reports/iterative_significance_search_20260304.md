# Iterative Significance Search (2026-03-04)

- tested runs (long-sample claim-grade family): **85**
- protocol base: by-date universe + next-open execution + benchmark features OFF + fallback OFF

## Top 10 by CAGR diff (2018-2025 sample)

- iter_mode_balanced_r3_20260304: CAGR diff -0.024pp | total diff -0.62pp | Sharpe diff -0.154 | MDD diff -18.80pp
- iter_refine_01_20260304: CAGR diff -0.024pp | total diff -0.62pp | Sharpe diff -0.154 | MDD diff -18.80pp
- iter_refine_02_20260304: CAGR diff -0.024pp | total diff -0.62pp | Sharpe diff -0.154 | MDD diff -18.80pp
- iter_refine_03_20260304: CAGR diff -0.024pp | total diff -0.62pp | Sharpe diff -0.154 | MDD diff -18.80pp
- iter_refine_04_20260304: CAGR diff -0.024pp | total diff -0.62pp | Sharpe diff -0.154 | MDD diff -18.80pp
- iter_topk_inv_vol_3_11_20260304: CAGR diff -0.024pp | total diff -0.62pp | Sharpe diff -0.154 | MDD diff -18.80pp
- iter_topk_equal_3_7_20260304: CAGR diff -0.201pp | total diff -5.18pp | Sharpe diff -0.186 | MDD diff -23.12pp
- iter_mode_pure_momo_r3_20260304: CAGR diff -1.094pp | total diff -27.43pp | Sharpe diff -0.187 | MDD diff -19.56pp
- iter_mode_pure_momo_r4_20260304: CAGR diff -1.432pp | total diff -35.57pp | Sharpe diff -0.191 | MDD diff -17.03pp
- iter_mode_balanced_r4_20260304: CAGR diff -1.519pp | total diff -37.63pp | Sharpe diff -0.188 | MDD diff -17.60pp

## Bottom 5

- iter_topk_equal_1_5_20260304: CAGR diff -15.520pp | total diff -258.67pp
- iter_sig_01_20260304: CAGR diff -16.284pp | total diff -265.88pp
- iter_sig_17_20260304: CAGR diff -19.035pp | total diff -288.88pp
- iter_sig_11_20260304: CAGR diff -21.670pp | total diff -307.10pp
- claimgrade_2018_2025_weekly_chart_bydate_nextopen_20260304: CAGR diff -22.471pp | total diff -311.99pp

## Statistical checkpoints

- verify_iter_topk_inv_vol_3_11_20260304.json: rows=416, CAGR diff=-0.024pp, NW t=0.242, p2=0.8092, P(alpha>0)=0.5954, bootstrap P(diff>0)=0.4854, CI95=[-16.82, 19.36]
- verify_iter_bestcfg_2025_20260304.json: rows=51, CAGR diff=57.981pp, NW t=1.316, p2=0.1880, P(alpha>0)=0.9060, bootstrap P(diff>0)=0.9128, CI95=[-22.46, 238.33]
- verify_iter_bestcfg_last12m_20260304.json: rows=51, CAGR diff=84.511pp, NW t=1.541, p2=0.1233, P(alpha>0)=0.9383, bootstrap P(diff>0)=0.9554, CI95=[-11.76, 260.34]
- verify_claimgrade_2018_2025_weekly_chart_bydate_nextopen_20260304.json: rows=416, CAGR diff=-22.471pp, NW t=-3.966, p2=0.0001, P(alpha>0)=0.0000, bootstrap P(diff>0)=0.0006, CI95=[-36.43, -9.16]

## Conclusion

- 반복 최적화로도 **장기(2018-2025) 양(+)의 통계적 우위**는 확인되지 않음.
- 최근 12개월에서는 강한 성과가 관측되지만, 장기 일관성/유의성 증거로는 부족함.
