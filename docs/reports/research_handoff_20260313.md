# Autostock Research Handoff

작성일: 2026-03-13  
목적: 새 컨텍스트에서 바로 이어서 연구할 수 있도록, 2026-03-13 기준 최신 전략 상태/검증 결과/교훈/다음 우선순위를 한 문서에 정리한다.

## 0.4 2026-03-23 Addendum

stock-first 라인을 더 좁게 재탐색한 결과,
현재 최선 후보는 `weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto325_entryfreeze1_pb007`로 업데이트됐다.

핵심 수치:

- 표준 backtest 기준  
  CAGR `22.68%`, Sharpe `0.999`, MDD `-29.07%`, turnover `0.210`
- baseline 대비  
  CAGR diff `+3.85%p`, Sharpe diff `+0.135`
- promotion check  
  최근 3년/5년/7년 horizon의 NW p(two-sided)가 아직 `0.10` 아래로 내려오지 못해서 **승격 실패**

현재 해석:

- stock-first 쪽에서 살아남은 구조는 여전히 `weak neutral new-entry freeze`다.
- `pit_bonus 0.07`을 더한 `entryfreeze1_pb007`가 현재 stock-first 최고점이다.
- 다만 아직 promotion-ready는 아니다. 이 라인은 여전히 `best stock-first candidate` 수준이다.

관련 파일:

- [stock_hypothesis_sweep_entryfreeze1_narrow_20260323.md](/D:/my/autostock/data/runs/stock_hypothesis_sweep_entryfreeze1_narrow_20260323.md)
- [stock_hypothesis_compare_weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto325_entryfreeze1_pb007_vs_weekly_baseline_v4_20260323T005440Z.md](/D:/my/autostock/data/runs/stock_hypothesis_compare_weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto325_entryfreeze1_pb007_vs_weekly_baseline_v4_20260323T005440Z.md)
- [promotion_check_stockfirst_entryfreeze1_pb007_20260323.md](/D:/my/autostock/data/runs/promotion_check_stockfirst_entryfreeze1_pb007_20260323.md)

## 0.3 2026-03-23 Addendum

stock-first 라인을 더 좁게 재탐색한 결과,
현재 최선 후보는 `weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto325_entryfreeze1_pb007`로 업데이트됐다.

핵심 수치:

- 표준 backtest 기준  
  CAGR `22.68%`, Sharpe `0.999`, MDD `-29.07%`, turnover `0.210`
- baseline 대비  
  CAGR diff `+3.85%p`, Sharpe diff `+0.135`
- strict annual probe 기준  
  CAGR diff `+2.78%p`, Sharpe diff `+0.010`, turnover `0.224`

현재 해석:

- `weak neutral new-entry freeze` 구조는 재현됐고, 여기에 `pit_bonus 0.07`을 얹은 버전이
  기존 `entryfreeze1`보다 소폭 더 좋았다.
- 다만 promotion check는 여전히 실패한다.
  최근 3년/5년/7년 horizon의 NW p(two-sided)가 아직 `0.10` 아래로 내려오지 못했다.
- 따라서 최신 stock-first 상태는
  `best stock-first candidate = entryfreeze1_pb007`,
  `promotion-ready = 아직 아님`
  으로 보는 게 맞다.

관련 파일:

- [stock_hypothesis_sweep_entryfreeze1_narrow_20260323.md](/D:/my/autostock/data/runs/stock_hypothesis_sweep_entryfreeze1_narrow_20260323.md)
- [stock_hypothesis_compare_weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto325_entryfreeze1_pb007_vs_weekly_baseline_v4_20260323T005440Z.md](/D:/my/autostock/data/runs/stock_hypothesis_compare_weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto325_entryfreeze1_pb007_vs_weekly_baseline_v4_20260323T005440Z.md)
- [promotion_check_stockfirst_entryfreeze1_pb007_20260323.md](/D:/my/autostock/data/runs/promotion_check_stockfirst_entryfreeze1_pb007_20260323.md)

## 0. 2026-03-16 Addendum

`weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto35` 주변을 더 좁게 재탐색한 결과,
`weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto325`가
`strict annual roll-forward static` 기준으로는 아주 근소하게 더 좋아졌다.

핵심 수치:

- strict annual static 기준  
  `veto325`: CAGR diff `+2.73%p`, Sharpe diff `+0.004`, turnover `0.2299`, NW p(two-sided) `0.527`
- strict annual static 기준  
  `veto35`: CAGR diff `+2.69%p`, Sharpe diff `+0.002`, turnover `0.2304`, NW p(two-sided) `0.530`
- 반면 fixed OOS와 표준 backtest headline은 `veto35`가 아주 근소하게 더 좋다.

현재 해석:

- `strict annual`을 최우선 승격 기준으로 두면 `veto325`가 새 leading promotion candidate다.
- 다만 `veto35`와 차이가 매우 작아서, 둘은 아직 사실상 `neck-and-neck` 상태로 보는 게 안전하다.
- 다음 full `champion/challenger pipeline` 재실행 때는 `veto325`도 sensitivity suite에 포함되도록 코드에는 이미 반영해뒀다.

후속 확인 (`2026-03-17` 관점):

- `veto325`를 sensitivity suite에 넣고 full `champion/challenger pipeline`도 다시 돌렸다.
- 결과적으로 `fixed OOS sensitivity` 기준 공식 best candidate는 여전히 `veto35`였다.
- 반면 `strict annual roll-forward static` 3-way 재검증에서는 여전히 `veto325`가 근소 우위였다.

즉 현재 상태는:

- `pipeline official best`: `weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto35`
- `strict-best candidate`: `weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto325`
- 둘의 차이는 매우 작으므로, 지금 단계에선 둘 다 `promotion-finalist`로 관리하는 게 맞다.

관련 파일:

- [stock_hypothesis_sweep_neutral_ro2_veto35_narrow_20260316.md](/D:/my/autostock/data/runs/stock_hypothesis_sweep_neutral_ro2_veto35_narrow_20260316.md)
- [stock_hypothesis_compare_weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto325_vs_weekly_baseline_v4_20260316T003135Z.md](/D:/my/autostock/data/runs/stock_hypothesis_compare_weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto325_vs_weekly_baseline_v4_20260316T003135Z.md)
- [strict_compare_weekly_baseline_vs_neutral_ro2_threshold_recheck_20260316.md](/D:/my/autostock/data/runs/strict_compare_weekly_baseline_vs_neutral_ro2_threshold_recheck_20260316.md)
- [champion_challenger_summary_champion_challenger_20260316T232327Z.md](/D:/my/autostock/data/runs/champion_challenger_summary_champion_challenger_20260316T232327Z.md)

## 0.1 2026-03-17 Addendum

`stock-first` 쪽이 애매하면 아예 새 전략축이 더 나은지 확인하려고,
기존 deterministic `regime-first` 계열인 V2/V3를 현재 비용 조건에서 다시 돌려봤다.

`2016-03-01`부터 `2026-03-01`까지의 공정 비교 기준:

- `V2 regime-first`: CAGR `31.24%`, Sharpe `1.10`, MDD `-39.93%`, QQQ 대비 CAGR diff `+12.28%p`, NW p(two-sided) `0.087`
- `V3 balance`: CAGR `28.09%`, Sharpe `1.03`, MDD `-38.68%`, QQQ 대비 CAGR diff `+9.13%p`, NW p(two-sided) `0.178`
- `stock-best (entryfreeze1_pb007, same 2016+ slice)`: CAGR `22.68%`, Sharpe `1.00`, MDD `-29.07%`, QQQ 대비 CAGR diff `+5.91%p`, NW p(two-sided) `0.269`

현재 해석:

- `새 전략축을 짜는 게 낫냐`는 질문에 대해, `프로덕션` 관점에서는 **그럴 가능성이 높다**.
- 특히 `V2 regime-first`는 stock-best보다 drawdown은 더 깊지만, 수익률과 통계적 지지가 훨씬 강하다.
- 반대로 `stock-best`는 drawdown/turnover는 더 좋고, `stock-first` 목표에도 더 잘 맞는다.
- 즉 현재 저장소에는 이미 두 개의 production path 후보가 공존한다.
  `growth/robustness 우선이면 V2`, `stock-first와 더 낮은 DD 우선이면 stock-best`.
- 다음 큰 연구 주제는 `둘 중 하나를 고르는 것`보다, `V2의 상단 exposure control`과 `stock-best의 stock sleeve`를 결합한 hybrid reset이 더 유망할 수 있다는 점이다.

관련 파일:

- [ai_portfolio_backtest_verification_strategy_v2_baseline_20260317_recheck.md](/D:/my/autostock/data/runs/ai_portfolio_backtest_verification_strategy_v2_baseline_20260317_recheck.md)
- [ai_portfolio_backtest_verification_strategy_v3_balance_20260317_recheck.md](/D:/my/autostock/data/runs/ai_portfolio_backtest_verification_strategy_v3_balance_20260317_recheck.md)
- [ai_portfolio_backtest_verification_stock_hypothesis_weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto325_20260316T003135Z.md](/D:/my/autostock/data/runs/ai_portfolio_backtest_verification_stock_hypothesis_weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto325_20260316T003135Z.md)

간단 hybrid overlay 결과:

- `strategy_v6_hybrid_stock_regime`로 `stock-best + regime exposure cap`을 붙여봤지만,
  CAGR `22.32%`, Sharpe `0.96`, MDD `-32.11%`, QQQ 대비 CAGR diff `+3.35%p`, NW p(two-sided) `0.591`로
  `stock-best`도 `V2`도 이기지 못했다.
- 따라서 **단순 exposure cap overlay만으로는 hybrid reset이 잘 안 된다**는 교훈이 추가됐다.
- hybrid를 다시 할 거면, `stock sleeve`를 단순 cash-scaling하는 게 아니라
  `regime core`와 `stock sleeve`를 구조적으로 결합하는 쪽으로 다시 설계하는 게 맞다.

관련 파일:

- [ai_portfolio_backtest_verification_strategy_v6_hybrid_stock_regime_20260317_recheck.md](/D:/my/autostock/data/runs/ai_portfolio_backtest_verification_strategy_v6_hybrid_stock_regime_20260317_recheck.md)

## 0.2 2026-03-19 Addendum

가장 강해 보이던 과거 `levered trend` 연구 결과를
현재 표준 verification 경로로 다시 올려 확인했다.

결과:

- `strategy_v8_levered_trend_best`: CAGR `32.54%`, Sharpe `1.08`, MDD `-32.35%`,
  QQQ 대비 CAGR diff `+13.58%p`, NW p(two-sided) `0.026`, bootstrap `P(diff>0)=0.960`
- 같은 기준의 `V2 regime-first`: CAGR `31.24%`, Sharpe `1.10`, MDD `-39.93%`,
  QQQ 대비 CAGR diff `+12.28%p`, NW p(two-sided) `0.087`

현재 해석:

- **현재 저장소 기준 가장 강한 deterministic production candidate는 `levered trend`다.**
- `V2`도 여전히 강하지만, `levered trend`가 더 높은 CAGR diff와 더 나은 drawdown, 더 강한 통계 지지를 동시에 보였다.
- 반면 `stock-best`는 여전히 `stock-first` 목표에는 더 잘 맞지만, pure production robustness 기준에선 한 단계 아래다.
- 따라서 프로덕션 라인의 우선순위는 이제
  `levered trend > V2 regime-first > stock-best > simple hybrid`로 보는 게 맞다.

promotion check 결과:

- 다만 `docs/strategy-v2.md`의 승격 기준을 그대로 적용한 promotion check에선
  `V2`는 **통과**했고, `levered trend`는 **3년 horizon NW p(two-sided)=0.332** 때문에 아직 **실패**했다.
- 즉,
  `raw strongest candidate = levered trend`,
  `current rule-based production-ready candidate = V2`
  로 나눠서 보는 게 맞다.

stock-first 구조 실험 결과:

- `veto325`에 `breadth + protective regime exposure`를 뒤에서 덧씌우는 방식도 시험했다.
- 결과는 `regime_off` 원형이 CAGR diff `+5.72%p`, MDD diff `+5.41%p`, turnover `0.215`로 더 좋았고,
  `protective` overlay는 CAGR diff `+4.00%p`, MDD diff `+2.37%p`, turnover `0.225`로 오히려 악화됐다.
- 따라서 stock-first 쪽에 시장 스캔을 붙일 거면, **후행 exposure overlay**가 아니라
  `top_k`나 `entry gate` 자체를 breadth/regime로 조절하는 쪽이 더 맞다.
- 다만 그 방향도 바로 검증해봤는데, `veto325_bgcap2`와 `veto325_bgcap2_tight`는
  strict annual 기준으로 각각 CAGR diff `+1.63%p`, `+0.41%p`에 그쳐
  base `veto325`의 `+2.73%p`를 넘지 못했다.
- 즉, **단순 breadth-based top_k cap도 현재 stock-best를 개선하지 못했다.**
- 섹터 스캔을 더 강하게 써서 `top 3 sectors`로만 제한하는 구조도 검증했지만,
  `sector3`는 annual diff `+1.17%p`, `sector3new`는 `+1.56%p`에 그쳐 역시 base를 못 넘었다.
- 다만 `약한 neutral에서 새 진입만 제한`하는 구조는 조금 달랐다.
  `entryfreeze1`은 strict annual 기준 CAGR diff `+2.73%p`, Sharpe diff `+0.010`, turnover `0.224`로
  base `veto325`의 `+2.73%p`, `+0.004`, `0.230`보다 아주 근소하게 더 좋았다.
- 표준 backtest에서도 `entryfreeze1`은 CAGR `22.52%`, Sharpe `0.994`, turnover `0.211`로
  base `veto325`보다 소폭 개선됐다.
- 즉 stock-first 쪽에서 지금까지 나온 구조 실험 중 **처음으로 살아남은 것은 `weak neutral new-entry freeze`**다.
- `entryfreeze1`에 `sector3new`나 `sector2new`를 겹치는 조합도 시험했지만 다시 악화됐다.
- 따라서 다음 stock-first 개선은 `종목 수를 줄이는 것`보다
  `약한 neutral에서 신규 진입만 막는 state machine` 쪽을 더 깊게 파는 게 맞다.

관련 파일:

- [ai_portfolio_backtest_verification_strategy_v8_levered_trend_best_20260319_recheck.md](/D:/my/autostock/data/runs/ai_portfolio_backtest_verification_strategy_v8_levered_trend_best_20260319_recheck.md)
- [run_strategy_v8_levered_trend_best.py](/D:/my/autostock/scripts/run_strategy_v8_levered_trend_best.py)
- [promotion_check_strategy_v8_levered_trend_best_20260319b.md](/D:/my/autostock/data/runs/promotion_check_strategy_v8_levered_trend_best_20260319b.md)
- [promotion_check_strategy_v2_baseline_20260319b.md](/D:/my/autostock/data/runs/promotion_check_strategy_v2_baseline_20260319b.md)
- [stock_hypothesis_breadth_sweep_veto325_universe_20260319.md](/D:/my/autostock/data/runs/stock_hypothesis_breadth_sweep_veto325_universe_20260319.md)
- [strict_compare_stockfirst_breadth_gate_20260319.md](/D:/my/autostock/data/runs/strict_compare_stockfirst_breadth_gate_20260319.md)
- [strict_compare_stockfirst_sector_focus_20260319.md](/D:/my/autostock/data/runs/strict_compare_stockfirst_sector_focus_20260319.md)
- [strict_compare_stockfirst_entryfreeze_20260319.md](/D:/my/autostock/data/runs/strict_compare_stockfirst_entryfreeze_20260319.md)
- [stock_hypothesis_compare_weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto325_entryfreeze1_vs_weekly_baseline_v4_20260319T045109Z.md](/D:/my/autostock/data/runs/stock_hypothesis_compare_weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto325_entryfreeze1_vs_weekly_baseline_v4_20260319T045109Z.md)

후속 확인:

- best `levered trend` 주변 18개 조합을 좁게 다시 훑었지만,
  `QLD / GLD / MA125 / no confirm / no vix gate`가
  full-window, recent-3y, robust score 모두 여전히 1등이었다.
- 즉 **근처 파라미터 미세조정만으로는 최근 3년 유의성 약점을 해결하지 못했다.**
- 따라서 다음 `levered trend` 개선은 `ma/vix/confirm` 미세조정보다,
  더 구조적인 최근-3y 보강 장치가 필요할 가능성이 크다.

관련 파일:

- [levered_trend_local_sweep_20260319.md](/D:/my/autostock/data/runs/levered_trend_local_sweep_20260319.md)
- [run_strategy_v9_levered_trend_sweep.py](/D:/my/autostock/scripts/run_strategy_v9_levered_trend_sweep.py)
- [ai_portfolio_backtest_verification_strategy_v10_trend_qld_qqq_gld100_targeted_v8_20260319_recheck.md](/D:/my/autostock/data/runs/ai_portfolio_backtest_verification_strategy_v10_trend_qld_qqq_gld100_targeted_v8_20260319_recheck.md)

추가 구조형 후보 확인:

- 과거 구조형 후보 `trend20y_qld_qqq_gld100_targeted_v8`도 현재 비용 조건에서 다시 검증했다.
- 결과는 CAGR `26.31%`, QQQ diff `+7.34%p`, NW p(two-sided) `0.123`, turnover `0.125`였다.
- 최근 3년 약점은 조금 완화됐지만, 여전히 `levered trend best`와 `V2`를 넘지는 못했다.

## 1. 원래 목표

사용자 목표는 그대로 같다.

- `Nasdaq-100` 또는 `S&P500` 안에서 개별 종목을 고른다.
- 장기적으로 `QQQ` 같은 단순 인덱스보다 높은 수익을 원한다.
- 가능하면 손실은 더 작고, 리스크 대비 효율은 더 좋았으면 한다.
- 나중에는 `고수 트레이더처럼 자주 매매해도 수익을 유지`하는 방향까지 확장하고 싶다.

연구 방향도 계속 `stock-first`, 즉 실제 매수 대상을 개별 종목으로 제한하는 쪽을 유지했다.

## 2. 이번 라운드의 핵심 업데이트

이번 라운드에서 새로 확인된 핵심은 아래 5개다.

1. `weekly_baseline_v4`를 기준으로 한 `champion/challenger pipeline`을 만들고 실제로 한 번 돌렸다.
2. `quality`는 메인 랭킹 축으로 쓰면 계속 망가지지만, `새 진입 후보에만 약하게 거는 veto/filter`로는 의미 있는 개선 가능성이 있다.
3. 다만 `fixed OOS`에서 좋아 보인 아이디어가 `annual roll-forward static`에선 쉽게 무너진다. 이 점이 다시 확인됐다.
4. 현재까지 새로 살아남은 최선의 stock-first challenger는 이제 `weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto325_entryfreeze1_pb007`다.
5. 표준 backtest/verification runner와 strict-compare runner를 붙여서 challenger들을 같은 포맷으로 직접 비교할 수 있게 됐고, `risk_off` sleeve를 실제로 열어봐야 `neutral only`와 `neutral+risk_off` 차이가 드러난다는 점도 확인됐다.

## 3. 지금 가장 신뢰해야 하는 산출물

현재 기준으로 가장 먼저 봐야 할 파일은 아래 6개다.

1. 최신 champion/challenger 자동 검증 결과 (`2026-03-13` 컨텍스트에서 재실행)  
- [champion_challenger_summary_champion_challenger_20260313T032512Z.md](/D:/my/autostock/data/runs/champion_challenger_summary_champion_challenger_20260313T032512Z.md)
- [champion_challenger_summary_champion_challenger_20260313T032512Z.json](/D:/my/autostock/data/runs/champion_challenger_summary_champion_challenger_20260313T032512Z.json)
- [champion_challenger_sensitivity_champion_challenger_20260313T032512Z.csv](/D:/my/autostock/data/runs/champion_challenger_sensitivity_champion_challenger_20260313T032512Z.csv)

2. 최신 quality-veto 가설 배치 결과  
- [stock_hypothesis_research_stock_hypothesis_research_20260312_quality_veto_regime.md](/D:/my/autostock/data/runs/stock_hypothesis_research_stock_hypothesis_research_20260312_quality_veto_regime.md)
- [stock_hypothesis_research_stock_hypothesis_research_20260312_quality_veto_regime.json](/D:/my/autostock/data/runs/stock_hypothesis_research_stock_hypothesis_research_20260312_quality_veto_regime.json)

3. `new-entry-only quality veto` 무조건 적용 버전이 엄격 검증에서 탈락한 비교  
- [strict_compare_weekly_baseline_vs_newonly_quality_veto_20260312.json](/D:/my/autostock/data/runs/strict_compare_weekly_baseline_vs_newonly_quality_veto_20260312.json)

4. `neutral/risk_off 조건부 new-entry veto`가 엄격 검증에서 baseline보다 약간 나았던 비교  
- [strict_compare_weekly_baseline_vs_regime_quality_veto_20260312.json](/D:/my/autostock/data/runs/strict_compare_weekly_baseline_vs_regime_quality_veto_20260312.json)

5. 최신 best challenger 표준 backtest/verification/baseline 비교 (`2026-03-13` 컨텍스트에서 추가)  
- [ai_portfolio_backtest_summary_stock_hypothesis_weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto35_20260313T030206Z.json](/D:/my/autostock/data/runs/ai_portfolio_backtest_summary_stock_hypothesis_weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto35_20260313T030206Z.json)
- [ai_portfolio_backtest_verification_stock_hypothesis_weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto35_20260313T030206Z.md](/D:/my/autostock/data/runs/ai_portfolio_backtest_verification_stock_hypothesis_weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto35_20260313T030206Z.md)
- [stock_hypothesis_compare_weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto35_vs_weekly_baseline_v4_20260313T030206Z.md](/D:/my/autostock/data/runs/stock_hypothesis_compare_weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto35_vs_weekly_baseline_v4_20260313T030206Z.md)

6. `neutral only` 표준 비교 결과 (`neutral+risk_off`와 동일 출력 확인)  
- [ai_portfolio_backtest_verification_stock_hypothesis_weekly_veto_recentq_newonly_neutral_soft_bonus_20260313T000115Z.md](/D:/my/autostock/data/runs/ai_portfolio_backtest_verification_stock_hypothesis_weekly_veto_recentq_newonly_neutral_soft_bonus_20260313T000115Z.md)
- [stock_hypothesis_compare_weekly_veto_recentq_newonly_neutral_soft_bonus_vs_weekly_baseline_v4_20260313T000115Z.md](/D:/my/autostock/data/runs/stock_hypothesis_compare_weekly_veto_recentq_newonly_neutral_soft_bonus_vs_weekly_baseline_v4_20260313T000115Z.md)
- [stock_hypothesis_probe_riskoff_sleeve_20260313.md](/D:/my/autostock/data/runs/stock_hypothesis_probe_riskoff_sleeve_20260313.md)

7. 최신 strict annual roll-forward static 재검증 (`ro2` 포함)  
- [strict_compare_weekly_baseline_vs_riskoff_sleeve_probe_20260313_recheck.json](/D:/my/autostock/data/runs/strict_compare_weekly_baseline_vs_riskoff_sleeve_probe_20260313_recheck.json)
- [strict_compare_weekly_baseline_vs_riskoff_sleeve_probe_20260313_recheck.md](/D:/my/autostock/data/runs/strict_compare_weekly_baseline_vs_riskoff_sleeve_probe_20260313_recheck.md)

8. `ro2` 주변 threshold/bonus local sweep  
- [stock_hypothesis_sweep_neutral_ro2_bonus_threshold_20260313.md](/D:/my/autostock/data/runs/stock_hypothesis_sweep_neutral_ro2_bonus_threshold_20260313.md)
- [stock_hypothesis_sweep_neutral_ro2_bonus_threshold_20260313.csv](/D:/my/autostock/data/runs/stock_hypothesis_sweep_neutral_ro2_bonus_threshold_20260313.csv)

해석:

- `fixed OOS`만 보고 판단하면 과대평가될 수 있다.
- `annual roll-forward static`까지 통과한 후보만 실제 challenger로 인정하는 게 맞다.

## 4. 현재 main champion

현재 main champion은 여전히 `weekly_baseline_v4`다.

### 4.1 고정 OOS 기준

`2011-03-01`부터 `2026-03-01`까지의 연구 배치에서, `2016-01-01` 이후 OOS 기준:

- CAGR diff `+2.87%p`
- Sharpe `0.928` vs QQQ `0.915`
- MDD diff `+3.47%p`
- avg turnover `0.225`
- NW p(two-sided) `0.644`

근거:

- [stock_hypothesis_research_20260312T053418Z.json](/D:/my/autostock/data/runs/stock_hypothesis_research_20260312T053418Z.json)

### 4.2 더 엄격한 annual roll-forward static 기준

`2006-03-01`부터 `2026-03-11`까지, 연간 roll-forward static 기준:

- CAGR diff `-0.86%p`
- Sharpe `0.794` vs QQQ `0.915`
- MDD diff `+3.98%p`
- avg turnover `0.242`
- NW p(two-sided) `0.979`

해석:

- 방향성은 여전히 괜찮다.
- 하지만 `strict OOS`에선 아직 benchmark 대비 확실한 우위라고 보기 어렵다.
- 즉 `best current champion`이지 `증명된 전략`은 아니다.

## 5. 새로 살아남은 best challenger

현재 best challenger는 `weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto35`다.

전략 아이디어는 다음과 같다.

- `weekly_baseline_v4`를 유지한다.
- `quality`를 전체 랭킹에 섞지 않는다.
- `최근 filing`이 있는 종목에 대해, `새로 진입하는 후보`에만 약한 quality veto를 건다.
- 이 veto는 `neutral` 구간에서만 켠다.
- `risk_off`에서도 stock sleeve를 완전히 닫지 않고 `top_k_risk_off=2`까지는 허용한다.
- `pit_veto_threshold`는 `-4.0`보다 약간 더 강한 `-3.5`가 현재까지는 더 잘 작동했다.
- 아주 작은 `pit_bonus=0.05`만 유지한다.

### 5.1 고정 OOS 기준

`2011-03-01`부터 `2026-03-01`, `2016-01-01` 이후 OOS 기준:

- CAGR diff `+4.95%p`
- Sharpe `0.990` vs QQQ `0.915`
- MDD diff `+5.41%p`
- avg turnover `0.218`

같은 기준의 champion:

- CAGR diff `+2.87%p`
- Sharpe `0.928`
- MDD diff `+3.47%p`

### 5.2 더 엄격한 annual roll-forward static 기준

`2006-03-01`부터 `2026-03-11`, 연간 roll-forward static 기준:

- challenger CAGR diff `+2.69%p`
- champion CAGR diff `-0.86%p`
- challenger Sharpe `0.916` vs champion `0.794`
- challenger MDD diff `+5.41%p` vs champion `+3.98%p`
- challenger avg turnover `0.2304` vs champion `0.2417`
- challenger NW p(two-sided) `0.530`

해석:

- 이 challenger는 현재까지 본 후보 중 처음으로 `strict annual roll-forward static`에서도 `QQQ 대비 +CAGR diff`를 만들었고, `ro2` base보다도 약간 더 좋다.
- drawdown과 turnover도 champion보다 좋아졌다.
- 다만 Sharpe는 아직 QQQ보다 약간 낮고, 통계적 유의성도 강하진 않다.
- 따라서 `유력한 promotion candidate`지만, 아직 자동 승격보다는 `승격 직전 후보`로 보는 게 안전하다.

## 6. 이번 라운드에서 실패한 아이디어와 교훈

### 6.1 `quality`를 랭킹 엔진으로 쓰는 방식

계속 실패했다.

- `monthly_quality_*`, `weekly_score_*`는 여전히 alpha를 죽였다.
- 결론: `quality`는 메인 알파가 아니라 방어형 보조 정보에 가깝다.

### 6.2 `quality veto`를 모든 후보에 항상 적용

고정 OOS에선 그럴듯해 보여도 엄격 검증에서 무너졌다.

대표 탈락 예:

- `weekly_veto_recentq_newonly_soft_bonus`
- 고정 OOS에선 `+3.56%p`까지 보였지만,
- strict annual roll-forward static에선 CAGR diff `-1.62%p`로 champion보다 오히려 나빴다.

교훈:

- `quality veto`는 항상 켜는 필터가 아니다.
- `new entry only`로 약하게 쓰고,
- 가능하면 `regime-aware`로 제한해야 한다.

### 6.3 `quality veto`를 너무 세게 거는 방식

`weekly_veto_recentq_mid`는 분명히 실패했다.

- OOS CAGR diff `-2.63%p`
- OOS MDD diff `-0.99%p`

교훈:

- 품질 필터가 너무 강하면 momentum 엔진을 망가뜨린다.

### 6.4 `neutral only` vs `neutral+risk_off` 비교를 지금 설정 그대로 보는 방식

이번 컨텍스트에서 표준 runner로 `weekly_veto_recentq_newonly_nrisk_soft_bonus`와
`weekly_veto_recentq_newonly_neutral_soft_bonus`를 각각 baseline과 비교해봤는데,
결과가 사실상 완전히 동일했다.

관찰:

- CAGR `19.19%`, Sharpe `0.882`, MDD `-31.00%`, turnover `0.226`가 둘 다 같았다.
- 연도별 compounded return 테이블도 완전히 같았다.

교훈:

- 두 가설 모두 `top_k_risk_off=0`이라 `risk_off` 구간에서는 애초에 stock sleeve가 없다.
- 그래서 `pit_veto_regimes=("neutral",)`와 `("neutral", "risk_off")` 차이는 현재 설정에선 비활성이다.
- 즉, `neutral only` vs `neutral+risk_off`를 의미 있게 비교하려면 먼저 `risk_off`에서도 stock을 조금이라도 보유하는 실험 축을 열어야 한다.

추가 probe:

- `top_k_risk_off=1`로만 열어도 결과가 여전히 완전히 같았다.
- 이유는 `min_positions_for_invest=2`라서 risk_off에서 1종목만 허용하면 실제 진입이 여전히 막히기 때문이다.
- `top_k_risk_off=2`까지 열면 비로소 차이가 생겼고, 빠른 fixed OOS probe에선 `neutral only` 쪽이 `neutral+risk_off`보다 더 좋게 나왔다.
- 이후 strict compare 재검증에서도 `neutral_soft_bonus_ro2`가 `nrisk_soft_bonus_ro2`보다 fixed OOS와 annual static 둘 다 더 좋았다.
- 즉, 이번 라운드에선 `risk_off에서도 quality veto를 켜는 것`보다 `risk_off sleeve를 소량 허용하되 veto는 neutral에만 거는 것`이 더 낫게 나왔다.
- 그 다음 local sweep에선 `pit_veto_threshold=-3.5`가 `-4.0`보다 annual static을 조금 더 개선했다.

관련 파일:

- [stock_hypothesis_probe_riskoff_sleeve_20260313.md](/D:/my/autostock/data/runs/stock_hypothesis_probe_riskoff_sleeve_20260313.md)

## 7. 이벤트/filing 연구 상태

이 부분은 지난 문서와 큰 결론이 같다.

- `earnings drift long-only`: 현재 구현/데이터 수준에선 실패
- `filing drift long-only`: 현재 구현에선 기각
- 더 진행하려면 `intraday timestamp`, `announcement time`, `premarket/postmarket`, `news timestamp`가 필요하다

관련 파일:

- [event_drift_hypotheses_20260312T072013Z.md](/D:/my/autostock/data/runs/event_drift_hypotheses_20260312T072013Z.md)
- [filing_drift_hypotheses_20260312T072238Z.md](/D:/my/autostock/data/runs/filing_drift_hypotheses_20260312T072238Z.md)

## 8. 이번 라운드에서 추가된 코드/로컬 변경

이번 라운드에서 특히 중요한 연구 관련 변경은 아래다.

- [run_champion_challenger_pipeline.py](/D:/my/autostock/scripts/run_champion_challenger_pipeline.py)
- [run_stock_hypothesis_eval.py](/D:/my/autostock/scripts/run_stock_hypothesis_eval.py)
- [run_stock_hypothesis_strict_compare.py](/D:/my/autostock/scripts/run_stock_hypothesis_strict_compare.py)
- [run_stock_hypothesis_param_sweep.py](/D:/my/autostock/scripts/run_stock_hypothesis_param_sweep.py)
- [run_stock_hypothesis_breadth_sweep.py](/D:/my/autostock/scripts/run_stock_hypothesis_breadth_sweep.py)
- [run_strategy_v6_hybrid_stock_regime.py](/D:/my/autostock/scripts/run_strategy_v6_hybrid_stock_regime.py)
- [run_strategy_v8_levered_trend_best.py](/D:/my/autostock/scripts/run_strategy_v8_levered_trend_best.py)
- [run_strategy_v9_levered_trend_sweep.py](/D:/my/autostock/scripts/run_strategy_v9_levered_trend_sweep.py)
- [run_strategy_promotion_check.py](/D:/my/autostock/scripts/run_strategy_promotion_check.py)
- [champion_challenger_pipeline.py](/D:/my/autostock/src/pipelines/champion_challenger_pipeline.py)
- [backtest_ai_portfolio_selector.py](/D:/my/autostock/scripts/backtest_ai_portfolio_selector.py)
- [research_stock_hypotheses.py](/D:/my/autostock/scripts/research_stock_hypotheses.py)
- [test_ai_portfolio_backtest_script.py](/D:/my/autostock/tests/test_ai_portfolio_backtest_script.py)
- [test_champion_challenger_pipeline.py](/D:/my/autostock/tests/test_champion_challenger_pipeline.py)
- [test_run_champion_challenger_pipeline_runner.py](/D:/my/autostock/tests/test_run_champion_challenger_pipeline_runner.py)
- [test_run_stock_hypothesis_eval.py](/D:/my/autostock/tests/test_run_stock_hypothesis_eval.py)
- [test_run_stock_hypothesis_strict_compare.py](/D:/my/autostock/tests/test_run_stock_hypothesis_strict_compare.py)
- [test_run_stock_hypothesis_param_sweep.py](/D:/my/autostock/tests/test_run_stock_hypothesis_param_sweep.py)
- [test_run_stock_hypothesis_breadth_sweep.py](/D:/my/autostock/tests/test_run_stock_hypothesis_breadth_sweep.py)
- [test_strategy_v6_hybrid_stock_regime_runner.py](/D:/my/autostock/tests/test_strategy_v6_hybrid_stock_regime_runner.py)
- [test_strategy_v8_levered_trend_best_runner.py](/D:/my/autostock/tests/test_strategy_v8_levered_trend_best_runner.py)
- [test_strategy_v9_levered_trend_sweep_runner.py](/D:/my/autostock/tests/test_strategy_v9_levered_trend_sweep_runner.py)
- [test_run_strategy_promotion_check.py](/D:/my/autostock/tests/test_run_strategy_promotion_check.py)

현재 상태 메모:

- 워크트리는 연구 외 변경까지 포함해서 넓게 dirty 상태다.
- 즉, 새 컨텍스트에서 커밋/정리 작업을 하려면 `연구 관련 변경`과 `기존 다른 로컬 변경`을 분리해서 봐야 한다.
- 이번 대화에서 만든 파일과 산출물은 아직 커밋되지 않았다.

## 9. 지금 시점의 판단

현재 판단은 아래와 같다.

- main champion: `weekly_baseline_v4`
- best new challenger: `weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto325_entryfreeze1_pb007`
- `monthly_score_*`: 여전히 challenger지만 승격 불가
- `quality_momentum`: 메인 전략으로는 부적합
- `earnings drift`/`filing drift` long-only: 현 버전에선 기각

즉:

- 지금 저장소/데이터에서 제일 잘 작동하는 축은 여전히 `주간 stock momentum`
- 다만 현재 가장 강한 개선은 `quality를 전체 랭킹에 섞는 것`이 아니라 `neutral 구간의 new-entry veto + 소량 risk_off sleeve`에서 나왔다
- `weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto35`는 현재 가장 강한 `승격 직전` 후보라고 볼 만하다
- 그래도 아직 main champion을 즉시 바꾸기보다는, 마지막 promotion sanity check를 한 번 더 거치는 게 안전하다

## 10. 다음 컨텍스트에서 바로 할 일

우선순위는 아래 순서가 좋다.

### 1순위 (완료)

`2026-03-13` 컨텍스트에서 새 `ro2_veto35` challenger까지 포함해 `champion/challenger pipeline`을 다시 돌렸고, 최신 자동 보고서는 아래 파일이다.

- [champion_challenger_summary_champion_challenger_20260313T032512Z.md](/D:/my/autostock/data/runs/champion_challenger_summary_champion_challenger_20260313T032512Z.md)
- [champion_challenger_summary_champion_challenger_20260313T032512Z.json](/D:/my/autostock/data/runs/champion_challenger_summary_champion_challenger_20260313T032512Z.json)
- [champion_challenger_sensitivity_champion_challenger_20260313T032512Z.csv](/D:/my/autostock/data/runs/champion_challenger_sensitivity_champion_challenger_20260313T032512Z.csv)

자동 판정 결과:

- fixed OOS best candidate: `weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto35`
- fixed OOS 기준 champion retained: `False`
- 즉, 새 `ro2_veto35` challenger가 민감도 그리드 안에서는 champion보다 가장 앞섰다.

다만 이 자동 판정은 `fixed OOS sensitivity ranking` 기준이므로, 승격 판단은 여전히 `strict annual roll-forward static` 비교를 함께 봐야 한다.

### 2순위 (완료)

`weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto35`를 포함해 표준 backtest/verification/baseline 비교와 strict compare, local sweep을 돌릴 수 있는 runner를 추가했고, 실제 산출물도 만들었다.

- [ai_portfolio_backtest_verification_stock_hypothesis_weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto35_20260313T030206Z.md](/D:/my/autostock/data/runs/ai_portfolio_backtest_verification_stock_hypothesis_weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto35_20260313T030206Z.md)
- [stock_hypothesis_compare_weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto35_vs_weekly_baseline_v4_20260313T030206Z.md](/D:/my/autostock/data/runs/stock_hypothesis_compare_weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto35_vs_weekly_baseline_v4_20260313T030206Z.md)
- [strict_compare_weekly_baseline_vs_riskoff_sleeve_probe_20260313_recheck.md](/D:/my/autostock/data/runs/strict_compare_weekly_baseline_vs_riskoff_sleeve_probe_20260313_recheck.md)
- [stock_hypothesis_sweep_neutral_ro2_bonus_threshold_20260313.md](/D:/my/autostock/data/runs/stock_hypothesis_sweep_neutral_ro2_bonus_threshold_20260313.md)

표준 backtest 기준 headline:

- challenger CAGR `22.51%` vs baseline `18.82%`
- challenger Sharpe `0.987` vs baseline `0.865`
- challenger MDD `-29.07%` vs baseline `-31.00%`
- QQQ 대비 CAGR diff는 challenger `+5.75%p`, baseline `+2.05%p`

### 3순위

`weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto35` promotion 여부를 마무리 판단하기

- threshold `-3.5` 주변(`-3.25`, `-3.75`) 미세 조정
- `pit_bonus` `0.05` vs `0.07`의 차이가 실제로 유의미한지 recheck
- `top_k_risk_off=2`를 유지한 채 turnover와 최근 연도(2024-2026) 안정성 재점검
- 충분히 유지되면 main champion 승격 여부를 결정하고, 아니면 `best challenger`로만 고정하기

### 4순위

이벤트 드리븐을 다시 할 거면 daily-bar 연구가 아니라 데이터 인프라부터 바꾸기

- 1분봉/5분봉
- earnings announcement time
- pre/post market reaction
- news timestamp

## 11. 새 컨텍스트 첫 메시지로 쓰기 좋은 문장

아래 문장을 그대로 시작 메시지로 써도 된다.

```text
이 저장소 연구는 docs/reports/research_handoff_20260313.md 기준으로 이어가자.
main champion은 아직 weekly_baseline_v4로 두고 있지만, best new challenger는 이제 weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto35다.
이 후보는 strict annual roll-forward static에서도 CAGR diff가 +2.69%p로 현재까지 가장 좋았고, MDD와 turnover도 baseline보다 좋아졌다.
다만 통계적 유의성은 아직 강하지 않아서, 이번 컨텍스트에서는 promotion sanity check를 한 번 더 하거나 `-3.5` 주변 민감도를 점검하자.
```

## 12. 한 줄 요약

`quality`는 메인 랭킹 엔진으로는 실패했지만,  
`neutral/risk_off 구간의 new-entry-only veto`로 쓰면 `weekly_baseline_v4`를 약간 개선하는 challenger는 만들 수 있었다.  
다만 아직 `strict OOS` 기준으로도 확실한 승격 사유는 없다.
