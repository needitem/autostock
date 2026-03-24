# Autostock Research Handoff

작성일: 2026-03-12  
목적: 새 대화에서 바로 이어서 연구할 수 있도록, 지금까지의 전략 가설/검증 결과/결론/다음 우선순위를 한 문서에 정리한다.

## 1. 원래 목표

사용자 목표는 다음과 같았다.

- `Nasdaq-100` 또는 `S&P500` 안에서 개별 종목을 고른다.
- 장기적으로 `QQQ` 같은 단순 인덱스보다 높은 수익을 원한다.
- 가능하면 손실은 더 작고, 리스크 대비 효율은 더 좋았으면 한다.
- 나중에는 `고수 트레이더처럼 자주 매매해도 수익을 유지`하는 방향까지 확장하고 싶다.

중요한 방향 전환이 하나 있었다.

- 초반에 잘 나온 것은 `ETF regime rotation`이었지만, 이건 사용자 목표인 `개별 종목 매수`와 다르다.
- 이후 연구는 의도적으로 `stock-first`, 즉 실제 매수 대상을 개별 종목으로 제한하는 쪽으로 다시 설계했다.

## 2. 지금까지의 큰 결론

현재 가장 중요한 결론은 아래 4개다.

1. `주간 stock_momentum` 계열이 아직 가장 유망하다.
2. `월간 quality-heavy` 전략은 드로우다운은 줄이지만, 장기 초과수익은 약하다.
3. `earnings event`와 `filing drift` 기반 단기 long-only 전략은 현재 데이터/구현 수준에선 실패했다.
4. 지금 저장소 기준으로는 `빠른 트레이딩 알파`보다 `중기 종목 선택 + 리스크 완화` 쪽이 더 잘 작동한다.

## 3. 가장 신뢰해야 하는 결과

가장 신뢰해야 하는 산출물은 아래 2개다.

- 주간/월간/quality 가설을 같은 기준으로 비교한 배치:
  - [stock_hypothesis_research_20260312T053418Z.json](/D:/my/autostock/data/runs/stock_hypothesis_research_20260312T053418Z.json)
  - [stock_hypothesis_research_20260312T053418Z.md](/D:/my/autostock/data/runs/stock_hypothesis_research_20260312T053418Z.md)
- 더 엄격한 기존 walk-forward 결과:
  - [walkforward_stock_momentum_summary_20260311T091315Z.json](/D:/my/autostock/data/runs/walkforward_stock_momentum_summary_20260311T091315Z.json)

이 두 결과를 같이 보면, 단일 백테스트가 좋아 보이더라도 OOS에서 다 무너질 수 있다는 점이 확인된다.

## 4. 현재 champion

현재 champion은 `weekly_baseline_v4`다.

근거 파일:

- [stock_hypothesis_research_20260312T053418Z.json](/D:/my/autostock/data/runs/stock_hypothesis_research_20260312T053418Z.json)

핵심 수치:

- Full sample
  - CAGR `18.92%`
  - QQQ CAGR `16.89%`
  - CAGR diff `+2.03%p`
  - Sharpe `0.868` vs QQQ `0.881`
  - MDD `-31.00%` vs QQQ `-34.47%`
  - MDD diff `+3.47%p`
  - avg turnover `0.226`
  - NW p(two-sided) `0.655`
- OOS proxy (`2016+`)
  - CAGR `21.20%`
  - QQQ CAGR `18.33%`
  - CAGR diff `+2.87%p`
  - Sharpe `0.928` vs QQQ `0.915`
  - MDD diff `+3.47%p`
  - avg turnover `0.225`
  - NW p(two-sided) `0.644`

해석:

- 방향성은 좋다.
- 수익과 드로우다운 모두 nominal하게 benchmark보다 낫다.
- 하지만 통계적으로 강하다고 말할 수는 없다.
- 즉 `현재 최선의 연구 후보`이지 `증명된 전략`은 아니다.

## 5. 월간 stock/quality 가설 결과

### 5.1 `monthly_score_noq`

- Full diff `-0.10%p`
- OOS diff `-1.65%p`
- OOS MDD diff `+13.18%p`

해석:

- 수익은 거의 QQQ 수준까지 접근했지만 결국 OOS에서 밀린다.
- 대신 낙폭 완화는 매우 강하다.
- `리스크 절감형` 성격은 분명하다.

### 5.2 `monthly_score_lightq`

관련 파일:

- [ai_portfolio_backtest_summary_strategy_v5_monthly_stock_momo_lightq.json](/D:/my/autostock/data/runs/ai_portfolio_backtest_summary_strategy_v5_monthly_stock_momo_lightq.json)
- [ai_portfolio_backtest_verification_strategy_v5_monthly_stock_momo_lightq.json](/D:/my/autostock/data/runs/ai_portfolio_backtest_verification_strategy_v5_monthly_stock_momo_lightq.json)
- [fixed_oos_monthly_stock_momo_lightq_2016_2026.md](/D:/my/autostock/data/runs/fixed_oos_monthly_stock_momo_lightq_2016_2026.md)

핵심 수치:

- Full sample
  - CAGR diff `+0.05%p`
  - Sharpe `1.051` vs QQQ `1.007`
  - MDD diff `+13.18%p`
  - avg turnover `0.481`
  - NW p(two-sided) `0.963`
- Fixed OOS (`2016-2026`)
  - CAGR diff `-3.60%p`
  - Sharpe `0.84` vs QQQ `0.98`
  - MDD diff `+12.38%p`

해석:

- 전체 구간만 보면 꽤 좋아 보인다.
- 하지만 OOS로 자르면 수익 우위가 사라진다.
- `quality를 약하게 보너스로 섞는 것`은 방어력은 주지만 초과수익은 아직 못 만든다.

### 5.3 `quality_momentum` 계열

관련 파일:

- [ai_portfolio_backtest_summary_strategy_v5_quality_momentum_eval.json](/D:/my/autostock/data/runs/ai_portfolio_backtest_summary_strategy_v5_quality_momentum_eval.json)
- [ai_portfolio_backtest_verification_strategy_v5_quality_momentum_eval.json](/D:/my/autostock/data/runs/ai_portfolio_backtest_verification_strategy_v5_quality_momentum_eval.json)

핵심 수치:

- Full sample
  - CAGR `12.14%`
  - QQQ CAGR `16.98%`
  - CAGR diff `-4.84%p`
  - MDD diff `+11.43%p` 내외
  - NW p(two-sided) `0.188`

해석:

- `quality`를 메인 랭킹 축으로 올리면 수익이 급격히 죽는다.
- 이 전략군은 현재 기준으론 메인 알파가 아니라 방어형 필터에 가깝다.

## 6. 더 엄격한 walk-forward 결과

관련 파일:

- [walkforward_stock_momentum_summary_20260311T091315Z.json](/D:/my/autostock/data/runs/walkforward_stock_momentum_summary_20260311T091315Z.json)

핵심 수치:

- OOS selected
  - CAGR `11.17%`
  - QQQ CAGR `17.52%`
  - CAGR diff `-6.35%p`
  - Sharpe `0.607` vs `0.915`
  - MDD diff `-0.31%p`
- OOS static baseline
  - CAGR `16.67%`
  - QQQ CAGR `17.52%`
  - CAGR diff `-0.86%p`
  - Sharpe `0.794` vs `0.915`
  - MDD diff `+3.98%p`

해석:

- 튜닝된 selected config는 과최적화 가능성이 높다.
- 고정 baseline은 benchmark에 더 가깝지만, 그래도 확실한 우위는 아니다.
- 앞으로는 `selected 결과`보다 `고정 규칙 OOS`를 더 믿는 게 맞다.

## 7. 단기 이벤트 전략 연구

### 7.1 Earnings drift 가설

관련 파일:

- [event_drift_hypotheses_20260312T072013Z.json](/D:/my/autostock/data/runs/event_drift_hypotheses_20260312T072013Z.json)
- [event_drift_hypotheses_20260312T072013Z.md](/D:/my/autostock/data/runs/event_drift_hypotheses_20260312T072013Z.md)

입력 메타:

- event rows `71`
- cached earnings symbols `267`

best hypothesis:

- `pead_high_volume_5d`
  - OOS CAGR diff `-15.35%p`
  - OOS Sharpe `0.51` vs QQQ `0.87`
  - OOS MDD diff `+28.76%p`
  - trades `15`

해석:

- 데이터는 연결됐지만 표본이 너무 적다.
- 이벤트 시각/장전 vs 장후 구분이 없어서 셋업 정밀도가 낮다.
- 현재 구조에선 `earnings event long-only`는 수익 전략이 아니라 초저노출 방어형 잡음 수준이다.

### 7.2 Filing drift 가설

관련 파일:

- [filing_drift_hypotheses_20260312T072238Z.json](/D:/my/autostock/data/runs/filing_drift_hypotheses_20260312T072238Z.json)
- [filing_drift_hypotheses_20260312T072238Z.md](/D:/my/autostock/data/runs/filing_drift_hypotheses_20260312T072238Z.md)

입력 메타:

- filing rows `5157`
- companyfacts loaded `185`

best hypothesis:

- `filing_growth_cont_5d`
  - OOS CAGR diff `-12.41%p`
  - OOS Sharpe `0.40` vs QQQ `0.82`
  - OOS MDD diff `+14.64%p`
  - trades `252`

해석:

- 표본은 충분했지만 전략 성과가 나쁘다.
- `filing date 후 drift`를 단순 long-only continuation으로 먹는 가설은 현재 구현에선 기각하는 게 맞다.

## 8. 연구 중 확인된 기술적 이슈와 수정

### 이미 넣은 중요한 수정

1. `by-date` 유니버스를 `as-of` 조회로 바꿈
- 월말 스냅샷이 주간 유니버스 날짜와 정확히 안 맞을 때도 가장 최근 시점의 실제 유니버스를 쓰도록 수정했다.
- 위치: [backtest_ai_portfolio_selector.py](/D:/my/autostock/scripts/backtest_ai_portfolio_selector.py)

2. `return_126d`, `ma200_trend_pct` 등 중기 팩터 추가
- 월간 quality/momentum 실험용.

3. `earnings_cache` 빈 캐시 재사용 버그 수정
- 비어 있는 캐시를 무조건 신뢰하지 않고, 실제 `reported_eps`가 있는 캐시만 바로 재사용하도록 수정했다.
- 위치: [earnings_pit.py](/D:/my/autostock/src/core/earnings_pit.py)

4. `earnings_date` timezone 정규화 버그 수정
- 이벤트가 0건으로 떨어지던 핵심 원인이었다.

### 현재 로컬 변경 상태

아래 파일은 아직 커밋되지 않았다.

- [backtest_ai_portfolio_selector.py](/D:/my/autostock/scripts/backtest_ai_portfolio_selector.py)
- [earnings_pit.py](/D:/my/autostock/src/core/earnings_pit.py)
- [sec_pit.py](/D:/my/autostock/src/core/sec_pit.py)
- [run_strategy_v5_quality_momentum.py](/D:/my/autostock/scripts/run_strategy_v5_quality_momentum.py)
- [walkforward_quality_momentum.py](/D:/my/autostock/scripts/walkforward_quality_momentum.py)
- [research_stock_hypotheses.py](/D:/my/autostock/scripts/research_stock_hypotheses.py)
- [research_event_drift_hypotheses.py](/D:/my/autostock/scripts/research_event_drift_hypotheses.py)
- [research_filing_drift_hypotheses.py](/D:/my/autostock/scripts/research_filing_drift_hypotheses.py)
- [test_ai_portfolio_backtest_script.py](/D:/my/autostock/tests/test_ai_portfolio_backtest_script.py)
- [test_strategy_v5_quality_momentum_runner.py](/D:/my/autostock/tests/test_strategy_v5_quality_momentum_runner.py)

## 9. 지금 시점의 판단

현재 판단은 아래와 같다.

- 메인 champion은 계속 `weekly_baseline_v4`
- `monthly_score_*` 계열은 challenger지만 승격 불가
- `quality_momentum` 계열은 방어형 보조 아이디어로만 보류
- `earnings drift`, `filing drift` long-only는 현재 버전 기준 기각

즉,

- `고수 트레이더처럼 자주 매매`에 가까운 전략은 아직 못 찾았다.
- 지금 저장소/데이터로 먹히는 것은 `빠른 이벤트 매매`보다 `주간 종목 회전`이다.

## 10. 다음 대화에서 바로 시작할 우선순위

새 대화에선 아래 우선순위로 시작하는 것이 좋다.

### 1순위

`weekly_baseline_v4`를 champion으로 고정하고,

- parameter freeze
- fixed OOS window
- annual roll-forward
- sector/turnover constraint sensitivity

를 자동 평가하는 `champion/challenger pipeline` 만들기

### 2순위

`true short-term trading`을 하고 싶다면 데이터 인프라부터 바꾸기

- 5분봉 또는 1분봉
- earnings announcement time
- premarket/postmarket reaction
- 뉴스 시각

없이는 현재처럼 daily-bar 기반으로는 한계가 크다.

### 3순위

event-driven을 계속할 거면 `long-only continuation`이 아니라 아래를 시험

- intraday breakout after earnings
- same-day fade/reclaim
- overnight gap continuation with time-of-day filters
- market-neutral long/short basket

## 11. 새 대화에 붙여넣기 좋은 시작 문장

아래 문장을 그대로 새 대화 첫 메시지로 써도 된다.

```text
이 저장소에서 지금까지의 연구는 docs/reports/research_handoff_20260312.md 기준으로 이어가자.
현재 champion은 weekly_baseline_v4이고, monthly quality/event/filing drift는 아직 실패한 상태다.
이번 대화에서는 champion/challenger 자동 검증 파이프라인부터 만들거나, intraday event 데이터 구조 설계로 넘어가자.
```

## 12. 한 줄 요약

지금까지의 연구에서 살아남은 건 `주간 stock_momentum`뿐이고,  
`월간 quality-heavy`와 `단기 event-driven long-only`는 아직 수익 알파를 증명하지 못했다.
