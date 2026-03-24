# Weekly Regime Allocator 연구 (2026-03-05)

기존 Codex 주간 리밸런싱(AI 직접판단) 대비, **로직을 새로 작성**해서 ETF 레짐 로테이션 전략을 연구/검증했다.
핵심 구조: `QQQ 추세/모멘텀으로 레짐 판정 -> 레짐별 ETF 바구니 배분`, 주간 리밸런싱(next-open), 턴오버 비용 20bps 반영.

## 새 로직 파일

- `scripts/research_weekly_regime_allocator.py`
  - 그리드 탐색(5,880개 설정) + 베스트 선택
  - 1~10년 horizon 의미성(Loose/Strict) 자동 산출
  - 연도별 성과/유의성 산출

## 프로파일 비교 (10년, 2016-03~2026-03, 주간)

```text
       profile  cagr_diff_pctp  mdd_diff_pctp  nw_t  nw_p_two  nw_p_gt0  loose10  strict10  year_win
new_aggressive          17.666        -11.031 1.988     0.047     0.977        9         8         9
  new_balanced          10.777         -0.527 1.529     0.126     0.937        9         7         9
 new_defensive           9.915          1.838 1.599     0.110     0.945        6         3         7
   old_ai_best           5.178        -20.454 1.297     0.195     0.903        6         2         6
```

## 추천 프로파일

### 1) Aggressive (수익 우선)
- config: `ma_fast=100, ma_slow=200, mom_lb=21, mom_thr=0, risk_on=TQQQ, neutral=QLD, risk_off=GLD`
- CAGR diff: **+17.67%p**, MDD diff: **-11.03%p**
- NW p(two): **0.047**, P(alpha>0): **0.977**
- Horizon pass: Loose **9/10**, Strict **8/10**, Year win **9**

### 2) Balanced (안정성 우선)
- config: `ma_fast=50, ma_slow=200, mom_lb=21, mom_thr=0, risk_on=QLD, neutral=QLD, risk_off=GLD`
- CAGR diff: **+10.78%p**, MDD diff: **-0.53%p**
- NW p(two): **0.126**, P(alpha>0): **0.937**
- Horizon pass: Loose **9/10**, Strict **7/10**, Year win **9**

### 3) Defensive (낙폭 방어 우선)
- config: `ma_fast=100, ma_slow=200, mom_lb=63, mom_thr=-0.02, risk_on=UPRO, neutral=TLT, risk_off=GLD`
- CAGR diff: **+9.92%p**, MDD diff: **+1.84%p**
- NW p(two): **0.110**, P(alpha>0): **0.945**
- Horizon pass: Loose **6/10**, Strict **3/10**, Year win **7**

## 실행 커맨드 예시

```bash
# 전체 그리드 탐색
python3 scripts/research_weekly_regime_allocator.py

# 밸런스형 단일 실행
RR_GRID_SEARCH=0 RR_MA_FAST=50 RR_MA_SLOW=200 RR_MOM_LB=21 RR_MOM_THR=0 \
RR_RISK_ON=QLD RR_NEUTRAL=QLD RR_RISK_OFF=GLD RR_TRADE_COST_BPS=20 \
python3 scripts/research_weekly_regime_allocator.py
```

## 결론

- 새 로직은 기존 AI 주간 로직 대비 장기 일관성(Loose/Strict horizon pass, year_win)에서 개선됨.
- 다만 “모든 연도 완전 유의”는 여전히 어려움(특히 2016, 2019 같은 특정 구간).
- 다음 단계는 이 레짐 로직을 실운영 리밸런서에 옵션 모드로 연결하고, AI는 레짐 내 미세 가중치 조정용으로 축소 적용하는 것이 합리적임.
