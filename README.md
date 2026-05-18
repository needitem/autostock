# Autostock Nautilus

이 저장소는 이제 **TSLA 이벤트 런타임 + Nautilus bundle/backtest + 로컬 Telegram 질의 응답** 중심의 최소 앱입니다.

핵심 흐름:
- `src/main.py --runtime --profile tsla`
  이벤트/뉴스/차트/5분 거래량 기반 런타임 갱신
- `src/main.py --nautilus-bundle --profile tsla`
  최신 런타임 결과를 Nautilus 입력 번들로 export
- `src/main.py --nautilus-backtest --profile tsla`
  최신 번들 기준 Nautilus backtest 실행
- `src/main.py --telegram-export --profile tsla`
  로컬 Telegram 제어용 snapshot 생성
- `src/main.py --telegram-bot`
  로컬 Telegram polling bot 실행. 질문 시 전체 후보를 차트+뉴스 기준으로 재분석

## Entry Points

```bash
python src/main.py --signal --profile tsla
python src/main.py --runtime --profile tsla
python src/main.py --nautilus-bundle --profile tsla
python src/main.py --nautilus-backtest --profile tsla
python src/main.py --telegram-export --profile tsla
python src/main.py --telegram-bot
python src/main.py --all --profile tsla
```

인자 없이 실행하면 도움말만 출력합니다.

## Layout

```text
src/
  ai/
    analyzer.py
  core/
    chart_structure.py
    data_collector.py
    earnings_pit.py
    event_watchlist.py
    indicators.py
    market_regime.py
    news_collectors.py
    sec_pit.py
    stock_data.py
  event_runtime/
  nautilus_v2/
  pipelines/
    autostock_v2_pipeline.py
  event_profile.py
  main.py
scripts/
  export_telegram_snapshot.py
  export_nautilus_tsla_inputs.py
  build_nautilus_tsla_run_config.py
  run_event_runtime.py
  run_nautilus_tsla_backtest.py
configs/
  event_profiles/
  event_rules/
```

## Telegram

로컬에서 `TELEGRAM_BOT_TOKEN` 기준으로 polling bot을 돌립니다.

기본 명령:
- `/trade`
- `/tradefull`
- `/journal`
- `/refresh`
- `/help`
- `/menu`

자유 질문도 가능합니다.
- `지금 들어갈만한 종목 알려줘`
- `기록 평가해줘`
- `전체 후보 다시 분석해`
- `all_us 전체 돌려줘`

앱 UI:
- Telegram 하단 고정 키보드 제공: `뉴스+차트 분석 / 전체 정밀 분석 / 기록 평가 / 새로고침 / 도움말`
- Telegram 메뉴 명령 등록: `/menu`, `/trade`, `/tradefull`, `/journal`, `/help`
- 저장된 채팅방이 있으면 봇 시작 시 메뉴와 하단 고정 키보드를 1회 자동 전송
- 결과 화면에는 별도 메시지 버튼을 붙이지 않고 하단 고정 키보드만 유지
- 결과는 요약 중심 메시지로 전송
- 긴 분석은 background worker에서 1개씩 실행되어 polling/menu 응답이 막히지 않습니다.
- `/trade`와 `/tradefull`은 전체 유니버스 재무 스냅샷을 먼저 훑고, Codex가 깊게 볼 종목을 고른 뒤 뉴스/SEC/차트/진입가를 수집합니다.
- `/trade`와 `/tradefull`은 종목별 뉴스 해석 뒤 `gpt-5.5` + `xhigh` 최종 종합 단계를 한 번 더 실행해 후보를 서로 비교합니다.

봇은 최신 `rebalance_recommendation_*.json` 후보군 전체를 기준으로
- all_us 우량주 유니버스 전체 재무 스냅샷
- Codex 기반 심층 수집 대상 선정
- 선정 후보의 fresh SEC/RSS 뉴스 + Codex 정밀 해석
- 선정 후보의 차트/진입가 검증
- 전체 후보 간 촉매/진입품질/RR/상대강도/유동성/시장상태 비교
- 편입 후보만 별도 리스크 리뷰로 재검토
- 진입가/손절/1차 목표가
- 현재가 대비 RR
- 포트폴리오 비중
을 다시 계산해 답합니다.
휴리스틱 fallback은 없습니다. `codex login`이 안 되어 있거나 Codex 후보 선정/이벤트 해석/최종 종합/리스크 리뷰가 실패하면 분석 불가로 응답합니다.

`/trade`와 `/tradefull`의 편입 후보는 단순 시총순이 아니고, 코드에 박힌 점수/임계값으로 통과시키지도 않습니다. 재무, 밸류에이션, 성장, 차트/진입 구조, RR, QQQ 대비 상대강도, 유동성, 뉴스/SEC, 숏볼륨, FRED 매크로, Cboe put/call, 시장 상태를 근거로 넘긴 뒤 Codex 최종 종합이 `편입 / 대기 / 참고 / 제외`를 판단합니다. 근거가 애매하면 편입 후보를 0개로 둘 수 있습니다.

데이터 수집은 `core.data_collector.DataCollector`가 조율합니다. 차트의 지지/저항 zone, 스윙 고점/저점, 추세선, 이탈/리테스트 근거는 `core.chart_structure.ChartStructureCollector`가 만들고, QQQ/SPY/IWM 같은 벤치마크 기반 급락장/시장 국면 근거는 `core.market_regime.MarketRegimeCollector`가 만듭니다. 기존 Yahoo Finance 가격/재무, Massive/Polygon 실시간 거래량, Google RSS/SEC 이벤트, 시장 상태, 공포탐욕 수집도 유지하며, 추가로 Cboe 옵션 put/call 요약, FINRA Reg SHO 일별 short-sale volume, FRED 매크로 시계열 훅을 제공합니다. FRED는 `FRED_API_KEY`가 있을 때만 활성화되며, 실패한 외부 데이터는 분석을 중단하지 않고 `unavailable` 상태로 기록합니다.

`/trade`, `/tradefull` 결과는 `outputs/telegram/shadow_journal.jsonl` 에 자동 기록됩니다. `/journal`은 이후 일봉으로 `지정가 체결 가능`, `TP1 도달`, `손절`, `미체결`, `평가 대기`를 보수적으로 평가하고 `outputs/telegram/shadow_journal_eval.json` 에 저장합니다.

## Notes

- 현재 기본 프로필은 `configs/event_profiles/tsla.json` 입니다.
- Telegram snapshot은 `outputs/telegram/snapshot.json` 으로 export 됩니다.
- Telegram bot 상태는 `outputs/telegram/bot_state.json` 에 저장됩니다.
- Telegram 추천 추적 기록은 `outputs/telegram/shadow_journal.jsonl` 에 저장됩니다.
- 시작 시 메뉴 자동 전송은 기본 활성화이며 `.env`에서 `TELEGRAM_STARTUP_MENU_PUSH_ENABLED=false` 로 끌 수 있습니다.
- 기본 동작은 `all_us` 전체 재무 스냅샷 후 Codex가 고른 후보를 뉴스/SEC/RSS/차트/Codex로 정밀분석합니다.
- 기본 Codex 모델은 `gpt-5.5`, reasoning effort는 `xhigh`입니다.
- 빠른 분석 후보 수는 기본 `120`개이며 `.env`에서 `TELEGRAM_RESEARCH_ANALYSIS_MAX_SYMBOLS`로 조정할 수 있습니다.
- 최종 종합 단계 후보 수는 기본 `240`개이며 `.env`에서 `TELEGRAM_FINAL_SYNTHESIS_MAX_SYMBOLS`로 조정할 수 있습니다.
- `--signal`, `--runtime`, `--nautilus-bundle`, `--all`, `--telegram-bot` 은 모델 기반 이벤트 해석 때문에 `codex login` 상태를 전제로 합니다.
- 이 저장소에는 별도 테스트 스위트가 없습니다. 검증은 수동 실행과 스모크 체크 기준입니다.
