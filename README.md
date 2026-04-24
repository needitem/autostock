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
    earnings_pit.py
    event_watchlist.py
    indicators.py
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
- `/chart`
- `/journal`
- `/refresh`
- `/help`
- `/menu`

자유 질문도 가능합니다.
- `지금 들어갈만한 종목 알려줘`
- `현재 차트 분석해줘`
- `기록 평가해줘`
- `전체 후보 다시 분석해`
- `all_us 전체 돌려줘`

앱 UI:
- 시작 시 inline menu 제공
- Telegram 하단 고정 키보드 제공: `차트 분석 / 뉴스+차트 분석 / 전체 정밀 분석 / 기록 평가 / 새로고침 / 도움말`
- Telegram 메뉴 명령 등록: `/menu`, `/chart`, `/trade`, `/tradefull`, `/journal`, `/help`
- 저장된 채팅방이 있으면 봇 시작 시 메뉴와 하단 고정 키보드를 1회 자동 전송
- 결과 화면에서는 inline 버튼으로 `요약 / 즉시 매수 / 눌림 대기 / 제외 / 포트폴리오 / 새로고침` 전환
- 결과는 카드형 요약으로 편집 갱신
- 긴 분석은 background worker에서 1개씩 실행되어 polling/menu 응답이 막히지 않습니다.
- `/trade`와 `/tradefull`은 전체 유니버스 뉴스/SEC를 먼저 훑고, 촉매 후보를 고른 뒤 차트/진입가를 검증합니다.
- `/trade`와 `/tradefull`은 종목별 뉴스 해석 뒤 `gpt-5.5` + `xhigh` 최종 종합 단계를 한 번 더 실행해 전체 후보를 서로 비교합니다.

봇은 최신 `rebalance_recommendation_*.json` 후보군 전체를 기준으로
- all_us 우량주 유니버스 전체 뉴스/SEC/RSS 탐색
- 촉매 후보 fresh SEC/RSS 뉴스 + Codex 정밀 해석
- 뉴스 촉매 후보의 차트/진입가 검증
- 전체 후보 간 촉매/진입품질/리스크 상대 비교
- 진입가/손절/1차 목표가
- 현재가 대비 RR
- 포트폴리오 비중
을 다시 계산해 답합니다.
휴리스틱 fallback은 없습니다. `codex login`이 안 되어 있거나 Codex 이벤트 해석이 실패하면 분석 불가로 응답합니다.

`/chart`는 뉴스/Codex 없이 현재 차트만 전수 스캔합니다. 별도 임의 점수는 만들지 않고 `진입가 근접`, `TP1 여력`, `RR`, `RSI`, `거래량`, `과열/추격 경고` 같은 조건 통과 여부로 분류합니다.

`/chart`, `/trade`, `/tradefull` 결과는 `outputs/telegram/shadow_journal.jsonl` 에 자동 기록됩니다. `/journal`은 이후 일봉으로 `지정가 체결 가능`, `TP1 도달`, `손절`, `미체결`, `평가 대기`를 보수적으로 평가하고 `outputs/telegram/shadow_journal_eval.json` 에 저장합니다.

봇이 켜져 있으면 `TELEGRAM_AUTO_COLLECT_INTERVAL_MINUTES` 주기로 최신 일봉을 확인하고, 아직 기록하지 않은 기준일이면 `/chart`와 같은 차트 수집을 자동 실행합니다. 사용자가 봇에 `/start`나 아무 메시지를 한 번 보낸 뒤에는 자동 수집 완료 알림도 전송합니다.

## Notes

- 현재 기본 프로필은 `configs/event_profiles/tsla.json` 입니다.
- Telegram snapshot은 `outputs/telegram/snapshot.json` 으로 export 됩니다.
- Telegram bot 상태는 `outputs/telegram/bot_state.json` 에 저장됩니다.
- Telegram 추천 추적 기록은 `outputs/telegram/shadow_journal.jsonl` 에 저장됩니다.
- 자동 차트 수집은 기본 활성화이며 `.env`에서 `TELEGRAM_AUTO_COLLECT_ENABLED=false` 로 끌 수 있습니다.
- 시작 시 메뉴 자동 전송은 기본 활성화이며 `.env`에서 `TELEGRAM_STARTUP_MENU_PUSH_ENABLED=false` 로 끌 수 있습니다.
- 기본 동작은 `all_us` 전체 뉴스/SEC/RSS 탐색 후 촉매 상위 `120`개 뉴스/Codex 정밀분석입니다.
- 기본 Codex 모델은 `gpt-5.5`, reasoning effort는 `xhigh`입니다.
- 뉴스 탐색 범위는 기본 전체 유니버스이며 `.env`에서 `TELEGRAM_NEWS_DISCOVERY_MAX_SYMBOLS`로 제한할 수 있습니다.
- 최종 종합 단계 후보 수는 기본 `240`개이며 `.env`에서 `TELEGRAM_FINAL_SYNTHESIS_MAX_SYMBOLS`로 조정할 수 있습니다.
- `--signal`, `--runtime`, `--nautilus-bundle`, `--all`, `--telegram-bot` 은 모델 기반 이벤트 해석 때문에 `codex login` 상태를 전제로 합니다.
- 이 저장소에는 별도 테스트 스위트가 없습니다. 검증은 수동 실행과 스모크 체크 기준입니다.
