# Nautilus Runtime

이 디렉터리는 이제 Autostock의 보조 레인이 아니라 **핵심 실행 엔진**입니다.

현재 구조:
- 이벤트/뉴스 수집과 차트 확인은 Python 런타임이 수행
- 그 결과를 Nautilus 입력 번들로 export
- 최신 번들을 Nautilus backtest로 재생
- Telegram Worker는 이 runtime + Nautilus 요약만 표시

## Commands

루트 기준:

```bash
python src/main.py --runtime --profile tsla
python src/main.py --nautilus-bundle --profile tsla
python src/main.py --nautilus-backtest --profile tsla
python src/main.py --telegram-export --profile tsla
python src/main.py --all --profile tsla
```

## Data

생성 산출물:

```text
data/nautilus_v2/<profile>/<date>/
  tsla_news_events.jsonl
  tsla_macro_events.jsonl
  tsla_signal_snapshot.json
  tsla_bars.csv
  tsla_nautilus_backtest_summary.json
```

## Package

이 레인은 별도 Python 패키지로 다음만 포함합니다.
- `bridge.py`
- `loader.py`
- `models.py`
- `strategy.py`
- `backtest.py`
- `config_builders.py`

현재 기본 의존성은 [nautilus_v2/requirements.txt](</D:/my/autostock/nautilus_v2/requirements.txt>) 에 있습니다.
