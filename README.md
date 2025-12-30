# 나스닥 100 주식 스캐너

나스닥 100 종목을 분석해서 매수 신호를 텔레그램으로 알려주는 시스템.

## 설치

```bash
pip install -r requirements.txt
```

## 설정

```bash
cd src

# 1. 봇 실행
python telegram_bot.py

# 2. 텔레그램에서 봇에게 /start 보내기
# 3. "등록 완료" 메시지 확인 후 Ctrl+C로 종료
```

## 실행

```bash
cd src

# 한 번 실행 (테스트)
python main.py

# 스케줄러 (매일 22:00 자동 실행)
python main.py --schedule
```

## 전략

- 시장 상태 확인 (QQQ 기준)
- 50일/200일 이동평균선 위에 있는 종목
- RSI 40~60 (과열/과매도 아닌 구간)
- 거래량 평균 이상
- 손절 -7%, 익절 +10%
