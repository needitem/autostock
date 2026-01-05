# 나스닥 100 주식 스캐너

나스닥 100 종목을 분석해서 매수 신호를 텔레그램으로 알려주는 시스템.

## 기능

### 📊 기본 분석
- 나스닥 100 전체 스캔
- 7가지 매매 전략 (보수적 모멘텀, 골든크로스, 볼린저 반등 등)
- 위험도 분석 (0-100점)
- AI 기반 종목/뉴스 분석 (Groq Llama 3)

### 🌐 외부 데이터 소스 (NEW!)
| 사이트 | 제공 정보 |
|--------|----------|
| **CNN Fear & Greed** | 공포탐욕 지수 (0-100) |
| **Finviz** | 섹터 성과, 밸류에이션, 기술적 지표 |
| **TipRanks** | 애널리스트 컨센서스, 목표가 |
| **Seeking Alpha** | 퀀트 레이팅 |
| **Finnhub** | 뉴스, 실적 일정, 내부자 거래 |

### 📱 텔레그램 봇 메뉴
- 🌟 추천: 저위험 + 전략 매칭 종목
- 🔍 스캔: 전체 종목 스캔
- 📊 종목분석: 개별 종목 상세 분석
- 📰 뉴스: 종목별/시장 뉴스
- 😱 공포탐욕: CNN Fear & Greed Index
- 🏭 섹터: 섹터별 성과
- 🔬 종합분석: 여러 사이트 데이터 통합 분석
- 📅 일정: 경제 지표, 실적 발표 일정
- 📚 전략: 전략별 설명

## 설치

```bash
pip install -r requirements.txt
```

## 설정

`.env` 파일 생성:
```
TELEGRAM_BOT_TOKEN=your_telegram_bot_token
FINNHUB_API_KEY=your_finnhub_api_key
GROQ_API_KEY=your_groq_api_key
```

## 실행

```bash
cd src

# 1. 봇 실행
python telegram_bot.py

# 2. 텔레그램에서 봇에게 /start 보내기
# 3. "등록 완료" 메시지 확인 후 Ctrl+C로 종료
```

```bash
cd src

# 한 번 실행 (테스트)
python main.py

# 스케줄러 (매일 22:00 자동 실행)
python main.py --schedule
```

## 전략

| 전략 | 위험도 | 설명 |
|------|--------|------|
| 🎯 보수적 모멘텀 | ⭐ 낮음 | 이미 상승 중인 안전한 종목 |
| ✨ 골든크로스 | ⭐⭐ 중간 | 5일선이 20일선 돌파 |
| 📊 볼린저 반등 | ⭐⭐ 중간 | 볼린저밴드 하단 반등 |
| 📈 MACD 크로스 | ⭐⭐ 중간 | MACD 골든크로스 |
| 🔥 거래량 급증 | ⭐⭐ 중간 | 거래량 2배 이상 + 상승 |
| 🏆 52주 신고가 | ⭐⭐⭐ 높음 | 52주 최고가 근접 |
| 📉 급락 반등 | ⭐⭐⭐ 높음 | -10% 이상 하락 후 반등 |

## 참고 사이트

미국주식 분석에 유용한 사이트들:
- [Seeking Alpha](https://seekingalpha.com) - 기업 분석
- [Finviz](https://finviz.com) - 시장 개요, 스크리너
- [ETF.com](https://etf.com) - ETF 정보
- [TipRanks](https://tipranks.com) - 애널리스트 의견
- [Marketscreener](https://marketscreener.com) - 사업부별 매출
- [CNN Fear & Greed](https://edition.cnn.com/markets/fear-and-greed) - 공포탐욕 지수
