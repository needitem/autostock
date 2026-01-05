# 나스닥 100 AI 주식 분석 시스템

나스닥 100 종목을 AI + 학술 연구 기반 팩터 모델 + 재무제표 분석으로 매수/매도 추천하는 시스템.

## 🚀 주요 기능

### 🤖 AI 매수/매도 추천
- **OpenRouter API** (Llama 4 Maverick) 기반 종합 분석
- 나스닥 100 전체 종목 자동 분석
- 매일 저녁 11시 텔레그램 자동 알림

### 📊 점수 체계
```
종합 점수 = 팩터 점수 (60%) + 재무 점수 (40%)
```

| 점수 | 구성 요소 | 근거 |
|------|----------|------|
| **팩터 점수** | Momentum 30% + Quality 25% + Value 20% + Profitability 15% + Low Vol 10% | Sharpe Ratio 기반 |
| **재무 점수** | 수익성 25% + 밸류에이션 25% + 성장성 20% + 재무건전성 20% + 배당 10% | 피터린치/버핏/그레이엄 |

**팩터 가중치 근거 (Swedroe & Berkin 2016, 1927-2015):**
| 팩터 | 연간 프리미엄 | Sharpe Ratio | 가중치 |
|------|-------------|--------------|--------|
| Momentum | 9.6% | **0.61** | 30% |
| Quality | 4.7% | **0.47** | 25% |
| Value | 4.8% | **0.34** | 20% |
| Profitability | 3.1% | **0.33** | 15% |
| Low Volatility | - | Robeco 연구 | 10% |

| 등급 | 점수 | 의미 |
|------|------|------|
| A | 70+ | 적극 매수 |
| B | 60+ | 매수 |
| C | 50+ | 보유/관망 |
| D | 40+ | 매도 고려 |
| F | 40- | 매도 |

### 📈 학술 연구 기반 팩터 모델
- **Fama-French 5-Factor Model** + Momentum
- Novy-Marx (2013): 수익성 프리미엄
- Jegadeesh & Titman (1993): 모멘텀 효과

### 💰 재무제표 분석
- **피터 린치**: PEG < 1 저평가
- **워렌 버핏**: ROE > 15%, 낮은 부채
- **벤저민 그레이엄**: P/E < 15, P/B < 1.5

### 🌐 외부 데이터 소스
| 사이트 | 제공 정보 |
|--------|----------|
| **CNN Fear & Greed** | 공포탐욕 지수 (0-100) |
| **Finviz** | 섹터 성과, 밸류에이션, 기술적 지표 |
| **TipRanks** | 애널리스트 컨센서스, 목표가 |
| **Seeking Alpha** | 퀀트 레이팅 |
| **Finnhub** | 뉴스, 실적 일정, 내부자 거래 |
| **yfinance** | 재무제표, 주가 데이터 |

### 📱 텔레그램 봇 메뉴
- 🤖 AI추천: 나스닥 100 AI 매수/매도 추천
- 🌟 추천: 저위험 + 전략 매칭 종목
- 🔍 스캔: 전체 종목 스캔
- 📊 종목분석: 개별 종목 상세 분석
- 📰 뉴스: 종목별/시장 뉴스
- 😱 공포탐욕: CNN Fear & Greed Index
- 🏭 섹터: 섹터별 성과
- 🔬 종합분석: 여러 사이트 데이터 통합 분석
- 📅 일정: 경제 지표, 실적 발표 일정

## 📁 프로젝트 구조

```
src/
├── openrouter_analyzer.py  # 🤖 OpenRouter AI 나스닥 100 분석
├── factor_model.py         # 📊 학술 기반 팩터 모델
├── financial_data.py    # 💰 재무제표 데이터 (yfinance)
├── market_data.py       # 🌐 외부 사이트 데이터
├── analyzer.py          # 기술적 분석, 스캔
├── ai_analyzer.py       # AI 뉴스/종목 분석
├── strategies.py        # 7가지 매매 전략
├── telegram_bot.py      # 텔레그램 봇
├── main.py              # 스케줄러
└── ...

tests/                   # 142개 테스트
```

## 🛠 설치

```bash
pip install -r requirements.txt
```

## ⚙️ 설정

`.env` 파일 생성:
```
TELEGRAM_BOT_TOKEN=your_telegram_bot_token
FINNHUB_API_KEY=your_finnhub_api_key
OPENROUTER_API_KEY=your_openrouter_api_key
```

## ▶️ 실행

### 텔레그램 봇 등록
```bash
cd src
python telegram_bot.py
# 텔레그램에서 봇에게 /start 보내기
```

### AI 추천 실행
```bash
cd src

# AI 매수/매도 추천 (1회)
python main.py --ai

# 스케줄러 (22:00 스캔, 23:00 AI추천)
python main.py --schedule
```

### AI 분석 직접 실행
```bash
cd src

# 기본 (Llama 4 Maverick)
python openrouter_analyzer.py

# 모델 선택
python openrouter_analyzer.py llama4-maverick
python openrouter_analyzer.py gemini-flash
python openrouter_analyzer.py deepseek-v3

# 빠른 스캔 (AI 없이)
python openrouter_analyzer.py --quick
```

## 🧪 테스트

```bash
# 전체 테스트 (142개)
python -m pytest tests/ -v

# 특정 모듈 테스트
python -m pytest tests/test_financial_data.py -v
```

## 📋 7가지 매매 전략

| 전략 | 위험도 | 설명 |
|------|--------|------|
| 🎯 보수적 모멘텀 | ⭐ 낮음 | 이미 상승 중인 안전한 종목 |
| ✨ 골든크로스 | ⭐⭐ 중간 | 5일선이 20일선 돌파 |
| 📊 볼린저 반등 | ⭐⭐ 중간 | 볼린저밴드 하단 반등 |
| 📈 MACD 크로스 | ⭐⭐ 중간 | MACD 골든크로스 |
| 🔥 거래량 급증 | ⭐⭐ 중간 | 거래량 2배 이상 + 상승 |
| 🏆 52주 신고가 | ⭐⭐⭐ 높음 | 52주 최고가 근접 |
| 📉 급락 반등 | ⭐⭐⭐ 높음 | -10% 이상 하락 후 반등 |

---

## 🧠 AI 추천 알고리즘 상세

### 1️⃣ 종합 점수 계산
```
종합 점수 = 팩터 점수 (60%) + 재무 점수 (40%)
```

### 2️⃣ 팩터 점수 (Sharpe Ratio 기반)

| 팩터 | 가중치 | 근거 | 측정 지표 |
|------|--------|------|----------|
| **Momentum** | 30% | Sharpe 0.61 | 52주 위치, 50일선 대비, RSI |
| **Quality** | 25% | Sharpe 0.47 | 부채비율, 배당, 유동비율, FCF |
| **Value** | 20% | Sharpe 0.34 | P/E, Forward P/E, PEG |
| **Profitability** | 15% | Sharpe 0.33 | ROE, ROA, 순이익률, 영업이익률 |
| **Low Volatility** | 10% | Robeco 연구 | 볼린저밴드 위치, 5일 변화율 |

### 3️⃣ 재무 점수 (투자 대가 기준)

| 항목 | 가중치 | 측정 지표 | 기준 |
|------|--------|----------|------|
| **수익성** | 25% | ROE, ROA, 순이익률 | ROE > 15% (버핏) |
| **밸류에이션** | 25% | P/E, P/B, PEG | P/E < 15, PEG < 1 (그레이엄/린치) |
| **성장성** | 20% | 매출성장률, 이익성장률 | 지속적 성장 |
| **재무건전성** | 20% | 부채비율, 유동비율, FCF | 유동비율 > 2 (그레이엄) |
| **배당** | 10% | 배당수익률, 배당성향 | 30-60% 배당성향 |

### 4️⃣ 기술적 분석 (위험도)

| 지표 | 설명 | 위험 신호 |
|------|------|----------|
| **RSI** | 과매수/과매도 | > 70 과매수, < 30 과매도 |
| **볼린저밴드** | 가격 위치 | 상단 근접 = 과열 |
| **52주 위치** | 고점/저점 대비 | > 95% 고점 근접 |
| **50일선 대비** | 이동평균 괴리 | > 20% 과열 |
| **5일 변화율** | 단기 변동성 | > 15% 급등/급락 |

### 5️⃣ 외부 데이터 소스

| 소스 | 데이터 | 용도 |
|------|--------|------|
| **CNN Fear & Greed** | 공포탐욕 지수 (0-100) | 시장 심리 판단 |
| **Finviz** | P/E, ROE, 섹터, 목표가 | 밸류에이션 |
| **TipRanks** | 애널리스트 컨센서스 | 전문가 의견 |
| **Seeking Alpha** | 퀀트 레이팅 | 퀀트 분석 |
| **yfinance** | 재무제표 전체 | 재무 분석 |

### 6️⃣ 매수/매도 필터링 조건

**매수 후보:**
- 종합 점수 ≥ 55
- 위험도 ≤ 35
- RSI 30-70 (과매수/과매도 아님)

**매도 후보:**
- 종합 점수 ≤ 45 또는
- 위험도 ≥ 50 또는
- RSI ≥ 70 (과매수)

### 7️⃣ 등급 체계

| 등급 | 점수 | 의미 |
|------|------|------|
| **A** | 70+ | 적극 매수 |
| **B** | 60-69 | 매수 |
| **C** | 50-59 | 보유/관망 |
| **D** | 40-49 | 매도 고려 |
| **F** | 0-39 | 매도 |

---

## 🔗 참고 사이트

- [Seeking Alpha](https://seekingalpha.com) - 기업 분석
- [Finviz](https://finviz.com) - 시장 개요, 스크리너
- [TipRanks](https://tipranks.com) - 애널리스트 의견
- [CNN Fear & Greed](https://edition.cnn.com/markets/fear-and-greed) - 공포탐욕 지수

## 📚 참고 연구

### 팩터 프리미엄 데이터 (가중치 근거)
| 연구 | 기간 | 핵심 데이터 |
|------|------|------------|
| **Swedroe & Berkin (2016)** | 1927-2015 | Momentum Sharpe 0.61, Value 0.34, Profitability 0.33 |
| **Alpha Architect (2024)** | 1964-2023 | Quality (QMJ) 연 4.7%, Sharpe 0.47 |
| **Robeco (2024)** | - | Low Volatility Anomaly 지속 확인 |

### 클래식 연구
| 연구 | 연도 | 핵심 내용 |
|------|------|----------|
| Fama & French | 2015 | 5-Factor Model |
| Novy-Marx | 2013 | Gross Profitability Premium |
| Jegadeesh & Titman | 1993 | Momentum (12-1개월) |
| Asness et al. (AQR) | 2019 | Quality Minus Junk |

### 최신 연구 (2023-2024)
| 연구 | 연도 | 핵심 내용 |
|------|------|----------|
| López de Prado | 2023 | Causal Factor Investing |
| Blitz, van Vliet, Hanauer | 2024 | FF5 비판 - 모멘텀/저변동성 누락 |
| Schwartz & Hanauer | 2024 | Formula Investing 검증 |
