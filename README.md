# 나스닥 100 AI 주식 분석 시스템

나스닥 100 종목을 AI + 학술 연구 기반 팩터 모델 + 재무제표 분석으로 매수/매도 추천하는 시스템.

## 🚀 주요 기능

### 🤖 AI 매수/매도 추천
- **Groq AI** (Llama 4 Maverick) 기반 종합 분석
- 나스닥 100 전체 종목 자동 분석
- 매일 저녁 11시 텔레그램 자동 알림

### 📊 점수 체계
```
종합 점수 = 팩터 점수 (60%) + 재무 점수 (40%)
```

| 점수 | 구성 요소 |
|------|----------|
| **팩터 점수** | 수익성 25% + 모멘텀 20% + 가치 15% + 퀄리티 15% + 저변동성 10% |
| **재무 점수** | 수익성 25% + 밸류에이션 25% + 성장성 20% + 재무건전성 20% + 배당 10% |

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
├── groq_analyzer.py     # 🤖 Groq AI 나스닥 100 분석
├── factor_model.py      # 📊 학술 기반 팩터 모델
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
GROQ_API_KEY=your_groq_api_key
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

### Groq 분석 직접 실행
```bash
cd src

# 기본 (Llama 4 Maverick)
python groq_analyzer.py

# 모델 선택
python groq_analyzer.py llama4-maverick
python groq_analyzer.py kimi-k2
python groq_analyzer.py qwen3-32b

# 빠른 스캔 (AI 없이)
python groq_analyzer.py --quick
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

## 🔗 참고 사이트

- [Seeking Alpha](https://seekingalpha.com) - 기업 분석
- [Finviz](https://finviz.com) - 시장 개요, 스크리너
- [TipRanks](https://tipranks.com) - 애널리스트 의견
- [CNN Fear & Greed](https://edition.cnn.com/markets/fear-and-greed) - 공포탐욕 지수

## 📚 참고 논문

- Fama & French (2015): 5-Factor Model
- Novy-Marx (2013): Gross Profitability Premium
- Jegadeesh & Titman (1993): Momentum
