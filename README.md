# 미국 주식 AI 분석 시스템

나스닥 100 + S&P 500 (518개 종목)을 AI + 기술적 지표 + 뉴스로 종합 분석하는 시스템.

## 🚀 주요 기능

### 📊 분석 대상
- **나스닥 100** + **S&P 500** = **518개 종목** (중복 제거)
- 위키피디아에서 자동 수집, 7일마다 갱신
- yfinance에서 섹터/산업 자동 분류

### 🤖 AI 종합 분석
- **Z.ai GLM-4.7** / **OpenRouter** 기반
- 기술적 지표 + 뉴스 + 시장 심리 종합 분석
- 초보자도 이해할 수 있는 근거 기반 추천

### 📈 기술적 지표
| 지표 | 설명 | 신호 |
|------|------|------|
| **RSI** | 과매수/과매도 | <30 매수, >70 매도 |
| **스토캐스틱** | 단기 모멘텀 | <20 매수, >80 매도 |
| **ADX** | 추세 강도 | >25 강한 추세 |
| **MACD** | 추세 전환 | 골든/데드크로스 |
| **볼린저밴드** | 가격 위치 | 하단=매수, 상단=매도 |
| **지지/저항선** | 가격 레벨 | 돌파 시 신호 |
| **캔들 패턴** | 8가지 패턴 | 망치형, 도지 등 |
| **거래량** | 관심도 | 2배↑ 급증 |

### 👀 관심종목 모니터링
- 30분마다 자동 체크
- 알림 조건:
  - 가격 ±3% 변동
  - RSI/스토캐스틱 과매도/과매수 진입
  - 지지선/저항선 돌파
  - 골든크로스/데드크로스
  - 거래량 급증, 캔들 패턴

### 📂 섹터별 분류 (자동)
| 섹터 | 종목 수 | ETF |
|------|--------|-----|
| 💻 기술 | 89개 | XLK |
| 🏭 산업재 | 75개 | XLI |
| 💳 금융 | 70개 | XLF |
| 🏥 헬스케어 | 63개 | XLV |
| 🛒 경기소비재 | 55개 | XLY |
| 🏪 필수소비재 | 37개 | XLP |
| 💡 유틸리티 | 31개 | XLU |
| 🏠 부동산 | 31개 | XLRE |
| 📡 통신서비스 | 24개 | XLC |
| ⛽ 에너지 | 22개 | XLE |
| 🧱 소재 | 20개 | XLB |

### 📱 텔레그램 봇
- 📈 추천종목: 전체 518개 분석 후 TOP 20
- 🔍 전체스캔: 518개 종목 스캔
- 🤖 AI 분석: 지표 + 뉴스 + AI 종합 추천
- 📊 종목분석: 개별 종목 상세 분석
- 📂 카테고리: 섹터별 추천
- � 관심텔종목: 30분 모니터링 + 알림
- 😱 시장심리: CNN Fear & Greed Index
- 💰 트레이딩: 자동매매 설정

### ⏰ 자동 스케줄
| 시간 | 기능 |
|------|------|
| 30분마다 | 관심종목 모니터링 |
| 21:00 | 자동매매 (저점매수/손절) |
| 22:00 | 일일 스캔 |
| 23:00 | AI 매수/매도 추천 |

## 📁 프로젝트 구조

```
src/
├── ai/
│   └── analyzer.py      # 🤖 AI 분석 (Z.ai/OpenRouter)
├── bot/
│   ├── bot.py           # 텔레그램 봇 + 스케줄러
│   ├── handlers.py      # 콜백 핸들러
│   ├── keyboards.py     # UI 키보드
│   └── formatters.py    # 메시지 포맷
├── core/
│   ├── indicators.py    # 📊 기술적 지표
│   ├── scoring.py       # 점수 계산
│   ├── signals.py       # 매매 신호
│   ├── stock_data.py    # 주가 데이터
│   └── news.py          # 뉴스 수집
├── trading/
│   ├── monitor.py       # 👀 관심종목 모니터링
│   ├── watchlist.py     # 관심종목 관리
│   ├── portfolio.py     # 포트폴리오
│   └── kis_api.py       # 한국투자증권 API
├── config.py            # 설정 (종목 목록, 섹터)
└── main.py              # 메인 실행

data/
├── nasdaq100_cache.json # 나스닥 100 캐시
├── sp500_cache.json     # S&P 500 캐시
├── sector_cache.json    # 섹터 정보 캐시
└── watchlist.json       # 관심종목
```

## 🛠 설치

```bash
pip install -r requirements.txt
```

## ⚙️ 설정

`.env` 파일:
```
TELEGRAM_BOT_TOKEN=your_telegram_bot_token
FINNHUB_API_KEY=your_finnhub_api_key

# AI API (둘 중 하나)
ZAI_API_KEY=your_zai_api_key
OPENROUTER_API_KEY=your_openrouter_api_key

# 한국투자증권 (자동매매용)
KIS_APP_KEY=your_kis_app_key
KIS_APP_SECRET=your_kis_app_secret
KIS_ACCOUNT_NO=your_account_no
```

## ▶️ 실행

### 텔레그램 봇
```bash
python src/main.py
# 텔레그램에서 /start
```

### 전체 분석 (CLI)
```bash
python run_full_analysis.py
```

## 🧪 테스트

```bash
python -m pytest tests/ -v
```

## 📚 데이터 소스

| 소스 | 데이터 |
|------|--------|
| **yfinance** | 주가, 재무제표, 섹터 |
| **Finnhub** | 뉴스, 애널리스트 목표가 |
| **CNN** | Fear & Greed Index |
| **Wikipedia** | 나스닥 100, S&P 500 목록 |

## 📋 점수 체계

| 등급 | 점수 | 의미 |
|------|------|------|
| A | 70+ | 적극 매수 |
| B | 60+ | 매수 |
| C | 50+ | 보유/관망 |
| D | 40+ | 매도 고려 |
| F | 40- | 매도 |
