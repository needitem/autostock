"""AAPL 종합 분석 스크립트"""
import sys
sys.path.insert(0, 'src')

from analyzer import analyze_single_stock
from market_data import get_comprehensive_stock_analysis, get_fear_greed_index
from news_fetcher import get_company_news, get_price_target

print('=' * 50)
print('📊 AAPL (Apple) 종합 분석')
print('=' * 50)

# 1. 기술적 분석
print('\n[1] 기술적 분석')
result = analyze_single_stock('AAPL')
if result:
    print(f"현재가: ${result['price']}")
    print(f"위험도: {result['risk_score']}/100 ({result['risk_grade']})")
    print(f"RSI: {result['rsi']}")
    print(f"볼린저 위치: {result['bb_position']}%")
    print(f"52주 범위 위치: {result['position_52w']}%")
    print(f"50일선 대비: {result['ma50_gap']:+.1f}%")
    print(f"5일 변화: {result['change_5d']:+.1f}%")
    if result['warnings']:
        print('경고:')
        for w in result['warnings']:
            print(f"  {w}")
    if result['strategies_matched']:
        print(f"매칭 전략: {result['strategies_matched']}")
    else:
        print('매칭 전략: 없음')

# 2. 공포탐욕 지수
print('\n[2] 시장 심리 (CNN Fear & Greed)')
fg = get_fear_greed_index()
print(f"{fg['emoji']} {fg['score']}/100 - {fg['rating']}")
print(f"조언: {fg['advice']}")

# 3. 외부 데이터
print('\n[3] 외부 데이터 (Finviz/TipRanks)')
comp = get_comprehensive_stock_analysis('AAPL')
sources = comp.get('sources', {})

fv = sources.get('finviz', {})
if fv:
    print('Finviz:')
    print(f"  - P/E: {fv.get('pe', 'N/A')}")
    print(f"  - Forward P/E: {fv.get('forward_pe', 'N/A')}")
    print(f"  - PEG: {fv.get('peg', 'N/A')}")
    print(f"  - ROE: {fv.get('roe', 'N/A')}")
    print(f"  - 목표가: ${fv.get('target_price', 'N/A')}")
    print(f"  - 섹터: {fv.get('sector', 'N/A')}")

tr = sources.get('tipranks', {})
if tr:
    print('TipRanks:')
    print(f"  - 컨센서스: {tr.get('consensus', 'N/A')}")
    print(f"  - 매수/보유/매도: {tr.get('buy', 0)}/{tr.get('hold', 0)}/{tr.get('sell', 0)}")

# 4. 애널리스트 목표가
print('\n[4] 애널리스트 목표가 (Finnhub)')
target = get_price_target('AAPL')
if target:
    print(f"최고: ${target['target_high']}")
    print(f"평균: ${target['target_mean']}")
    print(f"최저: ${target['target_low']}")

# 5. 최근 뉴스
print('\n[5] 최근 뉴스')
news = get_company_news('AAPL', days=7)
if news:
    for n in news[:5]:
        headline = n['headline'][:70] + '...' if len(n['headline']) > 70 else n['headline']
        print(f"- {headline}")
else:
    print("뉴스 없음 (API 키 확인 필요)")

print('\n' + '=' * 50)
print('분석 완료!')
