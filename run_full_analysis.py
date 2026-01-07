# -*- coding: utf-8 -*-
"""미국 주식 전체 AI 분석 (나스닥100 + S&P500)"""
import sys
sys.path.insert(0, 'src')

from core.signals import scan_stocks
from core.stock_data import get_market_condition, get_fear_greed_index
from core.news import get_bulk_news, get_market_news
from ai.analyzer import ai
from config import ALL_US_STOCKS, STOCK_CATEGORIES, get_category_summary

print('=' * 70)
print('🤖 미국 주식 전체 AI 종합 분석 (나스닥100 + S&P500)')
print('=' * 70)
print(get_category_summary())
print()

# 1. 전체 종목 스캔
print(f'[1/4] 전체 {len(ALL_US_STOCKS)}개 종목 스캔 중...')
result = scan_stocks(ALL_US_STOCKS)
stocks = result["results"]
print(f'  ✅ {len(stocks)}개 종목 스캔 완료')

# 2. 시장 데이터
print('\n[2/4] 시장 데이터 수집 중...')
market_data = {
    "fear_greed": get_fear_greed_index(),
    "market_condition": get_market_condition(),
    "market_news": get_market_news(),
}
fg = market_data["fear_greed"]
print(f'  ✅ 공포탐욕지수: {fg.get("score", "?")} ({fg.get("rating", "?")})')

# 3. 뉴스 수집 (상위 100개 종목만)
print('\n[3/4] 주요 종목 뉴스 수집 중...')
top_stocks = sorted(stocks, key=lambda x: -x.get("score", {}).get("total_score", 0))[:100]
top_symbols = [s['symbol'] for s in top_stocks]
news_data = get_bulk_news(top_symbols, days=7)
print(f'  ✅ {len(news_data)}개 종목 뉴스 수집 완료')

# 4. AI 분석
print('\n[4/4] AI 종합 분석 중... (3~5분 소요)')
ai_result = ai.analyze_full_market(stocks, news_data, market_data, STOCK_CATEGORIES)

if "error" in ai_result:
    print(f'\n❌ AI 분석 실패: {ai_result["error"]}')
else:
    stats = ai_result.get("stats", {})
    print('\n' + '=' * 70)
    print('📊 분석 통계')
    print('=' * 70)
    print(f'총 분석: {ai_result.get("total", 0)}개 종목')
    print(f'평균 RSI: {stats.get("avg_rsi", 0):.1f}')
    print(f'평균 점수: {stats.get("avg_score", 0):.1f}/100')
    print(f'과매도 (RSI<30): {stats.get("oversold", 0)}개')
    print(f'과매수 (RSI>70): {stats.get("overbought", 0)}개')
    
    print('\n' + '=' * 70)
    print('🤖 AI 분석 결과')
    print('=' * 70)
    print(ai_result.get("analysis", "분석 결과 없음"))

print('\n' + '=' * 70)
print('분석 완료!')
