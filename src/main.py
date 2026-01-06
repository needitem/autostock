"""
주식 분석 봇 메인 엔트리포인트

사용법:
  python main.py              # 봇 실행 (스케줄러 포함)
  python main.py --no-schedule # 봇 실행 (스케줄러 없음)
  python main.py --scan       # 스캔 한 번 실행
  python main.py --ai         # AI 추천 한 번 실행
"""
import sys
import os

# src 폴더를 path에 추가
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime


def run_scan_once():
    """스캔 한 번 실행"""
    print(f"[{datetime.now()}] 스캔 시작...")
    
    from core.signals import scan_stocks
    from config import NASDAQ_100
    
    result = scan_stocks(NASDAQ_100[:50])
    
    print(f"\n📊 스캔 결과: {result['total']}개 분석")
    print("=" * 50)
    
    for r in result["results"][:10]:
        score = r.get("score", {})
        strategies = r.get("strategies", [])
        strats = ", ".join([s["emoji"] for s in strategies]) if strategies else "-"
        
        print(f"{r['symbol']:6} ${r['price']:8.2f} | "
              f"점수: {score.get('total_score', 0):5.1f} | "
              f"RSI: {r.get('rsi', 50):5.1f} | {strats}")
    
    print("=" * 50)


def run_ai_once():
    """AI 추천 한 번 실행 (전체 카테고리 + 모든 뉴스 통합)"""
    print(f"[{datetime.now()}] AI 추천 분석 시작...")
    
    from core.signals import scan_stocks
    from core.stock_data import get_market_condition, get_fear_greed_index
    from core.news import get_bulk_news, get_market_news
    from ai.analyzer import ai
    from config import ALL_CATEGORY_STOCKS, STOCK_CATEGORIES
    
    # 1. 전체 카테고리 종목 스캔
    print(f"[1/4] 전체 카테고리 종목 스캔 중... ({len(ALL_CATEGORY_STOCKS)}개)")
    result = scan_stocks(ALL_CATEGORY_STOCKS)
    stocks = result["results"]
    print(f"  → {len(stocks)}개 종목 스캔 완료")
    
    # 2. 시장 데이터 수집
    print("[2/4] 시장 데이터 수집 중...")
    market_data = {
        "fear_greed": get_fear_greed_index(),
        "market_condition": get_market_condition(),
        "market_news": get_market_news(),
    }
    print(f"  → 공포탐욕: {market_data['fear_greed'].get('score', 'N/A')}, 시장: {market_data['market_condition'].get('message', 'N/A')}")
    
    # 3. 모든 종목 뉴스 수집
    print(f"[3/4] 전체 종목 뉴스 수집 중... ({len(stocks)}개)")
    all_symbols = [s['symbol'] for s in stocks]
    news_data = get_bulk_news(all_symbols, days=7)
    print(f"  → {len(news_data)}개 종목 뉴스 수집 완료")
    
    # 4. AI 분석
    print("[4/4] AI 분석 중...")
    ai_result = ai.analyze_full_market(stocks, news_data, market_data, STOCK_CATEGORIES)
    
    if "error" in ai_result:
        print(f"❌ AI 분석 실패: {ai_result['error']}")
        return
    
    print("\n🤖 AI 추천")
    print("=" * 60)
    print(ai_result["analysis"])
    print("=" * 60)
    
    # 통계 출력
    stats = ai_result.get("stats", {})
    if stats:
        print(f"\n📊 분석 통계: {ai_result['total']}개 종목")
        print(f"   평균RSI: {stats.get('avg_rsi', 0):.0f}, 평균점수: {stats.get('avg_score', 0):.0f}")
        print(f"   과매도: {stats.get('oversold', 0)}개, 과매수: {stats.get('overbought', 0)}개")


def run_bot(with_scheduler: bool = True):
    """텔레그램 봇 실행"""
    from bot import run_bot as bot_run
    bot_run(with_scheduler=with_scheduler)


def main():
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        
        if arg == "--scan":
            run_scan_once()
        elif arg == "--ai":
            run_ai_once()
        elif arg == "--no-schedule":
            run_bot(with_scheduler=False)
        elif arg == "--help":
            print(__doc__)
        else:
            print(f"알 수 없는 옵션: {arg}")
            print(__doc__)
    else:
        # 기본: 봇 실행 (스케줄러 포함)
        run_bot(with_scheduler=True)


if __name__ == "__main__":
    main()
