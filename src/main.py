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
    """AI 추천 한 번 실행"""
    print(f"[{datetime.now()}] AI 추천 분석 시작...")
    
    from core.signals import scan_stocks
    from ai.analyzer import ai
    from config import NASDAQ_100
    
    result = scan_stocks(NASDAQ_100)  # 전체 스캔
    ai_result = ai.analyze_recommendations(result["results"])
    
    if "error" in ai_result:
        print(f"❌ AI 분석 실패: {ai_result['error']}")
        return
    
    print("\n🤖 AI 추천")
    print("=" * 50)
    print(ai_result["analysis"])
    print("=" * 50)


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
