import schedule
import time
from datetime import datetime
import pytz

from analyzer import scan_all_stocks
from telegram_bot import send_sync, format_daily_report, format_ai_recommendation
from telegram_bot import get_saved_chat_id


def run_daily_scan():
    """일일 스캔 실행"""
    print(f"[{datetime.now()}] 스캔 시작...")
    
    result = scan_all_stocks()
    report = format_daily_report(result)
    
    print(report)
    
    if get_saved_chat_id():
        send_sync(report)
        print("텔레그램 전송 완료")
    else:
        print("Chat ID가 없습니다. 먼저 python telegram_bot.py 실행 후 /start 보내세요.")


def run_ai_recommendation():
    """AI 매수/매도 추천 실행 (매일 저녁 11시)"""
    print(f"[{datetime.now()}] AI 추천 분석 시작...")
    
    try:
        from openrouter_analyzer import run_full_analysis
        result = run_full_analysis()
        
        if "error" in result:
            print(f"AI 분석 실패: {result['error']}")
            return
        
        report = format_ai_recommendation(result)
        
        # 텔레그램 메시지 길이 제한 (4096자)
        if len(report) > 4000:
            report = report[:3900] + "\n\n... (메시지가 너무 길어 일부 생략)"
        
        print(report)
        
        if get_saved_chat_id():
            send_sync(report)
            print("텔레그램 전송 완료")
        else:
            print("Chat ID가 없습니다.")
    except Exception as e:
        print(f"AI 추천 실패: {e}")


def run_once():
    """한 번만 실행 (테스트용)"""
    run_daily_scan()


def run_ai_once():
    """AI 추천 한 번만 실행 (테스트용)"""
    run_ai_recommendation()


def run_scheduler():
    """스케줄러 실행
    - 매일 22:00 (오후 10시): 일일 스캔
    - 매일 23:00 (오후 11시): AI 매수/매도 추천
    (한국 시간 기준)
    """
    # 한국 시간 기준 오후 10시 - 일일 스캔
    schedule.every().day.at("22:00").do(run_daily_scan)
    
    # 한국 시간 기준 오후 11시 - AI 매수/매도 추천
    schedule.every().day.at("23:00").do(run_ai_recommendation)
    
    print("=" * 50)
    print("📅 스케줄러 시작됨 (한국 시간 기준)")
    print("=" * 50)
    print("• 22:00 - 일일 스캔")
    print("• 23:00 - AI 매수/매도 추천")
    print("=" * 50)
    print("Ctrl+C로 종료")
    print()
    
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--schedule":
            run_scheduler()
        elif sys.argv[1] == "--ai":
            run_ai_once()
        elif sys.argv[1] == "--help":
            print("""
사용법: python main.py [옵션]

옵션:
  (없음)       일일 스캔 한 번 실행
  --ai         AI 매수/매도 추천 한 번 실행
  --schedule   스케줄러 실행 (22:00 스캔, 23:00 AI추천)
  --help       도움말
""")
        else:
            run_once()
    else:
        # 기본: 한 번만 실행
        run_once()
