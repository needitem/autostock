import schedule
import time
from datetime import datetime
import pytz

from analyzer import scan_all_stocks
from telegram_bot import send_sync, format_daily_report
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


def run_once():
    """한 번만 실행 (테스트용)"""
    run_daily_scan()


def run_scheduler():
    """스케줄러 실행 (매일 오후 10시 - 미국장 시작 전)"""
    # 한국 시간 기준 오후 10시 (미국 동부 오전 9시)
    schedule.every().day.at("22:00").do(run_daily_scan)
    
    print("스케줄러 시작됨. 매일 22:00에 스캔합니다.")
    print("Ctrl+C로 종료")
    
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--schedule":
        run_scheduler()
    else:
        # 기본: 한 번만 실행
        run_once()
