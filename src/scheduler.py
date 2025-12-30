"""
매일 9시 자동 추천 스케줄러
"""
import asyncio
import schedule
import time
from datetime import datetime
from telegram import Bot
from config import TELEGRAM_BOT_TOKEN
from telegram_bot import get_saved_chat_id, format_recommendations


async def send_daily_recommendation():
    """매일 추천 종목 전송"""
    chat_id = get_saved_chat_id()
    if not TELEGRAM_BOT_TOKEN or not chat_id:
        print(f"[{datetime.now()}] 텔레그램 설정 없음")
        return
    
    try:
        print(f"[{datetime.now()}] 일일 추천 분석 시작...")
        from analyzer import get_recommendations
        result = get_recommendations()
        report = format_recommendations(result)
        
        # 헤더 추가
        report = f"🌅 <b>굿모닝! 오늘의 추천</b>\n{datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n" + report
        
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        await bot.send_message(chat_id=chat_id, text=report, parse_mode="HTML")
        print(f"[{datetime.now()}] 일일 추천 전송 완료")
    except Exception as e:
        print(f"[{datetime.now()}] 전송 실패: {e}")


def run_async_job():
    """동기 함수에서 비동기 실행"""
    asyncio.run(send_daily_recommendation())


def run_scheduler():
    """스케줄러 실행 (매일 9시)"""
    # 매일 오전 9시에 실행
    schedule.every().day.at("09:00").do(run_async_job)
    
    print("📅 스케줄러 시작 - 매일 09:00 추천 알림")
    print("   Ctrl+C로 종료")
    
    while True:
        schedule.run_pending()
        time.sleep(60)  # 1분마다 체크


if __name__ == "__main__":
    # 테스트: 바로 실행
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "now":
        print("즉시 실행 테스트...")
        run_async_job()
    else:
        run_scheduler()
