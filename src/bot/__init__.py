"""텔레그램 봇 모듈"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.bot import run_bot, send_message, send_sync
