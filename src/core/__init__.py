"""Core analysis package exports."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.backtest import backtest_symbols, simulate_swing_strategy
from core.indicators import calculate_indicators, get_full_analysis
from core.news import get_company_news, get_market_news
from core.scoring import calculate_score
from core.signals import check_entry_signal, check_exit_signal, scan_stocks
from core.stock_data import (
    get_fear_greed_index,
    get_finviz_data,
    get_market_condition,
    get_stock_data,
    get_stock_info,
)

__all__ = [
    "get_stock_data",
    "get_stock_info",
    "get_finviz_data",
    "get_market_condition",
    "get_fear_greed_index",
    "calculate_indicators",
    "get_full_analysis",
    "calculate_score",
    "check_entry_signal",
    "check_exit_signal",
    "scan_stocks",
    "get_company_news",
    "get_market_news",
    "simulate_swing_strategy",
    "backtest_symbols",
]

