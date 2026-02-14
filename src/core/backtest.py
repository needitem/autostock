"""
Lightweight backtesting utilities for recommendation quality validation.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import SMAIndicator
from ta.volatility import BollingerBands

from core.stock_data import get_stock_data


@dataclass
class Trade:
    entry_date: str
    exit_date: str
    entry_price: float
    exit_price: float
    return_pct: float
    hold_days: int
    exit_reason: str


def _max_drawdown(equity_curve: list[float]) -> float:
    if not equity_curve:
        return 0.0
    arr = np.array(equity_curve, dtype=float)
    peak = np.maximum.accumulate(arr)
    drawdown = (arr - peak) / peak
    return float(drawdown.min() * 100)


def simulate_swing_strategy(
    df: pd.DataFrame,
    entry_rsi: float = 35,
    exit_rsi: float = 68,
    stop_loss: float = -7,
    take_profit: float = 12,
    max_hold_days: int = 15,
) -> dict[str, Any]:
    """
    One-position swing strategy simulation.

    Entry:
    - RSI <= entry_rsi
    - Close near/under lower Bollinger band
    - Price above MA50*0.9 (avoid extreme breakdown only)

    Exit:
    - stop loss / take profit
    - RSI >= exit_rsi
    - max hold day
    """
    if df is None or len(df) < 250:
        return {"trades": [], "metrics": {"trade_count": 0}}

    frame = df[["Open", "High", "Low", "Close", "Volume"]].copy().dropna()
    close = frame["Close"].astype(float)

    rsi = RSIIndicator(close, window=14).rsi()
    ma50 = SMAIndicator(close, window=50).sma_indicator()
    bb = BollingerBands(close, window=20, window_dev=2)
    bb_lower = bb.bollinger_lband()

    trades: list[Trade] = []
    equity = 1.0
    equity_curve: list[float] = [equity]

    in_position = False
    entry_price = 0.0
    entry_idx = 0
    entry_date = ""

    for i in range(60, len(frame)):
        price = float(close.iloc[i])
        curr_rsi = float(rsi.iloc[i]) if pd.notna(rsi.iloc[i]) else 50.0
        curr_ma50 = float(ma50.iloc[i]) if pd.notna(ma50.iloc[i]) else price
        curr_bb_lower = float(bb_lower.iloc[i]) if pd.notna(bb_lower.iloc[i]) else price
        date_label = str(frame.index[i].date()) if hasattr(frame.index[i], "date") else str(frame.index[i])

        if not in_position:
            near_lower_band = price <= curr_bb_lower * 1.02
            trend_filter = price >= curr_ma50 * 0.9
            if curr_rsi <= entry_rsi and near_lower_band and trend_filter:
                in_position = True
                entry_price = price
                entry_idx = i
                entry_date = date_label
            continue

        hold_days = i - entry_idx
        ret = (price - entry_price) / entry_price * 100 if entry_price else 0.0

        reason = ""
        if ret <= stop_loss:
            reason = "stop_loss"
        elif ret >= take_profit:
            reason = "take_profit"
        elif curr_rsi >= exit_rsi:
            reason = "rsi_exit"
        elif hold_days >= max_hold_days:
            reason = "time_exit"

        if reason:
            trades.append(
                Trade(
                    entry_date=entry_date,
                    exit_date=date_label,
                    entry_price=round(entry_price, 2),
                    exit_price=round(price, 2),
                    return_pct=round(ret, 2),
                    hold_days=hold_days,
                    exit_reason=reason,
                )
            )
            equity *= 1 + ret / 100
            equity_curve.append(equity)
            in_position = False
            entry_price = 0.0
            entry_idx = 0
            entry_date = ""

    returns = [t.return_pct for t in trades]
    wins = [r for r in returns if r > 0]
    losses = [r for r in returns if r <= 0]

    metrics = {
        "trade_count": len(trades),
        "win_rate": round((len(wins) / len(trades) * 100), 1) if trades else 0.0,
        "avg_return": round(float(np.mean(returns)), 2) if returns else 0.0,
        "median_return": round(float(np.median(returns)), 2) if returns else 0.0,
        "avg_win": round(float(np.mean(wins)), 2) if wins else 0.0,
        "avg_loss": round(float(np.mean(losses)), 2) if losses else 0.0,
        "profit_factor": round(abs(sum(wins) / sum(losses)), 2) if wins and losses and sum(losses) != 0 else 0.0,
        "max_drawdown": round(_max_drawdown(equity_curve), 2),
        "equity_final": round(equity, 4),
    }
    return {"trades": [t.__dict__ for t in trades], "metrics": metrics}


def backtest_symbols(symbols: list[str], period: str = "3y") -> dict[str, Any]:
    """Run the swing simulation for multiple symbols and aggregate results."""
    results: dict[str, Any] = {}
    for symbol in symbols:
        df = get_stock_data(symbol, period=period)
        if df is None:
            continue
        out = simulate_swing_strategy(df)
        metrics = out.get("metrics", {})
        if metrics.get("trade_count", 0) > 0:
            results[symbol] = metrics

    ranked = sorted(
        (
            {
                "symbol": symbol,
                **metrics,
                "score": round(
                    metrics.get("win_rate", 0) * 0.35
                    + metrics.get("avg_return", 0) * 2.0
                    + metrics.get("profit_factor", 0) * 10
                    + metrics.get("max_drawdown", 0) * 0.3,  # drawdown is negative
                    2,
                ),
            }
            for symbol, metrics in results.items()
        ),
        key=lambda x: -x["score"],
    )

    summary = {
        "symbol_count": len(ranked),
        "avg_win_rate": round(float(np.mean([r["win_rate"] for r in ranked])), 1) if ranked else 0.0,
        "avg_return": round(float(np.mean([r["avg_return"] for r in ranked])), 2) if ranked else 0.0,
        "avg_drawdown": round(float(np.mean([r["max_drawdown"] for r in ranked])), 2) if ranked else 0.0,
    }
    return {"summary": summary, "ranked": ranked}


__all__ = ["simulate_swing_strategy", "backtest_symbols"]

