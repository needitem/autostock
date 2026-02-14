"""
Autostock main entrypoint.

Usage:
  python src/main.py                  # run telegram bot
  python src/main.py --no-schedule    # run bot without scheduler
  python src/main.py --scan           # one-time scan
  python src/main.py --ai             # one-time market analysis
  python src/main.py --backtest       # one-time strategy validation
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _configure_console_output() -> None:
    """Avoid UnicodeEncodeError on legacy Windows terminals."""
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream and hasattr(stream, "reconfigure"):
            try:
                stream.reconfigure(errors="replace")
            except Exception:
                pass


def run_scan_once(limit: int = 50) -> None:
    from config import load_nasdaq_100
    from core.signals import scan_stocks

    print(f"[{datetime.now()}] scan started...")
    symbols = load_nasdaq_100()[: max(1, limit)]
    result = scan_stocks(symbols)
    ranked = sorted(result["results"], key=lambda x: -x.get("investability_score", x.get("quality_score", 0)))

    print(f"\nscan result: {result['total']} analyzed")
    print("=" * 70)
    for row in ranked[:10]:
        score = row.get("score", {})
        emojis = ", ".join(s.get("emoji", "") for s in row.get("strategies", [])) or "-"
        print(
            f"{row['symbol']:6} ${row.get('price', 0):8.2f} | "
            f"inv {row.get('investability_score', 0):5.1f} | "
            f"quality {row.get('quality_score', 0):5.1f} | "
            f"score {score.get('total_score', 0):5.1f} | RSI {row.get('rsi', 50):5.1f} | "
            f"fin {row.get('financial_coverage', 0):.2f} | {emojis}"
        )
        plan = row.get("trade_plan", {})
        rr2 = plan.get("risk_reward", {}).get("rr2")
        stage = plan.get("positioning", {}).get("stage")
        pos_pct = plan.get("execution", {}).get("position_pct")
        liq = row.get("avg_dollar_volume_m", 0)
        evt = row.get("days_to_earnings")
        if rr2 is not None and stage:
            print(
                f"         entry {plan.get('entry', {}).get('buy2', 0):.2f} "
                f"stop {plan.get('stop_loss', 0):.2f} "
                f"t2 {plan.get('targets', {}).get('target2', 0):.2f} "
                f"RR2 {rr2:.2f} [{stage}] "
                f"size {float(pos_pct or 0):.1f}% "
                f"liq {float(liq):.1f}M "
                f"earnings D{evt if evt is not None else '-'}"
            )
    print("=" * 70)


def run_ai_once() -> None:
    from ai.analyzer import ai
    from config import load_all_us_stocks, load_stock_categories
    from core.news import get_bulk_news, get_market_news
    from core.signals import scan_stocks
    from core.stock_data import get_fear_greed_index, get_market_condition

    universe = load_all_us_stocks()
    categories = load_stock_categories()
    print(f"[{datetime.now()}] market analysis started...")
    print(f"[1/4] scanning symbols... ({len(universe)} tickers)")
    result = scan_stocks(universe)
    stocks = result["results"]
    print(
        f"  -> scan done: {len(stocks)} "
        f"(fundamentals enriched: {result.get('fundamentals_enriched', 0)})"
    )

    print("[2/4] loading market context...")
    market_data = {
        "fear_greed": get_fear_greed_index(),
        "market_condition": get_market_condition(),
        "market_news": get_market_news(),
    }
    print(
        "  -> "
        f"fear-greed {market_data['fear_greed'].get('score', 'N/A')}, "
        f"regime {market_data['market_condition'].get('message', 'N/A')}"
    )

    news_symbols = ai.select_news_symbols(stocks, limit=min(80, max(24, len(stocks) // 6)))
    print(f"[3/4] loading stock news... ({len(news_symbols)} tickers)")
    news_data = get_bulk_news(news_symbols, days=3)
    print(f"  -> news done: {len(news_data)} / requested {len(news_symbols)}")

    print("[4/4] generating analysis...")
    result = ai.analyze_full_market(stocks, news_data, market_data, categories)
    if "error" in result:
        print(f"analysis failed: {result['error']}")
        return

    print("\nAI market report")
    print("=" * 70)
    print(result["analysis"])
    print("=" * 70)

    stats = result.get("stats", {})
    if stats:
        print(f"\nsummary: {result.get('total', 0)} symbols")
        print(
            f"avg RSI {stats.get('avg_rsi', 0):.1f}, "
            f"avg score {stats.get('avg_score', 0):.1f}, "
            f"avg quality {stats.get('avg_quality', 0):.1f}, "
            f"avg inv {stats.get('avg_investability', 0):.1f}"
        )
        print(
            f"oversold {stats.get('oversold', 0)}, "
            f"overbought {stats.get('overbought', 0)}, "
            f"strong trend {stats.get('strong_trend', 0)}, "
            f"tradeable {stats.get('tradeable_count', 0)}"
        )


def run_backtest_once(limit: int = 40) -> None:
    from config import load_nasdaq_100
    from core.backtest import backtest_symbols

    symbols = load_nasdaq_100()[: max(1, limit)]
    print(f"[{datetime.now()}] backtest started... ({len(symbols)} tickers)")
    result = backtest_symbols(symbols, period="3y")

    summary = result.get("summary", {})
    ranked = result.get("ranked", [])
    print("\nbacktest summary")
    print("=" * 70)
    print(f"symbols: {summary.get('symbol_count', 0)}")
    print(f"avg win-rate: {summary.get('avg_win_rate', 0)}%")
    print(f"avg return/trade: {summary.get('avg_return', 0)}%")
    print(f"avg max-drawdown: {summary.get('avg_drawdown', 0)}%")
    print("-" * 70)
    for row in ranked[:10]:
        print(
            f"{row['symbol']:6} score {row['score']:6.2f} | "
            f"wr {row['win_rate']:5.1f}% | "
            f"avg {row['avg_return']:6.2f}% | "
            f"mdd {row['max_drawdown']:6.2f}% | trades {row['trade_count']:3}"
        )
    print("=" * 70)


def run_bot(with_scheduler: bool = True) -> None:
    from bot import run_bot

    run_bot(with_scheduler=with_scheduler)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Autostock runner")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--scan", action="store_true", help="Run one-time scan")
    mode.add_argument("--ai", action="store_true", help="Run one-time AI report")
    mode.add_argument("--backtest", action="store_true", help="Run one-time backtest")
    parser.add_argument("--no-schedule", action="store_true", help="Run bot without scheduler")
    parser.add_argument("--limit", type=int, default=50, help="Symbol limit for scan/backtest mode")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    _configure_console_output()
    args = _parse_args(argv or sys.argv[1:])

    if args.scan:
        run_scan_once(limit=args.limit)
        return
    if args.ai:
        run_ai_once()
        return
    if args.backtest:
        run_backtest_once(limit=args.limit)
        return

    run_bot(with_scheduler=not args.no_schedule)


if __name__ == "__main__":
    main()
