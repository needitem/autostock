"""
Autostock main entrypoint.

Usage:
  python src/main.py                  # run telegram bot
  python src/main.py --no-schedule    # run bot without scheduler
  python src/main.py --scan           # one-time scan
  python src/main.py --ai             # one-time market analysis
  python src/main.py --macro          # one-time macro pipeline report
  python src/main.py --deep           # deep research pipeline report
  python src/main.py --deep-us        # US-only free pipeline report
  python src/main.py --all-us         # US-only full run (engines + rendered report)
  python src/main.py --rebalance-us   # US-only rebalance using report + charts
  python src/main.py --backtest       # Strategy V2 baseline + verification
  python src/main.py --stock-backtest # Strategy V4 stock-momentum baseline + verification
  python src/main.py --legacy-backtest  # legacy ticker-level backtest
  python src/main.py --inventory-report  # inventory ledger/replenishment report (beta)
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
            f"RS63 {row.get('relative_strength_63d', 0):+5.1f} | "
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
    from core.signals import scan_stocks
    from core.stock_data import get_fear_greed_index, get_market_condition

    universe = load_all_us_stocks()
    categories = load_stock_categories()
    print(f"[{datetime.now()}] market analysis started...")
    print(f"[1/3] scanning symbols... ({len(universe)} tickers)")
    result = scan_stocks(universe)
    stocks = result["results"]
    print(
        f"  -> scan done: {len(stocks)} "
        f"(fundamentals enriched: {result.get('fundamentals_enriched', 0)})"
    )

    print("[2/3] loading market context...")
    market_data = {
        "fear_greed": get_fear_greed_index(),
        "market_condition": get_market_condition(),
    }
    print(
        "  -> "
        f"fear-greed {market_data['fear_greed'].get('score', 'N/A')}, "
        f"regime {market_data['market_condition'].get('message', 'N/A')}"
    )

    print("[3/3] generating analysis...")
    result = ai.analyze_full_market(stocks, {}, market_data, categories)
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


def run_strategy_v2_once(verify: bool = True) -> None:
    from pipelines.strategy_v2_pipeline import run_strategy_v2_pipeline

    print(f"[{datetime.now()}] strategy v2 baseline started...")
    result = run_strategy_v2_pipeline(run_verify=verify)
    summary = result.get("summary", {}) if isinstance(result, dict) else {}
    verification = result.get("verification", {}) if isinstance(result, dict) else {}

    metrics = verification.get("metrics", {}) if isinstance(verification, dict) else {}
    portfolio_metrics = summary.get("portfolio_metrics", {}) if isinstance(summary, dict) else {}
    ai = metrics.get("ai_portfolio") if isinstance(metrics.get("ai_portfolio"), dict) else portfolio_metrics.get("ai_portfolio", {})
    qqq = metrics.get("benchmark") if isinstance(metrics.get("benchmark"), dict) else portfolio_metrics.get("benchmark", {})
    alpha = verification.get("alpha", {}) if isinstance(verification, dict) else {}
    turnover = verification.get("turnover", {}) if isinstance(verification, dict) else {}
    turnover_ai = turnover.get("ai", {}) if isinstance(turnover.get("ai"), dict) else {}

    ai_cagr = float(ai.get("cagr_pct", 0.0) or 0.0)
    qqq_cagr = float(qqq.get("cagr_pct", 0.0) or 0.0)
    ai_mdd = float(ai.get("max_drawdown_pct", 0.0) or 0.0)
    qqq_mdd = float(qqq.get("max_drawdown_pct", 0.0) or 0.0)

    print("\nstrategy v2 baseline")
    print("=" * 70)
    print(f"run_tag: {summary.get('run_tag', '-')}")
    print(
        f"window: {summary.get('start_date', '-')} -> {summary.get('end_date', '-')} "
        f"({summary.get('snapshot_freq', '-')})"
    )
    print(f"engine: {summary.get('decision_engine', '-')}")
    print(f"strategy CAGR: {ai_cagr:.2f}% | Sharpe: {float(ai.get('sharpe', 0.0) or 0.0):.2f} | MDD: {ai_mdd:.2f}%")
    print(
        f"benchmark CAGR: {qqq_cagr:.2f}% | Sharpe: {float(qqq.get('sharpe', 0.0) or 0.0):.2f} | "
        f"MDD: {qqq_mdd:.2f}%"
    )
    print(f"CAGR diff: {ai_cagr - qqq_cagr:.2f}%p | MDD diff: {ai_mdd - qqq_mdd:.2f}%p")
    if alpha:
        print(
            "alpha stats: "
            f"NW p(two-sided)={float(alpha.get('nw_p_two_sided', 1.0) or 1.0):.3f} | "
            f"P(alpha>0)={float(alpha.get('nw_p_gt0', 0.5) or 0.5):.3f}"
        )
    if turnover_ai:
        print(f"avg turnover: {float(turnover_ai.get('mean', 0.0) or 0.0):.3f}")
    print(f"summary_json: {result.get('summary_path')}")
    if verify:
        print(f"verification_json: {result.get('verification_json_path')}")
        print(f"verification_md: {result.get('verification_md_path')}")
    print("=" * 70)


def run_strategy_v4_stock_once(verify: bool = True) -> None:
    from pipelines.strategy_v4_stock_pipeline import run_strategy_v4_stock_pipeline

    print(f"[{datetime.now()}] strategy v4 stock-momentum baseline started...")
    result = run_strategy_v4_stock_pipeline(run_verify=verify)
    summary = result.get("summary", {}) if isinstance(result, dict) else {}
    verification = result.get("verification", {}) if isinstance(result, dict) else {}

    metrics = verification.get("metrics", {}) if isinstance(verification, dict) else {}
    portfolio_metrics = summary.get("portfolio_metrics", {}) if isinstance(summary, dict) else {}
    ai = metrics.get("ai_portfolio") if isinstance(metrics.get("ai_portfolio"), dict) else portfolio_metrics.get("ai_portfolio", {})
    qqq = metrics.get("benchmark") if isinstance(metrics.get("benchmark"), dict) else portfolio_metrics.get("benchmark", {})
    alpha = verification.get("alpha", {}) if isinstance(verification, dict) else {}
    turnover = verification.get("turnover", {}) if isinstance(verification, dict) else {}
    turnover_ai = turnover.get("ai", {}) if isinstance(turnover.get("ai"), dict) else {}

    ai_cagr = float(ai.get("cagr_pct", 0.0) or 0.0)
    qqq_cagr = float(qqq.get("cagr_pct", 0.0) or 0.0)
    ai_mdd = float(ai.get("max_drawdown_pct", 0.0) or 0.0)
    qqq_mdd = float(qqq.get("max_drawdown_pct", 0.0) or 0.0)

    print("\nstrategy v4 stock-momentum baseline")
    print("=" * 70)
    print(f"run_tag: {summary.get('run_tag', '-')}")
    print(
        f"window: {summary.get('start_date', '-')} -> {summary.get('end_date', '-')} "
        f"({summary.get('snapshot_freq', '-')})"
    )
    print(f"engine: {summary.get('decision_engine', '-')}")
    print(f"universe: {summary.get('universe', '-')}")
    print(f"strategy CAGR: {ai_cagr:.2f}% | Sharpe: {float(ai.get('sharpe', 0.0) or 0.0):.2f} | MDD: {ai_mdd:.2f}%")
    print(
        f"benchmark CAGR: {qqq_cagr:.2f}% | Sharpe: {float(qqq.get('sharpe', 0.0) or 0.0):.2f} | "
        f"MDD: {qqq_mdd:.2f}%"
    )
    print(f"CAGR diff: {ai_cagr - qqq_cagr:.2f}%p | MDD diff: {ai_mdd - qqq_mdd:.2f}%p")
    if alpha:
        print(
            "alpha stats: "
            f"NW p(two-sided)={float(alpha.get('nw_p_two_sided', 1.0) or 1.0):.3f} | "
            f"P(alpha>0)={float(alpha.get('nw_p_gt0', 0.5) or 0.5):.3f}"
        )
    if turnover_ai:
        print(f"avg turnover: {float(turnover_ai.get('mean', 0.0) or 0.0):.3f}")
    print(f"summary_json: {result.get('summary_path')}")
    if verify:
        print(f"verification_json: {result.get('verification_json_path')}")
        print(f"verification_md: {result.get('verification_md_path')}")
    print("=" * 70)


def run_legacy_backtest_once(limit: int = 40) -> None:
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


def run_macro_once() -> None:
    from pipelines.us_macro_pipeline import run_us_macro_pipeline

    print(f"[{datetime.now()}] macro pipeline started...")
    result = run_us_macro_pipeline()
    report = result.get("report", {})
    risk = (report.get("risk_on_off") or {}).get("label", "unknown")
    score = (report.get("risk_on_off") or {}).get("score", "n/a")
    print(f"macro risk-on/off: {risk} (score={score})")
    print(f"json: {result.get('json_path')}")
    print(f"md: {result.get('md_path')}")


def run_deep_once() -> None:
    from pipelines.deep_research_pipeline import run_deep_research_pipeline

    print(f"[{datetime.now()}] deep research pipeline started...")
    result = run_deep_research_pipeline()
    report = result.get("report", {})
    risk = ((report.get("module1_liquidity") or {}).get("risk_on_off") or {}).get("us", {}).get("risk", {}).get("label", "unknown")
    print(f"deep research risk-on/off (US): {risk}")
    print(f"json: {result.get('json_path')}")
    print(f"md: {result.get('md_path')}")


def run_deep_us_once() -> None:
    from pipelines.us_free_pipeline import run_us_free_pipeline

    print(f"[{datetime.now()}] us free pipeline started...")
    result = run_us_free_pipeline()
    report = result.get("report", {})
    risk = (report.get("module1_liquidity") or {}).get("risk_on_off", {}).get("label", "unknown")
    print(f"us free risk-on/off: {risk}")
    if result.get("json_path"):
        print(f"json: {result.get('json_path')}")
    if result.get("md_path"):
        print(f"md: {result.get('md_path')}")


def run_all_us_once() -> None:
    from pipelines.us_orchestrator import run_all_us_engines

    print(f"[{datetime.now()}] us full run started...")
    result = run_all_us_engines()
    report_path = result.get("report_path", "")
    if report_path:
        print(f"report: {report_path}")


def run_rebalance_us_once() -> None:
    from pipelines.us_orchestrator import run_all_us_engines
    from pipelines.us_rebalance import run_us_rebalance

    print(f"[{datetime.now()}] us rebalance started...")
    report_result = run_all_us_engines()
    report_path = str(report_result.get("report_path", "") or "")
    result = run_us_rebalance(report_path if report_path else None)
    if report_path:
        print(f"source_report_json: {report_path}")
    print(f"orders_csv: {result.get('orders_csv')}")


def run_inventory_report_once() -> None:
    from pipelines.inventory_report import run_inventory_report

    print(f"[{datetime.now()}] inventory report started...")
    result = run_inventory_report()
    summary = result.get("summary", {}) if isinstance(result, dict) else {}
    print(f"inventory_report_json: {result.get('report_path')}")
    print(f"inventory_report_md: {result.get('md_path')}")
    if isinstance(summary, dict):
        print(
            "summary: "
            f"movements={summary.get('movement_count', 0)}, "
            f"balances={summary.get('balance_count', 0)}, "
            f"low_stock={summary.get('low_stock_count', 0)}, "
            f"snapshot={summary.get('channel_snapshot_count', 0)}, "
            f"mismatch={summary.get('mismatch_count', 0)}"
        )


def run_bot(with_scheduler: bool = True) -> None:
    from bot import run_bot

    run_bot(with_scheduler=with_scheduler)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Autostock runner")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--scan", action="store_true", help="Run one-time scan")
    mode.add_argument("--ai", action="store_true", help="Run one-time AI report")
    mode.add_argument("--macro", action="store_true", help="Run one-time macro pipeline")
    mode.add_argument("--deep", action="store_true", help="Run deep research pipeline")
    mode.add_argument("--deep-us", action="store_true", help="Run US-only free pipeline")
    mode.add_argument("--all-us", action="store_true", help="Run US-only full engines + report")
    mode.add_argument("--rebalance-us", action="store_true", help="Run US-only rebalance")
    mode.add_argument(
        "--backtest",
        "--strategy-v2",
        dest="backtest",
        action="store_true",
        help="Run Strategy V2 baseline + verification",
    )
    mode.add_argument("--stock-backtest", action="store_true", help="Run Strategy V4 stock-momentum baseline + verification")
    mode.add_argument("--legacy-backtest", action="store_true", help="Run legacy ticker-level backtest")
    mode.add_argument("--inventory-report", action="store_true", help="Run inventory report (beta)")
    parser.add_argument("--no-schedule", action="store_true", help="Run bot without scheduler")
    parser.add_argument("--limit", type=int, default=50, help="Symbol limit for scan/legacy-backtest mode")
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
    if args.macro:
        run_macro_once()
        return
    if args.deep:
        run_deep_once()
        return
    if args.deep_us:
        run_deep_us_once()
        return
    if args.all_us:
        run_all_us_once()
        return
    if args.rebalance_us:
        run_rebalance_us_once()
        return
    if args.backtest:
        run_strategy_v2_once(verify=True)
        return
    if args.stock_backtest:
        run_strategy_v4_stock_once(verify=True)
        return
    if args.legacy_backtest:
        run_legacy_backtest_once(limit=args.limit)
        return
    if args.inventory_report:
        run_inventory_report_once()
        return

    run_bot(with_scheduler=not args.no_schedule)


if __name__ == "__main__":
    main()
