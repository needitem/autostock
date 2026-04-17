# Autostock

Autostock is a US equity research and backtesting project with Telegram UX.
The AI path uses **Codex CLI login only** (ChatGPT session), with no manual API keys.

## Scope

- Broad US stock scanning and scoring
- AI-written market/stock commentary across a wide stock universe
- Stock-first portfolio backtests targeting high return before defensive ETF fallbacks
- Telegram bot workflow for beginner-friendly operation

`News collection modules were removed from runtime and tests.`

## Current Direction

The current default research path is stock-first and broad-universe. Regime/ETF systems remain available as defensive alternatives, but they are no longer the only headline path.

- Default high-return baseline: `python scripts/run_strategy_v4_stock_momentum.py`
- Stock-first research loop: `python scripts/run_stock_hypothesis_promotion_loop.py`

The V4 stock-momentum runner now defaults to the broader `all_us` universe so the system can search a much wider price surface for higher-upside names.
The checked-in V4 baseline also keeps up to `2` names in weak `risk_off` weeks, freezes most new neutral entries when breadth slips, and adds a point-in-time filing quality bonus plus a neutral-regime veto for weak fresh filings. In the latest broad-universe simulation that moved the baseline to roughly `53.73% CAGR / 1.45 Sharpe / -33.81% MDD` versus `QQQ 18.96% / 0.94 / -34.47%`.

## Requirements

- Python 3.11+
- `codex` CLI installed
- ChatGPT login for Codex (`codex login`)
- Telegram bot token (only if running bot)

## Install

```bash
pip install -r requirements.txt
```

## Environment

Create `.env` with at least:

```env
TELEGRAM_BOT_TOKEN="your_telegram_bot_token"

# Codex CLI only (no API key flow)
AI_PROVIDER="codex-cli"
AI_MODEL="gpt-5.4"
CODEX_BIN="codex"

# If your shell has a dead local proxy like http://127.0.0.1:9
AI_DISABLE_PROXY=1

# Optional message style: beginner | standard | detail
BOT_MESSAGE_STYLE="beginner"

# Optional scheduler controls (Telegram bot)
BOT_TIMEZONE="Asia/Seoul"
US_REPORT_TIME="00:00"          # daily report
US_REBALANCE_TIME="00:10"       # weekly rebalance
US_REBALANCE_WEEKDAY="0"        # 0=Mon ... 6=Sun

# Optional Codex retry/fallback
AI_CLI_RETRIES=2
AI_CLI_RETRY_DELAY_SEC=1.5
AI_MODEL_FALLBACKS="gpt-5.4-pro,gpt-5.3,gpt-5.2"
AI_FALLBACK_ON_AI_FAIL=1

# Optional benchmark path
AI_BENCHMARK_SYMBOL="QQQ"
AI_MARKET_INDICATOR="QQQ"

# Optional KIS integration
KIS_APP_KEY=""
KIS_APP_SECRET=""
KIS_ACCOUNT_NO=""
KIS_ACCOUNT_PROD="01"
KIS_IS_PAPER="true"
```

## Codex Login

```bash
codex login
codex login status
```

`--ai` mode and AI backtests require a valid Codex login session.

## Run

Telegram bot (with scheduler):

```bash
python src/main.py
```

Telegram manual triggers (in chat):

- `/us_report` : run report now
- `/us_rebalance` : run rebalance now
- `/inventory_report` : run inventory report (beta)

Inventory report one-time (CLI):

```bash
python src/main.py --inventory-report
```

Telegram bot (without scheduler):

```bash
python src/main.py --no-schedule
```

One-time scan:

```bash
python src/main.py --scan --limit 50
```

One-time AI report:

```bash
python src/main.py --ai
```

One-time strategy backtest summary:

```bash
python src/main.py --backtest
python src/main.py --strategy-v4
```

## AI Portfolio Backtest (Primary)

This mode builds a long-only top-K portfolio each rebalance from chart/price features across a broad stock universe.

```bash
set AI_UNIVERSE=all_us
set AI_UNIVERSE_MODE=static

set AI_SNAPSHOT_FREQ=monthly
set AI_HORIZON_MODE=next_snapshot
set AI_PORTFOLIO_TOP_K=5

set AI_PROMPT_MAX_SYMBOLS=30
set AI_PROMPT_SELECT_MODE=top_rs63

set AI_TRADE_COST_BPS=20
set AI_SLIPPAGE_BPS=0
set AI_SPREAD_BPS=0
set AI_TAX_BPS=0

set AI_START_DATE=2016-01-01
set AI_END_DATE=2025-12-31

python scripts/backtest_ai_portfolio_selector.py
```

Outputs:

- `data/ai_portfolio_backtest_results.csv`
- `data/ai_portfolio_backtest_summary.json`
- `data/runs/ai_portfolio_backtest_results_<run_tag>.csv`
- `data/runs/ai_portfolio_backtest_summary_<run_tag>.json`

Verification:

```bash
python scripts/verify_ai_portfolio_backtest.py
```

Verification outputs:

- `data/ai_portfolio_backtest_verification.md`
- `data/ai_portfolio_backtest_verification.json`

## AI Chart Decision Backtest

```bash
set PYTHONPATH=src
python scripts/backtest_ai_chart_decisions.py
```

Useful knobs:

```bash
set AI_UNIVERSE=all_us
set AI_UNIVERSE_MODE=static

set AI_SNAPSHOT_FREQ=quarterly
set AI_HORIZON_MODE=next_snapshot
set AI_HORIZON_DAYS=63
set AI_START_DATE=2016-01-01
set AI_END_DATE=2025-12-31
set AI_PORTFOLIO_TOP_K=5
set AI_TRADE_COST_BPS=0
```

## Universe Builder

Build historical Nasdaq-100 constituents (time-varying snapshots):

```bash
python scripts/build_nasdaq100_universe_by_date.py
```

Common outputs:

- `data/universe/nasdaq100_by_date.json`
- `data/universe/nasdaq100_by_date_monthly.json`

## Data Sources

- OHLCV: `yfinance`
- Universe composition: Wikipedia-derived tables with local snapshots
- Market sentiment proxy: Fear & Greed API (through `core.stock_data`)

## Project Structure

```text
src/
  ai/
    analyzer.py
  bot/
    bot.py
    handlers.py
    keyboards.py
    formatters.py
  core/
    indicators.py
    scoring.py
    signals.py
    stock_data.py
    backtest.py
  trading/
    kis_api.py
    monitor.py
    portfolio.py
    watchlist.py
  config.py
  main.py
scripts/
  backtest_ai_chart_decisions.py
  backtest_ai_portfolio_selector.py
  verify_ai_portfolio_backtest.py
  build_nasdaq100_universe_by_date.py
```

## Disclaimer

This repository is for research and decision support.
All investment decisions and risk management are your responsibility.
