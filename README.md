# Autostock

Autostock is a US equity research and backtesting project with Telegram UX.
The AI path uses **Codex CLI login only** (ChatGPT session), with no manual API keys.

## Scope

- Stock scanning and scoring
- AI-written market/stock commentary (chart + regime driven)
- AI portfolio backtests targeting QQQ outperformance
- Telegram bot workflow for beginner-friendly operation

`News collection modules were removed from runtime and tests.`

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
AI_MODEL="gpt-5.2"
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
AI_MODEL_FALLBACKS="gpt-5.3"
AI_FALLBACK_ON_AI_FAIL=1

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
python src/main.py --backtest --limit 40
```

## AI Portfolio Backtest (Primary)

This mode builds a long-only top-K portfolio each rebalance from chart/regime features.

```bash
set AI_UNIVERSE=nasdaq100
set AI_UNIVERSE_MODE=by_date
set AI_UNIVERSE_BY_DATE_FILE=data/universe/nasdaq100_by_date_monthly.json

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
set AI_UNIVERSE=nasdaq100
set AI_UNIVERSE_MODE=by_date
set AI_UNIVERSE_BY_DATE_FILE=data/universe/nasdaq100_by_date.json

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

## Testing

Run all tests:

```bash
python -m pytest tests -q
```

Current suite: `122` passing tests (post news-module removal).

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
tests/
  ...
```

## Disclaimer

This repository is for research and decision support.
All investment decisions and risk management are your responsibility.
