# Autostock

Autostock is a Telegram-based US stock scanning assistant.
It focuses on a simple pipeline:

1. Scan a stock universe (Nasdaq-100 + S&P 500)
2. Rank candidates with technical + quality scores
3. Generate a market report using Codex CLI login (no API key)

## Requirements

- Python 3.11+
- `codex` CLI installed and logged in (for AI report)
- Telegram bot token

## Install

```bash
pip install -r requirements.txt
```

## Environment Variables

Create `.env` with at least:

```env
TELEGRAM_BOT_TOKEN="your_telegram_bot_token"

# AI uses Codex CLI login, not manual API keys.
AI_PROVIDER="codex-cli"
AI_MODEL="gpt-5.2"
CODEX_BIN="codex"

# Optional default Telegram message style
# compact | standard | detail
BOT_MESSAGE_STYLE="compact"

# Optional trading integration (KIS)
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

`--ai` mode and scheduled AI reports require a valid Codex login session.

## Run

Run Telegram bot (with scheduler):

```bash
python src/main.py
```

Run Telegram bot (without scheduler):

```bash
python src/main.py --no-schedule
```

One-time scan:

```bash
python src/main.py --scan --limit 50
```

One-time AI market report:

```bash
python src/main.py --ai
```

One-time backtest summary:

```bash
python src/main.py --backtest --limit 40
```

## Telegram UX

- `/start`: opens main menu
- `/menu`: reopen main menu
- `/style [compact|standard|detail]`: change message style
- `/scan`: quick ranked scan
- `/analyze <SYMBOL>`: single-symbol analysis
- Send a symbol directly (for example `AAPL`) to analyze quickly
- Main menu `표시 설정` button for style toggle

## Data Sources

- Price/OHLCV: `yfinance`
- Universe lists: Wikipedia (Nasdaq-100, S&P 500) with local cache
- News: Google News RSS fallback + optional enriched sources in core module
- Market sentiment: Fear & Greed index API

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
    news.py
    backtest.py
  trading/
    kis_api.py
    monitor.py
    portfolio.py
    watchlist.py
  config.py
  main.py
```

## Test

```bash
python -m pytest tests/ -q
```

## Disclaimer

This project is for research and decision support.
All investment decisions and risk management remain your responsibility.
