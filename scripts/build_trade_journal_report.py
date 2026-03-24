from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yfinance as yf


ROOT = Path(__file__).resolve().parents[1]
RUNS_DIR = ROOT / "data" / "runs"
REPORTS_DIR = ROOT / "docs" / "reports"

INPUT_CSV = Path(
    os.getenv("TRADE_REPORT_RESULTS_CSV", str(ROOT / "data" / "ai_portfolio_backtest_results.csv"))
).resolve()
RUN_TAG = (os.getenv("TRADE_REPORT_RUN_TAG") or "").strip()
if not RUN_TAG:
    stem = INPUT_CSV.stem
    RUN_TAG = stem.replace("ai_portfolio_backtest_results_", "") if "ai_portfolio_backtest_results_" in stem else stem

OUT_MD = Path(
    os.getenv("TRADE_REPORT_OUT_MD", str(REPORTS_DIR / f"trade_journal_{RUN_TAG}.md"))
).resolve()
OUT_CSV = Path(
    os.getenv("TRADE_REPORT_TRADES_CSV", str(RUNS_DIR / f"trade_journal_trades_{RUN_TAG}.csv"))
).resolve()


def _f(x: Any, d: float = 0.0) -> float:
    try:
        y = float(x)
        return d if np.isnan(y) or np.isinf(y) else y
    except Exception:
        return d


def _risk_metrics(r: pd.Series, periods_per_year: int = 52) -> dict[str, float]:
    s = pd.to_numeric(r, errors="coerce").dropna().astype(float)
    if len(s) == 0:
        return {"periods": 0, "cagr_pct": 0.0, "total_return_pct": 0.0, "sharpe": 0.0, "max_drawdown_pct": 0.0}
    n = len(s)
    c = (1.0 + s).cumprod()
    total = float(c.iloc[-1] - 1.0)
    cagr = float(c.iloc[-1] ** (periods_per_year / n) - 1.0) if c.iloc[-1] > 0 else 0.0
    sd = float(s.std(ddof=1)) if n > 1 else 0.0
    sharpe = float((s.mean() / sd) * np.sqrt(periods_per_year)) if sd > 1e-12 else 0.0
    dd = (c / c.cummax()) - 1.0
    return {
        "periods": int(n),
        "cagr_pct": float(cagr * 100.0),
        "total_return_pct": float(total * 100.0),
        "sharpe": float(sharpe),
        "max_drawdown_pct": float(dd.min() * 100.0),
    }


def _load_positions(raw: Any) -> dict[str, float]:
    if isinstance(raw, dict):
        out = {}
        for k, v in raw.items():
            sym = str(k).strip().upper()
            if not sym:
                continue
            w = _f(v, 0.0)
            if w > 0:
                out[sym] = float(w)
        return out
    txt = str(raw or "").strip()
    if not txt:
        return {}
    try:
        obj = json.loads(txt)
    except Exception:
        return {}
    if not isinstance(obj, dict):
        return {}
    return _load_positions(obj)


def _download_open(symbols: list[str], start: str, end: str) -> dict[str, pd.Series]:
    raw = yf.download(
        tickers=sorted(set(symbols)),
        start=start,
        end=end,
        auto_adjust=False,
        progress=False,
        group_by="ticker",
        threads=True,
    )
    out: dict[str, pd.Series] = {}
    if not isinstance(raw.columns, pd.MultiIndex):
        return out
    for s in sorted(set(symbols)):
        if s not in raw.columns.get_level_values(0):
            continue
        f = raw[s].copy()
        if "Open" not in f.columns:
            continue
        out[s] = pd.to_numeric(f["Open"], errors="coerce").dropna().sort_index()
    return out


def _open_on_or_after(px: pd.Series, date: pd.Timestamp) -> tuple[pd.Timestamp | None, float | None]:
    if px.empty:
        return None, None
    idx = px.index
    p = int(idx.searchsorted(pd.Timestamp(date), side="left"))
    if p < 0 or p >= len(idx):
        return None, None
    d = pd.Timestamp(idx[p])
    v = _f(px.iloc[p], np.nan)
    if not np.isfinite(v) or v <= 0:
        return None, None
    return d, float(v)


@dataclass
class OpenTrade:
    symbol: str
    buy_day: pd.Timestamp
    buy_price: float
    buy_row_idx: int


def run() -> None:
    if not INPUT_CSV.exists():
        raise FileNotFoundError(f"Missing backtest result CSV: {INPUT_CSV}")
    df = pd.read_csv(INPUT_CSV)
    if df.empty:
        raise RuntimeError("Input CSV is empty")

    for c in ("entry_day", "exit_day"):
        if c not in df.columns:
            raise RuntimeError(f"Missing column in input CSV: {c}")
    df["entry_day"] = pd.to_datetime(df["entry_day"], errors="coerce")
    df["exit_day"] = pd.to_datetime(df["exit_day"], errors="coerce")
    df = df.dropna(subset=["entry_day", "exit_day"]).sort_values("entry_day").reset_index(drop=True)
    if df.empty:
        raise RuntimeError("No valid rows after date parsing")

    pos_col = "positions"
    if pos_col not in df.columns:
        raise RuntimeError("Missing positions column in input CSV")

    position_rows = [_load_positions(x) for x in df[pos_col].tolist()]
    all_syms = sorted({s for d in position_rows for s in d.keys()} | {"QQQ"})
    start = str((df["entry_day"].min() - pd.Timedelta(days=7)).date())
    end = str((df["exit_day"].max() + pd.Timedelta(days=7)).date())
    open_px = _download_open(all_syms, start=start, end=end)
    if "QQQ" not in open_px:
        raise RuntimeError("QQQ open-price series not available")

    open_trades: dict[str, OpenTrade] = {}
    closed_rows: list[dict[str, Any]] = []
    prev_set: set[str] = set()

    for i, row in df.iterrows():
        entry_day = pd.Timestamp(row["entry_day"])
        cur_set = {s for s in position_rows[i].keys() if s}

        sells = sorted(prev_set - cur_set)
        buys = sorted(cur_set - prev_set)

        # Execute sells at this week's entry open.
        for sym in sells:
            ot = open_trades.get(sym)
            if ot is None:
                continue
            px_s = open_px.get(sym)
            if px_s is None:
                continue
            sell_day, sell_px = _open_on_or_after(px_s, entry_day)
            q_buy_day, q_buy_px = _open_on_or_after(open_px["QQQ"], ot.buy_day)
            q_sell_day, q_sell_px = _open_on_or_after(open_px["QQQ"], entry_day)
            if sell_day is None or sell_px is None:
                continue
            if ot.buy_price <= 0:
                continue
            pnl_pct = (sell_px / ot.buy_price - 1.0) * 100.0
            qqq_pct = None
            if (
                q_buy_day is not None
                and q_sell_day is not None
                and q_buy_px is not None
                and q_sell_px is not None
                and q_buy_px > 0
            ):
                qqq_pct = (q_sell_px / q_buy_px - 1.0) * 100.0
            closed_rows.append(
                {
                    "symbol": sym,
                    "buy_day": str(pd.Timestamp(ot.buy_day).date()),
                    "buy_price": float(ot.buy_price),
                    "sell_day": str(pd.Timestamp(sell_day).date()),
                    "sell_price": float(sell_px),
                    "holding_weeks": int(max(1, i - ot.buy_row_idx)),
                    "pnl_pct": float(pnl_pct),
                    "qqq_same_period_pct": float(qqq_pct) if qqq_pct is not None else np.nan,
                    "alpha_vs_qqq_pctp": float(pnl_pct - qqq_pct) if qqq_pct is not None else np.nan,
                    "exit_reason": "rebalanced_out",
                }
            )
            open_trades.pop(sym, None)

        # Execute buys at this week's entry open.
        for sym in buys:
            px_b = open_px.get(sym)
            if px_b is None:
                continue
            buy_day, buy_px = _open_on_or_after(px_b, entry_day)
            if buy_day is None or buy_px is None:
                continue
            open_trades[sym] = OpenTrade(symbol=sym, buy_day=buy_day, buy_price=float(buy_px), buy_row_idx=i)

        prev_set = cur_set

    # Close remaining open positions at test end (last exit_day open)
    final_exit_day = pd.Timestamp(df["exit_day"].max())
    for sym, ot in sorted(open_trades.items()):
        px_s = open_px.get(sym)
        if px_s is None:
            continue
        sell_day, sell_px = _open_on_or_after(px_s, final_exit_day)
        q_buy_day, q_buy_px = _open_on_or_after(open_px["QQQ"], ot.buy_day)
        q_sell_day, q_sell_px = _open_on_or_after(open_px["QQQ"], final_exit_day)
        if sell_day is None or sell_px is None or ot.buy_price <= 0:
            continue
        pnl_pct = (sell_px / ot.buy_price - 1.0) * 100.0
        qqq_pct = None
        if (
            q_buy_day is not None
            and q_sell_day is not None
            and q_buy_px is not None
            and q_sell_px is not None
            and q_buy_px > 0
        ):
            qqq_pct = (q_sell_px / q_buy_px - 1.0) * 100.0
        closed_rows.append(
            {
                "symbol": sym,
                "buy_day": str(pd.Timestamp(ot.buy_day).date()),
                "buy_price": float(ot.buy_price),
                "sell_day": str(pd.Timestamp(sell_day).date()),
                "sell_price": float(sell_px),
                "holding_weeks": int(max(1, len(df) - ot.buy_row_idx)),
                "pnl_pct": float(pnl_pct),
                "qqq_same_period_pct": float(qqq_pct) if qqq_pct is not None else np.nan,
                "alpha_vs_qqq_pctp": float(pnl_pct - qqq_pct) if qqq_pct is not None else np.nan,
                "exit_reason": "end_of_test",
            }
        )

    trades = pd.DataFrame(closed_rows).sort_values(["buy_day", "symbol"]).reset_index(drop=True)
    trades.insert(0, "trade_id", np.arange(1, len(trades) + 1))
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    trades.to_csv(OUT_CSV, index=False)

    sr = pd.to_numeric(df["net_return_pct"], errors="coerce").fillna(0.0).astype(float) / 100.0
    br = pd.to_numeric(df["benchmark_return_pct"], errors="coerce").fillna(0.0).astype(float) / 100.0
    sm = _risk_metrics(sr, periods_per_year=52)
    bm = _risk_metrics(br, periods_per_year=52)

    df_year = df.copy()
    df_year["year"] = pd.to_datetime(df_year["entry_day"], errors="coerce").dt.year.astype(int)
    yearly_rows: list[dict[str, Any]] = []
    for y, g in df_year.groupby("year"):
        gr = pd.to_numeric(g["net_return_pct"], errors="coerce").fillna(0.0).astype(float) / 100.0
        gb = pd.to_numeric(g["benchmark_return_pct"], errors="coerce").fillna(0.0).astype(float) / 100.0
        yearly_rows.append(
            {
                "year": int(y),
                "weeks": int(len(g)),
                "strategy_total_pct": float(((1.0 + gr).prod() - 1.0) * 100.0),
                "qqq_total_pct": float(((1.0 + gb).prod() - 1.0) * 100.0),
                "diff_pctp": float((((1.0 + gr).prod() - 1.0) - ((1.0 + gb).prod() - 1.0)) * 100.0),
            }
        )
    yearly = pd.DataFrame(yearly_rows).sort_values("year")

    trade_summary = {
        "n_closed": int(len(trades)),
        "win_rate_pct": float((pd.to_numeric(trades.get("pnl_pct"), errors="coerce") > 0).mean() * 100.0)
        if len(trades)
        else 0.0,
        "avg_trade_pnl_pct": float(pd.to_numeric(trades.get("pnl_pct"), errors="coerce").mean()) if len(trades) else 0.0,
        "median_trade_pnl_pct": float(pd.to_numeric(trades.get("pnl_pct"), errors="coerce").median())
        if len(trades)
        else 0.0,
        "avg_alpha_vs_qqq_pctp": float(pd.to_numeric(trades.get("alpha_vs_qqq_pctp"), errors="coerce").mean())
        if len(trades)
        else 0.0,
    }

    lines: list[str] = []
    lines.append("# Weekly Rebalance Trade Journal (Codex Decision)\n")
    lines.append(f"- source run: `{INPUT_CSV.relative_to(ROOT) if INPUT_CSV.is_relative_to(ROOT) else INPUT_CSV}`")
    lines.append("- decision mode: `AI_DECISION_ENGINE=ai` (Codex weekly buy/sell)")
    lines.append("- features used for decision: chart indicators + market regime/context (VIX, breadth 등)")
    lines.append("- execution: weekly next-open")
    lines.append(f"- period: {pd.Timestamp(df['entry_day'].min()).date()} ~ {pd.Timestamp(df['exit_day'].max()).date()}\n")

    lines.append("## Headline Performance\n")
    lines.append(
        f"- Strategy: CAGR **{sm['cagr_pct']:.2f}%**, total **{sm['total_return_pct']:.2f}%**, "
        f"MDD **{sm['max_drawdown_pct']:.2f}%**, Sharpe **{sm['sharpe']:.2f}**"
    )
    lines.append(
        f"- QQQ: CAGR **{bm['cagr_pct']:.2f}%**, total **{bm['total_return_pct']:.2f}%**, "
        f"MDD **{bm['max_drawdown_pct']:.2f}%**, Sharpe **{bm['sharpe']:.2f}**"
    )
    lines.append(
        f"- Diff (Strategy - QQQ): CAGR **{(sm['cagr_pct']-bm['cagr_pct']):+.2f}%p**, "
        f"Total **{(sm['total_return_pct']-bm['total_return_pct']):+.2f}%p**, "
        f"MDD **{(sm['max_drawdown_pct']-bm['max_drawdown_pct']):+.2f}%p**\n"
    )

    lines.append("## Trade Summary\n")
    lines.append(f"- Closed trades: **{trade_summary['n_closed']}**")
    lines.append(f"- Win rate: **{trade_summary['win_rate_pct']:.2f}%**")
    lines.append(f"- Avg trade PnL: **{trade_summary['avg_trade_pnl_pct']:+.2f}%**")
    lines.append(f"- Median trade PnL: **{trade_summary['median_trade_pnl_pct']:+.2f}%**")
    lines.append(f"- Avg alpha vs QQQ (same holding window): **{trade_summary['avg_alpha_vs_qqq_pctp']:+.2f}%p**\n")

    lines.append("## Yearly Comparison (Strategy vs QQQ)\n")
    if not yearly.empty:
        lines.append("```text")
        lines.append(
            yearly[["year", "weeks", "strategy_total_pct", "qqq_total_pct", "diff_pctp"]].to_string(
                index=False, float_format=lambda x: f"{x:.2f}"
            )
        )
        lines.append("```")
    lines.append("")

    lines.append("## Trade Journal (buy/sell price & PnL)\n")
    if not trades.empty:
        lines.append("```text")
        show_cols = [
            "trade_id",
            "symbol",
            "buy_day",
            "buy_price",
            "sell_day",
            "sell_price",
            "holding_weeks",
            "pnl_pct",
            "qqq_same_period_pct",
            "alpha_vs_qqq_pctp",
            "exit_reason",
        ]
        lines.append(trades[show_cols].to_string(index=False, float_format=lambda x: f"{x:.3f}"))
        lines.append("```")
    else:
        lines.append("- no closed trade rows generated")
    lines.append("")

    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")

    print(f"Saved: {OUT_CSV.relative_to(ROOT)}")
    print(f"Saved: {OUT_MD.relative_to(ROOT)}")


if __name__ == "__main__":
    run()

