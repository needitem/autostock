from __future__ import annotations

import ast
import hashlib
import io
import json
import os
from contextlib import contextmanager, redirect_stderr, redirect_stdout
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yfinance as yf


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

DATA_DIR = ROOT / "data"
RUNS_DIR = DATA_DIR / "runs"
RESULT_CSV = DATA_DIR / "ai_portfolio_backtest_results.csv"
SUMMARY_JSON = DATA_DIR / "ai_portfolio_backtest_summary.json"
AI_CACHE = DATA_DIR / "ai_portfolio_backtest_cache.json"
AI_PARSE_FAIL_DIR = RUNS_DIR / "ai_parse_failures"
SECTOR_CACHE_JSON = DATA_DIR / "sector_cache.json"

RUN_TAG = (os.getenv("AI_RUN_TAG") or "").strip() or (
    datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"_{os.getpid()}"
)
RUN_RESULT_CSV = RUNS_DIR / f"ai_portfolio_backtest_results_{RUN_TAG}.csv"
RUN_SUMMARY_JSON = RUNS_DIR / f"ai_portfolio_backtest_summary_{RUN_TAG}.json"

DEFAULT_SYMBOLS = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AVGO", "AMD", "NFLX", "COST", "CSCO"]
BENCH = os.getenv("AI_BENCHMARK_SYMBOL", "QQQ").strip().upper() or "QQQ"
VIX = os.getenv("AI_VIX_SYMBOL", "^VIX").strip() or "^VIX"
MODEL = os.getenv("AI_MODEL", "gpt-5.4").strip() or "gpt-5.4"

PROMPT_VERSION = str(os.getenv("AI_PORTFOLIO_PROMPT_VERSION", "v1_momentum")).strip() or "v1_momentum"
SNAPSHOT_FREQ = (os.getenv("AI_SNAPSHOT_FREQ", "monthly") or "").strip().lower() or "monthly"
START_DATE = (os.getenv("AI_START_DATE", "2016-01-01") or "").strip() or "2016-01-01"
END_DATE = (os.getenv("AI_END_DATE", "2025-12-31") or "").strip() or "2025-12-31"
_NO_PROXY_MODE = str(os.getenv("AI_DISABLE_PROXY", "0")).strip().lower() in {"1", "true", "yes", "on"}

HORIZON_MODE = (os.getenv("AI_HORIZON_MODE", "next_snapshot") or "").strip().lower() or "next_snapshot"
if HORIZON_MODE not in {"fixed_days", "next_snapshot"}:
    HORIZON_MODE = "next_snapshot"
try:
    HORIZON_DAYS = int(str(os.getenv("AI_HORIZON_DAYS", "63")).strip() or "63")
except Exception:
    HORIZON_DAYS = 63
HORIZON_DAYS = max(5, min(252, HORIZON_DAYS))

try:
    MAX_SNAPSHOTS = int(str(os.getenv("AI_MAX_SNAPSHOTS", "0")).strip() or "0")
except Exception:
    MAX_SNAPSHOTS = 0
MAX_SNAPSHOTS = max(0, MAX_SNAPSHOTS)

try:
    MIN_HISTORY_DAYS = int(str(os.getenv("AI_MIN_HISTORY_DAYS", "220")).strip() or "220")
except Exception:
    MIN_HISTORY_DAYS = 220
MIN_HISTORY_DAYS = max(80, MIN_HISTORY_DAYS)


def _download_start_date() -> str:
    raw = (os.getenv("AI_DATA_START_DATE") or "").strip()
    if raw:
        return raw
    try:
        d = pd.Timestamp(START_DATE) - pd.DateOffset(years=3)
        return str(pd.Timestamp(d).date())
    except Exception:
        return "2000-01-01"


def _download_end_date() -> str:
    raw = (os.getenv("AI_DATA_END_DATE") or "").strip()
    if raw:
        return raw
    try:
        d = pd.Timestamp(END_DATE) + pd.DateOffset(years=1)
        return str(pd.Timestamp(d).date())
    except Exception:
        return "2026-12-31"


def _f(x: Any, d: float = 0.0) -> float:
    try:
        y = float(x)
        return d if np.isnan(y) or np.isinf(y) else y
    except Exception:
        return d


def _i_env(k: str, d: int) -> int:
    try:
        return int(os.getenv(k, str(d)))
    except Exception:
        return d


def _f_env(k: str, d: float) -> float:
    try:
        return float(os.getenv(k, str(d)))
    except Exception:
        return d


def _normalize_symbol(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    sym = value.strip().upper()
    if not sym:
        return None
    return sym.replace(".", "-")


def _parse_symbols(raw: str) -> list[str]:
    text = (raw or "").replace("\n", ",").replace(";", ",").replace("|", ",")
    parts = [p.strip() for p in text.split(",") if p.strip()]
    out: list[str] = []
    seen: set[str] = set()
    for p in parts:
        sym = _normalize_symbol(p)
        if not sym or sym in seen:
            continue
        seen.add(sym)
        out.append(sym)
    return out


@contextmanager
def _temporary_proxy_env():
    keys = (
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "GIT_HTTP_PROXY",
        "GIT_HTTPS_PROXY",
    )
    backup: dict[str, str | None] = {}
    try:
        for key in keys:
            val = os.getenv(key, "")
            if _NO_PROXY_MODE or (val and "127.0.0.1:9" in val):
                backup[key] = os.environ.get(key)
                os.environ[key] = ""
        yield
    finally:
        for key, old in backup.items():
            if old is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old


def _as_bool_env(k: str, d: bool) -> bool:
    raw = (os.getenv(k, str(int(d) if isinstance(d, bool) else str(d))).strip().lower())
    if not raw:
        return bool(d)
    return raw in {"1", "true", "yes", "on"}


def _load_json(path: Path) -> dict[str, Any]:
    try:
        if path.exists():
            with path.open("r", encoding="utf-8") as fh:
                obj = json.load(fh)
            if isinstance(obj, dict):
                return obj
    except Exception:
        pass
    return {}


def _save_json(path: Path, data: dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)


def _atomic_write_text(dst: Path, text: str) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.parent / (dst.name + f".tmp_{os.getpid()}")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, dst)


def _load_universe_by_date(path: Path) -> dict[str, list[str]]:
    raw = _load_json(path)
    if not raw:
        return {}
    if isinstance(raw.get("dates"), dict):
        raw = raw["dates"]
    out: dict[str, list[str]] = {}
    if not isinstance(raw, dict):
        return out
    for k, v in raw.items():
        if not isinstance(k, str) or not isinstance(v, list):
            continue
        syms: list[str] = []
        seen: set[str] = set()
        for item in v:
            sym = _normalize_symbol(item)
            if not sym or sym in seen:
                continue
            seen.add(sym)
            syms.append(sym)
        if syms:
            out[k.strip()] = syms
    return out


def _load_sector_lookup(path: Path | None = None) -> dict[str, dict[str, str]]:
    raw = _load_json(path or SECTOR_CACHE_JSON)
    data = raw.get("data") if isinstance(raw, dict) else {}
    if not isinstance(data, dict):
        return {}
    out: dict[str, dict[str, str]] = {}
    for key, meta in data.items():
        sym = _normalize_symbol(key)
        if not sym or not isinstance(meta, dict):
            continue
        out[sym] = {
            "sector": str(meta.get("sector") or "Unknown"),
            "industry": str(meta.get("industry") or "Unknown"),
        }
    return out


def _periods_per_year(freq: str) -> int:
    f = (freq or "").strip().lower()
    if f in {"m", "month", "months", "monthly"}:
        return 12
    if f in {"w", "week", "weeks", "weekly"}:
        return 52
    return 4


def _asof_pos(idx: pd.Index, dt: pd.Timestamp) -> int:
    try:
        pos = int(idx.searchsorted(pd.Timestamp(dt), side="right")) - 1
        return pos if pos >= 0 else -1
    except Exception:
        return -1


def _snapshot_dates() -> list[pd.Timestamp]:
    start = pd.Timestamp(START_DATE)
    end = pd.Timestamp(END_DATE)
    if end < start:
        start, end = end, start

    f = SNAPSHOT_FREQ
    if f in {"q", "quarter", "quarters", "quarterly"}:
        dates = list(pd.date_range(start=start, end=end, freq="QE-DEC"))
    elif f in {"m", "month", "months", "monthly"}:
        dates = list(pd.date_range(start=start, end=end, freq="ME"))
    elif f in {"w", "week", "weeks", "weekly"}:
        dates = list(pd.date_range(start=start, end=end, freq="W-FRI"))
    else:
        dates = list(pd.date_range(start=start, end=end, freq="ME"))

    if MAX_SNAPSHOTS > 0:
        dates = dates[-MAX_SNAPSHOTS:]
    return [pd.Timestamp(d).normalize() for d in dates]


def _last_day(frame: pd.DataFrame, dt: pd.Timestamp) -> pd.Timestamp | None:
    idx = frame.index[frame.index <= dt]
    return None if len(idx) == 0 else idx[-1]


def _ret_lookback(frame: pd.DataFrame, dt: pd.Timestamp, days: int) -> float:
    sd = _last_day(frame, dt)
    if sd is None:
        return 0.0
    i = int(frame.index.get_indexer([sd])[0])
    if i - days < 0:
        return 0.0
    p0, p1 = _f(frame.iloc[i - days]["Close"], -1.0), _f(frame.iloc[i]["Close"], -1.0)
    if p0 <= 0 or p1 <= 0:
        return 0.0
    return (p1 / p0 - 1.0) * 100.0


def _execution_pos(
    idx: pd.Index,
    signal_day: pd.Timestamp,
    execution_timing: str,
) -> int:
    """Return execution position from a signal day.

    - same_close: execute at signal-day close.
    - next_open: execute at next trading-day open.
    """
    signal_pos = _asof_pos(idx, signal_day)
    if signal_pos < 0:
        return -1
    mode = str(execution_timing or "same_close").strip().lower()
    if mode == "next_open":
        return signal_pos + 1
    return signal_pos


def _execution_price(frame: pd.DataFrame, pos: int, execution_timing: str) -> float:
    mode = str(execution_timing or "same_close").strip().lower()
    col = "Open" if mode == "next_open" else "Close"
    if pos < 0 or pos >= len(frame):
        return -1.0
    return _f(frame.iloc[pos].get(col), -1.0)


def _market_ctx(
    bench: pd.DataFrame,
    vix: pd.DataFrame | None,
    dt: pd.Timestamp,
    use_benchmark_features: bool = True,
) -> dict[str, Any] | None:
    sd = _last_day(bench, dt)
    if sd is None:
        return None
    hist = bench.loc[:sd]
    if hist.empty:
        return None
    close = _f(hist.iloc[-1]["Close"], 0.0)
    ma200 = _f(hist["Close"].rolling(200).mean().iloc[-1], close)
    r63 = _ret_lookback(bench, sd, 63)
    r21 = _ret_lookback(bench, sd, 21)
    v = None
    if vix is not None and not vix.empty:
        vd = _last_day(vix, sd)
        if vd is not None:
            v = _f(vix.loc[vd]["Close"], 0.0)

    regime = "neutral"
    if use_benchmark_features:
        if close >= ma200 and r63 >= 0 and (v is None or v <= 22):
            regime = "risk_on"
        elif (close < ma200 and r63 < 0) or (v is not None and v >= 28):
            regime = "risk_off"
        bench_r21 = r21
        bench_r63 = r63
    else:
        # Comparator/benchmark(QQQ) remains for evaluation only, not for model features.
        if v is not None and v >= 28:
            regime = "risk_off"
        elif v is not None and v <= 20:
            regime = "risk_on"
        bench_r21 = 0.0
        bench_r63 = 0.0

    return {
        "day": sd,
        "regime": regime,
        "bench_r21": bench_r21,
        "bench_r63": bench_r63,
        "vix_close": v,
        "algo_uses_benchmark": bool(use_benchmark_features),
    }


def _build_frames(symbols: list[str]) -> dict[str, pd.DataFrame]:
    tickers = sorted(set(symbols + [BENCH, VIX]))
    dl_start = _download_start_date()
    dl_end = _download_end_date()
    with _temporary_proxy_env():
        # yfinance prints noisy stderr for delisted/unmapped historical names
        # (for example ANSS in the by-date universe). The run already records
        # missing symbols separately, so suppress the terminal spam here.
        with io.StringIO() as _yf_stdout, io.StringIO() as _yf_stderr:
            with redirect_stdout(_yf_stdout), redirect_stderr(_yf_stderr):
                raw = yf.download(
                    tickers=tickers,
                    start=dl_start,
                    end=dl_end,
                    auto_adjust=False,
                    progress=False,
                    group_by="ticker",
                    threads=True,
                )
    out: dict[str, pd.DataFrame] = {}
    if not isinstance(raw.columns, pd.MultiIndex):
        return out
    for t in tickers:
        if t not in raw.columns.get_level_values(0):
            continue
        f = raw[t].copy()
        cols = ["Open", "High", "Low", "Close", "Volume"]
        if any(c not in f.columns for c in cols):
            continue
        out[t] = f[cols].dropna(subset=["Open", "High", "Low", "Close"]).sort_index()
    return out


def _indicator_frame(frame: pd.DataFrame) -> pd.DataFrame:
    from ta.momentum import RSIIndicator
    from ta.trend import ADXIndicator
    from ta.volatility import AverageTrueRange, BollingerBands

    cols = ["Open", "High", "Low", "Close", "Volume"]
    f = frame[cols].copy()
    f = f.dropna(subset=["Open", "High", "Low", "Close"]).sort_index()
    if f.empty:
        return pd.DataFrame()

    close = pd.to_numeric(f["Close"], errors="coerce").ffill().bfill()
    high = pd.to_numeric(f["High"], errors="coerce").ffill().bfill()
    low = pd.to_numeric(f["Low"], errors="coerce").ffill().bfill()
    volume = pd.to_numeric(f["Volume"], errors="coerce").fillna(0.0)

    ma50 = close.rolling(50).mean()
    ma100 = close.rolling(100).mean()
    ma125 = close.rolling(125).mean()
    ma150 = close.rolling(150).mean()
    ma175 = close.rolling(175).mean()
    ma200 = close.rolling(200).mean()
    ma200_30d_ago = ma200.shift(30)
    ma50_gap = (close / ma50 - 1.0) * 100.0
    ma100_gap = (close / ma100 - 1.0) * 100.0
    ma125_gap = (close / ma125 - 1.0) * 100.0
    ma150_gap = (close / ma150 - 1.0) * 100.0
    ma175_gap = (close / ma175 - 1.0) * 100.0
    ma200_gap = (close / ma200 - 1.0) * 100.0
    high_52w = high.rolling(252).max()
    low_52w = low.rolling(252).min()

    rsi = RSIIndicator(close, window=14).rsi()

    bb = BollingerBands(close, window=20, window_dev=2)
    bb_upper = bb.bollinger_hband()
    bb_lower = bb.bollinger_lband()
    bb_range = (bb_upper - bb_lower).replace(0.0, np.nan)
    bb_pos = ((close - bb_lower) / bb_range) * 100.0

    atr = AverageTrueRange(high, low, close, window=14).average_true_range()
    atr_pct = (atr / close.replace(0.0, np.nan)) * 100.0

    adx = ADXIndicator(high, low, close, window=14).adx()

    volume_avg = volume.rolling(window=20, min_periods=1).mean()
    volume_ratio = (volume / volume_avg.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan)

    ret21 = (close / close.shift(21) - 1.0) * 100.0
    ret63 = (close / close.shift(63) - 1.0) * 100.0
    vol20 = close.pct_change().rolling(20).std()
    dd_63 = (close / close.rolling(63).max()) - 1.0
    dd_252 = (close / close.rolling(252).max()) - 1.0

    out = pd.DataFrame(
        {
            "close": close,
            "volume": volume,
            "volume_ratio": volume_ratio,
            "ma50": ma50,
            "ma100": ma100,
            "ma125": ma125,
            "ma150": ma150,
            "ma175": ma175,
            "ma200": ma200,
            "ma200_30d_ago": ma200_30d_ago,
            "ma50_gap": ma50_gap,
            "ma100_gap": ma100_gap,
            "ma125_gap": ma125_gap,
            "ma150_gap": ma150_gap,
            "ma175_gap": ma175_gap,
            "ma200_gap": ma200_gap,
            "high_52w": high_52w,
            "low_52w": low_52w,
            "rsi": rsi,
            "return_21d": ret21,
            "return_63d": ret63,
            "vol_20": vol20,
            "dd_63": dd_63,
            "dd_252": dd_252,
            "bb_position": bb_pos,
            "atr_pct": atr_pct,
            "adx": adx,
        }
    )
    return out


def _load_symbols() -> tuple[str, list[str]]:
    raw = (os.getenv("AI_SYMBOLS") or "").strip()
    if raw:
        return ("custom", _parse_symbols(raw))

    universe = (os.getenv("AI_UNIVERSE") or "mega12").strip().lower() or "mega12"
    if universe in {"mega12", "default"}:
        return ("mega12", list(DEFAULT_SYMBOLS))

    if universe in {"nasdaq100", "nasdaq-100", "ndx"}:
        from config import load_nasdaq_100

        return ("nasdaq100", load_nasdaq_100())

    if universe in {"sp500", "s&p500", "snp500"}:
        from config import load_sp500

        return ("sp500", load_sp500())

    if universe in {"all_us", "all-us", "all"}:
        from config import load_all_us_stocks

        return ("all_us", load_all_us_stocks())

    return (universe, list(DEFAULT_SYMBOLS))


def _select_candidates(features: list[dict[str, Any]], max_symbols: int, mode: str) -> list[dict[str, Any]]:
    if not features:
        return []
    nmax = int(max_symbols)
    if nmax <= 0 or len(features) <= nmax:
        return features

    df = pd.DataFrame(features).copy()
    for col in ["relative_strength_63d", "relative_strength_21d", "volume_ratio", "adx", "rsi", "atr_pct", "ma50_gap"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        else:
            df[col] = 0.0

    pick_mode = str(mode or "top_rs63").strip().lower() or "top_rs63"
    if pick_mode not in {"mix", "top_rs63", "top_rs21", "top_vol"}:
        pick_mode = "top_rs63"

    if pick_mode == "top_rs21":
        order = df.sort_values(["relative_strength_21d", "relative_strength_63d"], ascending=False).index.tolist()
    elif pick_mode == "top_vol":
        order = df.sort_values(["volume_ratio", "relative_strength_63d"], ascending=False).index.tolist()
    else:
        order = df.sort_values(["relative_strength_63d", "relative_strength_21d"], ascending=False).index.tolist()

    if pick_mode != "mix":
        return [features[i] for i in order[:nmax]]

    n_pos = max(1, nmax // 2)
    n_neg = max(1, nmax // 4)
    n_vol = max(1, nmax - n_pos - n_neg)
    pos = df.nlargest(n_pos, "relative_strength_63d").index.tolist()
    neg = df.nsmallest(n_neg, "relative_strength_63d").index.tolist()
    vol = df.nlargest(n_vol, "volume_ratio").index.tolist()

    chosen: list[int] = []
    seen: set[int] = set()
    for i in pos + neg + vol + order:
        if i in seen:
            continue
        seen.add(i)
        chosen.append(i)
        if len(chosen) >= nmax:
            break
    return [features[i] for i in chosen]


def _select_candidates_with_includes(
    features: list[dict[str, Any]],
    max_symbols: int,
    mode: str,
    include_symbols: list[str] | None,
) -> list[dict[str, Any]]:
    """Select prompt candidates, ensuring currently held symbols remain visible to the model.

    This reduces churn by allowing the model to keep a position even if it fell just outside the
    selection filter.
    """
    base = _select_candidates(features, max_symbols, mode)
    include = [s for s in (include_symbols or []) if isinstance(s, str) and s.strip()]
    if not include:
        return base

    nmax = int(max_symbols)
    if nmax <= 0:
        nmax = 0

    feat_by_sym = {
        str(x.get("symbol")): x for x in features if isinstance(x, dict) and isinstance(x.get("symbol"), str)
    }

    chosen: dict[str, dict[str, Any]] = {}
    for x in base:
        if not isinstance(x, dict):
            continue
        sym = x.get("symbol")
        if isinstance(sym, str) and sym:
            chosen[sym] = x

    for sym in include:
        fx = feat_by_sym.get(sym)
        if fx is not None:
            chosen[sym] = fx

    if nmax <= 0 or len(chosen) <= nmax:
        return list(chosen.values())

    pick_mode = str(mode or "top_rs63").strip().lower() or "top_rs63"

    def _score(x: dict[str, Any]) -> tuple[float, float]:
        if pick_mode == "top_rs21":
            return (float(x.get("relative_strength_21d", 0.0)), float(x.get("relative_strength_63d", 0.0)))
        if pick_mode == "top_vol":
            return (float(x.get("volume_ratio", 0.0)), float(x.get("relative_strength_63d", 0.0)))
        return (float(x.get("relative_strength_63d", 0.0)), float(x.get("relative_strength_21d", 0.0)))

    locked = [sym for sym in include if sym in chosen]
    locked_feats = [chosen[sym] for sym in locked]
    others = [x for sym, x in chosen.items() if sym not in set(locked)]
    others_sorted = sorted(others, key=_score, reverse=True)

    if len(locked_feats) >= nmax:
        return locked_feats[:nmax]
    return locked_feats + others_sorted[: max(0, nmax - len(locked_feats))]


def _trend_template_checks(ind_row: pd.Series, rs63: float, rs63_min: float = 0.0) -> dict[str, Any]:
    price = _f(ind_row.get("close"), 0.0)
    ma50 = _f(ind_row.get("ma50"), 0.0)
    ma150 = _f(ind_row.get("ma150"), 0.0)
    ma200 = _f(ind_row.get("ma200"), 0.0)
    ma200_30d_ago = _f(ind_row.get("ma200_30d_ago"), ma200)
    high_52w = _f(ind_row.get("high_52w"), 0.0)
    low_52w = _f(ind_row.get("low_52w"), 0.0)

    checks = {
        "price_above_ma50_150_200": price > ma50 and price > ma150 and price > ma200,
        "ma_stack_50_150_200": ma50 > ma150 > ma200,
        "ma200_trend_up_30d": ma200 >= ma200_30d_ago,
        "above_52w_low_plus_30pct": low_52w > 0 and price >= low_52w * 1.30,
        "within_25pct_of_52w_high": high_52w > 0 and price >= high_52w * 0.75,
        "relative_strength_63d": float(rs63) >= float(rs63_min),
    }
    return {
        "pass": all(checks.values()),
        "checks": checks,
    }


def _dynamic_position_target(
    candidates: list[dict[str, Any]],
    base_top_k: int,
    market_regime: str,
    safe_mode: bool,
    require_risk_on: bool,
    min_positions: int,
) -> int:
    base = max(1, int(base_top_k))
    min_pos = max(0, int(min_positions))
    regime = str(market_regime or "neutral").strip().lower()

    if safe_mode and require_risk_on and regime != "risk_on":
        return 0
    if not candidates:
        return 0

    n = len(candidates)
    strong = sum(1 for x in candidates if float(x.get("relative_strength_63d", 0.0)) >= 5.0)
    trend_ok = sum(1 for x in candidates if bool(x.get("trend_template_pass", False)))
    quality = max(strong, trend_ok, min(base, n))

    if safe_mode:
        target = min(base, quality, n)
    else:
        target = min(base, n)
    if target <= 0:
        return 0
    return max(min_pos if min_pos > 0 else 1, target)


def _chart_rank_score(x: dict[str, Any], scoring_mode: str = "balanced") -> float:
    r63 = _f(x.get("return_63d"), 0.0)
    r21 = _f(x.get("return_21d"), 0.0)
    adx = _f(x.get("adx"), 0.0)
    rsi = _f(x.get("rsi"), 50.0)
    atr = _f(x.get("atr_pct"), 0.0)
    vol = _f(x.get("volume_ratio"), 1.0)
    ma50 = _f(x.get("ma50_gap"), 0.0)
    ma150 = _f(x.get("ma150_gap"), 0.0)
    ma200 = _f(x.get("ma200_gap"), 0.0)
    bb = _f(x.get("bb_position"), 50.0)

    mode = str(scoring_mode or "balanced").strip().lower() or "balanced"
    if mode == "pure_momo":
        trend_bonus = 5.0 if (ma50 > 0 and ma150 > 0 and ma200 > 0) else -3.0
        vol_bonus = max(-0.5, min(2.5, vol - 1.0)) * 2.0
        score = (
            r63 * 1.35
            + r21 * 0.70
            + adx * 0.20
            + trend_bonus
            + vol_bonus
            - atr * 0.70
        )
        return float(score)
    if mode == "low_vol_trend":
        trend_bonus = 8.0 if (ma50 > 0 and ma150 > 0 and ma200 > 0) else -10.0
        extension_penalty = max(0.0, rsi - 72.0) * 1.1 + max(0.0, ma50 - 10.0) * 0.60
        score = (
            r63 * 0.90
            + r21 * 0.45
            + adx * 0.35
            + trend_bonus
            - atr * 1.40
            - extension_penalty
        )
        return float(score)

    # balanced (default)
    trend_bonus = 7.0 if (ma50 > 0 and ma150 > 0 and ma200 > 0) else -8.0
    extension_penalty = max(0.0, rsi - 74.0) * 0.9 + max(0.0, ma50 - 12.0) * 0.45
    bb_penalty = abs(bb - 68.0) * 0.10
    vol_bonus = max(-0.5, min(2.5, vol - 1.0)) * 5.5
    score = (
        r63 * 1.10
        + r21 * 0.55
        + adx * 0.30
        + trend_bonus
        + vol_bonus
        - atr * 1.05
        - extension_penalty
        - bb_penalty
    )
    return float(score)


def _chart_momentum_portfolio(
    candidates: list[dict[str, Any]],
    top_k: int,
    weight_mode: str = "inv_vol",
    min_positions_for_invest: int = 2,
    scoring_mode: str = "balanced",
) -> dict[str, Any]:
    if not candidates or top_k <= 0:
        return {"positions": [], "cash_pct": 100.0, "_chart_mode": True, "_sit_out": True}

    eligible: list[dict[str, Any]] = []
    for row in candidates:
        if not isinstance(row, dict):
            continue
        r63 = _f(row.get("return_63d"), 0.0)
        atr = _f(row.get("atr_pct"), 0.0)
        rsi = _f(row.get("rsi"), 50.0)
        vol = _f(row.get("volume_ratio"), 1.0)
        trend_ok = bool(row.get("trend_template_pass", False)) or (
            _f(row.get("ma200_gap"), -99.0) > 0 and _f(row.get("ma50_gap"), -99.0) > 0
        )
        if not trend_ok:
            continue
        if r63 <= 0:
            continue
        if atr > 10.0:
            continue
        if not (38.0 <= rsi <= 80.0):
            continue
        if vol < 0.7:
            continue
        eligible.append(row)

    min_required = max(1, int(min_positions_for_invest))
    if len(eligible) < max(min_required, min(top_k, 4)):
        eligible = sorted(
            [x for x in candidates if isinstance(x, dict)],
            key=lambda x: (_f(x.get("return_63d"), -999.0), _f(x.get("return_21d"), -999.0)),
            reverse=True,
        )

    ranked = sorted(
        eligible,
        key=lambda x: (
            _chart_rank_score(x, scoring_mode=scoring_mode),
            _f(x.get("return_63d"), -999.0),
            _f(x.get("return_21d"), -999.0),
        ),
        reverse=True,
    )
    picked = ranked[: max(1, min(int(top_k), len(ranked)))]
    if not picked:
        return {"positions": [], "cash_pct": 100.0, "_chart_mode": True, "_sit_out": True}

    wmode = str(weight_mode or "inv_vol").strip().lower()
    if wmode not in {"inv_vol", "score", "equal"}:
        wmode = "inv_vol"
    raw_scores: dict[str, float] = {}
    if wmode == "equal":
        raw_scores = {str(x.get("symbol")): 1.0 for x in picked}
    elif wmode == "score":
        min_score = min(_chart_rank_score(x, scoring_mode=scoring_mode) for x in picked)
        raw_scores = {
            str(x.get("symbol")): max(0.1, _chart_rank_score(x, scoring_mode=scoring_mode) - min_score + 1.0)
            for x in picked
        }
    else:
        for x in picked:
            sym = str(x.get("symbol"))
            atr = max(0.6, _f(x.get("atr_pct"), 2.0))
            raw_scores[sym] = float(1.0 / atr)
    total = float(sum(raw_scores.values()))
    if total <= 0:
        w = 100.0 / float(len(picked))
        positions = [{"symbol": str(x.get("symbol")), "weight_pct": w} for x in picked]
    else:
        positions = [
            {"symbol": sym, "weight_pct": float(score / total * 100.0)}
            for sym, score in raw_scores.items()
            if sym
        ]
    return {"positions": positions, "cash_pct": 0.0, "_chart_mode": True, "_sit_out": False}


def _stock_momentum_portfolio(
    candidates: list[dict[str, Any]],
    top_k: int,
    weight_mode: str = "equal",
    min_positions_for_invest: int = 2,
    max_per_sector: int = 0,
    sector_bonus_mult: float = 0.0,
    sector_scores: dict[str, float] | None = None,
) -> dict[str, Any]:
    if not candidates or top_k <= 0:
        return {"positions": [], "cash_pct": 100.0, "_stock_momo_mode": True, "_sit_out": True}

    rows = [x for x in candidates if isinstance(x, dict) and x.get("symbol")]
    if not rows:
        return {"positions": [], "cash_pct": 100.0, "_stock_momo_mode": True, "_sit_out": True}

    sector_scores = dict(sector_scores or {})
    sector_bonus_mult = float(max(0.0, sector_bonus_mult))

    def _rank_key(x: dict[str, Any]) -> tuple[float, float, float, float]:
        sector = str(x.get("sector") or "Unknown")
        sector_score = float(sector_scores.get(sector, 0.0))
        composite = (
            _f(x.get("relative_strength_63d"), -999.0)
            + 0.35 * _f(x.get("relative_strength_21d"), -999.0)
            + sector_bonus_mult * sector_score
        )
        return (
            composite,
            _f(x.get("relative_strength_63d"), -999.0),
            _f(x.get("relative_strength_21d"), -999.0),
            _f(x.get("ma50_gap"), -999.0),
        )

    ranked = sorted(rows, key=_rank_key, reverse=True)

    picked: list[dict[str, Any]] = []
    sector_counts: dict[str, int] = {}
    sector_cap = max(0, int(max_per_sector))
    target = max(1, min(int(top_k), len(ranked)))
    for row in ranked:
        sector = str(row.get("sector") or "Unknown")
        if sector_cap > 0 and sector_counts.get(sector, 0) >= sector_cap:
            continue
        picked.append(row)
        sector_counts[sector] = sector_counts.get(sector, 0) + 1
        if len(picked) >= target:
            break
    if len(picked) < target:
        seen_syms = {str(x.get("symbol")) for x in picked}
        for row in ranked:
            sym = str(row.get("symbol"))
            if sym in seen_syms:
                continue
            picked.append(row)
            seen_syms.add(sym)
            if len(picked) >= target:
                break
    if len(picked) < max(1, int(min_positions_for_invest)):
        return {"positions": [], "cash_pct": 100.0, "_stock_momo_mode": True, "_sit_out": True}

    wmode = str(weight_mode or "equal").strip().lower()
    if wmode not in {"equal", "inv_vol", "score"}:
        wmode = "equal"
    raw_scores: dict[str, float] = {}
    if wmode == "equal":
        raw_scores = {str(x.get("symbol")): 1.0 for x in picked}
    elif wmode == "score":
        min_score = min(_f(x.get("relative_strength_63d"), 0.0) for x in picked)
        raw_scores = {
            str(x.get("symbol")): max(0.1, _f(x.get("relative_strength_63d"), 0.0) - min_score + 1.0)
            for x in picked
        }
    else:
        for x in picked:
            sym = str(x.get("symbol"))
            vol20 = _f(x.get("vol_20"), np.nan)
            if not np.isfinite(vol20):
                vol20 = max(0.6, _f(x.get("atr_pct"), 2.0)) / 100.0
            raw_scores[sym] = float(1.0 / max(0.0025, vol20))
    total = float(sum(raw_scores.values()))
    if total <= 0:
        w = 100.0 / float(len(picked))
        positions = [{"symbol": str(x.get("symbol")), "weight_pct": w} for x in picked]
    else:
        positions = [
            {"symbol": sym, "weight_pct": float(score / total * 100.0)}
            for sym, score in raw_scores.items()
            if sym
        ]
    return {"positions": positions, "cash_pct": 0.0, "_stock_momo_mode": True, "_sit_out": False}


def _sector_strength_scores(features: list[dict[str, Any]]) -> dict[str, float]:
    rows = [x for x in features if isinstance(x, dict) and x.get("sector")]
    if not rows:
        return {}

    df = pd.DataFrame(rows).copy()
    for col in ("relative_strength_63d", "relative_strength_21d", "ma200_gap", "ma50_gap"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        else:
            df[col] = 0.0
    if "sector" not in df.columns:
        return {}
    df["sector"] = df["sector"].astype(str).fillna("Unknown")

    out: dict[str, float] = {}
    for sector, grp in df.groupby("sector"):
        med_rs63 = float(grp["relative_strength_63d"].median())
        med_rs21 = float(grp["relative_strength_21d"].median())
        up200 = float((grp["ma200_gap"] > 0).mean())
        up50 = float((grp["ma50_gap"] > 0).mean())
        score = med_rs63 + 0.35 * med_rs21 + 6.0 * (up200 - 0.5) + 3.0 * (up50 - 0.5)
        out[str(sector)] = float(score)
    return out


def _ma_gap_for_window(row: dict[str, Any], ma_window: int) -> float:
    w = int(ma_window)
    key = f"ma{w}_gap"
    if key in row:
        return _f(row.get(key), -999.0)
    nearest = min([50, 100, 125, 150, 175, 200], key=lambda x: abs(x - w))
    return _f(row.get(f"ma{nearest}_gap"), -999.0)


def _ma_value_for_window(row: dict[str, Any], ma_window: int) -> float:
    w = int(ma_window)
    key = f"ma{w}"
    if key in row:
        return _f(row.get(key), np.nan)
    nearest = min([50, 100, 125, 150, 175, 200], key=lambda x: abs(x - w))
    return _f(row.get(f"ma{nearest}"), np.nan)


def _return_for_lookback(row: dict[str, Any], lookback: int) -> float:
    lb = int(lookback)
    key = f"return_{lb}d"
    if key in row:
        return _f(row.get(key), np.nan)
    nearest = min([21, 63], key=lambda x: abs(x - lb))
    return _f(row.get(f"return_{nearest}d"), np.nan)


def _parse_allocation_spec(raw: Any) -> dict[str, float]:
    if not isinstance(raw, str):
        return {}
    text = raw.strip()
    if not text:
        return {}

    weights: dict[str, float] = {}
    for token in re.split(r"[+,;|]", text):
        part = token.strip()
        if not part:
            continue
        match = re.match(r"^([^:=\s]+)\s*(?:[:=]\s*([-+]?\d*\.?\d+))?$", part)
        sym = _normalize_symbol(match.group(1) if match else part)
        if not sym:
            continue
        weight = _f(match.group(2), 1.0) if match and match.group(2) is not None else 1.0
        if weight <= 0:
            continue
        weights[sym] = float(weights.get(sym, 0.0) + weight)

    total = float(sum(weights.values()))
    if total <= 0:
        return {}
    return {sym: float(weight / total) for sym, weight in weights.items() if float(weight) > 0}


def _allocation_to_positions_pct(weights: dict[str, float]) -> list[dict[str, float]]:
    return [
        {"symbol": str(sym), "weight_pct": float(weight) * 100.0}
        for sym, weight in weights.items()
        if isinstance(sym, str) and sym and float(weight) > 0
    ]


def _regime_defensive_score(row: dict[str, Any]) -> float:
    r21 = _f(row.get("return_21d"), -999.0)
    r63 = _f(row.get("return_63d"), -999.0)
    ma200_gap = _f(row.get("ma200_gap"), -999.0)
    vol20 = _f(row.get("vol_20"), np.nan)
    dd252 = _f(row.get("dd_252"), np.nan)

    vol_penalty = float(vol20 * 100.0) if np.isfinite(vol20) else 8.0
    dd_penalty = float(abs(min(0.0, dd252)) * 100.0) if np.isfinite(dd252) else 12.0
    return float((0.65 * r21) + (0.35 * r63) + (0.50 * ma200_gap) - (0.75 * vol_penalty) - (0.25 * dd_penalty))


def _select_regime_defensive_allocation(
    by_symbol: dict[str, dict[str, Any]],
    pool_symbols: list[str] | None,
    top_n: int,
    fallback_alloc: dict[str, float] | None,
    min_ma_gap: float,
    min_ret21: float,
    min_ret63: float,
    max_vol: float,
    min_dd252: float,
    weight_mode: str,
) -> tuple[dict[str, float], str]:
    pool: list[str] = []
    for raw in list(pool_symbols or []):
        sym = _normalize_symbol(raw)
        if sym and sym not in pool:
            pool.append(sym)

    fallback = dict(fallback_alloc or {})
    if not pool:
        return fallback, "fallback"

    ranked: list[tuple[str, float, dict[str, Any]]] = []
    for sym in pool:
        row = by_symbol.get(sym) or {}
        if not row:
            continue
        ma200_gap = _f(row.get("ma200_gap"), -999.0)
        r21 = _f(row.get("return_21d"), -999.0)
        r63 = _f(row.get("return_63d"), -999.0)
        vol20 = _f(row.get("vol_20"), np.nan)
        dd252 = _f(row.get("dd_252"), np.nan)
        if ma200_gap < float(min_ma_gap):
            continue
        if r21 < float(min_ret21) or r63 < float(min_ret63):
            continue
        if float(max_vol) > 0 and np.isfinite(vol20) and vol20 > float(max_vol):
            continue
        if np.isfinite(dd252) and dd252 < float(min_dd252):
            continue
        ranked.append((sym, _regime_defensive_score(row), row))

    if not ranked:
        return fallback, "fallback"

    ranked.sort(
        key=lambda item: (
            float(item[1]),
            _f(item[2].get("return_21d"), -999.0),
            _f(item[2].get("return_63d"), -999.0),
            _f(item[2].get("ma200_gap"), -999.0),
        ),
        reverse=True,
    )
    picked = ranked[: max(1, min(int(top_n), len(ranked)))]
    mode = str(weight_mode or "inv_vol").strip().lower()
    if mode not in {"equal", "inv_vol", "score"}:
        mode = "inv_vol"

    raw_scores: dict[str, float] = {}
    if mode == "equal":
        raw_scores = {sym: 1.0 for sym, _, _ in picked}
    elif mode == "score":
        min_score = min(score for _, score, _ in picked)
        raw_scores = {sym: max(0.1, float(score - min_score + 1.0)) for sym, score, _ in picked}
    else:
        for sym, _, row in picked:
            vol20 = _f(row.get("vol_20"), np.nan)
            raw_scores[sym] = float(1.0 / max(0.0025, vol20 if np.isfinite(vol20) else 0.02))

    total = float(sum(raw_scores.values()))
    if total <= 0:
        return fallback, "fallback"
    alloc = {sym: float(score / total) for sym, score in raw_scores.items() if sym and float(score) > 0}
    picked_tag = "+".join(sym for sym, _, _ in picked)
    return alloc, f"dynamic:{picked_tag}"


def _regime_bucket_from_state(state_key: str) -> str:
    state = str(state_key or "neutral").strip().lower()
    if state in {"risk_on", "risk_on_alt"}:
        return "risk_on"
    if state in {"risk_off", "crash"}:
        return "risk_off"
    return "neutral"


def _regime_portfolio_from_features(
    by_symbol: dict[str, dict[str, Any]],
    prev_state: str | None,
    regime_source: str,
    ma_fast: int,
    ma_slow: int,
    mom_lb: int,
    mom_thr: float,
    risk_on_alloc: dict[str, float],
    risk_on_alt_alloc: dict[str, float],
    neutral_alloc: dict[str, float],
    recovery_alloc: dict[str, float],
    risk_off_alloc: dict[str, float],
    crash_alloc: dict[str, float],
    vol_cap: float,
    vol_low: float,
    vol_mid: float,
    mom_strong: float,
    crash_vol: float,
    crash_dd: float,
    hysteresis: float,
    recovery_slow_buffer: float,
    recovery_min_mom: float,
    recovery_max_vol: float,
    recovery_dd_floor: float,
    risk_on_filter_asset: str | None,
    risk_on_filter_ma: int,
    risk_on_filter_safe_alloc: dict[str, float],
    risk_off_dynamic: bool = False,
    risk_off_pool_symbols: list[str] | None = None,
    risk_off_top_n: int = 1,
    risk_off_min_ma_gap: float = 0.0,
    risk_off_min_ret21: float = 0.0,
    risk_off_min_ret63: float = 0.0,
    risk_off_max_vol: float = 0.0,
    risk_off_min_dd252: float = -1.0,
    risk_off_weight_mode: str = "inv_vol",
    risk_off_fallback_alloc: dict[str, float] | None = None,
    crash_dynamic: bool = False,
    crash_pool_symbols: list[str] | None = None,
    crash_top_n: int = 1,
    crash_min_ma_gap: float = 0.0,
    crash_min_ret21: float = 0.0,
    crash_min_ret63: float = 0.0,
    crash_max_vol: float = 0.0,
    crash_min_dd252: float = -1.0,
    crash_weight_mode: str = "inv_vol",
    crash_fallback_alloc: dict[str, float] | None = None,
) -> dict[str, Any]:
    alloc_by_state = {
        "risk_on": dict(risk_on_alloc),
        "risk_on_alt": dict(risk_on_alt_alloc or neutral_alloc),
        "neutral": dict(neutral_alloc or risk_off_alloc or risk_on_alt_alloc or risk_on_alloc),
        "recovery": dict(recovery_alloc or neutral_alloc or risk_on_alt_alloc or risk_off_alloc),
        "risk_off": dict(risk_off_alloc or neutral_alloc),
        "crash": dict(crash_alloc or risk_off_alloc or neutral_alloc),
    }

    source_row = by_symbol.get(regime_source) or {}
    price = _f(source_row.get("close"), np.nan)
    ma_fast_val = _ma_value_for_window(source_row, ma_fast) if source_row else np.nan
    ma_slow_val = _ma_value_for_window(source_row, ma_slow) if source_row else np.nan
    mom = (_return_for_lookback(source_row, mom_lb) / 100.0) if source_row else np.nan
    vol20 = _f(source_row.get("vol_20"), np.nan)
    dd252 = _f(source_row.get("dd_252"), np.nan)

    state_key = "crash"
    reason = "source_unavailable"
    if all(np.isfinite(x) for x in (price, ma_fast_val, ma_slow_val, mom, vol20, dd252)):
        risk_on_core = price > ma_fast_val and ma_fast_val > ma_slow_val and mom > float(mom_thr)
        risk_on_cond = risk_on_core and vol20 <= float(vol_cap)
        crash_cond = (price < ma_slow_val and dd252 <= float(crash_dd)) or (vol20 >= float(crash_vol))
        risk_off_cond = price < ma_slow_val or mom < -max(0.0, float(mom_thr))
        recovery_buffer = max(0.0, float(recovery_slow_buffer))
        recovery_cond = (
            price < ma_slow_val
            and price >= ma_slow_val * (1.0 - recovery_buffer)
            and mom >= float(recovery_min_mom)
            and vol20 <= float(recovery_max_vol)
            and dd252 >= float(recovery_dd_floor)
        )

        if crash_cond:
            state_key = "crash"
            reason = "crash"
        elif risk_on_cond:
            if vol20 <= float(vol_low) and mom >= float(mom_strong):
                state_key = "risk_on"
                reason = "risk_on"
            elif vol20 <= float(vol_mid):
                state_key = "risk_on_alt"
                reason = "risk_on_alt"
            else:
                state_key = "neutral"
                reason = "risk_on_neutralized"
        elif recovery_cond:
            state_key = "recovery"
            reason = "recovery"
        elif risk_off_cond:
            state_key = "risk_off"
            reason = "risk_off"
        else:
            state_key = "neutral"
            reason = "neutral"

        h = max(0.0, float(hysteresis))
        prev_state_key = str(prev_state or "").strip().lower()
        if prev_state_key in {"risk_on", "risk_on_alt"} and state_key not in {"risk_on", "risk_on_alt"}:
            if price > ma_slow_val and mom > (float(mom_thr) - h) and vol20 <= (float(vol_mid) + 0.005):
                state_key = prev_state_key if prev_state_key == "risk_on_alt" else "risk_on_alt"
                reason = "hold_risk_on_alt"
        elif prev_state_key in {"risk_off", "crash"} and state_key == "risk_on":
            if mom < (float(mom_strong) + h):
                state_key = "risk_on_alt"
                reason = "recovery_alt"
        elif prev_state_key == "neutral" and state_key in {"risk_off", "crash"}:
            if mom > -h and price >= ma_slow_val * 0.99:
                state_key = "neutral"
                reason = "hold_neutral"

        filt_asset = _normalize_symbol(risk_on_filter_asset)
        if filt_asset and int(risk_on_filter_ma) > 0 and state_key in {"risk_on", "risk_on_alt"}:
            filt_row = by_symbol.get(filt_asset) or {}
            filt_px = _f(filt_row.get("close"), np.nan)
            filt_ma = _ma_value_for_window(filt_row, int(risk_on_filter_ma)) if filt_row else np.nan
            if not np.isfinite(filt_px) or not np.isfinite(filt_ma) or filt_px <= filt_ma:
                state_key = "neutral"
                reason = "risk_filter_safe"
                if risk_on_filter_safe_alloc:
                    alloc_by_state["neutral"] = dict(risk_on_filter_safe_alloc)

    alloc = dict(alloc_by_state.get(state_key) or {})
    if state_key == "risk_off" and bool(risk_off_dynamic):
        alloc, defensive_reason = _select_regime_defensive_allocation(
            by_symbol=by_symbol,
            pool_symbols=risk_off_pool_symbols,
            top_n=max(1, int(risk_off_top_n)),
            fallback_alloc=risk_off_fallback_alloc or risk_off_alloc,
            min_ma_gap=float(risk_off_min_ma_gap),
            min_ret21=float(risk_off_min_ret21),
            min_ret63=float(risk_off_min_ret63),
            max_vol=float(risk_off_max_vol),
            min_dd252=float(risk_off_min_dd252),
            weight_mode=risk_off_weight_mode,
        )
        if defensive_reason:
            reason = f"{reason}:{defensive_reason}"
    elif state_key == "crash" and bool(crash_dynamic):
        alloc, defensive_reason = _select_regime_defensive_allocation(
            by_symbol=by_symbol,
            pool_symbols=crash_pool_symbols,
            top_n=max(1, int(crash_top_n)),
            fallback_alloc=crash_fallback_alloc or crash_alloc,
            min_ma_gap=float(crash_min_ma_gap),
            min_ret21=float(crash_min_ret21),
            min_ret63=float(crash_min_ret63),
            max_vol=float(crash_max_vol),
            min_dd252=float(crash_min_dd252),
            weight_mode=crash_weight_mode,
        )
        if defensive_reason:
            reason = f"{reason}:{defensive_reason}"
    positions = _allocation_to_positions_pct(alloc)
    return {
        "positions": positions,
        "cash_pct": 0.0 if positions else 100.0,
        "_regime_mode": True,
        "_regime_state": state_key,
        "_regime_bucket": _regime_bucket_from_state(state_key),
        "_regime_reason": reason,
        "_sit_out": not bool(positions),
    }


def _regime_from_universe_features(features: list[dict[str, Any]], vix_close: float | None) -> str:
    if not features:
        return "neutral"
    df = pd.DataFrame(features).copy()
    if df.empty:
        return "neutral"
    for col in ("ma200_gap", "ma50_gap", "return_63d", "return_21d"):
        if col not in df:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    up200 = float((df["ma200_gap"] > 0).mean())
    up50 = float((df["ma50_gap"] > 0).mean())
    med63 = float(df["return_63d"].median())
    med21 = float(df["return_21d"].median())
    v = _f(vix_close, 0.0) if vix_close is not None else None

    risk_on = up200 >= 0.58 and up50 >= 0.60 and med63 >= 2.5 and med21 >= 0.4 and (v is None or v <= 23)
    risk_off = up200 <= 0.42 or med63 <= -3.5 or med21 <= -1.2 or (v is not None and v >= 30)
    if risk_on:
        return "risk_on"
    if risk_off:
        return "risk_off"
    return "neutral"


def _market_breadth(features: list[dict[str, Any]]) -> dict[str, float]:
    if not features:
        return {"up200": 0.0, "up50": 0.0, "positive_63d": 0.0, "n": 0.0}
    df = pd.DataFrame(features).copy()
    if df.empty:
        return {"up200": 0.0, "up50": 0.0, "positive_63d": 0.0, "n": 0.0}
    for col in ("ma200_gap", "ma50_gap", "return_63d"):
        if col not in df:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return {
        "up200": float((df["ma200_gap"] > 0).mean()),
        "up50": float((df["ma50_gap"] > 0).mean()),
        "positive_63d": float((df["return_63d"] > 0).mean()),
        "n": float(len(df)),
    }


def _resolve_breadth_features(
    universe_features: list[dict[str, Any]],
    safe_features: list[dict[str, Any]],
    source_mode: str,
) -> list[dict[str, Any]]:
    mode = str(source_mode or "universe").strip().lower() or "universe"
    if mode in {"safe", "filtered"}:
        return safe_features if safe_features else universe_features
    return universe_features if universe_features else safe_features


def _resolve_momentum_blend_scope(
    decision_engine: str,
    sitout_only_flag: bool,
) -> str:
    raw = (os.getenv("AI_MOMENTUM_BLEND_SCOPE") or "").strip().lower()
    if raw in {"disabled", "sitout_only", "portfolio"}:
        return raw
    if bool(sitout_only_flag):
        return "sitout_only"
    # For AI runs, preserve the model-selected portfolio by default.
    if str(decision_engine or "").strip().lower() == "ai":
        return "disabled"
    return "portfolio"


def _ai_regime_target_cap(
    target_top_k: int,
    regime: str,
    safe_feats: list[dict[str, Any]],
    breadth: dict[str, float],
    vix_close: float | None,
    neutral_cap: int,
    neutral_min_up200: float,
    neutral_min_pos63: float,
    risk_off_cap: int,
    risk_off_min_up200: float,
    risk_off_min_pos63: float,
    risk_off_max_vix: float,
) -> int:
    regime_now = str(regime or "neutral").strip().lower()
    current = max(0, int(target_top_k))
    n_safe = len(safe_feats)
    if n_safe <= 0:
        return 0

    up200 = float(_f(breadth.get("up200"), 0.0))
    pos63 = float(_f(breadth.get("positive_63d"), 0.0))
    vix = _f(vix_close, np.nan)

    if regime_now == "neutral":
        if int(neutral_cap) <= 0:
            return current
        constructive = up200 >= float(neutral_min_up200) and pos63 >= float(neutral_min_pos63)
        if not constructive:
            return current
        cap = min(int(neutral_cap), n_safe)
        if current <= 0:
            return cap
        return min(current, cap)

    if regime_now == "risk_off":
        if int(risk_off_cap) <= 0:
            return current
        permissive = (
            up200 >= float(risk_off_min_up200)
            and pos63 >= float(risk_off_min_pos63)
            and (not np.isfinite(vix) or float(risk_off_max_vix) <= 0 or vix <= float(risk_off_max_vix))
        )
        if not permissive:
            return current
        cap = min(int(risk_off_cap), n_safe)
        if current <= 0:
            return cap
        return min(current, cap)

    return current


def _chart_candidates_with_holds(
    features: list[dict[str, Any]],
    max_symbols: int,
    include_symbols: list[str] | None,
    scoring_mode: str = "balanced",
) -> list[dict[str, Any]]:
    nmax = max(1, int(max_symbols))
    rows = [x for x in features if isinstance(x, dict)]
    rows_sorted = sorted(
        rows,
        key=lambda x: (
            _chart_rank_score(x, scoring_mode=scoring_mode),
            _f(x.get("return_63d"), -999.0),
            _f(x.get("return_21d"), -999.0),
        ),
        reverse=True,
    )
    chosen: dict[str, dict[str, Any]] = {}
    for x in rows_sorted[:nmax]:
        sym = str(x.get("symbol") or "")
        if sym:
            chosen[sym] = x
    include = [s for s in (include_symbols or []) if isinstance(s, str) and s.strip()]
    if include:
        by_sym = {str(x.get("symbol") or ""): x for x in rows if str(x.get("symbol") or "")}
        for sym in include:
            if sym in by_sym:
                chosen[sym] = by_sym[sym]
    if len(chosen) <= nmax:
        return list(chosen.values())
    # keep held names and fill remainder with highest chart score
    locked = [s for s in include if s in chosen]
    out: list[dict[str, Any]] = [chosen[s] for s in locked][:nmax]
    if len(out) >= nmax:
        return out[:nmax]
    for x in rows_sorted:
        sym = str(x.get("symbol") or "")
        if not sym or sym in locked:
            continue
        out.append(x)
        if len(out) >= nmax:
            break
    return out[:nmax]


def _extract_json(text: str) -> dict[str, Any] | None:
    if not text:
        return None
    text = text.strip().replace("```json", "").replace("```", "")
    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else None
    except Exception:
        pass
    decoder = json.JSONDecoder()
    for i, ch in enumerate(text):
        if ch != "{":
            continue
        try:
            obj, _ = decoder.raw_decode(text[i:])
            if isinstance(obj, dict):
                return obj
        except Exception:
            continue
    s, e = text.find("{"), text.rfind("}")
    if s >= 0 and e > s:
        cand = re.sub(r",\s*([}\]])", r"\1", text[s : e + 1])
        try:
            obj = json.loads(cand)
            return obj if isinstance(obj, dict) else None
        except Exception:
            pass
        try:
            obj = ast.literal_eval(cand)
            if isinstance(obj, dict):
                return json.loads(json.dumps(obj, ensure_ascii=False))
        except Exception:
            pass
    return None


def _fallback_portfolio_from_features(
    features: list[dict[str, Any]],
    top_k: int,
    max_weight_pct: float,
    prev_portfolio_pct: dict[str, float] | None,
) -> dict[str, Any]:
    if not features:
        raise ValueError("No candidate features for fallback portfolio")

    ordered = sorted(features, key=lambda x: float(x.get("relative_strength_63d", 0.0)), reverse=True)
    feature_by_symbol = {
        str((x.get("symbol") or "").upper()): x
        for x in ordered
        if isinstance(x, dict) and x.get("symbol")
    }
    feature_symbols = list(feature_by_symbol.keys())
    if not feature_symbols:
        raise ValueError("No symbols in fallback candidate pool")

    prev_syms: list[str] = []
    if isinstance(prev_portfolio_pct, dict):
        prev_syms = [
            str(s).upper()
            for s, w in prev_portfolio_pct.items()
            if isinstance(s, str)
            and s != "__CASH__"
            and float(w) > 0
            and str(s).upper() in feature_by_symbol
        ]
        prev_syms.sort(key=lambda s: -float(prev_portfolio_pct.get(s, 0.0)))

    picked: list[str] = []
    seen: set[str] = set()
    for sym in prev_syms + feature_symbols:
        us = str(sym).upper()
        if us in seen:
            continue
        seen.add(us)
        picked.append(us)
        if len(picked) >= max(1, min(top_k, len(feature_symbols))):
            break

    if not picked:
        raise ValueError("No symbols selected for fallback portfolio")

    n = len(picked)
    w = 100.0 / float(n)
    max_w = max(1.0, min(100.0, float(max_weight_pct)))
    w = min(w, max_w)
    positions = [{"symbol": s, "weight_pct": w} for s in picked]
    total = w * n
    cash = max(0.0, 100.0 - total)
    return {"positions": positions, "cash_pct": cash, "_fallback": True, "_fallback_reason": "ai_parse_failed"}


def _portfolio_prompt(
    snapshot_date: str,
    market_ctx: dict[str, Any],
    features: list[dict[str, Any]],
    top_k: int,
    max_weight_pct: float,
    trade_cost_bps: float,
    turnover_target_pct: float,
    prev_portfolio_pct: dict[str, float] | None,
    forced_sells: list[str] | None,
) -> str:
    lines: list[str] = []
    sorted_feats = sorted(features, key=lambda x: float(x.get("relative_strength_63d", 0.0)), reverse=True)
    for x in sorted_feats:
        lines.append(
            f"- {x['symbol']} rs63={x['relative_strength_63d']:+.2f}%p rs21={x.get('relative_strength_21d', 0.0):+.2f}%p "
            f"rsi={x['rsi']:.1f} adx={x['adx']:.1f} atr={x['atr_pct']:.2f}% ma50_gap={x['ma50_gap']:+.2f}% "
            f"bb={x['bb_position']:.1f} vol={x['volume_ratio']:.2f}"
        )

    vix = market_ctx.get("vix_close")
    vix_txt = "NA" if vix is None else f"{float(vix):.2f}"
    if HORIZON_MODE == "next_snapshot":
        horizon_txt = f"until next {SNAPSHOT_FREQ} rebalance"
    else:
        horizon_txt = (
            "a 3-month"
            if HORIZON_DAYS == 63
            else f"{HORIZON_DAYS}-trading-day (~{HORIZON_DAYS/21:.1f} month)"
        )

    prev_lines: list[str] = []
    ignored_prev: list[str] = []
    if isinstance(prev_portfolio_pct, dict) and prev_portfolio_pct:
        for sym, w in sorted(prev_portfolio_pct.items(), key=lambda kv: (-float(kv[1]), str(kv[0]))):
            if sym == "__CASH__":
                continue
            if float(w) <= 0:
                continue
            prev_lines.append(f"- {sym} {float(w):.2f}%")
    if forced_sells:
        ignored_prev = [s for s in forced_sells if isinstance(s, str) and s.strip()]

    max_w = float(max_weight_pct)
    if not np.isfinite(max_w) or max_w <= 0:
        max_w = 40.0
    max_w = max(1.0, min(100.0, max_w))

    cost_bps = float(trade_cost_bps)
    if not np.isfinite(cost_bps) or cost_bps < 0:
        cost_bps = 0.0

    turnover_txt = ""
    ttp = float(turnover_target_pct)
    if np.isfinite(ttp) and ttp > 0:
        ttp = max(1.0, min(100.0, ttp))
        turnover_txt = f"- Target turnover (soft): <= {ttp:.0f}% per rebalance.\n"

    prev_txt = ""
    if prev_lines:
        cash_w = float(prev_portfolio_pct.get("__CASH__", 0.0)) if isinstance(prev_portfolio_pct, dict) else 0.0
        prev_txt = (
            "Current portfolio (weights %):\n"
            f"{chr(10).join(prev_lines)}\n"
            f"- CASH {cash_w:.2f}%\n\n"
            "Turnover preference:\n"
            "- Minimize turnover; keep existing holdings unless there is a strong reason to replace.\n"
            "- If two options have similar momentum, prefer the one already held.\n"
            f"{turnover_txt}"
        )
        if ignored_prev:
            prev_txt += (
                "\nForced sells (not in candidates / not eligible):\n"
                + chr(10).join(f"- {s}" for s in ignored_prev[:12])
                + ("\n" if len(ignored_prev) <= 12 else "\n- ...\n")
            )
        prev_txt += "\n"

    bench_line = (
        f"Benchmark ret63: {float(market_ctx.get('bench_r63', 0.0)):.2f}%\n"
        if bool(market_ctx.get("algo_uses_benchmark", False))
        else "Benchmark-relative features: disabled (benchmark used for evaluation only)\n"
    )

    return (
        "You are a disciplined systematic trader.\n"
        "Goal: Build a LONG-ONLY portfolio that outperforms QQQ over the next holding period.\n"
        "Use only the inputs below (charts + regime). Ignore news and fundamentals.\n"
        "Be consistent: momentum (rs63/rs21) is the primary signal; use other indicators as risk filters.\n"
        f"Holding period: {horizon_txt}.\n\n"
        "Transaction costs:\n"
        f"- Estimated round-trip cost: {cost_bps:.0f} bps per 100% turnover.\n"
        "- Turnover = 0.5 * sum(|new_weight - current_weight|) across positions including cash.\n\n"
        f"{prev_txt}"
        "Constraints:\n"
        f"- Choose 1 to {int(top_k)} positions from the provided list.\n"
        f"- Max weight per position: {max_w:.0f}%.\n"
        "- Weights must sum to 100% including cash.\n"
        "- Cash is allowed, but prefer to stay mostly invested when regime is risk_on/neutral.\n"
        "- In risk_off or when VIX is high, you may increase cash.\n\n"
        "Output STRICT JSON only (no markdown):\n"
        '{"cash_pct":0-100,"positions":[{"symbol":"AAPL","weight_pct":0-100},...]}\n\n'
        f"Snapshot date: {snapshot_date}\n"
        f"Market regime: {market_ctx.get('regime', 'neutral')}\n"
        f"{bench_line}"
        f"VIX close: {vix_txt}\n\n"
        "Candidates:\n"
        f"{chr(10).join(lines)}\n"
    )


def _ai_portfolio(
    snapshot_date: str,
    market_ctx: dict[str, Any],
    features: list[dict[str, Any]],
    top_k: int,
    cache: dict[str, Any],
    max_weight_pct: float,
    trade_cost_bps: float,
    turnover_target_pct: float,
    prev_portfolio_pct: dict[str, float] | None,
    forced_sells: list[str] | None,
    fallback_on_fail: bool,
) -> dict[str, Any]:
    from ai.analyzer import AIAnalyzer

    analyzer = AIAnalyzer(model=MODEL)
    verbose_progress = _as_bool_env("AI_VERBOSE_PROGRESS", False)
    parse_repair_enabled = _as_bool_env("AI_PARSE_REPAIR", True)
    if not analyzer.has_api_access:
        raise RuntimeError("Codex login required. Run: codex login")

    prompt = _portfolio_prompt(
        snapshot_date,
        market_ctx,
        features,
        top_k=top_k,
        max_weight_pct=max_weight_pct,
        trade_cost_bps=trade_cost_bps,
        turnover_target_pct=turnover_target_pct,
        prev_portfolio_pct=prev_portfolio_pct,
        forced_sells=forced_sells,
    )
    ph = hashlib.sha256(prompt.encode("utf-8", errors="ignore")).hexdigest()[:12]
    cache_key = f"{MODEL}:{PROMPT_VERSION}:{snapshot_date}:{ph}"
    if cache_key in cache and isinstance(cache[cache_key], dict):
        if verbose_progress:
            print(f"[ai-cache-hit] {snapshot_date} symbols={len(features)}", flush=True)
        return cache[cache_key]

    raw_attempts: list[str] = []
    parsed: dict[str, Any] | None = None
    prompts = [
        prompt,
        prompt + "\nReturn one minified JSON object only.",
    ]
    for idx, prompt_try in enumerate(prompts, start=1):
        if verbose_progress:
            label = "[ai-call]" if idx == 1 else "[ai-retry-json]"
            print(f"{label} {snapshot_date} symbols={len(features)}", flush=True)
        raw = analyzer._call(prompt_try, max_tokens=1800)
        raw_attempts.append(raw or "")
        parsed = _extract_json(raw or "")
        if parsed:
            break

    if not parsed and parse_repair_enabled:
        for raw_text in reversed(raw_attempts):
            if not raw_text.strip():
                continue
            if verbose_progress:
                print(f"[ai-repair-json] {snapshot_date}", flush=True)
            repair_prompt = (
                "Convert the following model output into one strict minified JSON object only.\n"
                "Required schema:\n"
                '{"cash_pct":0-100,"positions":[{"symbol":"AAPL","weight_pct":0-100}]}\n'
                "Keep the original decision intent. Do not add commentary.\n\n"
                "Model output to repair:\n"
                f"{raw_text}"
            )
            repaired = analyzer._call(repair_prompt, max_tokens=600)
            raw_attempts.append(repaired or "")
            parsed = _extract_json(repaired or "")
            if parsed:
                break

    if not parsed:
        fail_name = f"{snapshot_date}_{ph}.txt".replace(":", "_")
        fail_path = AI_PARSE_FAIL_DIR / fail_name
        _atomic_write_text(
            fail_path,
            "\n\n===== ATTEMPT =====\n\n".join(x for x in raw_attempts if isinstance(x, str)),
        )
        if fallback_on_fail:
            if verbose_progress:
                print(f"[ai-fallback] {snapshot_date}", flush=True)
            return _fallback_portfolio_from_features(
                features,
                top_k=top_k,
                max_weight_pct=max_weight_pct,
                prev_portfolio_pct=prev_portfolio_pct,
            )
        raise RuntimeError(f"AI JSON parse failed for {snapshot_date} (saved {fail_path})")

    cache[cache_key] = parsed
    _save_json(AI_CACHE, cache)
    if verbose_progress:
        print(f"[ai-saved] {snapshot_date}", flush=True)
    return parsed


def _portfolio_from_ai(
    obj: dict[str, Any],
    allowed: set[str],
    top_k: int,
    max_weight_pct: float,
) -> tuple[dict[str, float], float]:
    pos = obj.get("positions")
    if not isinstance(pos, list):
        raise ValueError("positions missing")

    cash_in = _f(obj.get("cash_pct"), 0.0)
    if not np.isfinite(cash_in):
        cash_in = 0.0
    cash_in = max(0.0, min(100.0, float(cash_in)))

    weights: dict[str, float] = {}
    for row in pos:
        if not isinstance(row, dict):
            continue
        sym = _normalize_symbol(row.get("symbol"))
        if not sym or sym not in allowed:
            continue
        w = _f(row.get("weight_pct"), 0.0)
        if w <= 0:
            continue
        weights[sym] = weights.get(sym, 0.0) + float(w)

    if not weights:
        # Allow explicit cash-only output from model.
        if cash_in > 0.0:
            return {}, float(cash_in)
        raise ValueError("no valid weights")

    items = sorted(weights.items(), key=lambda x: x[1], reverse=True)
    if top_k > 0 and len(items) > top_k:
        items = items[:top_k]
    weights = {k: float(v) for k, v in items}

    max_w = float(max_weight_pct)
    if not np.isfinite(max_w) or max_w <= 0:
        max_w = 40.0
    max_w = max(1.0, min(100.0, max_w))
    clamped: dict[str, float] = {}
    for k, v in weights.items():
        clamped[k] = float(min(float(v), max_w))
    weights = clamped

    total = float(sum(weights.values()))
    if total <= 0:
        raise ValueError("sum weights <= 0")
    if total > 100.0:
        scale = 100.0 / total
        weights = {k: float(v) * scale for k, v in weights.items()}
        total = 100.0

    cash = float(100.0 - total)
    if cash < 0:
        cash = 0.0
    return weights, cash


def _apply_regime_exposure(
    weights_pct: dict[str, float],
    market_ctx: dict[str, Any],
    on_exposure_pct: float,
    risk_on_alt_exposure_pct: float,
    neutral_exposure_pct: float,
    recovery_exposure_pct: float,
    risk_off_exposure_pct: float,
    crash_exposure_pct: float,
    risk_off_vix_threshold: float,
    risk_off_vix_hard_exposure_pct: float,
    risk_off_vix_extreme: float,
    risk_off_vix_extreme_exposure_pct: float,
) -> tuple[dict[str, float], float]:
    if not weights_pct:
        return {}, 100.0

    regime = str(market_ctx.get("regime", "neutral")).lower()
    state_key = str(market_ctx.get("regime_state", regime)).lower()
    vix = market_ctx.get("vix_close")

    on_exposure_pct = max(0.0, min(100.0, _f(on_exposure_pct, 100.0)))
    risk_on_alt_exposure_pct = max(0.0, min(100.0, _f(risk_on_alt_exposure_pct, on_exposure_pct)))
    neutral_exposure_pct = max(0.0, min(100.0, _f(neutral_exposure_pct, on_exposure_pct)))
    recovery_exposure_pct = max(0.0, min(100.0, _f(recovery_exposure_pct, neutral_exposure_pct)))
    risk_off_exposure_pct = max(0.0, min(100.0, _f(risk_off_exposure_pct, neutral_exposure_pct)))
    crash_exposure_pct = max(0.0, min(100.0, _f(crash_exposure_pct, risk_off_exposure_pct)))
    risk_off_vix_threshold = _f(risk_off_vix_threshold, 28.0)
    risk_off_vix_extreme = _f(risk_off_vix_extreme, 34.0)
    risk_off_vix_hard_exposure_pct = max(0.0, min(100.0, _f(risk_off_vix_hard_exposure_pct, risk_off_exposure_pct)))
    risk_off_vix_extreme_exposure_pct = max(
        0.0, min(100.0, _f(risk_off_vix_extreme_exposure_pct, risk_off_vix_hard_exposure_pct))
    )

    if state_key == "risk_on":
        target_exp = on_exposure_pct
    elif state_key == "risk_on_alt":
        target_exp = risk_on_alt_exposure_pct
    elif state_key == "recovery":
        target_exp = recovery_exposure_pct
    elif state_key in {"risk_off", "crash"} or regime == "risk_off":
        v = _f(vix, 0.0)
        target_exp = risk_off_exposure_pct if state_key != "crash" else crash_exposure_pct
        if np.isfinite(v) and v >= risk_off_vix_extreme:
            target_exp = min(target_exp, risk_off_vix_extreme_exposure_pct)
        elif np.isfinite(v) and v >= risk_off_vix_threshold:
            target_exp = min(target_exp, risk_off_vix_hard_exposure_pct)
    else:
        target_exp = neutral_exposure_pct

    curr_exp = float(sum(weights_pct.values()))
    if curr_exp <= 0.0:
        return {}, 100.0

    scale = target_exp / curr_exp
    out = {k: float(v) * scale for k, v in weights_pct.items()}
    tgt_cash = 100.0 - float(sum(out.values()))
    if tgt_cash < 0.0:
        tgt_cash = 0.0
    return out, tgt_cash


def _enforce_min_overlap(
    weights_pct: dict[str, float],
    prev_port: dict[str, float],
    allowed: set[str],
    feats_by_symbol: dict[str, dict[str, Any]] | None,
    min_overlap: int,
    top_k: int,
) -> dict[str, float]:
    """Force-keep at least N previously held names (soft safety rail).

    This is a simple guard against full churn; it swaps the smallest new positions
    with the strongest previously held names (by current rs63).
    """
    n = int(min_overlap)
    if n <= 0 or top_k <= 0:
        return weights_pct

    prev_syms = [
        s
        for s, w in (prev_port or {}).items()
        if isinstance(s, str) and s != "__CASH__" and float(w) > 0 and s in allowed
    ]
    if not prev_syms:
        return weights_pct

    target = min(int(top_k), n, len(prev_syms))
    if target <= 0:
        return weights_pct

    new_syms = [s for s in weights_pct.keys() if s in allowed]
    overlap = len(set(new_syms) & set(prev_syms))
    if overlap >= target:
        return weights_pct

    need = target - overlap

    add_pool = [s for s in prev_syms if s not in weights_pct]
    if feats_by_symbol:
        add_pool.sort(
            key=lambda s: float((feats_by_symbol.get(s) or {}).get("relative_strength_63d", -1e9)),
            reverse=True,
        )
    else:
        add_pool.sort(key=lambda s: -float((prev_port or {}).get(s, 0.0)))

    prev_set = set(prev_syms)
    drop_pool = [s for s in list(weights_pct.keys()) if s not in prev_set]
    drop_pool.sort(
        key=lambda s: (
            float(weights_pct.get(s, 0.0)),
            float((feats_by_symbol.get(s) or {}).get("relative_strength_63d", 0.0)) if feats_by_symbol else 0.0,
        )
    )

    out = dict(weights_pct)
    while need > 0 and add_pool and drop_pool:
        add = add_pool.pop(0)
        drop = drop_pool.pop(0)
        w = float(out.get(drop, 0.0))
        if w <= 0:
            continue
        out.pop(drop, None)
        out[add] = float(out.get(add, 0.0) + w)
        need -= 1
    return out


def _turnover(prev_w: dict[str, float], new_w: dict[str, float]) -> float:
    keys = set(prev_w) | set(new_w)
    return float(0.5 * sum(abs(new_w.get(k, 0.0) - prev_w.get(k, 0.0)) for k in keys))


def _blend_portfolios(
    core_port: dict[str, float],
    mom_port: dict[str, float],
    blend_ratio: float,
    sitout_only: bool,
    sit_out_flag: bool,
) -> dict[str, float]:
    b = max(0.0, min(1.0, float(blend_ratio)))
    if b <= 0.0:
        return dict(core_port)
    if sitout_only and not bool(sit_out_flag):
        return dict(core_port)

    keys = set(core_port) | set(mom_port)
    out: dict[str, float] = {}
    for k in keys:
        w = (1.0 - b) * float(core_port.get(k, 0.0)) + b * float(mom_port.get(k, 0.0))
        if w > 1e-12:
            out[k] = float(w)

    cash = float(out.get("__CASH__", 0.0))
    if cash < 0.0:
        cash = 0.0
    out["__CASH__"] = cash

    total = float(sum(v for k, v in out.items() if k != "__CASH__")) + cash
    if total <= 0.0:
        return {"__CASH__": 1.0}
    if abs(total - 1.0) > 1e-12:
        out = {k: float(v) / total for k, v in out.items()}
    return out


def _risk_metrics(series_pct: pd.Series, periods_per_year: int) -> dict[str, float]:
    s = pd.to_numeric(series_pct, errors="coerce").dropna().astype(float)
    if len(s) == 0:
        return {
            "periods": 0,
            "mean_period_return_pct": 0.0,
            "cagr_pct": 0.0,
            "vol_annual_pct": 0.0,
            "sharpe": 0.0,
            "sortino": 0.0,
            "max_drawdown_pct": 0.0,
            "win_rate_pct": 0.0,
            "total_return_pct": 0.0,
        }
    r = s / 100.0
    n = len(r)
    ppy = max(1, int(periods_per_year))
    c = (1 + r).cumprod()
    tot = float(c.iloc[-1] - 1.0)
    cagr = float(c.iloc[-1] ** (ppy / n) - 1.0) if c.iloc[-1] > 0 else 0.0
    sharpe, vol = 0.0, 0.0
    if n > 1:
        sd = float(r.std(ddof=1))
        if sd > 1e-12:
            vol = float(sd * np.sqrt(ppy))
            sharpe = float((r.mean() / sd) * np.sqrt(ppy))
    sortino = 0.0
    dn = r[r < 0]
    if len(dn) > 1:
        dsd = float(dn.std(ddof=1))
        if dsd > 1e-12:
            sortino = float((r.mean() / dsd) * np.sqrt(ppy))
    dd = (c / c.cummax()) - 1.0
    return {
        "periods": int(n),
        "mean_period_return_pct": float(r.mean() * 100.0),
        "cagr_pct": float(cagr * 100.0),
        "vol_annual_pct": float(vol * 100.0),
        "sharpe": float(sharpe),
        "sortino": float(sortino),
        "max_drawdown_pct": float(dd.min() * 100.0),
        "win_rate_pct": float((r > 0).mean() * 100.0),
        "total_return_pct": float(tot * 100.0),
    }


def run() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    cache = _load_json(AI_CACHE)
    sector_lookup = _load_sector_lookup()

    snaps = _snapshot_dates()
    if HORIZON_MODE == "next_snapshot" and len(snaps) < 2:
        raise RuntimeError("Not enough snapshots for next_snapshot horizon")

    universe_name, symbols = _load_symbols()
    universe_limit = _i_env("AI_UNIVERSE_LIMIT", 0)
    symbols = [s for s in symbols if isinstance(s, str) and s.strip()]
    if not symbols:
        raise RuntimeError("No symbols loaded (AI_UNIVERSE / AI_SYMBOLS)")

    universe_mode = str(os.getenv("AI_UNIVERSE_MODE", "static")).strip().lower() or "static"
    by_date_file = (os.getenv("AI_UNIVERSE_BY_DATE_FILE") or "").strip()
    universe_by_date: dict[str, list[str]] = {}
    if universe_mode in {"by_date", "by-date", "historical", "history"} and by_date_file:
        up = Path(by_date_file)
        if not up.is_absolute():
            up = ROOT / up
        if up.exists():
            universe_by_date = _load_universe_by_date(up)

    if universe_by_date:
        want = {str(d.date()) for d in snaps}
        universe_by_date = {k: v for k, v in universe_by_date.items() if k in want and v}
        union_syms = sorted({s for arr in universe_by_date.values() for s in (arr or [])})
        if union_syms:
            symbols = union_syms
            universe_name = f"{universe_name}_by_date"

    if universe_limit > 0:
        symbols = symbols[:universe_limit]

    requested_symbols = list(symbols)
    prefetch_decision_engine = str(os.getenv("AI_DECISION_ENGINE", "chart")).strip().lower() or "chart"
    prefetch_symbols: list[str] = []
    if prefetch_decision_engine == "trend":
        for env_key, default_symbol in (
            ("AI_TREND_RISK_SYMBOL", "TQQQ"),
            ("AI_TREND_MID_SYMBOL", "QQQ"),
            ("AI_TREND_SAFE_SYMBOL", "BIL"),
            ("AI_TREND_ALT_SYMBOL", "QLD"),
        ):
            sym = _normalize_symbol(os.getenv(env_key, default_symbol))
            if sym:
                prefetch_symbols.append(sym)
    elif prefetch_decision_engine == "regime":
        regime_prefetch_specs = [
            os.getenv("AI_REGIME_RISK_ON", "TQQQ"),
            os.getenv("AI_REGIME_RISK_ON_ALT", "QLD"),
            os.getenv("AI_REGIME_NEUTRAL", "QLD"),
            os.getenv("AI_REGIME_RECOVERY", ""),
            os.getenv("AI_REGIME_RISK_OFF", "GLD"),
            os.getenv("AI_REGIME_CRASH", "GLD"),
            os.getenv("AI_REGIME_FILTER_SAFE", "BIL"),
            os.getenv("AI_REGIME_RISK_OFF_FALLBACK", ""),
            os.getenv("AI_REGIME_CRASH_FALLBACK", ""),
        ]
        for spec in regime_prefetch_specs:
            prefetch_symbols.extend(_parse_allocation_spec(spec).keys())
        for raw in (
            os.getenv("AI_REGIME_RISK_OFF_POOL", ""),
            os.getenv("AI_REGIME_CRASH_POOL", ""),
        ):
            prefetch_symbols.extend(_parse_symbols(raw))
        for sym in (
            _normalize_symbol(os.getenv("AI_REGIME_SOURCE", BENCH)),
            _normalize_symbol(os.getenv("AI_REGIME_FILTER_ASSET", "")),
        ):
            if sym:
                prefetch_symbols.append(sym)

    download_symbols = list(dict.fromkeys(requested_symbols + prefetch_symbols))
    frames = _build_frames(download_symbols)
    if not frames:
        raise RuntimeError("No market data loaded")
    if BENCH not in frames:
        raise RuntimeError(f"Benchmark data not found: {BENCH}")

    missing_price_symbols = sorted([s for s in requested_symbols if s not in frames])
    symbols = [s for s in symbols if s in frames]
    if not symbols:
        raise RuntimeError("No symbol frames loaded")

    bench_ind = _indicator_frame(frames[BENCH])
    if bench_ind.empty:
        raise RuntimeError("Benchmark indicator frame empty")

    ind_by_symbol: dict[str, pd.DataFrame] = {}
    for s in symbols:
        try:
            ind_df = _indicator_frame(frames[s])
        except Exception:
            ind_df = pd.DataFrame()
        if not ind_df.empty:
            ind_by_symbol[s] = ind_df
    symbols = [s for s in symbols if s in ind_by_symbol]
    if not symbols:
        raise RuntimeError("No symbol indicator frames computed")

    top_k = max(1, min(_i_env("AI_PORTFOLIO_TOP_K", 5), len(symbols)))
    periods_per_year = _periods_per_year(SNAPSHOT_FREQ)
    execution_timing = str(os.getenv("AI_EXECUTION_TIMING", "next_open")).strip().lower() or "next_open"
    if execution_timing not in {"same_close", "next_open"}:
        execution_timing = "next_open"
    decision_engine = str(os.getenv("AI_DECISION_ENGINE", "chart")).strip().lower() or "chart"
    if decision_engine not in {"ai", "chart", "stock_momentum", "trend", "regime"}:
        decision_engine = "chart"
    trend_risk_symbol = _normalize_symbol(os.getenv("AI_TREND_RISK_SYMBOL", "TQQQ")) or "TQQQ"
    trend_mid_symbol = _normalize_symbol(os.getenv("AI_TREND_MID_SYMBOL", "QQQ")) or "QQQ"
    trend_safe_symbol = _normalize_symbol(os.getenv("AI_TREND_SAFE_SYMBOL", "BIL")) or "BIL"
    trend_alt_symbol = _normalize_symbol(os.getenv("AI_TREND_ALT_SYMBOL", "QLD")) or "QLD"
    trend_use_mid = _as_bool_env("AI_TREND_USE_MID", True)
    trend_use_alt = _as_bool_env("AI_TREND_USE_ALT", False)
    trend_require_risk_on = _as_bool_env("AI_TREND_REQUIRE_RISK_ON", False)
    trend_ma_window = _i_env("AI_TREND_MA_WINDOW", 150)
    if trend_ma_window not in {50, 100, 125, 150, 175, 200}:
        trend_ma_window = 150
    trend_mid_ma_window = _i_env("AI_TREND_MID_MA_WINDOW", 200)
    if trend_mid_ma_window not in {50, 100, 125, 150, 175, 200}:
        trend_mid_ma_window = 200
    trend_ma_window_alt = _i_env("AI_TREND_ALT_MA_WINDOW", 200)
    if trend_ma_window_alt not in {50, 100, 125, 150, 175, 200}:
        trend_ma_window_alt = 200
    trend_min_risk_mom63 = _f_env("AI_TREND_MIN_RISK_MOM63", -999.0)
    trend_min_risk_mom21 = _f_env("AI_TREND_MIN_RISK_MOM21", -999.0)
    trend_min_risk_rs63 = _f_env("AI_TREND_MIN_RISK_RS63", -999.0)
    trend_risk_dd_max = _f_env("AI_TREND_RISK_DD_MAX", -1.0)
    trend_min_mid_mom63 = _f_env("AI_TREND_MIN_MID_MOM63", -999.0)
    trend_vol_max = _f_env("AI_TREND_VOL_MAX", 0.0)
    trend_hysteresis_gap = _f_env("AI_TREND_HYSTERESIS_GAP", 0.0)
    trend_vix_max = _f_env("AI_TREND_VIX_MAX", 0.0)
    regime_source = _normalize_symbol(os.getenv("AI_REGIME_SOURCE", BENCH)) or BENCH
    regime_ma_fast = _i_env("AI_REGIME_MA_FAST", 100)
    if regime_ma_fast not in {50, 100, 125, 150, 175, 200}:
        regime_ma_fast = 100
    regime_ma_slow = _i_env("AI_REGIME_MA_SLOW", 200)
    if regime_ma_slow not in {50, 100, 125, 150, 175, 200}:
        regime_ma_slow = 200
    if regime_ma_fast > regime_ma_slow:
        regime_ma_fast, regime_ma_slow = regime_ma_slow, regime_ma_fast
    regime_mom_lb = max(1, _i_env("AI_REGIME_MOM_LB", 21))
    regime_mom_thr = _f_env("AI_REGIME_MOM_THR", 0.0)
    regime_risk_on_alloc = _parse_allocation_spec(os.getenv("AI_REGIME_RISK_ON", "TQQQ")) or {"TQQQ": 1.0}
    regime_risk_on_alt_alloc = _parse_allocation_spec(os.getenv("AI_REGIME_RISK_ON_ALT", "QLD")) or {"QLD": 1.0}
    regime_neutral_alloc = _parse_allocation_spec(os.getenv("AI_REGIME_NEUTRAL", "QLD")) or {"QLD": 1.0}
    regime_recovery_alloc = (
        _parse_allocation_spec(os.getenv("AI_REGIME_RECOVERY", "")) or dict(regime_neutral_alloc)
    )
    regime_risk_off_alloc = _parse_allocation_spec(os.getenv("AI_REGIME_RISK_OFF", "GLD")) or {"GLD": 1.0}
    regime_crash_alloc = _parse_allocation_spec(os.getenv("AI_REGIME_CRASH", "GLD")) or {"GLD": 1.0}
    regime_risk_off_dynamic = _as_bool_env("AI_REGIME_RISK_OFF_DYNAMIC", False)
    regime_risk_off_pool_symbols = _parse_symbols(os.getenv("AI_REGIME_RISK_OFF_POOL", ""))
    regime_risk_off_top_n = max(1, _i_env("AI_REGIME_RISK_OFF_TOP_N", 1))
    regime_risk_off_min_ma_gap = _f_env("AI_REGIME_RISK_OFF_MIN_MA_GAP", 0.0)
    regime_risk_off_min_ret21 = _f_env("AI_REGIME_RISK_OFF_MIN_RET21", 0.0)
    regime_risk_off_min_ret63 = _f_env("AI_REGIME_RISK_OFF_MIN_RET63", 0.0)
    regime_risk_off_max_vol = _f_env("AI_REGIME_RISK_OFF_MAX_VOL", 0.0)
    regime_risk_off_min_dd252 = _f_env("AI_REGIME_RISK_OFF_MIN_DD252", -1.0)
    regime_risk_off_weight_mode = (os.getenv("AI_REGIME_RISK_OFF_WEIGHT_MODE", "inv_vol") or "").strip().lower() or "inv_vol"
    regime_risk_off_fallback_alloc = (
        _parse_allocation_spec(os.getenv("AI_REGIME_RISK_OFF_FALLBACK", "")) or dict(regime_risk_off_alloc)
    )
    regime_crash_dynamic = _as_bool_env("AI_REGIME_CRASH_DYNAMIC", False)
    regime_crash_pool_symbols = _parse_symbols(os.getenv("AI_REGIME_CRASH_POOL", ""))
    regime_crash_top_n = max(1, _i_env("AI_REGIME_CRASH_TOP_N", 1))
    regime_crash_min_ma_gap = _f_env("AI_REGIME_CRASH_MIN_MA_GAP", 0.0)
    regime_crash_min_ret21 = _f_env("AI_REGIME_CRASH_MIN_RET21", 0.0)
    regime_crash_min_ret63 = _f_env("AI_REGIME_CRASH_MIN_RET63", 0.0)
    regime_crash_max_vol = _f_env("AI_REGIME_CRASH_MAX_VOL", 0.0)
    regime_crash_min_dd252 = _f_env("AI_REGIME_CRASH_MIN_DD252", -1.0)
    regime_crash_weight_mode = (os.getenv("AI_REGIME_CRASH_WEIGHT_MODE", "inv_vol") or "").strip().lower() or "inv_vol"
    regime_crash_fallback_alloc = (
        _parse_allocation_spec(os.getenv("AI_REGIME_CRASH_FALLBACK", "")) or dict(regime_crash_alloc)
    )
    regime_vol_cap = _f_env("AI_REGIME_VOL_CAP", 0.05)
    regime_vol_low = _f_env("AI_REGIME_VOL_LOW", 0.035)
    regime_vol_mid = _f_env("AI_REGIME_VOL_MID", 0.04)
    regime_mom_strong = _f_env("AI_REGIME_MOM_STRONG", 0.06)
    regime_crash_vol = _f_env("AI_REGIME_CRASH_VOL", 0.06)
    regime_crash_dd = _f_env("AI_REGIME_CRASH_DD", -0.2)
    regime_hysteresis = _f_env("AI_REGIME_HYSTERESIS", 0.0)
    regime_recovery_slow_buffer = _f_env("AI_REGIME_RECOVERY_SLOW_BUFFER", 0.0)
    regime_recovery_min_mom = _f_env("AI_REGIME_RECOVERY_MIN_MOM", 0.0)
    regime_recovery_max_vol = _f_env("AI_REGIME_RECOVERY_MAX_VOL", regime_vol_cap)
    regime_recovery_dd_floor = _f_env("AI_REGIME_RECOVERY_DD_FLOOR", 0.0)
    regime_filter_asset = _normalize_symbol(os.getenv("AI_REGIME_FILTER_ASSET", "")) or None
    regime_filter_ma = max(0, _i_env("AI_REGIME_FILTER_MA", 0))
    regime_filter_safe_alloc = _parse_allocation_spec(os.getenv("AI_REGIME_FILTER_SAFE", "BIL")) or {"BIL": 1.0}
    regime_portfolio_symbols = sorted(
        {
            *regime_risk_on_alloc.keys(),
            *regime_risk_on_alt_alloc.keys(),
            *regime_neutral_alloc.keys(),
            *regime_recovery_alloc.keys(),
            *regime_risk_off_alloc.keys(),
            *regime_crash_alloc.keys(),
            *regime_risk_off_fallback_alloc.keys(),
            *regime_crash_fallback_alloc.keys(),
            *regime_filter_safe_alloc.keys(),
            *regime_risk_off_pool_symbols,
            *regime_crash_pool_symbols,
        }
    )
    regime_feature_symbols = sorted(
        {s for s in [regime_source, regime_filter_asset, *regime_portfolio_symbols] if isinstance(s, str) and s}
    )
    regime_max_positions = max(
        1,
        len(regime_risk_on_alloc),
        len(regime_risk_on_alt_alloc),
        len(regime_neutral_alloc),
        len(regime_recovery_alloc),
        len(regime_risk_off_alloc),
        len(regime_crash_alloc),
        max(int(regime_risk_off_top_n), len(regime_risk_off_fallback_alloc)),
        max(int(regime_crash_top_n), len(regime_crash_fallback_alloc)),
        len(regime_filter_safe_alloc),
    )
    regime_cfg_blob = {
        "regime_source": regime_source,
        "regime_ma_fast": int(regime_ma_fast),
        "regime_ma_slow": int(regime_ma_slow),
        "regime_mom_lb": int(regime_mom_lb),
        "regime_mom_thr": float(regime_mom_thr),
        "regime_risk_on_alloc": dict(regime_risk_on_alloc),
        "regime_risk_on_alt_alloc": dict(regime_risk_on_alt_alloc),
        "regime_neutral_alloc": dict(regime_neutral_alloc),
        "regime_recovery_alloc": dict(regime_recovery_alloc),
        "regime_risk_off_alloc": dict(regime_risk_off_alloc),
        "regime_crash_alloc": dict(regime_crash_alloc),
        "regime_risk_off_dynamic": bool(regime_risk_off_dynamic),
        "regime_risk_off_pool_symbols": list(regime_risk_off_pool_symbols),
        "regime_risk_off_top_n": int(regime_risk_off_top_n),
        "regime_risk_off_min_ma_gap": float(regime_risk_off_min_ma_gap),
        "regime_risk_off_min_ret21": float(regime_risk_off_min_ret21),
        "regime_risk_off_min_ret63": float(regime_risk_off_min_ret63),
        "regime_risk_off_max_vol": float(regime_risk_off_max_vol),
        "regime_risk_off_min_dd252": float(regime_risk_off_min_dd252),
        "regime_risk_off_weight_mode": str(regime_risk_off_weight_mode),
        "regime_risk_off_fallback_alloc": dict(regime_risk_off_fallback_alloc),
        "regime_crash_dynamic": bool(regime_crash_dynamic),
        "regime_crash_pool_symbols": list(regime_crash_pool_symbols),
        "regime_crash_top_n": int(regime_crash_top_n),
        "regime_crash_min_ma_gap": float(regime_crash_min_ma_gap),
        "regime_crash_min_ret21": float(regime_crash_min_ret21),
        "regime_crash_min_ret63": float(regime_crash_min_ret63),
        "regime_crash_max_vol": float(regime_crash_max_vol),
        "regime_crash_min_dd252": float(regime_crash_min_dd252),
        "regime_crash_weight_mode": str(regime_crash_weight_mode),
        "regime_crash_fallback_alloc": dict(regime_crash_fallback_alloc),
        "regime_vol_cap": float(regime_vol_cap),
        "regime_vol_low": float(regime_vol_low),
        "regime_vol_mid": float(regime_vol_mid),
        "regime_mom_strong": float(regime_mom_strong),
        "regime_crash_vol": float(regime_crash_vol),
        "regime_crash_dd": float(regime_crash_dd),
        "regime_hysteresis": float(regime_hysteresis),
        "regime_recovery_slow_buffer": float(regime_recovery_slow_buffer),
        "regime_recovery_min_mom": float(regime_recovery_min_mom),
        "regime_recovery_max_vol": float(regime_recovery_max_vol),
        "regime_recovery_dd_floor": float(regime_recovery_dd_floor),
        "regime_filter_asset": regime_filter_asset or "",
        "regime_filter_ma": int(regime_filter_ma),
        "regime_filter_safe_alloc": dict(regime_filter_safe_alloc),
    }
    prompt_max_symbols = max(10, _i_env("AI_PROMPT_MAX_SYMBOLS", 30))
    prompt_select_mode = str(os.getenv("AI_PROMPT_SELECT_MODE", "top_rs63")).strip().lower() or "top_rs63"
    trade_cost_bps_base = _f_env("AI_TRADE_COST_BPS", 0.0)
    slippage_bps = _f_env("AI_SLIPPAGE_BPS", 0.0)
    spread_bps = _f_env("AI_SPREAD_BPS", 0.0)
    tax_bps = _f_env("AI_TAX_BPS", 0.0)
    trade_cost_bps = float(trade_cost_bps_base + slippage_bps + spread_bps + tax_bps)
    trade_cost_pct = float(trade_cost_bps) / 100.0  # bps -> percent points
    max_weight_pct = _f_env("AI_PORTFOLIO_MAX_WEIGHT_PCT", 40.0)
    min_overlap = max(0, _i_env("AI_PORTFOLIO_MIN_OVERLAP", 0))
    turnover_target_pct = _f_env("AI_PORTFOLIO_TURNOVER_TARGET_PCT", 0.0)
    use_regime_exposure = _as_bool_env("AI_REGIME_EXPOSURE", False)
    regime_on_exposure_pct = _f_env("AI_REGIME_ON_EXPOSURE_PCT", 100.0)
    regime_risk_on_alt_exposure_pct = _f_env("AI_REGIME_RISK_ON_ALT_EXPOSURE_PCT", regime_on_exposure_pct)
    regime_neutral_exposure_pct = _f_env("AI_REGIME_NEUTRAL_EXPOSURE_PCT", 100.0)
    regime_recovery_exposure_pct = _f_env("AI_REGIME_RECOVERY_EXPOSURE_PCT", regime_neutral_exposure_pct)
    regime_risk_off_exposure_pct = _f_env("AI_REGIME_RISK_OFF_EXPOSURE_PCT", 100.0)
    regime_crash_exposure_pct = _f_env("AI_REGIME_CRASH_EXPOSURE_PCT", regime_risk_off_exposure_pct)
    regime_risk_off_vix_threshold = _f_env("AI_REGIME_RISK_OFF_VIX", 30.0)
    regime_risk_off_vix_hard_exposure_pct = _f_env("AI_REGIME_RISK_OFF_VIX_HARD_EXPOSURE_PCT", 60.0)
    regime_risk_off_vix_extreme = _f_env("AI_REGIME_RISK_OFF_VIX_EXTREME", 34.0)
    regime_risk_off_vix_extreme_exposure_pct = _f_env(
        "AI_REGIME_RISK_OFF_VIX_EXTREME_EXPOSURE_PCT", 40.0
    )
    use_benchmark_features = _as_bool_env("AI_ALGO_USE_BENCHMARK_FEATURES", False)
    safe_mode_enabled = _as_bool_env("AI_SAFE_MODE", True)
    safe_require_risk_on = _as_bool_env("AI_SAFE_REQUIRE_RISK_ON", True)
    safe_use_trend_template = _as_bool_env("AI_SAFE_USE_TREND_TEMPLATE", True)
    safe_trend_rs63_min = _f_env("AI_SAFE_TREND_RS63_MIN", 0.0)
    safe_min_volume_ratio = _f_env("AI_SAFE_MIN_VOLUME_RATIO", 0.8)
    dynamic_position_count = _as_bool_env("AI_DYNAMIC_POSITION_COUNT", True)
    min_positions = max(0, _i_env("AI_MIN_POSITIONS", 1))
    chart_weight_mode = str(os.getenv("AI_CHART_WEIGHT_MODE", "inv_vol")).strip().lower() or "inv_vol"
    chart_scoring_mode = str(os.getenv("AI_CHART_SCORING_MODE", "balanced")).strip().lower() or "balanced"
    if chart_scoring_mode not in {"balanced", "pure_momo", "low_vol_trend"}:
        chart_scoring_mode = "balanced"
    chart_use_universe_regime = _as_bool_env("AI_CHART_USE_UNIVERSE_REGIME", True)
    chart_min_breadth_up200 = _f_env("AI_CHART_MIN_BREADTH_UP200", 0.45)
    chart_min_breadth_positive63 = _f_env("AI_CHART_MIN_BREADTH_POS63", 0.40)
    chart_top_k_neutral = max(1, _i_env("AI_CHART_TOP_K_NEUTRAL", max(2, min(top_k, 5))))
    chart_top_k_risk_off = max(0, _i_env("AI_CHART_TOP_K_RISK_OFF", 0))
    chart_min_positions_for_invest = max(1, _i_env("AI_CHART_MIN_POSITIONS_FOR_INVEST", 2))
    stock_momo_weight_mode = str(os.getenv("AI_STOCK_MOMO_WEIGHT_MODE", "equal")).strip().lower() or "equal"
    if stock_momo_weight_mode not in {"equal", "inv_vol", "score"}:
        stock_momo_weight_mode = "equal"
    stock_momo_top_k_neutral = max(0, _i_env("AI_STOCK_MOMO_TOP_K_NEUTRAL", min(2, top_k)))
    stock_momo_top_k_risk_off = max(0, _i_env("AI_STOCK_MOMO_TOP_K_RISK_OFF", 0))
    stock_momo_min_positions_for_invest = max(1, _i_env("AI_STOCK_MOMO_MIN_POSITIONS_FOR_INVEST", 2))
    stock_momo_max_per_sector = max(0, _i_env("AI_STOCK_MOMO_MAX_PER_SECTOR", 0))
    stock_momo_sector_bonus = max(0.0, _f_env("AI_STOCK_MOMO_SECTOR_BONUS", 0.0))
    ai_top_k_neutral = max(0, _i_env("AI_AI_TOP_K_NEUTRAL", 0))
    ai_top_k_risk_off = max(0, _i_env("AI_AI_TOP_K_RISK_OFF", 0))
    ai_neutral_min_breadth_up200 = _f_env("AI_AI_NEUTRAL_MIN_BREADTH_UP200", chart_min_breadth_up200)
    ai_neutral_min_breadth_positive63 = _f_env("AI_AI_NEUTRAL_MIN_BREADTH_POS63", chart_min_breadth_positive63)
    ai_risk_off_min_breadth_up200 = _f_env("AI_AI_RISK_OFF_MIN_BREADTH_UP200", 0.25)
    ai_risk_off_min_breadth_positive63 = _f_env("AI_AI_RISK_OFF_MIN_BREADTH_POS63", 0.25)
    ai_risk_off_max_vix = _f_env("AI_AI_RISK_OFF_MAX_VIX", 45.0)
    breadth_source_mode = str(os.getenv("AI_BREADTH_SOURCE", "universe")).strip().lower() or "universe"
    if breadth_source_mode not in {"universe", "safe", "filtered"}:
        breadth_source_mode = "universe"
    ignore_prev_context = _as_bool_env("AI_IGNORE_PREV_CONTEXT", False)
    momentum_blend_pct = max(0.0, min(100.0, _f_env("AI_MOMENTUM_BLEND_PCT", 0.0)))
    momentum_blend_ratio = float(momentum_blend_pct / 100.0)
    momentum_blend_sitout_only = _as_bool_env("AI_MOMENTUM_BLEND_SITOUT_ONLY", False)
    momentum_blend_scope = _resolve_momentum_blend_scope(decision_engine, momentum_blend_sitout_only)
    momentum_blend_risk_on_pct = max(
        0.0, min(100.0, _f_env("AI_MOMENTUM_BLEND_RISK_ON_PCT", momentum_blend_pct))
    )
    momentum_blend_neutral_pct = max(
        0.0, min(100.0, _f_env("AI_MOMENTUM_BLEND_NEUTRAL_PCT", momentum_blend_pct))
    )
    momentum_blend_risk_off_pct = max(
        0.0, min(100.0, _f_env("AI_MOMENTUM_BLEND_RISK_OFF_PCT", momentum_blend_pct))
    )
    momentum_blend_dynamic = _as_bool_env("AI_MOMENTUM_BLEND_DYNAMIC", False)
    momentum_blend_high_pct = max(
        0.0, min(100.0, _f_env("AI_MOMENTUM_BLEND_HIGH_PCT", momentum_blend_pct))
    )
    momentum_blend_low_pct = max(
        0.0, min(100.0, _f_env("AI_MOMENTUM_BLEND_LOW_PCT", momentum_blend_pct))
    )
    momentum_blend_high_breadth_up200_min = _f_env("AI_MOMENTUM_BLEND_HIGH_BREADTH_UP200_MIN", -1.0)
    momentum_blend_high_breadth_up50_min = _f_env("AI_MOMENTUM_BLEND_HIGH_BREADTH_UP50_MIN", -1.0)
    momentum_blend_high_breadth_pos63_min = _f_env("AI_MOMENTUM_BLEND_HIGH_BREADTH_POS63_MIN", -1.0)
    momentum_blend_high_vix_max = _f_env("AI_MOMENTUM_BLEND_HIGH_VIX_MAX", 0.0)

    started_at = datetime.now(timezone.utc)
    ai_calls = 0
    prompt_symbol_total = 0
    ai_fallback_count = 0
    use_ai_fallback = _as_bool_env("AI_FALLBACK_ON_AI_FAIL", False)
    force_fallback = _as_bool_env("AI_FORCE_FALLBACK", False)
    sit_out_count = 0

    rows: list[dict[str, Any]] = []
    prev_port: dict[str, float] = {"__CASH__": 1.0}
    prev_exec_port: dict[str, float] = {"__CASH__": 1.0}
    prev_mom: dict[str, float] = {"__CASH__": 1.0}
    prev_regime_state: str | None = None
    coverage_by_snapshot: dict[str, dict[str, Any]] = {}

    for i, qdt in enumerate(snaps):
        mkt = _market_ctx(frames[BENCH], frames.get(VIX), qdt, use_benchmark_features=use_benchmark_features)
        if not mkt:
            continue
        next_mkt = None
        if HORIZON_MODE == "next_snapshot":
            if i + 1 >= len(snaps):
                continue
            next_mkt = _market_ctx(
                frames[BENCH],
                frames.get(VIX),
                snaps[i + 1],
                use_benchmark_features=use_benchmark_features,
            )
            if not next_mkt:
                continue

        signal_day = pd.Timestamp(mkt["day"])
        bench_signal_pos = _asof_pos(bench_ind.index, signal_day)
        if bench_signal_pos < MIN_HISTORY_DAYS:
            continue

        if HORIZON_MODE == "next_snapshot" and next_mkt is not None:
            exit_signal_day = pd.Timestamp(next_mkt["day"])
        else:
            exit_signal_day = None

        bench_frame = frames[BENCH]
        bench_entry_pos = _execution_pos(bench_frame.index, signal_day, execution_timing)
        if HORIZON_MODE == "next_snapshot" and exit_signal_day is not None:
            bench_exit_pos = _execution_pos(bench_frame.index, exit_signal_day, execution_timing)
        else:
            bench_exit_pos = bench_entry_pos + HORIZON_DAYS

        if (
            bench_entry_pos < 0
            or bench_exit_pos <= bench_entry_pos
            or bench_exit_pos >= len(bench_frame)
            or bench_entry_pos >= len(bench_frame)
        ):
            continue

        bench_entry_px = _execution_price(bench_frame, bench_entry_pos, execution_timing)
        bench_exit_px = _execution_price(bench_frame, bench_exit_pos, execution_timing)
        if bench_entry_px <= 0 or bench_exit_px <= 0:
            continue

        bench_entry_trade_day = bench_frame.index[bench_entry_pos]
        bench_exit_trade_day = bench_frame.index[bench_exit_pos]

        bench_ret = (bench_exit_px / bench_entry_px - 1.0) * 100.0
        snap = str(qdt.date())
        raw_universe = universe_by_date.get(snap, requested_symbols) if universe_by_date else symbols
        raw_universe = [s for s in raw_universe if isinstance(s, str) and s.strip()]
        engine_extra_symbols: list[str] = []
        if decision_engine == "trend":
            engine_extra_symbols = [trend_risk_symbol, trend_mid_symbol, trend_alt_symbol, trend_safe_symbol]
        elif decision_engine == "regime":
            engine_extra_symbols = list(regime_feature_symbols)
        active_symbol_order = list(dict.fromkeys(raw_universe + engine_extra_symbols))
        active_symbols = [s for s in active_symbol_order if s in ind_by_symbol]
        missing_in_universe = sorted([s for s in raw_universe if s not in ind_by_symbol])
        investable_in_universe = [s for s in raw_universe if s in ind_by_symbol]
        coverage_by_snapshot[snap] = {
            "universe_size": int(len(raw_universe)),
            "investable_size": int(len(investable_in_universe)),
            "missing_size": int(len(missing_in_universe)),
            "coverage_pct": float(len(investable_in_universe) / len(raw_universe) * 100.0) if raw_universe else 0.0,
            "missing_symbols": missing_in_universe[:50],
        }

        feats: list[dict[str, Any]] = []
        fwd_ret_by_symbol: dict[str, float] = {}
        for s in active_symbols:
            ind_df = ind_by_symbol.get(s)
            if ind_df is None or ind_df.empty:
                continue
            signal_pos = _asof_pos(ind_df.index, signal_day)
            if signal_pos < MIN_HISTORY_DAYS or signal_pos >= len(ind_df):
                continue
            raw_df = frames.get(s)
            if raw_df is None or raw_df.empty:
                continue

            entry_pos = _execution_pos(raw_df.index, signal_day, execution_timing)
            if HORIZON_MODE == "next_snapshot" and exit_signal_day is not None:
                exit_pos = _execution_pos(raw_df.index, exit_signal_day, execution_timing)
            else:
                exit_pos = entry_pos + HORIZON_DAYS
            if entry_pos < 0 or exit_pos <= entry_pos or exit_pos >= len(raw_df) or entry_pos >= len(raw_df):
                continue

            entry_px = _execution_price(raw_df, entry_pos, execution_timing)
            exit_px = _execution_price(raw_df, exit_pos, execution_timing)
            if entry_px <= 0 or exit_px <= 0:
                continue

            fr = (exit_px / entry_px - 1.0) * 100.0
            ind_row = ind_df.iloc[signal_pos]
            if use_benchmark_features:
                rs63 = _f(ind_row.get("return_63d")) - _f(mkt["bench_r63"])
                rs21 = _f(ind_row.get("return_21d")) - _f(mkt["bench_r21"])
            else:
                rs63 = _f(ind_row.get("return_63d"))
                rs21 = _f(ind_row.get("return_21d"))
            tt = _trend_template_checks(ind_row, rs63, rs63_min=safe_trend_rs63_min)
            sector_meta = sector_lookup.get(s, {})

            feats.append(
                {
                    "symbol": s,
                    "sector": str(sector_meta.get("sector") or "Unknown"),
                    "industry": str(sector_meta.get("industry") or "Unknown"),
                    "close": _f(ind_row.get("close")),
                    "relative_strength_63d": float(rs63),
                    "relative_strength_21d": float(rs21),
                    "ma50": _f(ind_row.get("ma50")),
                    "ma100": _f(ind_row.get("ma100")),
                    "ma125": _f(ind_row.get("ma125")),
                    "ma150": _f(ind_row.get("ma150")),
                    "ma175": _f(ind_row.get("ma175")),
                    "ma200": _f(ind_row.get("ma200")),
                    "return_63d": _f(ind_row.get("return_63d")),
                    "vol_20": _f(ind_row.get("vol_20")),
                    "dd_63": _f(ind_row.get("dd_63")),
                    "dd_252": _f(ind_row.get("dd_252")),
                    "return_21d": _f(ind_row.get("return_21d")),
                    "rsi": _f(ind_row.get("rsi"), 50.0),
                    "adx": _f(ind_row.get("adx")),
                    "atr_pct": _f(ind_row.get("atr_pct")),
                    "ma50_gap": _f(ind_row.get("ma50_gap")),
                    "ma100_gap": _f(ind_row.get("ma100_gap")),
                    "ma125_gap": _f(ind_row.get("ma125_gap")),
                    "ma150_gap": _f(ind_row.get("ma150_gap")),
                    "ma175_gap": _f(ind_row.get("ma175_gap")),
                    "ma200_gap": _f(ind_row.get("ma200_gap")),
                    "bb_position": _f(ind_row.get("bb_position"), 50.0),
                    "volume_ratio": _f(ind_row.get("volume_ratio"), 1.0),
                    "trend_template_pass": bool(tt.get("pass", False)),
                    "trend_template_checks": tt.get("checks", {}),
                }
            )
            fwd_ret_by_symbol[s] = float(fr)

        min_candidates = max(8, top_k * 2)
        if len(feats) < min_candidates and not safe_mode_enabled:
            continue

        safe_feats = feats
        if safe_mode_enabled:
            filtered: list[dict[str, Any]] = []
            for x in feats:
                if safe_use_trend_template and not bool(x.get("trend_template_pass", False)):
                    continue
                if float(x.get("volume_ratio", 0.0)) < float(safe_min_volume_ratio):
                    continue
                filtered.append(x)
            safe_feats = filtered

        algo_mkt = dict(mkt)
        breadth_src = _resolve_breadth_features(
            universe_features=feats,
            safe_features=safe_feats,
            source_mode=breadth_source_mode,
        )
        if decision_engine == "chart" and chart_use_universe_regime:
            algo_mkt["regime"] = _regime_from_universe_features(breadth_src, mkt.get("vix_close"))
        breadth = _market_breadth(breadth_src)
        sector_scores = _sector_strength_scores(safe_feats)

        candidate_symbol_set = {x.get("symbol") for x in safe_feats if isinstance(x, dict)}
        held_syms = [
            s
            for s, w in prev_port.items()
            if isinstance(s, str) and s != "__CASH__" and float(w) > 0 and s in candidate_symbol_set
        ]
        held_syms.sort(key=lambda s: -float(prev_port.get(s, 0.0)))

        if decision_engine == "trend":
            target_top_k = 1
        elif decision_engine == "regime":
            target_top_k = max(1, int(regime_max_positions))
        elif dynamic_position_count:
            target_top_k = _dynamic_position_target(
                safe_feats,
                base_top_k=top_k,
                market_regime=str(algo_mkt.get("regime", "neutral")),
                safe_mode=safe_mode_enabled,
                require_risk_on=safe_require_risk_on,
                min_positions=min_positions,
            )
            if decision_engine == "ai":
                target_top_k = _ai_regime_target_cap(
                    target_top_k=target_top_k,
                    regime=str(algo_mkt.get("regime", "neutral")),
                    safe_feats=safe_feats,
                    breadth=breadth,
                    vix_close=algo_mkt.get("vix_close"),
                    neutral_cap=ai_top_k_neutral,
                    neutral_min_up200=ai_neutral_min_breadth_up200,
                    neutral_min_pos63=ai_neutral_min_breadth_positive63,
                    risk_off_cap=ai_top_k_risk_off,
                    risk_off_min_up200=ai_risk_off_min_breadth_up200,
                    risk_off_min_pos63=ai_risk_off_min_breadth_positive63,
                    risk_off_max_vix=ai_risk_off_max_vix,
                )
        else:
            target_top_k = min(top_k, len(safe_feats))
            if safe_mode_enabled and safe_require_risk_on and str(algo_mkt.get("regime", "neutral")).lower() != "risk_on":
                target_top_k = 0

        if decision_engine == "chart":
            if (
                float(breadth.get("up200", 0.0)) < float(chart_min_breadth_up200)
                or float(breadth.get("positive_63d", 0.0)) < float(chart_min_breadth_positive63)
            ):
                target_top_k = min(target_top_k, chart_top_k_risk_off)
            elif str(algo_mkt.get("regime", "neutral")).lower() == "neutral":
                target_top_k = min(target_top_k, chart_top_k_neutral)
            elif str(algo_mkt.get("regime", "neutral")).lower() == "risk_off":
                target_top_k = min(target_top_k, chart_top_k_risk_off)
            candidates = _chart_candidates_with_holds(
                safe_feats,
                prompt_max_symbols,
                held_syms,
                scoring_mode=chart_scoring_mode,
            )
        elif decision_engine == "stock_momentum":
            if str(algo_mkt.get("regime", "neutral")).lower() == "neutral":
                target_top_k = min(target_top_k, stock_momo_top_k_neutral)
            elif str(algo_mkt.get("regime", "neutral")).lower() == "risk_off":
                target_top_k = min(target_top_k, stock_momo_top_k_risk_off)
            candidates = _select_candidates_with_includes(
                safe_feats,
                prompt_max_symbols,
                "top_rs63",
                held_syms,
            )
        elif decision_engine == "trend":
            by = {str(x.get("symbol")): x for x in feats if isinstance(x, dict) and x.get("symbol")}
            candidates = [x for x in safe_feats if isinstance(x, dict)]
            for sym in (trend_risk_symbol, trend_mid_symbol, trend_alt_symbol, trend_safe_symbol):
                if sym in by and all(str(y.get("symbol")) != sym for y in candidates):
                    candidates.append(by[sym])
        elif decision_engine == "regime":
            by = {str(x.get("symbol")): x for x in feats if isinstance(x, dict) and x.get("symbol")}
            candidates = [x for x in safe_feats if isinstance(x, dict)]
            for sym in regime_portfolio_symbols:
                if sym in by and all(str(y.get("symbol")) != sym for y in candidates):
                    candidates.append(by[sym])
        else:
            candidates = _select_candidates_with_includes(
                safe_feats,
                prompt_max_symbols,
                prompt_select_mode,
                held_syms,
            )
        allowed = {x["symbol"] for x in candidates if isinstance(x, dict) and x.get("symbol")}

        out: dict[str, Any] = {"_fallback": False}
        weights_pct: dict[str, float] = {}
        cash_pct = 100.0
        if target_top_k > 0 and allowed:
            if decision_engine == "chart":
                out = _chart_momentum_portfolio(
                    candidates,
                    top_k=target_top_k,
                    weight_mode=chart_weight_mode,
                    min_positions_for_invest=chart_min_positions_for_invest,
                    scoring_mode=chart_scoring_mode,
                )
            elif decision_engine == "stock_momentum":
                out = _stock_momentum_portfolio(
                    candidates,
                    top_k=target_top_k,
                    weight_mode=stock_momo_weight_mode,
                    min_positions_for_invest=stock_momo_min_positions_for_invest,
                    max_per_sector=stock_momo_max_per_sector,
                    sector_bonus_mult=stock_momo_sector_bonus,
                    sector_scores=sector_scores,
                )
            elif decision_engine == "trend":
                by = {str(x.get("symbol")): x for x in feats if isinstance(x, dict) and x.get("symbol")}
                risk_row = by.get(trend_risk_symbol, {})
                mid_row = by.get(trend_mid_symbol, {})
                alt_row = by.get(trend_alt_symbol, {})
                regime_now = str(algo_mkt.get("regime", "neutral")).strip().lower()
                vix_now = _f(algo_mkt.get("vix_close"), 0.0)
                prev_major = "__CASH__"
                prev_max_w = -1.0
                for sym, w in (prev_port or {}).items():
                    if not isinstance(sym, str) or sym == "__CASH__":
                        continue
                    ww = float(w)
                    if ww > prev_max_w:
                        prev_max_w = ww
                        prev_major = sym

                risk_ma_gap = _ma_gap_for_window(risk_row, trend_ma_window) if risk_row else -999.0
                risk_mom63 = _f(risk_row.get("return_63d"), -999.0) if risk_row else -999.0
                risk_mom21 = _f(risk_row.get("return_21d"), -999.0) if risk_row else -999.0
                risk_vol20 = _f(risk_row.get("vol_20"), np.nan) if risk_row else np.nan
                risk_dd63 = _f(risk_row.get("dd_63"), np.nan) if risk_row else np.nan
                risk_gap_gate = -abs(float(trend_hysteresis_gap)) if prev_major == trend_risk_symbol else 0.0

                mid_ma_gap = _ma_gap_for_window(mid_row, trend_mid_ma_window) if mid_row else -999.0
                mid_mom63 = _f(mid_row.get("return_63d"), -999.0) if mid_row else -999.0
                mid_gap_gate = -abs(float(trend_hysteresis_gap)) if prev_major == trend_mid_symbol else 0.0
                risk_rs63 = risk_mom63 - mid_mom63 if np.isfinite(risk_mom63) and np.isfinite(mid_mom63) else risk_mom63

                risk_cond = (
                    risk_row
                    and risk_ma_gap > risk_gap_gate
                    and risk_mom63 >= float(trend_min_risk_mom63)
                    and risk_mom21 >= float(trend_min_risk_mom21)
                    and risk_rs63 >= float(trend_min_risk_rs63)
                    and (float(trend_risk_dd_max) <= -0.99 or (np.isfinite(risk_dd63) and risk_dd63 >= float(trend_risk_dd_max)))
                    and (not np.isfinite(risk_vol20) or float(trend_vol_max) <= 0 or risk_vol20 <= float(trend_vol_max))
                    and (not trend_require_risk_on or regime_now == "risk_on")
                    and (float(trend_vix_max) <= 0 or vix_now <= float(trend_vix_max))
                )

                mid_cond = (
                    bool(mid_row)
                    and bool(trend_use_mid)
                    and mid_ma_gap > mid_gap_gate
                    and mid_mom63 >= float(trend_min_mid_mom63)
                    and (float(trend_vix_max) <= 0 or vix_now <= float(trend_vix_max))
                )

                alt_ma_gap = _ma_gap_for_window(alt_row, trend_ma_window_alt) if alt_row else -999.0
                alt_cond = bool(alt_row) and alt_ma_gap > 0 and (float(trend_vix_max) <= 0 or vix_now <= float(trend_vix_max))

                selected = trend_safe_symbol
                reason = "safe_default"
                if risk_cond and trend_risk_symbol in allowed:
                    selected = trend_risk_symbol
                    reason = "risk_trend_on"
                elif mid_cond and trend_mid_symbol in allowed:
                    selected = trend_mid_symbol
                    reason = "mid_trend_on"
                elif trend_use_alt and alt_cond and trend_alt_symbol in allowed:
                    selected = trend_alt_symbol
                    reason = "alt_trend_on"
                elif trend_safe_symbol not in allowed:
                    if trend_mid_symbol in allowed and mid_cond:
                        selected = trend_mid_symbol
                        reason = "safe_missing_mid_used"
                    elif trend_alt_symbol in allowed and alt_cond:
                        selected = trend_alt_symbol
                        reason = "safe_missing_alt_used"
                    elif trend_risk_symbol in allowed and risk_cond:
                        selected = trend_risk_symbol
                        reason = "safe_missing_risk_used"

                if selected in allowed:
                    out = {
                        "positions": [{"symbol": selected, "weight_pct": 100.0}],
                        "cash_pct": 0.0,
                        "_trend_mode": True,
                        "_trend_reason": reason,
                    }
                else:
                    out = {"positions": [], "cash_pct": 100.0, "_trend_mode": True, "_sit_out": True, "_trend_reason": "no_allowed_asset"}
            elif decision_engine == "regime":
                by = {str(x.get("symbol")): x for x in feats if isinstance(x, dict) and x.get("symbol")}
                out = _regime_portfolio_from_features(
                    by_symbol=by,
                    prev_state=prev_regime_state,
                    regime_source=regime_source,
                    ma_fast=regime_ma_fast,
                    ma_slow=regime_ma_slow,
                    mom_lb=regime_mom_lb,
                    mom_thr=regime_mom_thr,
                    risk_on_alloc=regime_risk_on_alloc,
                    risk_on_alt_alloc=regime_risk_on_alt_alloc,
                    neutral_alloc=regime_neutral_alloc,
                    recovery_alloc=regime_recovery_alloc,
                    risk_off_alloc=regime_risk_off_alloc,
                    crash_alloc=regime_crash_alloc,
                    vol_cap=regime_vol_cap,
                    vol_low=regime_vol_low,
                    vol_mid=regime_vol_mid,
                    mom_strong=regime_mom_strong,
                    crash_vol=regime_crash_vol,
                    crash_dd=regime_crash_dd,
                    hysteresis=regime_hysteresis,
                    recovery_slow_buffer=regime_recovery_slow_buffer,
                    recovery_min_mom=regime_recovery_min_mom,
                    recovery_max_vol=regime_recovery_max_vol,
                    recovery_dd_floor=regime_recovery_dd_floor,
                    risk_on_filter_asset=regime_filter_asset,
                    risk_on_filter_ma=regime_filter_ma,
                    risk_on_filter_safe_alloc=regime_filter_safe_alloc,
                    risk_off_dynamic=regime_risk_off_dynamic,
                    risk_off_pool_symbols=regime_risk_off_pool_symbols,
                    risk_off_top_n=regime_risk_off_top_n,
                    risk_off_min_ma_gap=regime_risk_off_min_ma_gap,
                    risk_off_min_ret21=regime_risk_off_min_ret21,
                    risk_off_min_ret63=regime_risk_off_min_ret63,
                    risk_off_max_vol=regime_risk_off_max_vol,
                    risk_off_min_dd252=regime_risk_off_min_dd252,
                    risk_off_weight_mode=regime_risk_off_weight_mode,
                    risk_off_fallback_alloc=regime_risk_off_fallback_alloc,
                    crash_dynamic=regime_crash_dynamic,
                    crash_pool_symbols=regime_crash_pool_symbols,
                    crash_top_n=regime_crash_top_n,
                    crash_min_ma_gap=regime_crash_min_ma_gap,
                    crash_min_ret21=regime_crash_min_ret21,
                    crash_min_ret63=regime_crash_min_ret63,
                    crash_max_vol=regime_crash_max_vol,
                    crash_min_dd252=regime_crash_min_dd252,
                    crash_weight_mode=regime_crash_weight_mode,
                    crash_fallback_alloc=regime_crash_fallback_alloc,
                )
                algo_mkt["regime"] = str(out.get("_regime_bucket", algo_mkt.get("regime", "neutral")))
                algo_mkt["regime_state"] = str(out.get("_regime_state", algo_mkt.get("regime_state", "neutral")))
            else:
                ai_calls += 1
                prompt_symbol_total += len(allowed)
                prev_for_prompt: dict[str, float] | None
                forced_sells: list[str] | None
                if ignore_prev_context:
                    prev_for_prompt = None
                    forced_sells = None
                else:
                    prev_for_prompt = {"__CASH__": float(prev_port.get("__CASH__", 0.0)) * 100.0}
                    forced_sells = [
                        s for s, w in prev_port.items() if s != "__CASH__" and float(w) > 0 and s not in allowed
                    ]
                    for s, w in prev_port.items():
                        if s == "__CASH__" or float(w) <= 0:
                            continue
                        if s in allowed:
                            prev_for_prompt[str(s)] = float(w) * 100.0

                if force_fallback:
                    out = _fallback_portfolio_from_features(
                        candidates,
                        top_k=target_top_k,
                        max_weight_pct=max_weight_pct,
                        prev_portfolio_pct=prev_for_prompt,
                    )
                    out["_fallback"] = True
                    out["_fallback_reason"] = "forced_by_env"
                else:
                    out = _ai_portfolio(
                        snap,
                        algo_mkt,
                        candidates,
                        top_k=target_top_k,
                        cache=cache,
                        max_weight_pct=max_weight_pct,
                        trade_cost_bps=trade_cost_bps,
                        turnover_target_pct=turnover_target_pct,
                        prev_portfolio_pct=prev_for_prompt,
                        forced_sells=forced_sells,
                        fallback_on_fail=use_ai_fallback,
                    )
            weights_pct, cash_pct = _portfolio_from_ai(
                out,
                allowed=allowed,
                top_k=target_top_k,
                max_weight_pct=max_weight_pct,
            )
            if out.get("_fallback"):
                ai_fallback_count += 1

            feats_by_symbol = {str(x.get("symbol")): x for x in feats if isinstance(x, dict) and x.get("symbol")}
            weights_pct = _enforce_min_overlap(
                weights_pct,
                prev_port=prev_port,
                allowed=allowed,
                feats_by_symbol=feats_by_symbol,
                min_overlap=min_overlap,
                top_k=target_top_k,
            )
            # Re-apply weight clamp after overlap enforcement.
            weights_pct = {k: float(v) for k, v in weights_pct.items() if float(v) > 0 and k in allowed}
            if weights_pct:
                weights_pct, cash_pct = _portfolio_from_ai(
                    {"positions": [{"symbol": k, "weight_pct": v} for k, v in weights_pct.items()]},
                    allowed=allowed,
                    top_k=target_top_k,
                    max_weight_pct=max_weight_pct,
                )
                if use_regime_exposure:
                    weights_pct, cash_pct = _apply_regime_exposure(
                        weights_pct,
                        algo_mkt,
                        on_exposure_pct=regime_on_exposure_pct,
                        risk_on_alt_exposure_pct=regime_risk_on_alt_exposure_pct,
                        neutral_exposure_pct=regime_neutral_exposure_pct,
                        recovery_exposure_pct=regime_recovery_exposure_pct,
                        risk_off_exposure_pct=regime_risk_off_exposure_pct,
                        crash_exposure_pct=regime_crash_exposure_pct,
                        risk_off_vix_threshold=regime_risk_off_vix_threshold,
                        risk_off_vix_hard_exposure_pct=regime_risk_off_vix_hard_exposure_pct,
                        risk_off_vix_extreme=regime_risk_off_vix_extreme,
                        risk_off_vix_extreme_exposure_pct=regime_risk_off_vix_extreme_exposure_pct,
                    )
        else:
            sit_out_count += 1
            out["_sit_out"] = True

        core_weights_pct = {sym: float(w) for sym, w in weights_pct.items()}
        core_cash_pct = float(cash_pct)
        core_port = {sym: float(w) / 100.0 for sym, w in core_weights_pct.items()}
        core_port["__CASH__"] = core_cash_pct / 100.0

        if feats:
            df_feat = pd.DataFrame(feats)
            df_feat["relative_strength_63d"] = (
                pd.to_numeric(df_feat["relative_strength_63d"], errors="coerce").fillna(-1e9)
            )
            mom_syms = df_feat.nlargest(top_k, "relative_strength_63d")["symbol"].astype(str).tolist()
        else:
            mom_syms = []
        mom = {sym: (1.0 / len(mom_syms)) for sym in mom_syms if sym}
        mom["__CASH__"] = 1.0 - float(sum(mom.values()))

        regime_for_blend = str(algo_mkt.get("regime", "neutral")).strip().lower()
        if momentum_blend_dynamic:
            breadth_up200_now = float(breadth.get("up200", 0.0))
            breadth_up50_now = float(breadth.get("up50", 0.0))
            breadth_pos63_now = float(breadth.get("positive_63d", 0.0))
            vix_now = _f(algo_mkt.get("vix_close"), np.nan)
            high_ok = True
            if float(momentum_blend_high_breadth_up200_min) >= 0:
                high_ok = high_ok and breadth_up200_now >= float(momentum_blend_high_breadth_up200_min)
            if float(momentum_blend_high_breadth_up50_min) >= 0:
                high_ok = high_ok and breadth_up50_now >= float(momentum_blend_high_breadth_up50_min)
            if float(momentum_blend_high_breadth_pos63_min) >= 0:
                high_ok = high_ok and breadth_pos63_now >= float(momentum_blend_high_breadth_pos63_min)
            if float(momentum_blend_high_vix_max) > 0 and np.isfinite(vix_now):
                high_ok = high_ok and vix_now <= float(momentum_blend_high_vix_max)
            blend_ratio_now = float((momentum_blend_high_pct if high_ok else momentum_blend_low_pct) / 100.0)
        else:
            if regime_for_blend == "risk_on":
                blend_ratio_now = float(momentum_blend_risk_on_pct / 100.0)
            elif regime_for_blend == "risk_off":
                blend_ratio_now = float(momentum_blend_risk_off_pct / 100.0)
            else:
                blend_ratio_now = float(momentum_blend_neutral_pct / 100.0)

        if momentum_blend_scope == "disabled":
            blend_ratio_now = 0.0
        blend_sitout_only = momentum_blend_scope == "sitout_only"
        exec_port = _blend_portfolios(
            core_port,
            mom,
            blend_ratio=blend_ratio_now,
            sitout_only=blend_sitout_only,
            sit_out_flag=bool(out.get("_sit_out", False)),
        )
        exec_cash_pct = float(exec_port.get("__CASH__", 0.0) * 100.0)
        exec_weights_pct = {sym: float(w) * 100.0 for sym, w in exec_port.items() if sym != "__CASH__" and float(w) > 0}
        exp = float((1.0 - exec_port.get("__CASH__", 0.0)) * 100.0)

        gross = 0.0
        for sym, w in exec_port.items():
            if sym == "__CASH__":
                continue
            gross += float(w) * float(fwd_ret_by_symbol.get(sym, 0.0))

        mom_gross = 0.0
        for sym, w in mom.items():
            if sym == "__CASH__":
                continue
            mom_gross += float(w) * float(fwd_ret_by_symbol.get(sym, 0.0))

        turn = _turnover(prev_exec_port, exec_port)
        core_turn = _turnover(prev_port, core_port)
        mom_turn = _turnover(prev_mom, mom)
        cost = float(trade_cost_pct * turn)
        mom_cost = float(trade_cost_pct * mom_turn)

        net = float(gross - cost)
        mom_net = float(mom_gross - mom_cost)

        rows.append(
            {
                "date": snap,
                "signal_day": str(pd.Timestamp(signal_day).date()),
                "exit_signal_day": str(pd.Timestamp(exit_signal_day).date()) if exit_signal_day is not None else "",
                "entry_day": str(pd.Timestamp(bench_entry_trade_day).date()),
                "exit_day": str(pd.Timestamp(bench_exit_trade_day).date()),
                "execution_timing": str(execution_timing),
                "market_regime": str(algo_mkt.get("regime", "neutral")),
                "market_regime_base": str(mkt.get("regime", "neutral")),
                "regime_exposure_enabled": bool(use_regime_exposure),
                "vix_close": mkt.get("vix_close"),
                "breadth_up200": float(breadth.get("up200", 0.0)),
                "breadth_up50": float(breadth.get("up50", 0.0)),
                "breadth_positive_63d": float(breadth.get("positive_63d", 0.0)),
                "universe_size": int(coverage_by_snapshot.get(snap, {}).get("universe_size", 0)),
                "universe_investable": int(coverage_by_snapshot.get(snap, {}).get("investable_size", 0)),
                "universe_missing": int(coverage_by_snapshot.get(snap, {}).get("missing_size", 0)),
                "universe_coverage_pct": float(coverage_by_snapshot.get(snap, {}).get("coverage_pct", 0.0)),
                "prompt_symbols": int(len(allowed)),
                "candidate_count_raw": int(len(feats)),
                "candidate_count_safe": int(len(safe_feats)),
                "target_positions": int(target_top_k),
                "sit_out": bool(out.get("_sit_out", False)),
                "regime_state": str(out.get("_regime_state", "")),
                "regime_reason": str(out.get("_regime_reason", "")),
                "positions": json.dumps(exec_weights_pct, ensure_ascii=True),
                "ai_core_positions": json.dumps(core_weights_pct, ensure_ascii=True),
                "ai_fallback": bool(out.get("_fallback", False)),
                "cash_pct": float(exec_cash_pct),
                "ai_core_cash_pct": float(core_cash_pct),
                "exposure_pct": float(exp),
                "momentum_blend_ratio_applied": float(blend_ratio_now),
                "turnover": float(turn),
                "ai_core_turnover": float(core_turn),
                "gross_return_pct": float(gross),
                "net_return_pct": float(net),
                "benchmark_return_pct": float(bench_ret),
                "alpha_net_pct": float(net - bench_ret),
                "mom_positions": json.dumps(
                    {s: round(100.0 / len(mom_syms), 4) for s in mom_syms},
                    ensure_ascii=True,
                ),
                "breadth_source_mode": str(breadth_source_mode),
                "momentum_blend_scope": str(momentum_blend_scope),
                "mom_turnover": float(mom_turn),
                "mom_gross_return_pct": float(mom_gross),
                "mom_net_return_pct": float(mom_net),
                "mom_alpha_net_pct": float(mom_net - bench_ret),
            }
        )

        prev_port = core_port
        prev_exec_port = exec_port
        prev_mom = mom
        if decision_engine == "regime":
            prev_regime_state = str(out.get("_regime_state", prev_regime_state or "")).strip().lower() or prev_regime_state

    if not rows:
        raise RuntimeError("No backtest rows generated")

    df = pd.DataFrame(rows).sort_values(["date"]).reset_index(drop=True)

    cfg_blob = {
        "model": MODEL,
        "prompt_version": PROMPT_VERSION,
        "decision_engine": decision_engine,
        **regime_cfg_blob,
        "trend_risk_symbol": trend_risk_symbol,
        "trend_mid_symbol": trend_mid_symbol,
        "trend_safe_symbol": trend_safe_symbol,
        "trend_alt_symbol": trend_alt_symbol,
        "trend_use_mid": bool(trend_use_mid),
        "trend_use_alt": bool(trend_use_alt),
        "trend_require_risk_on": bool(trend_require_risk_on),
        "trend_ma_window": int(trend_ma_window),
        "trend_mid_ma_window": int(trend_mid_ma_window),
        "trend_ma_window_alt": int(trend_ma_window_alt),
        "trend_min_risk_mom63": float(trend_min_risk_mom63),
        "trend_min_risk_mom21": float(trend_min_risk_mom21),
        "trend_min_risk_rs63": float(trend_min_risk_rs63),
        "trend_risk_dd_max": float(trend_risk_dd_max),
        "trend_min_mid_mom63": float(trend_min_mid_mom63),
        "trend_vol_max": float(trend_vol_max),
        "trend_hysteresis_gap": float(trend_hysteresis_gap),
        "trend_vix_max": float(trend_vix_max),
        "execution_timing": execution_timing,
        "universe": universe_name,
        "symbols": len(symbols),
        "benchmark_symbol": BENCH,
        "vix_symbol": VIX,
        "horizon_days": HORIZON_DAYS,
        "horizon_mode": HORIZON_MODE,
        "snapshot_freq": SNAPSHOT_FREQ,
        "periods_per_year": periods_per_year,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "top_k": top_k,
        "prompt_max_symbols": prompt_max_symbols,
        "prompt_select_mode": prompt_select_mode,
        "trade_cost_bps": trade_cost_bps,
        "trade_cost_bps_base": float(trade_cost_bps_base),
        "slippage_bps": float(slippage_bps),
        "spread_bps": float(spread_bps),
        "tax_bps": float(tax_bps),
        "portfolio_max_weight_pct": float(max_weight_pct),
        "portfolio_min_overlap": int(min_overlap),
        "portfolio_turnover_target_pct": float(turnover_target_pct),
        "regime_exposure_enabled": bool(use_regime_exposure),
        "regime_on_exposure_pct": float(regime_on_exposure_pct),
        "regime_risk_on_alt_exposure_pct": float(regime_risk_on_alt_exposure_pct),
        "regime_neutral_exposure_pct": float(regime_neutral_exposure_pct),
        "regime_recovery_exposure_pct": float(regime_recovery_exposure_pct),
        "regime_risk_off_exposure_pct": float(regime_risk_off_exposure_pct),
        "regime_crash_exposure_pct": float(regime_crash_exposure_pct),
        "regime_risk_off_vix_threshold": float(regime_risk_off_vix_threshold),
        "regime_risk_off_vix_hard_exposure_pct": float(regime_risk_off_vix_hard_exposure_pct),
        "regime_risk_off_vix_extreme": float(regime_risk_off_vix_extreme),
        "regime_risk_off_vix_extreme_exposure_pct": float(regime_risk_off_vix_extreme_exposure_pct),
        "algo_use_benchmark_features": bool(use_benchmark_features),
        "safe_mode_enabled": bool(safe_mode_enabled),
        "safe_require_risk_on": bool(safe_require_risk_on),
        "safe_use_trend_template": bool(safe_use_trend_template),
        "safe_trend_rs63_min": float(safe_trend_rs63_min),
        "safe_min_volume_ratio": float(safe_min_volume_ratio),
        "dynamic_position_count": bool(dynamic_position_count),
        "min_positions": int(min_positions),
        "chart_weight_mode": str(chart_weight_mode),
        "chart_scoring_mode": str(chart_scoring_mode),
        "chart_use_universe_regime": bool(chart_use_universe_regime),
        "chart_min_breadth_up200": float(chart_min_breadth_up200),
        "chart_min_breadth_positive63": float(chart_min_breadth_positive63),
        "chart_top_k_neutral": int(chart_top_k_neutral),
        "chart_top_k_risk_off": int(chart_top_k_risk_off),
        "chart_min_positions_for_invest": int(chart_min_positions_for_invest),
        "stock_momo_weight_mode": str(stock_momo_weight_mode),
        "stock_momo_top_k_neutral": int(stock_momo_top_k_neutral),
        "stock_momo_top_k_risk_off": int(stock_momo_top_k_risk_off),
        "stock_momo_min_positions_for_invest": int(stock_momo_min_positions_for_invest),
        "stock_momo_max_per_sector": int(stock_momo_max_per_sector),
        "stock_momo_sector_bonus": float(stock_momo_sector_bonus),
        "ai_top_k_neutral": int(ai_top_k_neutral),
        "ai_top_k_risk_off": int(ai_top_k_risk_off),
        "ai_neutral_min_breadth_up200": float(ai_neutral_min_breadth_up200),
        "ai_neutral_min_breadth_positive63": float(ai_neutral_min_breadth_positive63),
        "ai_risk_off_min_breadth_up200": float(ai_risk_off_min_breadth_up200),
        "ai_risk_off_min_breadth_positive63": float(ai_risk_off_min_breadth_positive63),
        "ai_risk_off_max_vix": float(ai_risk_off_max_vix),
        "breadth_source_mode": str(breadth_source_mode),
        "momentum_blend_pct": float(momentum_blend_pct),
        "momentum_blend_scope": str(momentum_blend_scope),
        "momentum_blend_risk_on_pct": float(momentum_blend_risk_on_pct),
        "momentum_blend_neutral_pct": float(momentum_blend_neutral_pct),
        "momentum_blend_risk_off_pct": float(momentum_blend_risk_off_pct),
        "momentum_blend_dynamic": bool(momentum_blend_dynamic),
        "momentum_blend_high_pct": float(momentum_blend_high_pct),
        "momentum_blend_low_pct": float(momentum_blend_low_pct),
        "momentum_blend_high_breadth_up200_min": float(momentum_blend_high_breadth_up200_min),
        "momentum_blend_high_breadth_up50_min": float(momentum_blend_high_breadth_up50_min),
        "momentum_blend_high_breadth_pos63_min": float(momentum_blend_high_breadth_pos63_min),
        "momentum_blend_high_vix_max": float(momentum_blend_high_vix_max),
        "momentum_blend_sitout_only": bool(momentum_blend_sitout_only),
        "ignore_prev_context": bool(ignore_prev_context),
        "force_fallback": bool(force_fallback),
    }
    cfg_hash = hashlib.sha256(
        json.dumps(cfg_blob, sort_keys=True, ensure_ascii=True, default=str).encode("utf-8")
    ).hexdigest()[:12]

    df["run_tag"] = RUN_TAG
    df["config_hash"] = cfg_hash
    df["universe"] = universe_name
    df["snapshot_freq"] = SNAPSHOT_FREQ
    df["periods_per_year"] = periods_per_year
    df["horizon_days"] = HORIZON_DAYS
    df["horizon_mode"] = HORIZON_MODE
    df["execution_timing"] = str(execution_timing)
    df["trade_cost_bps"] = float(trade_cost_bps)
    df["prompt_max_symbols"] = int(prompt_max_symbols)
    df["prompt_select_mode"] = str(prompt_select_mode)
    df["decision_engine"] = str(decision_engine)
    df["regime_source"] = str(regime_source)
    df["regime_ma_fast"] = int(regime_ma_fast)
    df["regime_ma_slow"] = int(regime_ma_slow)
    df["regime_mom_lb"] = int(regime_mom_lb)
    df["chart_scoring_mode"] = str(chart_scoring_mode)
    df["algo_use_benchmark_features"] = bool(use_benchmark_features)
    df["ai_top_k_neutral"] = int(ai_top_k_neutral)
    df["ai_top_k_risk_off"] = int(ai_top_k_risk_off)
    df["ai_neutral_min_breadth_up200"] = float(ai_neutral_min_breadth_up200)
    df["ai_neutral_min_breadth_positive63"] = float(ai_neutral_min_breadth_positive63)
    df["ai_risk_off_min_breadth_up200"] = float(ai_risk_off_min_breadth_up200)
    df["ai_risk_off_min_breadth_positive63"] = float(ai_risk_off_min_breadth_positive63)
    df["ai_risk_off_max_vix"] = float(ai_risk_off_max_vix)
    df["breadth_source_mode"] = str(breadth_source_mode)
    df["momentum_blend_pct"] = float(momentum_blend_pct)
    df["momentum_blend_scope"] = str(momentum_blend_scope)
    df["momentum_blend_risk_on_pct"] = float(momentum_blend_risk_on_pct)
    df["momentum_blend_neutral_pct"] = float(momentum_blend_neutral_pct)
    df["momentum_blend_risk_off_pct"] = float(momentum_blend_risk_off_pct)
    df["momentum_blend_dynamic"] = bool(momentum_blend_dynamic)
    df["momentum_blend_high_pct"] = float(momentum_blend_high_pct)
    df["momentum_blend_low_pct"] = float(momentum_blend_low_pct)
    df["momentum_blend_high_breadth_up200_min"] = float(momentum_blend_high_breadth_up200_min)
    df["momentum_blend_high_breadth_up50_min"] = float(momentum_blend_high_breadth_up50_min)
    df["momentum_blend_high_breadth_pos63_min"] = float(momentum_blend_high_breadth_pos63_min)
    df["momentum_blend_high_vix_max"] = float(momentum_blend_high_vix_max)
    df["momentum_blend_sitout_only"] = bool(momentum_blend_sitout_only)

    pm = {
        "ai_portfolio": _risk_metrics(df["net_return_pct"], periods_per_year),
        "ai_portfolio_gross": _risk_metrics(df["gross_return_pct"], periods_per_year),
        "momentum_topk": _risk_metrics(df["mom_net_return_pct"], periods_per_year),
        "momentum_topk_gross": _risk_metrics(df["mom_gross_return_pct"], periods_per_year),
        "benchmark": _risk_metrics(df["benchmark_return_pct"], periods_per_year),
    }

    finished_at = datetime.now(timezone.utc)
    summary = {
        "run_tag": RUN_TAG,
        "config_hash": cfg_hash,
        "model": MODEL,
        "prompt_version": PROMPT_VERSION,
        "decision_engine": decision_engine,
        **regime_cfg_blob,
        "trend_risk_symbol": trend_risk_symbol,
        "trend_mid_symbol": trend_mid_symbol,
        "trend_safe_symbol": trend_safe_symbol,
        "trend_alt_symbol": trend_alt_symbol,
        "trend_use_mid": bool(trend_use_mid),
        "trend_use_alt": bool(trend_use_alt),
        "trend_require_risk_on": bool(trend_require_risk_on),
        "trend_ma_window": int(trend_ma_window),
        "trend_mid_ma_window": int(trend_mid_ma_window),
        "trend_ma_window_alt": int(trend_ma_window_alt),
        "trend_min_risk_mom63": float(trend_min_risk_mom63),
        "trend_min_risk_mom21": float(trend_min_risk_mom21),
        "trend_min_risk_rs63": float(trend_min_risk_rs63),
        "trend_risk_dd_max": float(trend_risk_dd_max),
        "trend_min_mid_mom63": float(trend_min_mid_mom63),
        "trend_vol_max": float(trend_vol_max),
        "trend_hysteresis_gap": float(trend_hysteresis_gap),
        "trend_vix_max": float(trend_vix_max),
        "execution_timing": execution_timing,
        "universe": universe_name,
        "symbols": len(symbols),
        "benchmark_symbol": BENCH,
        "vix_symbol": VIX,
        "horizon_days": HORIZON_DAYS,
        "horizon_mode": HORIZON_MODE,
        "snapshot_freq": SNAPSHOT_FREQ,
        "periods_per_year": periods_per_year,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "top_k": int(top_k),
        "prompt_max_symbols": int(prompt_max_symbols),
        "prompt_select_mode": str(prompt_select_mode),
        "trade_cost_bps": float(trade_cost_bps),
        "trade_cost_pct": float(trade_cost_pct),
        "trade_cost_bps_base": float(trade_cost_bps_base),
        "slippage_bps": float(slippage_bps),
        "spread_bps": float(spread_bps),
        "tax_bps": float(tax_bps),
        "portfolio_max_weight_pct": float(max_weight_pct),
        "portfolio_min_overlap": int(min_overlap),
        "portfolio_turnover_target_pct": float(turnover_target_pct),
        "regime_exposure_enabled": bool(use_regime_exposure),
        "regime_on_exposure_pct": float(regime_on_exposure_pct),
        "regime_risk_on_alt_exposure_pct": float(regime_risk_on_alt_exposure_pct),
        "regime_neutral_exposure_pct": float(regime_neutral_exposure_pct),
        "regime_recovery_exposure_pct": float(regime_recovery_exposure_pct),
        "regime_risk_off_exposure_pct": float(regime_risk_off_exposure_pct),
        "regime_crash_exposure_pct": float(regime_crash_exposure_pct),
        "regime_risk_off_vix_threshold": float(regime_risk_off_vix_threshold),
        "regime_risk_off_vix_hard_exposure_pct": float(regime_risk_off_vix_hard_exposure_pct),
        "regime_risk_off_vix_extreme": float(regime_risk_off_vix_extreme),
        "regime_risk_off_vix_extreme_exposure_pct": float(regime_risk_off_vix_extreme_exposure_pct),
        "algo_use_benchmark_features": bool(use_benchmark_features),
        "safe_mode_enabled": bool(safe_mode_enabled),
        "safe_require_risk_on": bool(safe_require_risk_on),
        "safe_use_trend_template": bool(safe_use_trend_template),
        "safe_trend_rs63_min": float(safe_trend_rs63_min),
        "safe_min_volume_ratio": float(safe_min_volume_ratio),
        "dynamic_position_count": bool(dynamic_position_count),
        "min_positions": int(min_positions),
        "chart_weight_mode": str(chart_weight_mode),
        "chart_scoring_mode": str(chart_scoring_mode),
        "chart_use_universe_regime": bool(chart_use_universe_regime),
        "chart_min_breadth_up200": float(chart_min_breadth_up200),
        "chart_min_breadth_positive63": float(chart_min_breadth_positive63),
        "chart_top_k_neutral": int(chart_top_k_neutral),
        "chart_top_k_risk_off": int(chart_top_k_risk_off),
        "chart_min_positions_for_invest": int(chart_min_positions_for_invest),
        "ai_top_k_neutral": int(ai_top_k_neutral),
        "ai_top_k_risk_off": int(ai_top_k_risk_off),
        "ai_neutral_min_breadth_up200": float(ai_neutral_min_breadth_up200),
        "ai_neutral_min_breadth_positive63": float(ai_neutral_min_breadth_positive63),
        "ai_risk_off_min_breadth_up200": float(ai_risk_off_min_breadth_up200),
        "ai_risk_off_min_breadth_positive63": float(ai_risk_off_min_breadth_positive63),
        "ai_risk_off_max_vix": float(ai_risk_off_max_vix),
        "breadth_source_mode": str(breadth_source_mode),
        "momentum_blend_pct": float(momentum_blend_pct),
        "momentum_blend_scope": str(momentum_blend_scope),
        "momentum_blend_risk_on_pct": float(momentum_blend_risk_on_pct),
        "momentum_blend_neutral_pct": float(momentum_blend_neutral_pct),
        "momentum_blend_risk_off_pct": float(momentum_blend_risk_off_pct),
        "momentum_blend_dynamic": bool(momentum_blend_dynamic),
        "momentum_blend_high_pct": float(momentum_blend_high_pct),
        "momentum_blend_low_pct": float(momentum_blend_low_pct),
        "momentum_blend_high_breadth_up200_min": float(momentum_blend_high_breadth_up200_min),
        "momentum_blend_high_breadth_up50_min": float(momentum_blend_high_breadth_up50_min),
        "momentum_blend_high_breadth_pos63_min": float(momentum_blend_high_breadth_pos63_min),
        "momentum_blend_high_vix_max": float(momentum_blend_high_vix_max),
        "momentum_blend_sitout_only": bool(momentum_blend_sitout_only),
        "ignore_prev_context": bool(ignore_prev_context),
        "force_fallback": bool(force_fallback),
        "started_at_utc": started_at.isoformat(),
        "finished_at_utc": finished_at.isoformat(),
        "periods": int(len(df)),
        "ai_calls": int(ai_calls),
        "avg_prompt_symbols": float(prompt_symbol_total / max(1, ai_calls)) if ai_calls > 0 else 0.0,
        "ai_fallback_count": int(ai_fallback_count),
        "ai_fallback_rate": float(ai_fallback_count / max(1, ai_calls)),
        "sit_out_count": int(sit_out_count),
        "sit_out_rate_pct": float((sit_out_count / max(1, len(df))) * 100.0),
        "avg_exposure_pct": float(df["exposure_pct"].mean()),
        "avg_turnover": float(df["turnover"].mean()),
        "avg_candidate_count_raw": float(df["candidate_count_raw"].mean()) if "candidate_count_raw" in df else 0.0,
        "avg_candidate_count_safe": float(df["candidate_count_safe"].mean()) if "candidate_count_safe" in df else 0.0,
        "avg_target_positions": float(df["target_positions"].mean()) if "target_positions" in df else 0.0,
        "missing_price_symbols": missing_price_symbols[:200],
        "avg_universe_coverage_pct": float(df["universe_coverage_pct"].mean()) if "universe_coverage_pct" in df else 0.0,
        "min_universe_coverage_pct": float(df["universe_coverage_pct"].min()) if "universe_coverage_pct" in df else 0.0,
        "portfolio_metrics": pm,
    }

    skip_latest_write = _as_bool_env("AI_SKIP_LATEST_WRITE", False)
    df.to_csv(RUN_RESULT_CSV, index=False)
    _save_json(RUN_SUMMARY_JSON, summary)
    if not skip_latest_write:
        df.to_csv(RESULT_CSV.parent / (RESULT_CSV.name + f".tmp_{RUN_TAG}"), index=False)
        os.replace(RESULT_CSV.parent / (RESULT_CSV.name + f".tmp_{RUN_TAG}"), RESULT_CSV)
        _atomic_write_text(
            SUMMARY_JSON.parent / (SUMMARY_JSON.name + f".tmp_{RUN_TAG}"),
            json.dumps(summary, ensure_ascii=False, indent=2),
        )
        os.replace(SUMMARY_JSON.parent / (SUMMARY_JSON.name + f".tmp_{RUN_TAG}"), SUMMARY_JSON)

    print(f"Saved: {RUN_RESULT_CSV.relative_to(ROOT)} ({len(df)} rows)")
    print(f"Saved: {RUN_SUMMARY_JSON.relative_to(ROOT)}")
    if not skip_latest_write:
        print(f"Updated latest: {RESULT_CSV.relative_to(ROOT)}")
        print(f"Updated latest: {SUMMARY_JSON.relative_to(ROOT)}")
    else:
        print("Skipped latest output update (AI_SKIP_LATEST_WRITE=1)")
    print(
        "AI portfolio -> "
        f"CAGR {pm['ai_portfolio']['cagr_pct']:.2f}% | Sharpe {pm['ai_portfolio']['sharpe']:.2f} | "
        f"MDD {pm['ai_portfolio']['max_drawdown_pct']:.2f}%"
    )
    print(
        "Momentum topK -> "
        f"CAGR {pm['momentum_topk']['cagr_pct']:.2f}% | Sharpe {pm['momentum_topk']['sharpe']:.2f} | "
        f"MDD {pm['momentum_topk']['max_drawdown_pct']:.2f}%"
    )
    print(
        f"Benchmark({BENCH}) -> "
        f"CAGR {pm['benchmark']['cagr_pct']:.2f}% | Sharpe {pm['benchmark']['sharpe']:.2f} | "
        f"MDD {pm['benchmark']['max_drawdown_pct']:.2f}%"
    )


if __name__ == "__main__":
    run()
