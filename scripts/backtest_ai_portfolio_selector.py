from __future__ import annotations

import ast
import hashlib
import json
import os
from contextlib import contextmanager
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

RUN_TAG = (os.getenv("AI_RUN_TAG") or "").strip() or (
    datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"_{os.getpid()}"
)
RUN_RESULT_CSV = RUNS_DIR / f"ai_portfolio_backtest_results_{RUN_TAG}.csv"
RUN_SUMMARY_JSON = RUNS_DIR / f"ai_portfolio_backtest_summary_{RUN_TAG}.json"

DEFAULT_SYMBOLS = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AVGO", "AMD", "NFLX", "COST", "CSCO"]
BENCH = os.getenv("AI_BENCHMARK_SYMBOL", "QQQ").strip().upper() or "QQQ"
VIX = os.getenv("AI_VIX_SYMBOL", "^VIX").strip() or "^VIX"
MODEL = os.getenv("AI_MODEL", "gpt-5.2").strip() or "gpt-5.2"

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


def _market_ctx(bench: pd.DataFrame, vix: pd.DataFrame | None, dt: pd.Timestamp) -> dict[str, Any] | None:
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
    if close >= ma200 and r63 >= 0 and (v is None or v <= 22):
        regime = "risk_on"
    elif (close < ma200 and r63 < 0) or (v is not None and v >= 28):
        regime = "risk_off"
    return {"day": sd, "regime": regime, "bench_r21": r21, "bench_r63": r63, "vix_close": v}


def _build_frames(symbols: list[str]) -> dict[str, pd.DataFrame]:
    tickers = sorted(set(symbols + [BENCH, VIX]))
    with _temporary_proxy_env():
        raw = yf.download(
            tickers=tickers,
            start="2014-01-01",
            end="2026-12-31",
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
    ma200 = close.rolling(200).mean()
    ma50_gap = (close / ma50 - 1.0) * 100.0

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

    out = pd.DataFrame(
        {
            "close": close,
            "volume": volume,
            "volume_ratio": volume_ratio,
            "ma50": ma50,
            "ma200": ma200,
            "ma50_gap": ma50_gap,
            "rsi": rsi,
            "return_21d": ret21,
            "return_63d": ret63,
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
        f"Benchmark ret63: {float(market_ctx.get('bench_r63', 0.0)):.2f}%\n"
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
        return cache[cache_key]

    raw = analyzer._call(prompt, max_tokens=1800)
    parsed = _extract_json(raw or "")
    if not parsed:
        raw2 = analyzer._call(prompt + "\nReturn one minified JSON object only.", max_tokens=1800)
        parsed = _extract_json(raw2 or "")
    if not parsed:
        if fallback_on_fail:
            return _fallback_portfolio_from_features(
                features,
                top_k=top_k,
                max_weight_pct=max_weight_pct,
                prev_portfolio_pct=prev_portfolio_pct,
            )
        raise RuntimeError(f"AI JSON parse failed for {snapshot_date}")

    cache[cache_key] = parsed
    _save_json(AI_CACHE, cache)
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
    neutral_exposure_pct: float,
    risk_off_exposure_pct: float,
    risk_off_vix_threshold: float,
    risk_off_vix_hard_exposure_pct: float,
    risk_off_vix_extreme: float,
    risk_off_vix_extreme_exposure_pct: float,
) -> tuple[dict[str, float], float]:
    if not weights_pct:
        return {}, 100.0

    regime = str(market_ctx.get("regime", "neutral")).lower()
    vix = market_ctx.get("vix_close")

    on_exposure_pct = max(0.0, min(100.0, _f(on_exposure_pct, 100.0)))
    neutral_exposure_pct = max(0.0, min(100.0, _f(neutral_exposure_pct, on_exposure_pct)))
    risk_off_exposure_pct = max(0.0, min(100.0, _f(risk_off_exposure_pct, neutral_exposure_pct)))
    risk_off_vix_threshold = _f(risk_off_vix_threshold, 28.0)
    risk_off_vix_extreme = _f(risk_off_vix_extreme, 34.0)
    risk_off_vix_hard_exposure_pct = max(0.0, min(100.0, _f(risk_off_vix_hard_exposure_pct, risk_off_exposure_pct)))
    risk_off_vix_extreme_exposure_pct = max(
        0.0, min(100.0, _f(risk_off_vix_extreme_exposure_pct, risk_off_vix_hard_exposure_pct))
    )

    if regime == "risk_on":
        target_exp = on_exposure_pct
    elif regime == "risk_off":
        v = _f(vix, 0.0)
        if np.isfinite(v) and v >= risk_off_vix_extreme:
            target_exp = risk_off_vix_extreme_exposure_pct
        elif np.isfinite(v) and v >= risk_off_vix_threshold:
            target_exp = risk_off_vix_hard_exposure_pct
        else:
            target_exp = risk_off_exposure_pct
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
    frames = _build_frames(symbols)
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
    regime_neutral_exposure_pct = _f_env("AI_REGIME_NEUTRAL_EXPOSURE_PCT", 100.0)
    regime_risk_off_exposure_pct = _f_env("AI_REGIME_RISK_OFF_EXPOSURE_PCT", 100.0)
    regime_risk_off_vix_threshold = _f_env("AI_REGIME_RISK_OFF_VIX", 30.0)
    regime_risk_off_vix_hard_exposure_pct = _f_env("AI_REGIME_RISK_OFF_VIX_HARD_EXPOSURE_PCT", 60.0)
    regime_risk_off_vix_extreme = _f_env("AI_REGIME_RISK_OFF_VIX_EXTREME", 34.0)
    regime_risk_off_vix_extreme_exposure_pct = _f_env(
        "AI_REGIME_RISK_OFF_VIX_EXTREME_EXPOSURE_PCT", 40.0
    )

    started_at = datetime.now(timezone.utc)
    ai_calls = 0
    prompt_symbol_total = 0
    ai_fallback_count = 0
    use_ai_fallback = _as_bool_env("AI_FALLBACK_ON_AI_FAIL", True)

    rows: list[dict[str, Any]] = []
    prev_port: dict[str, float] = {"__CASH__": 1.0}
    prev_mom: dict[str, float] = {"__CASH__": 1.0}
    coverage_by_snapshot: dict[str, dict[str, Any]] = {}

    for i, qdt in enumerate(snaps):
        mkt = _market_ctx(frames[BENCH], frames.get(VIX), qdt)
        if not mkt:
            continue
        next_mkt = None
        if HORIZON_MODE == "next_snapshot":
            if i + 1 >= len(snaps):
                continue
            next_mkt = _market_ctx(frames[BENCH], frames.get(VIX), snaps[i + 1])
            if not next_mkt:
                continue

        signal_day = pd.Timestamp(mkt["day"])
        bench_entry_pos = _asof_pos(bench_ind.index, signal_day)
        if bench_entry_pos < MIN_HISTORY_DAYS:
            continue
        bench_entry_px = _f(bench_ind.iloc[bench_entry_pos].get("close"), -1.0)
        if bench_entry_px <= 0:
            continue

        if HORIZON_MODE == "next_snapshot" and next_mkt is not None:
            exit_day = pd.Timestamp(next_mkt["day"])
            bench_exit_pos = _asof_pos(bench_ind.index, exit_day)
        else:
            exit_day = None
            bench_exit_pos = bench_entry_pos + HORIZON_DAYS
        if bench_exit_pos <= bench_entry_pos or bench_exit_pos >= len(bench_ind):
            continue
        bench_exit_px = _f(bench_ind.iloc[bench_exit_pos].get("close"), -1.0)
        if bench_exit_px <= 0:
            continue

        bench_ret = (bench_exit_px / bench_entry_px - 1.0) * 100.0
        snap = str(qdt.date())
        raw_universe = universe_by_date.get(snap, requested_symbols) if universe_by_date else symbols
        raw_universe = [s for s in raw_universe if isinstance(s, str) and s.strip()]
        active_symbols = [s for s in raw_universe if s in ind_by_symbol]
        missing_in_universe = sorted([s for s in raw_universe if s not in ind_by_symbol])
        coverage_by_snapshot[snap] = {
            "universe_size": int(len(raw_universe)),
            "investable_size": int(len(active_symbols)),
            "missing_size": int(len(missing_in_universe)),
            "coverage_pct": float(len(active_symbols) / len(raw_universe) * 100.0) if raw_universe else 0.0,
            "missing_symbols": missing_in_universe[:50],
        }

        feats: list[dict[str, Any]] = []
        fwd_ret_by_symbol: dict[str, float] = {}
        for s in active_symbols:
            ind_df = ind_by_symbol.get(s)
            if ind_df is None or ind_df.empty:
                continue
            entry_pos = _asof_pos(ind_df.index, signal_day)
            if entry_pos < MIN_HISTORY_DAYS or entry_pos >= len(ind_df):
                continue
            entry_px = _f(ind_df.iloc[entry_pos].get("close"), -1.0)
            if entry_px <= 0:
                continue

            if HORIZON_MODE == "next_snapshot" and exit_day is not None:
                exit_pos = _asof_pos(ind_df.index, exit_day)
            else:
                exit_pos = entry_pos + HORIZON_DAYS
            if exit_pos <= entry_pos or exit_pos >= len(ind_df):
                continue
            exit_px = _f(ind_df.iloc[exit_pos].get("close"), -1.0)
            if exit_px <= 0:
                continue

            fr = (exit_px / entry_px - 1.0) * 100.0
            ind_row = ind_df.iloc[entry_pos]
            rs63 = _f(ind_row.get("return_63d")) - _f(mkt["bench_r63"])
            rs21 = _f(ind_row.get("return_21d")) - _f(mkt["bench_r21"])

            feats.append(
                {
                    "symbol": s,
                    "relative_strength_63d": float(rs63),
                    "relative_strength_21d": float(rs21),
                    "return_63d": _f(ind_row.get("return_63d")),
                    "return_21d": _f(ind_row.get("return_21d")),
                    "rsi": _f(ind_row.get("rsi"), 50.0),
                    "adx": _f(ind_row.get("adx")),
                    "atr_pct": _f(ind_row.get("atr_pct")),
                    "ma50_gap": _f(ind_row.get("ma50_gap")),
                    "bb_position": _f(ind_row.get("bb_position"), 50.0),
                    "volume_ratio": _f(ind_row.get("volume_ratio"), 1.0),
                }
            )
            fwd_ret_by_symbol[s] = float(fr)

        if len(feats) < max(8, top_k * 2):
            continue

        held_syms = [
            s
            for s, w in prev_port.items()
            if isinstance(s, str) and s != "__CASH__" and float(w) > 0 and s in {x.get("symbol") for x in feats}
        ]
        held_syms.sort(key=lambda s: -float(prev_port.get(s, 0.0)))

        candidates = _select_candidates_with_includes(feats, prompt_max_symbols, prompt_select_mode, held_syms)
        allowed = {x["symbol"] for x in candidates if isinstance(x, dict) and x.get("symbol")}

        ai_calls += 1
        prompt_symbol_total += len(allowed)
        prev_for_prompt: dict[str, float] = {"__CASH__": float(prev_port.get("__CASH__", 0.0)) * 100.0}
        forced_sells = [s for s, w in prev_port.items() if s != "__CASH__" and float(w) > 0 and s not in allowed]
        for s, w in prev_port.items():
            if s == "__CASH__" or float(w) <= 0:
                continue
            if s in allowed:
                prev_for_prompt[str(s)] = float(w) * 100.0

        out = _ai_portfolio(
            snap,
            mkt,
            candidates,
            top_k=top_k,
            cache=cache,
            max_weight_pct=max_weight_pct,
            trade_cost_bps=trade_cost_bps,
            turnover_target_pct=turnover_target_pct,
            prev_portfolio_pct=prev_for_prompt,
            forced_sells=forced_sells,
            fallback_on_fail=use_ai_fallback,
        )
        weights_pct, cash_pct = _portfolio_from_ai(out, allowed=allowed, top_k=top_k, max_weight_pct=max_weight_pct)
        if out.get("_fallback"):
            ai_fallback_count += 1

        feats_by_symbol = {str(x.get("symbol")): x for x in feats if isinstance(x, dict) and x.get("symbol")}
        weights_pct = _enforce_min_overlap(
            weights_pct,
            prev_port=prev_port,
            allowed=allowed,
            feats_by_symbol=feats_by_symbol,
            min_overlap=min_overlap,
            top_k=top_k,
        )
        # Re-apply weight clamp after overlap enforcement.
        weights_pct = {k: float(v) for k, v in weights_pct.items() if float(v) > 0 and k in allowed}
        if weights_pct:
            weights_pct, cash_pct = _portfolio_from_ai(
                {"positions": [{"symbol": k, "weight_pct": v} for k, v in weights_pct.items()]},
                allowed=allowed,
                top_k=top_k,
                max_weight_pct=max_weight_pct,
            )
            if use_regime_exposure:
                weights_pct, cash_pct = _apply_regime_exposure(
                    weights_pct,
                    mkt,
                    on_exposure_pct=regime_on_exposure_pct,
                    neutral_exposure_pct=regime_neutral_exposure_pct,
                    risk_off_exposure_pct=regime_risk_off_exposure_pct,
                    risk_off_vix_threshold=regime_risk_off_vix_threshold,
                    risk_off_vix_hard_exposure_pct=regime_risk_off_vix_hard_exposure_pct,
                    risk_off_vix_extreme=regime_risk_off_vix_extreme,
                    risk_off_vix_extreme_exposure_pct=regime_risk_off_vix_extreme_exposure_pct,
                )

        port = {sym: float(w) / 100.0 for sym, w in weights_pct.items()}
        port["__CASH__"] = float(cash_pct) / 100.0
        exp = float((1.0 - port.get("__CASH__", 0.0)) * 100.0)

        df_feat = pd.DataFrame(feats)
        df_feat["relative_strength_63d"] = (
            pd.to_numeric(df_feat["relative_strength_63d"], errors="coerce").fillna(-1e9)
        )
        mom_syms = df_feat.nlargest(top_k, "relative_strength_63d")["symbol"].astype(str).tolist()
        mom = {sym: (1.0 / len(mom_syms)) for sym in mom_syms if sym}
        mom["__CASH__"] = 1.0 - float(sum(mom.values()))

        gross = 0.0
        for sym, w in port.items():
            if sym == "__CASH__":
                continue
            gross += float(w) * float(fwd_ret_by_symbol.get(sym, 0.0))

        mom_gross = 0.0
        for sym, w in mom.items():
            if sym == "__CASH__":
                continue
            mom_gross += float(w) * float(fwd_ret_by_symbol.get(sym, 0.0))

        turn = _turnover(prev_port, port)
        mom_turn = _turnover(prev_mom, mom)
        cost = float(trade_cost_pct * turn)
        mom_cost = float(trade_cost_pct * mom_turn)

        net = float(gross - cost)
        mom_net = float(mom_gross - mom_cost)

        rows.append(
            {
                "date": snap,
                "entry_day": str(pd.Timestamp(signal_day).date()),
                "exit_day": str(pd.Timestamp(exit_day).date()) if exit_day is not None else "",
                "market_regime": str(mkt.get("regime", "neutral")),
                "regime_exposure_enabled": bool(use_regime_exposure),
                "vix_close": mkt.get("vix_close"),
                "universe_size": int(coverage_by_snapshot.get(snap, {}).get("universe_size", 0)),
                "universe_investable": int(coverage_by_snapshot.get(snap, {}).get("investable_size", 0)),
                "universe_missing": int(coverage_by_snapshot.get(snap, {}).get("missing_size", 0)),
                "universe_coverage_pct": float(coverage_by_snapshot.get(snap, {}).get("coverage_pct", 0.0)),
                "prompt_symbols": int(len(allowed)),
                "positions": json.dumps(weights_pct, ensure_ascii=True),
                "ai_fallback": bool(out.get("_fallback", False)),
                "cash_pct": float(cash_pct),
                "exposure_pct": float(exp),
                "turnover": float(turn),
                "gross_return_pct": float(gross),
                "net_return_pct": float(net),
                "benchmark_return_pct": float(bench_ret),
                "alpha_net_pct": float(net - bench_ret),
                "mom_positions": json.dumps(
                    {s: round(100.0 / len(mom_syms), 4) for s in mom_syms},
                    ensure_ascii=True,
                ),
                "mom_turnover": float(mom_turn),
                "mom_gross_return_pct": float(mom_gross),
                "mom_net_return_pct": float(mom_net),
                "mom_alpha_net_pct": float(mom_net - bench_ret),
            }
        )

        prev_port = port
        prev_mom = mom

    if not rows:
        raise RuntimeError("No backtest rows generated")

    df = pd.DataFrame(rows).sort_values(["date"]).reset_index(drop=True)

    cfg_blob = {
        "model": MODEL,
        "prompt_version": PROMPT_VERSION,
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
        "regime_neutral_exposure_pct": float(regime_neutral_exposure_pct),
        "regime_risk_off_exposure_pct": float(regime_risk_off_exposure_pct),
        "regime_risk_off_vix_threshold": float(regime_risk_off_vix_threshold),
        "regime_risk_off_vix_hard_exposure_pct": float(regime_risk_off_vix_hard_exposure_pct),
        "regime_risk_off_vix_extreme": float(regime_risk_off_vix_extreme),
        "regime_risk_off_vix_extreme_exposure_pct": float(regime_risk_off_vix_extreme_exposure_pct),
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
    df["trade_cost_bps"] = float(trade_cost_bps)
    df["prompt_max_symbols"] = int(prompt_max_symbols)
    df["prompt_select_mode"] = str(prompt_select_mode)

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
        "regime_neutral_exposure_pct": float(regime_neutral_exposure_pct),
        "regime_risk_off_exposure_pct": float(regime_risk_off_exposure_pct),
        "regime_risk_off_vix_threshold": float(regime_risk_off_vix_threshold),
        "regime_risk_off_vix_hard_exposure_pct": float(regime_risk_off_vix_hard_exposure_pct),
        "regime_risk_off_vix_extreme": float(regime_risk_off_vix_extreme),
        "regime_risk_off_vix_extreme_exposure_pct": float(regime_risk_off_vix_extreme_exposure_pct),
        "started_at_utc": started_at.isoformat(),
        "finished_at_utc": finished_at.isoformat(),
        "periods": int(len(df)),
        "ai_calls": int(ai_calls),
        "avg_prompt_symbols": float(prompt_symbol_total / max(1, ai_calls)) if ai_calls > 0 else 0.0,
        "ai_fallback_count": int(ai_fallback_count),
        "ai_fallback_rate": float(ai_fallback_count / max(1, ai_calls)),
        "avg_exposure_pct": float(df["exposure_pct"].mean()),
        "avg_turnover": float(df["turnover"].mean()),
        "missing_price_symbols": missing_price_symbols[:200],
        "avg_universe_coverage_pct": float(df["universe_coverage_pct"].mean()) if "universe_coverage_pct" in df else 0.0,
        "min_universe_coverage_pct": float(df["universe_coverage_pct"].min()) if "universe_coverage_pct" in df else 0.0,
        "portfolio_metrics": pm,
    }

    df.to_csv(RUN_RESULT_CSV, index=False)
    _save_json(RUN_SUMMARY_JSON, summary)
    df.to_csv(RESULT_CSV.parent / (RESULT_CSV.name + f".tmp_{RUN_TAG}"), index=False)
    os.replace(RESULT_CSV.parent / (RESULT_CSV.name + f".tmp_{RUN_TAG}"), RESULT_CSV)
    _atomic_write_text(
        SUMMARY_JSON.parent / (SUMMARY_JSON.name + f".tmp_{RUN_TAG}"),
        json.dumps(summary, ensure_ascii=False, indent=2),
    )
    os.replace(SUMMARY_JSON.parent / (SUMMARY_JSON.name + f".tmp_{RUN_TAG}"), SUMMARY_JSON)

    print(f"Saved: {RUN_RESULT_CSV.relative_to(ROOT)} ({len(df)} rows)")
    print(f"Saved: {RUN_SUMMARY_JSON.relative_to(ROOT)}")
    print(f"Updated latest: {RESULT_CSV.relative_to(ROOT)}")
    print(f"Updated latest: {SUMMARY_JSON.relative_to(ROOT)}")
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
