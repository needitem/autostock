from __future__ import annotations

import ast
import hashlib
import itertools
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
RESULT_CSV = DATA_DIR / "ai_chart_backtest_results.csv"
SUMMARY_JSON = DATA_DIR / "ai_chart_backtest_summary.json"
AI_CACHE = DATA_DIR / "ai_chart_backtest_cache.json"
EARN_CACHE = DATA_DIR / "ai_chart_earnings_cache.json"
RUNS_DIR = DATA_DIR / "runs"
RUN_TAG = (os.getenv("AI_RUN_TAG") or "").strip() or (
    datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"_{os.getpid()}"
)
RUN_RESULT_CSV = RUNS_DIR / f"ai_chart_backtest_results_{RUN_TAG}.csv"
RUN_SUMMARY_JSON = RUNS_DIR / f"ai_chart_backtest_summary_{RUN_TAG}.json"

DEFAULT_SYMBOLS = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AVGO", "AMD", "NFLX", "COST", "CSCO"]
BENCH = os.getenv("AI_BENCHMARK_SYMBOL", "QQQ").strip().upper() or "QQQ"
VIX = os.getenv("AI_VIX_SYMBOL", "^VIX").strip() or "^VIX"
MODEL = os.getenv("AI_MODEL", "gpt-5.2")
try:
    HORIZON_DAYS = int(str(os.getenv("AI_HORIZON_DAYS", "63")).strip() or "63")
except Exception:
    HORIZON_DAYS = 63
HORIZON_DAYS = max(5, min(252, HORIZON_DAYS))
HORIZON_MODE = (os.getenv("AI_HORIZON_MODE", "fixed_days") or "").strip().lower() or "fixed_days"
if HORIZON_MODE not in {"fixed_days", "next_snapshot"}:
    HORIZON_MODE = "fixed_days"
PROMPT_VERSION = "v3_regime_event"
SNAPSHOT_FREQ = (os.getenv("AI_SNAPSHOT_FREQ", "quarterly") or "").strip().lower() or "quarterly"
START_DATE = (os.getenv("AI_START_DATE", "2016-01-01") or "").strip() or "2016-01-01"
END_DATE = (os.getenv("AI_END_DATE", "2025-12-31") or "").strip() or "2025-12-31"
_NO_PROXY_MODE = str(os.getenv("AI_DISABLE_PROXY", "0")).strip().lower() in {"1", "true", "yes", "on"}
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


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _config_hash(payload: dict[str, Any]) -> str:
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=True, default=str).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()[:12]


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


def _b_env(k: str, d: bool) -> bool:
    v = str(os.getenv(k, str(d))).strip().lower()
    if v in {"1", "true", "yes", "on"}:
        return True
    if v in {"0", "false", "no", "off"}:
        return False
    return d


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


def _atomic_write_text(dst: Path, text: str) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.parent / (dst.name + f".tmp_{os.getpid()}")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, dst)


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


def _load_symbols() -> tuple[str, list[str]]:
    raw = (os.getenv("AI_SYMBOLS") or "").strip()
    if raw:
        syms = _parse_symbols(raw)
        return ("custom", syms)

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


def _periods_per_year(freq: str) -> int:
    f = (freq or "").strip().lower()
    if f in {"m", "month", "months", "monthly"}:
        return 12
    if f in {"w", "week", "weeks", "weekly"}:
        return 52
    return 4


def _asof_pos(idx: pd.Index, dt: pd.Timestamp) -> int:
    """Return integer position of last index <= dt, or -1 when none."""
    try:
        pos = int(idx.searchsorted(pd.Timestamp(dt), side="right")) - 1
        return pos if pos >= 0 else -1
    except Exception:
        return -1


def _indicator_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Compute indicator series once so snapshot sampling is fast."""
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
            "volume_avg": volume_avg,
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


def _prompt_batches(
    features: list[dict[str, Any]],
    max_symbols: int,
    mode: str,
    batching: bool,
) -> list[list[dict[str, Any]]]:
    if not features:
        return []

    nmax = int(max_symbols)
    if nmax <= 0 or len(features) <= nmax:
        return [features]

    df = pd.DataFrame(features).copy()
    for col in ["relative_strength_63d", "relative_strength_21d", "volume_ratio", "adx", "rsi", "atr_pct"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        else:
            df[col] = 0.0

    pick_mode = str(mode or "mix").strip().lower() or "mix"
    if pick_mode not in {"mix", "top_rs63", "top_rs21", "top_vol"}:
        pick_mode = "mix"

    if pick_mode == "top_rs21":
        order = df.sort_values(["relative_strength_21d", "relative_strength_63d"], ascending=False).index.tolist()
    elif pick_mode == "top_vol":
        order = df.sort_values(["volume_ratio", "relative_strength_63d"], ascending=False).index.tolist()
    else:
        order = df.sort_values(["relative_strength_63d", "relative_strength_21d"], ascending=False).index.tolist()

    if batching:
        return [[features[i] for i in order[j : j + nmax]] for j in range(0, len(order), nmax)]

    if pick_mode != "mix":
        chosen = order[:nmax]
        return [[features[i] for i in chosen]]

    # mix: top momentum + some laggards + volume spikes to keep diversity
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
    return [[features[i] for i in chosen]]


def _snapshot_dates() -> list[pd.Timestamp]:
    start = pd.Timestamp(START_DATE)
    end = pd.Timestamp(END_DATE)
    if end < start:
        start, end = end, start

    freq = SNAPSHOT_FREQ
    if freq in {"q", "quarter", "quarters", "quarterly"}:
        dates = list(pd.date_range(start=start, end=end, freq="QE-DEC"))
    elif freq in {"m", "month", "months", "monthly"}:
        dates = list(pd.date_range(start=start, end=end, freq="ME"))
    elif freq in {"w", "week", "weeks", "weekly"}:
        dates = list(pd.date_range(start=start, end=end, freq="W-FRI"))
    else:
        dates = list(pd.date_range(start=start, end=end, freq="QE-DEC"))

    if MAX_SNAPSHOTS > 0:
        dates = dates[-MAX_SNAPSHOTS:]
    return [pd.Timestamp(d).normalize() for d in dates]


def _last_day(frame: pd.DataFrame, dt: pd.Timestamp) -> pd.Timestamp | None:
    idx = frame.index[frame.index <= dt]
    return None if len(idx) == 0 else idx[-1]


def _ret_forward(frame: pd.DataFrame, dt: pd.Timestamp, days: int = HORIZON_DAYS) -> float | None:
    sd = _last_day(frame, dt)
    if sd is None:
        return None
    i = int(frame.index.get_indexer([sd])[0])
    if i < 0 or i + days >= len(frame):
        return None
    p0, p1 = _f(frame.iloc[i]["Close"], -1), _f(frame.iloc[i + days]["Close"], -1)
    if p0 <= 0 or p1 <= 0:
        return None
    return (p1 / p0 - 1.0) * 100.0


def _ret_lookback(frame: pd.DataFrame, dt: pd.Timestamp, days: int) -> float:
    sd = _last_day(frame, dt)
    if sd is None:
        return 0.0
    i = int(frame.index.get_indexer([sd])[0])
    if i - days < 0:
        return 0.0
    p0, p1 = _f(frame.iloc[i - days]["Close"], -1), _f(frame.iloc[i]["Close"], -1)
    if p0 <= 0 or p1 <= 0:
        return 0.0
    return (p1 / p0 - 1.0) * 100.0


def _ret_between(frame: pd.DataFrame, dt0: pd.Timestamp, dt1: pd.Timestamp) -> float | None:
    """Return % change from last trade day <= dt0 to last trade day <= dt1."""
    sd0 = _last_day(frame, dt0)
    sd1 = _last_day(frame, dt1)
    if sd0 is None or sd1 is None or sd1 <= sd0:
        return None
    p0 = _f(frame.loc[sd0]["Close"], -1)
    p1 = _f(frame.loc[sd1]["Close"], -1)
    if p0 <= 0 or p1 <= 0:
        return None
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


def _earnings_calendar(symbols: list[str]) -> dict[str, list[pd.Timestamp]]:
    raw = _load_json(EARN_CACHE)
    changed = False
    for s in symbols:
        if s not in raw or not raw.get(s):
            out: list[str] = []
            try:
                df = yf.Ticker(s).get_earnings_dates(limit=240)
                if isinstance(df, pd.DataFrame) and len(df) > 0:
                    out = sorted({pd.Timestamp(x).date().isoformat() for x in df.index})
            except Exception:
                out = []
            raw[s] = out
            changed = True
    if changed:
        _save_json(EARN_CACHE, raw)
    parsed: dict[str, list[pd.Timestamp]] = {}
    for s, dates in raw.items():
        if isinstance(dates, list):
            arr: list[pd.Timestamp] = []
            for d in dates:
                try:
                    arr.append(pd.Timestamp(d).normalize())
                except Exception:
                    continue
            if arr:
                parsed[s] = sorted(set(arr))
    return parsed


def _days_to_next_earnings(signal_day: pd.Timestamp, dates: list[pd.Timestamp]) -> int | None:
    sd = pd.Timestamp(signal_day).normalize()
    left = [int((d - sd).days) for d in dates if d >= sd]
    return min(left) if left else None


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


def _norm_action(v: Any) -> str:
    t = str(v or "").strip().upper()
    if t in {"BUY", "SELL", "HOLD"}:
        return t
    if "BUY" in t:
        return "BUY"
    if "SELL" in t:
        return "SELL"
    return "HOLD"


def _strat_ret(action: str, ret: float) -> float:
    return float(ret if action == "BUY" else (-ret if action == "SELL" else 0.0))


def _label_cfg() -> dict[str, Any]:
    return {
        "mode": str(os.getenv("AI_LABEL_MODE", "dynamic_alpha")).strip().lower(),
        "buy_th": _f_env("AI_ABS_BUY_TH", 2.0),
        "sell_th": _f_env("AI_ABS_SELL_TH", -2.0),
        "alpha_buy": _f_env("AI_ALPHA_BUY_BASE_PCT", _f_env("AI_ALPHA_BASE_PCT", 0.8)),
        "alpha_sell": _f_env("AI_ALPHA_SELL_BASE_PCT", _f_env("AI_ALPHA_BASE_PCT", 0.8)),
        "hold_mult": _f_env("AI_HOLD_ATR_MULT", 1.1),
        "hold_min": _f_env("AI_HOLD_MIN_PCT", 2.0),
        "hold_max": _f_env("AI_HOLD_MAX_PCT", 5.5),
        "alpha_hold_mult": _f_env("AI_ALPHA_HOLD_MULT", 1.5),
        "riskoff_cut": _f_env("AI_RISKOFF_CUT_PCT", -3.0),
        "riskoff_relax": _f_env("AI_RISKOFF_RELAX", 0.85),
    }


def _true_label(fret: float, alpha: float, atr: float, bench_ret: float, cfg: dict[str, Any]) -> str:
    mode = cfg.get("mode", "dynamic_alpha")
    if mode == "absolute":
        return "BUY" if fret >= cfg["buy_th"] else ("SELL" if fret <= cfg["sell_th"] else "HOLD")
    if mode == "alpha":
        return "BUY" if alpha >= cfg["alpha_buy"] else ("SELL" if alpha <= -cfg["alpha_sell"] else "HOLD")
    band = min(max(atr * cfg["hold_mult"], cfg["hold_min"]), cfg["hold_max"])
    buy_th = max(cfg["alpha_buy"], band * cfg["alpha_hold_mult"])
    sell_th = max(cfg["alpha_sell"], band * cfg["alpha_hold_mult"])
    if bench_ret <= cfg["riskoff_cut"]:
        sell_th *= cfg["riskoff_relax"]
    return "BUY" if alpha >= buy_th else ("SELL" if alpha <= -sell_th else "HOLD")


def _exec_cfg() -> dict[str, Any]:
    return {
        "mode": str(os.getenv("AI_EXECUTION_MODE", "long_cash")).strip().lower(),
        "earn_block": _i_env("AI_EXEC_EARNINGS_BLOCK_DAYS", 1),
        "riskoff_high_conf": _b_env("AI_EXEC_RISKOFF_HIGH_CONF", False),
        "min_rs_buy": _f_env("AI_EXEC_MIN_RS63_BUY", -1.0),
        "riskoff_min_rs": _f_env("AI_EXEC_RISKOFF_MIN_RS63", 1.5),
        "min_adx_buy": _f_env("AI_EXEC_MIN_ADX_BUY", 10.0),
        "min_vol_buy": _f_env("AI_EXEC_MIN_VOL_RATIO_BUY", 0.7),
        "max_rsi_buy": _f_env("AI_EXEC_MAX_RSI_BUY", 76.0),
        "max_vix_buy": _f_env("AI_EXEC_MAX_VIX_BUY", 30.0),
        "risk_on": _f_env("AI_EXEC_RISK_BUDGET_ON", 1.0),
        "risk_neutral": _f_env("AI_EXEC_RISK_BUDGET_NEUTRAL", 0.9),
        "risk_off": _f_env("AI_EXEC_RISK_BUDGET_OFF", 0.55),
        "w_high": _f_env("AI_EXEC_CONF_W_HIGH", 1.0),
        "w_med": _f_env("AI_EXEC_CONF_W_MEDIUM", 0.75),
        "w_low": _f_env("AI_EXEC_CONF_W_LOW", 0.55),
        "riskoff_buy_scale": _f_env("AI_EXEC_RISKOFF_BUY_SCALE", 0.60),
    }


def _exec_action(wf_action: str, row: pd.Series, cfg: dict[str, Any]) -> str:
    a = _norm_action(wf_action)
    mode = cfg["mode"] if cfg["mode"] in {"long_cash", "long_short"} else "long_cash"
    if mode == "long_cash" and a == "SELL":
        a = "HOLD"
    dte = row.get("days_to_earnings")
    if dte is not None and not pd.isna(dte) and int(dte) <= cfg["earn_block"] and a in {"BUY", "SELL"}:
        return "HOLD"
    regime = str(row.get("market_regime", "neutral"))
    rs63, adx, vol, rsi = _f(row.get("relative_strength_63d")), _f(row.get("adx")), _f(row.get("volume_ratio"), 1), _f(row.get("rsi"), 50)
    conf = str(row.get("confidence", "medium")).lower().strip()
    vix = row.get("vix_close")
    vix = None if vix is None or pd.isna(vix) else _f(vix)
    if a == "BUY":
        if adx < cfg["min_adx_buy"] or vol < cfg["min_vol_buy"] or rsi > cfg["max_rsi_buy"]:
            return "HOLD"
        if vix is not None and vix > cfg["max_vix_buy"]:
            return "HOLD"
        if regime == "risk_off":
            if cfg["riskoff_high_conf"] and conf != "high":
                return "HOLD"
            if rs63 < cfg["riskoff_min_rs"]:
                return "HOLD"
        elif rs63 < cfg["min_rs_buy"]:
            return "HOLD"
    if a == "SELL":
        if mode != "long_short" or regime != "risk_off":
            return "HOLD"
    return a


def _exec_weight(exec_action: str, row: pd.Series, cfg: dict[str, Any]) -> float:
    if exec_action == "HOLD":
        return 0.0
    conf = str(row.get("confidence", "medium")).strip().lower()
    regime = str(row.get("market_regime", "neutral"))
    wc = {"high": cfg["w_high"], "medium": cfg["w_med"], "low": cfg["w_low"]}.get(conf, cfg["w_med"])
    wr = {"risk_on": cfg["risk_on"], "neutral": cfg["risk_neutral"], "risk_off": cfg["risk_off"]}.get(regime, cfg["risk_neutral"])
    w = wc * wr
    dte = row.get("days_to_earnings")
    if dte is not None and not pd.isna(dte) and int(dte) <= 3:
        w *= 0.7
    vix = row.get("vix_close")
    if vix is not None and not pd.isna(vix):
        vv = _f(vix)
        if vv >= 30:
            w *= 0.6
        elif vv >= 25:
            w *= 0.85
    if exec_action == "BUY" and regime == "risk_off":
        w *= cfg["riskoff_buy_scale"]
    return float(round(_clamp(w, 0.0, 1.0), 4))


def _eval_df(df: pd.DataFrame, action_col: str, truth_col: str = "true_label") -> dict[str, Any]:
    act = df[action_col].astype(str).str.upper()
    truth = df[truth_col].astype(str).str.upper()
    hit = float((act == truth).mean() * 100.0)
    abs_ret = float(np.mean([_strat_ret(a, r) for a, r in zip(act.tolist(), df["future_return_63d"].astype(float).tolist())]))
    alpha_ret = float(np.mean([_strat_ret(a, r) for a, r in zip(act.tolist(), df["alpha_63d"].astype(float).tolist())]))
    cls = {}
    recs = []
    for c in ["BUY", "SELL", "HOLD"]:
        p = act == c
        t = truth == c
        prec = float((truth[p] == c).mean() * 100.0) if int(p.sum()) > 0 else 0.0
        rec = float((act[t] == c).mean() * 100.0) if int(t.sum()) > 0 else 0.0
        f1 = 0.0 if prec + rec == 0 else (2 * prec * rec / (prec + rec))
        cls[c] = {"support_pred": int(p.sum()), "support_true": int(t.sum()), "precision_pct": prec, "recall_pct": rec, "f1_pct": f1}
        recs.append(rec)
    sh = float((cls["SELL"]["precision_pct"] + cls["HOLD"]["precision_pct"]) / 2.0)
    return {
        "hit_rate_pct": hit,
        "avg_strategy_return_pct": abs_ret,
        "avg_strategy_alpha_return_pct": alpha_ret,
        "avg_forward_return_pct": float(df["future_return_63d"].mean()),
        "avg_alpha_return_pct": float(df["alpha_63d"].mean()),
        "balanced_recall_pct": float(np.mean(recs)),
        "sell_hold_precision_pct": sh,
        "class_metrics": cls,
    }


def _map_score(m: dict[str, Any], obj: str, min_support: int) -> tuple[float, ...]:
    s = int((m["class_metrics"]["SELL"]["support_pred"]))
    h = int((m["class_metrics"]["HOLD"]["support_pred"]))
    if obj == "overall_hit":
        return (m["hit_rate_pct"], m["avg_strategy_return_pct"], m["avg_strategy_alpha_return_pct"])
    if obj == "balanced_recall":
        return (m["balanced_recall_pct"], m["sell_hold_precision_pct"], m["avg_strategy_alpha_return_pct"], m["hit_rate_pct"])
    if obj == "sell_hold_precision":
        if s < min_support or h < min_support:
            return (-1e9, -1e9, -1e9, -1e9)
        return (m["sell_hold_precision_pct"], m["balanced_recall_pct"], m["avg_strategy_alpha_return_pct"], m["hit_rate_pct"])
    if obj == "execution_alpha":
        return (m["avg_strategy_alpha_return_pct"], m["avg_strategy_return_pct"], m["sell_hold_precision_pct"], m["balanced_recall_pct"], m["hit_rate_pct"])
    if s < min_support or h < min_support:
        return (-1e9, -1e9, -1e9, -1e9, -1e9)
    return (m["sell_hold_precision_pct"], m["balanced_recall_pct"], m["avg_strategy_alpha_return_pct"], m["avg_strategy_return_pct"], m["hit_rate_pct"])


def _best_map(train: pd.DataFrame, source_col: str, obj: str, min_support: int) -> dict[str, str]:
    best_map = {"BUY": "BUY", "SELL": "SELL", "HOLD": "HOLD"}
    best_sc = _map_score(_eval_df(train, source_col), obj, min_support)
    for m in [{"BUY": b, "SELL": s, "HOLD": h} for b, s, h in itertools.product(["BUY", "SELL", "HOLD"], repeat=3)]:
        t = train.copy()
        t["_m"] = t[source_col].map(m).fillna("HOLD")
        sc = _map_score(_eval_df(t, "_m"), obj, min_support)
        if sc > best_sc:
            best_map, best_sc = m, sc
    return best_map


def _risk_metrics(series_pct: pd.Series, periods_per_year: int) -> dict[str, float]:
    s = pd.to_numeric(series_pct, errors="coerce").dropna().astype(float)
    if len(s) == 0:
        return {"periods": 0, "mean_period_return_pct": 0.0, "cagr_pct": 0.0, "vol_annual_pct": 0.0, "sharpe": 0.0, "sortino": 0.0, "max_drawdown_pct": 0.0, "win_rate_pct": 0.0, "total_return_pct": 0.0}
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


def _ai_decisions(snapshot_date: str, market_ctx: dict[str, Any], features: list[dict[str, Any]], cache: dict[str, Any]) -> dict[str, Any]:
    from ai.analyzer import AIAnalyzer

    analyzer = AIAnalyzer(model=MODEL)
    if not analyzer.has_api_access:
        raise RuntimeError("Codex login required. Run: codex login")
    lines = []
    for x in features:
        dte = x.get("days_to_earnings")
        dte = "NA" if dte is None else str(int(dte))
        lines.append(
            f"- {x['symbol']} rsi={x['rsi']:.1f} ma50_gap={x['ma50_gap']:.1f}% "
            f"ret21={x['return_21d']:.2f}% ret63={x['return_63d']:.2f}% "
            f"rs21={x.get('relative_strength_21d', 0.0):.2f}%p rs63={x['relative_strength_63d']:.2f}%p "
            f"bb={x['bb_position']:.1f} atr={x['atr_pct']:.2f}% adx={x['adx']:.1f} vol={x['volume_ratio']:.2f} dte={dte}"
        )
    vix = market_ctx.get("vix_close")
    vix_txt = "NA" if vix is None else f"{float(vix):.2f}"
    if HORIZON_MODE == "next_snapshot":
        horizon_txt = f"until next {SNAPSHOT_FREQ} rebalance"
    else:
        horizon_txt = "a 3-month" if HORIZON_DAYS == 63 else f"{HORIZON_DAYS}-trading-day (~{HORIZON_DAYS/21:.1f} month)"
    prompt = (
        "You are a disciplined quantitative trader.\n"
        "Use only chart and market-regime inputs below. Ignore news/fundamentals.\n"
        f"Task: Decide BUY/SELL/HOLD for each symbol for {horizon_txt} horizon.\n"
        "Output STRICT JSON only.\n\n"
        f"Snapshot date: {snapshot_date}\n"
        f"Market regime: {market_ctx.get('regime', 'neutral')}\n"
        f"Benchmark ret63: {float(market_ctx.get('bench_r63', 0.0)):.2f}%\n"
        f"VIX close: {vix_txt}\n\n"
        "Symbols:\n"
        f"{chr(10).join(lines)}\n\n"
        'JSON format: {"AAPL":{"action":"BUY|SELL|HOLD","confidence":"high|medium|low","reason":"<=10 words"}, "...":{...}}'
    )
    ph = hashlib.sha256(prompt.encode("utf-8", errors="ignore")).hexdigest()[:12]
    cache_key = f"{MODEL}:{PROMPT_VERSION}:{snapshot_date}:{ph}"
    if cache_key in cache and isinstance(cache[cache_key], dict):
        return cache[cache_key]

    legacy_key = f"{MODEL}:{PROMPT_VERSION}:{snapshot_date}"
    if HORIZON_DAYS == 63 and HORIZON_MODE == "fixed_days":
        legacy = cache.get(legacy_key)
        if isinstance(legacy, dict) and legacy:
            cache[cache_key] = legacy
            _save_json(AI_CACHE, cache)
            return legacy

    raw = analyzer._call(prompt, max_tokens=2200)
    parsed = _extract_json(raw or "")
    if not parsed:
        raw2 = analyzer._call(prompt + "\nReturn one minified JSON object only.", max_tokens=2200)
        parsed = _extract_json(raw2 or "")
    if not parsed:
        raise RuntimeError(f"AI JSON parse failed for {snapshot_date}")
    cache[cache_key] = parsed
    _save_json(AI_CACHE, cache)
    return parsed


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

    frames = _build_frames(symbols)
    if not frames:
        raise RuntimeError("No market data loaded")
    if BENCH not in frames:
        raise RuntimeError(f"Benchmark data not found: {BENCH}")

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

    use_earnings = _b_env("AI_USE_EARNINGS_CALENDAR", True)
    earn = _earnings_calendar(symbols) if use_earnings else {}
    label_cfg = _label_cfg()
    exec_cfg = _exec_cfg()
    trade_cost_bps = _f_env("AI_TRADE_COST_BPS", _f_env("AI_COST_BPS", 0.0))
    trade_cost_pct = float(trade_cost_bps) / 100.0
    top_k = max(0, min(_i_env("AI_PORTFOLIO_TOP_K", 5), len(symbols)))
    periods_per_year = _periods_per_year(SNAPSHOT_FREQ)
    prompt_max_symbols = max(5, _i_env("AI_PROMPT_MAX_SYMBOLS", 30))
    prompt_select_mode = str(os.getenv("AI_PROMPT_SELECT_MODE", "mix")).strip().lower() or "mix"
    prompt_batching = _b_env("AI_PROMPT_BATCHING", False)

    started_at = datetime.now(timezone.utc)
    ai_calls = 0
    prompt_symbol_total = 0
    rows: list[dict[str, Any]] = []

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
        br = (bench_exit_px / bench_entry_px - 1.0) * 100.0
        snap = str(qdt.date())
        active_symbols = universe_by_date.get(snap, symbols) if universe_by_date else symbols
        active_symbols = [s for s in active_symbols if s in ind_by_symbol]
        feats: list[dict[str, Any]] = []
        evals: dict[str, dict[str, Any]] = {}
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
            entry_day = pd.Timestamp(ind_df.index[entry_pos])
            dte = _days_to_next_earnings(entry_day, earn.get(s, []))
            evals[s] = {
                "future_return_63d": float(fr),
                "benchmark_return_63d": float(br),
                "alpha_63d": float(fr - br),
                "atr_pct": _f(ind_row.get("atr_pct")),
                "return_21d": _f(ind_row.get("return_21d")),
                "return_63d": _f(ind_row.get("return_63d")),
                "relative_strength_21d": float(rs21),
                "relative_strength_63d": float(rs63),
                "rsi": _f(ind_row.get("rsi"), 50.0),
                "ma50_gap": _f(ind_row.get("ma50_gap")),
                "adx": _f(ind_row.get("adx")),
                "volume_ratio": _f(ind_row.get("volume_ratio"), 1.0),
                "days_to_earnings": dte,
                "market_regime": str(mkt["regime"]),
                "vix_close": mkt.get("vix_close"),
            }
            feats.append({
                "symbol": s,
                "rsi": _f(ind_row.get("rsi"), 50.0),
                "ma50_gap": _f(ind_row.get("ma50_gap")),
                "return_21d": _f(ind_row.get("return_21d")),
                "return_63d": _f(ind_row.get("return_63d")),
                "relative_strength_21d": float(rs21),
                "relative_strength_63d": float(rs63),
                "bb_position": _f(ind_row.get("bb_position"), 50.0),
                "atr_pct": _f(ind_row.get("atr_pct")),
                "adx": _f(ind_row.get("adx")),
                "volume_ratio": _f(ind_row.get("volume_ratio"), 1.0),
                "days_to_earnings": dte,
            })
        if len(feats) < 5:
            continue
        batches = _prompt_batches(feats, prompt_max_symbols, prompt_select_mode, prompt_batching)
        prompted: set[str] = set()
        dec: dict[str, Any] = {}
        for b in batches:
            bsyms = {str(x.get("symbol", "")).upper().strip() for x in b if isinstance(x, dict)}
            prompted |= {s for s in bsyms if s}
            prompt_symbol_total += len(bsyms)
            ai_calls += 1
            out = _ai_decisions(snap, mkt, b, cache)
            if isinstance(out, dict):
                for k, v in out.items():
                    kk = str(k).upper().strip()
                    if kk and kk in bsyms:
                        dec[kk] = v
        for s, vals in evals.items():
            d = dec.get(s, {}) if isinstance(dec, dict) else {}
            rows.append({
                "date": snap,
                "symbol": s,
                "prompted": int(str(s).upper().strip() in prompted),
                "action": _norm_action(d.get("action")),
                "confidence": str(d.get("confidence", "medium")).strip().lower() or "medium",
                "reason": str(d.get("reason", "")).strip()[:160],
                **vals,
            })

    if not rows:
        raise RuntimeError("No backtest rows generated")

    df = pd.DataFrame(rows).sort_values(["date", "symbol"]).reset_index(drop=True)
    df["year"] = pd.to_datetime(df["date"]).dt.year
    df["true_label"] = [
        _true_label(float(r), float(a), float(atr), float(br), label_cfg)
        for r, a, atr, br in zip(df["future_return_63d"], df["alpha_63d"], df["atr_pct"], df["benchmark_return_63d"])
    ]

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
        "label_config": label_cfg,
        "execution_config": exec_cfg,
        "prompt_max_symbols": prompt_max_symbols,
        "prompt_select_mode": prompt_select_mode,
        "prompt_batching": prompt_batching,
        "trade_cost_bps": trade_cost_bps,
    }
    cfg_hash = _config_hash(cfg_blob)
    df["run_tag"] = RUN_TAG
    df["config_hash"] = cfg_hash
    df["universe"] = universe_name
    df["snapshot_freq"] = SNAPSHOT_FREQ
    df["periods_per_year"] = periods_per_year
    df["horizon_days"] = HORIZON_DAYS
    df["horizon_mode"] = HORIZON_MODE
    df["trade_cost_bps"] = trade_cost_bps
    df["prompt_max_symbols"] = prompt_max_symbols
    df["prompt_select_mode"] = prompt_select_mode
    df["prompt_batching"] = int(prompt_batching)

    def _trade_cost(action: str, weight: float = 1.0) -> float:
        if trade_cost_pct <= 0:
            return 0.0
        a = str(action or "").strip().upper()
        if a in {"BUY", "SELL"}:
            return float(trade_cost_pct * float(weight))
        return 0.0

    df["correct"] = (df["action"].str.upper() == df["true_label"].str.upper()).astype(int)
    df["strategy_return"] = [
        _strat_ret(a, r) - _trade_cost(a)
        for a, r in zip(df["action"].str.upper(), df["future_return_63d"])
    ]
    df["strategy_alpha_return"] = [
        _strat_ret(a, r) - _trade_cost(a)
        for a, r in zip(df["action"].str.upper(), df["alpha_63d"])
    ]
    raw_eval = _eval_df(df, "action")

    obj = str(os.getenv("AI_WF_OBJECTIVE", "execution_alpha")).strip().lower()
    if obj not in {"overall_hit", "balanced_recall", "sell_hold_precision", "execution_alpha", "composite"}:
        obj = "execution_alpha"
    warmup = max(0, _i_env("AI_WF_WARMUP_ROWS", 24))
    min_support = max(0, _i_env("AI_WF_MIN_CLASS_SUPPORT", 8))
    prior_mode = str(os.getenv("AI_WF_PRIOR_MODE", "identity")).strip().lower()
    prior = {"BUY": "BUY", "SELL": "SELL", "HOLD": "HOLD"} if prior_mode != "always_buy" else {"BUY": "BUY", "SELL": "BUY", "HOLD": "BUY"}

    df["wf_action"] = "HOLD"
    df["wf_mapping"] = "BUY->BUY,SELL->SELL,HOLD->HOLD"
    for d in sorted(df["date"].unique()):
        tr = df[df["date"] < d]
        te_mask = df["date"] == d
        m = prior if len(tr) < warmup else _best_map(tr, "action", obj, min_support)
        df.loc[te_mask, "wf_action"] = df.loc[te_mask, "action"].map(m).fillna("HOLD")
        df.loc[te_mask, "wf_mapping"] = f"BUY->{m['BUY']},SELL->{m['SELL']},HOLD->{m['HOLD']}"

    df["wf_correct"] = (df["wf_action"].str.upper() == df["true_label"].str.upper()).astype(int)
    df["wf_strategy_return"] = [
        _strat_ret(a, r) - _trade_cost(a)
        for a, r in zip(df["wf_action"].str.upper(), df["future_return_63d"])
    ]
    df["wf_strategy_alpha_return"] = [
        _strat_ret(a, r) - _trade_cost(a)
        for a, r in zip(df["wf_action"].str.upper(), df["alpha_63d"])
    ]
    wf_eval = _eval_df(df, "wf_action")

    df["exec_action"] = [_exec_action(a, row, exec_cfg) for a, (_, row) in zip(df["wf_action"], df.iterrows())]
    df["exec_weight"] = [_exec_weight(a, row, exec_cfg) for a, (_, row) in zip(df["exec_action"], df.iterrows())]
    df["exec_correct"] = (df["exec_action"].str.upper() == df["true_label"].str.upper()).astype(int)
    df["exec_return"] = [
        (_strat_ret(a, r) * w) - _trade_cost(a, w)
        for a, r, w in zip(df["exec_action"].str.upper(), df["future_return_63d"], df["exec_weight"])
    ]
    df["exec_alpha_return"] = [
        (_strat_ret(a, r) * w) - _trade_cost(a, w)
        for a, r, w in zip(df["exec_action"].str.upper(), df["alpha_63d"], df["exec_weight"])
    ]
    exec_eval = _eval_df(df, "exec_action")

    # Rotation-style portfolio: buy top-K signals (by exec_weight) and size positions with cash as residual.
    df["exec_portfolio_weight"] = 0.0
    for d in sorted(df["date"].unique()):
        g = df[df["date"] == d]
        picks = g[g["exec_action"] == "BUY"].copy()
        if picks.empty:
            continue
        if top_k > 0 and len(picks) > top_k:
            picks = picks.nlargest(top_k, "exec_weight")
        w = pd.to_numeric(picks["exec_weight"], errors="coerce").fillna(0.0).clip(lower=0.0).astype(float)
        sw = float(w.sum())
        if sw <= 0:
            continue
        if sw > 1.0:
            w = w / sw
        df.loc[w.index, "exec_portfolio_weight"] = w

    df["exec_portfolio_return"] = [
        (_strat_ret(a, r) * w) - _trade_cost(a, w)
        for a, r, w in zip(df["exec_action"].str.upper(), df["future_return_63d"], df["exec_portfolio_weight"])
    ]
    df["exec_portfolio_alpha_return"] = [
        (_strat_ret(a, r) * w) - _trade_cost(a, w)
        for a, r, w in zip(df["exec_action"].str.upper(), df["alpha_63d"], df["exec_portfolio_weight"])
    ]
    exec_port_series = df.groupby("date")["exec_portfolio_return"].sum()
    exec_port_alpha_series = df.groupby("date")["exec_portfolio_alpha_return"].sum()
    exec_port_exposure = float(df.groupby("date")["exec_portfolio_weight"].sum().mean() * 100.0)
    exec_port_positions = float(df.groupby("date")["exec_portfolio_weight"].apply(lambda s: int((pd.to_numeric(s, errors="coerce").fillna(0.0) > 0).sum())).mean())

    always_buy_abs_series = df.groupby("date")["future_return_63d"].mean() - trade_cost_pct
    always_buy_alpha_series = df.groupby("date")["alpha_63d"].mean() - trade_cost_pct
    pm = {
        "raw_abs": _risk_metrics(df.groupby("date")["strategy_return"].mean(), periods_per_year),
        "raw_alpha": _risk_metrics(df.groupby("date")["strategy_alpha_return"].mean(), periods_per_year),
        "walkforward_abs": _risk_metrics(df.groupby("date")["wf_strategy_return"].mean(), periods_per_year),
        "walkforward_alpha": _risk_metrics(df.groupby("date")["wf_strategy_alpha_return"].mean(), periods_per_year),
        "execution_abs": _risk_metrics(df.groupby("date")["exec_return"].mean(), periods_per_year),
        "execution_alpha": _risk_metrics(df.groupby("date")["exec_alpha_return"].mean(), periods_per_year),
        "execution_portfolio_abs": _risk_metrics(exec_port_series, periods_per_year),
        "execution_portfolio_alpha": _risk_metrics(exec_port_alpha_series, periods_per_year),
        "benchmark_abs": _risk_metrics(df.groupby("date")["benchmark_return_63d"].mean(), periods_per_year),
        "always_buy_abs": _risk_metrics(always_buy_abs_series, periods_per_year),
        "always_buy_alpha": _risk_metrics(always_buy_alpha_series, periods_per_year),
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
        "label_config": label_cfg,
        "execution_config": exec_cfg,
        "trade_cost_bps": trade_cost_bps,
        "trade_cost_pct": trade_cost_pct,
        "started_at_utc": started_at.isoformat(),
        "finished_at_utc": finished_at.isoformat(),
        "label_distribution_pct": {str(k): float(v) for k, v in df["true_label"].value_counts(normalize=True).mul(100).to_dict().items()},
        "walkforward_objective": obj,
        "walkforward_min_class_support": min_support,
        "walkforward_warmup_rows": warmup,
        "walkforward_prior_mode": prior_mode,
        "total_judgments": int(len(df)),
        "ai_calls": int(ai_calls),
        "avg_prompt_symbols": float(prompt_symbol_total / max(1, ai_calls)) if ai_calls > 0 else 0.0,
        "overall_hit_rate_pct": float(raw_eval["hit_rate_pct"]),
        "avg_strategy_return_pct": float(df["strategy_return"].mean()),
        "avg_strategy_alpha_return_pct": float(df["strategy_alpha_return"].mean()),
        "balanced_recall_pct": float(raw_eval["balanced_recall_pct"]),
        "sell_hold_precision_pct": float(raw_eval["sell_hold_precision_pct"]),
        "walkforward_tuned_hit_rate_pct": float(wf_eval["hit_rate_pct"]),
        "walkforward_tuned_avg_strategy_return_pct": float(df["wf_strategy_return"].mean()),
        "walkforward_tuned_avg_strategy_alpha_return_pct": float(df["wf_strategy_alpha_return"].mean()),
        "walkforward_tuned_balanced_recall_pct": float(wf_eval["balanced_recall_pct"]),
        "walkforward_tuned_sell_hold_precision_pct": float(wf_eval["sell_hold_precision_pct"]),
        "execution_hit_rate_pct": float(exec_eval["hit_rate_pct"]),
        "execution_avg_strategy_return_pct": float(df["exec_return"].mean()),
        "execution_avg_strategy_alpha_return_pct": float(df["exec_alpha_return"].mean()),
        "execution_balanced_recall_pct": float(exec_eval["balanced_recall_pct"]),
        "execution_sell_hold_precision_pct": float(exec_eval["sell_hold_precision_pct"]),
        "execution_avg_exposure_pct": float(df["exec_weight"].mean() * 100.0),
        "execution_active_signal_pct": float((df["exec_action"] != "HOLD").mean() * 100.0),
        "execution_portfolio_top_k": int(top_k),
        "execution_portfolio_avg_exposure_pct": float(exec_port_exposure),
        "execution_portfolio_avg_positions": float(exec_port_positions),
        "portfolio_metrics": pm,
        "walkforward_mapping_usage": {k: int(v) for k, v in df["wf_mapping"].value_counts().to_dict().items()},
        "always_buy_hit_rate_pct": float((df["true_label"] == "BUY").mean() * 100.0),
        "always_buy_baseline_abs_return_pct": float(df["future_return_63d"].mean() - trade_cost_pct),
        "always_buy_baseline_alpha_return_pct": float(df["alpha_63d"].mean() - trade_cost_pct),
    }

    df.to_csv(RUN_RESULT_CSV, index=False)
    _save_json(RUN_SUMMARY_JSON, summary)
    df.to_csv(RESULT_CSV.parent / (RESULT_CSV.name + f".tmp_{RUN_TAG}"), index=False)
    os.replace(RESULT_CSV.parent / (RESULT_CSV.name + f".tmp_{RUN_TAG}"), RESULT_CSV)
    _atomic_write_text(SUMMARY_JSON.parent / (SUMMARY_JSON.name + f".tmp_{RUN_TAG}"), json.dumps(summary, ensure_ascii=False, indent=2))
    os.replace(SUMMARY_JSON.parent / (SUMMARY_JSON.name + f".tmp_{RUN_TAG}"), SUMMARY_JSON)
    print(f"Saved: {RUN_RESULT_CSV.relative_to(ROOT)} ({len(df)} rows)")
    print(f"Saved: {RUN_SUMMARY_JSON.relative_to(ROOT)}")
    print(f"Updated latest: {RESULT_CSV.relative_to(ROOT)}")
    print(f"Updated latest: {SUMMARY_JSON.relative_to(ROOT)}")
    print(f"Raw AI -> hit {summary['overall_hit_rate_pct']:.2f}% | alpha {summary['avg_strategy_alpha_return_pct']:.2f}%")
    print(f"WF map -> hit {summary['walkforward_tuned_hit_rate_pct']:.2f}% | alpha {summary['walkforward_tuned_avg_strategy_alpha_return_pct']:.2f}%")
    print(
        f"Exec({exec_cfg['mode']}) -> hit {summary['execution_hit_rate_pct']:.2f}% | alpha {summary['execution_avg_strategy_alpha_return_pct']:.2f}% | "
        f"exposure {summary['execution_avg_exposure_pct']:.2f}% | Sharpe {pm['execution_abs']['sharpe']:.2f}"
    )
    print(
        f"ExecPortfolio(top_k={top_k or 'all'}) -> CAGR {pm['execution_portfolio_abs']['cagr_pct']:.2f}% | "
        f"Sharpe {pm['execution_portfolio_abs']['sharpe']:.2f} | exposure {exec_port_exposure:.2f}% | "
        f"pos {exec_port_positions:.1f}"
    )


if __name__ == "__main__":
    run()
