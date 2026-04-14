from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import requests


ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data" / "sec"
TICKERS_JSON = DATA_DIR / "company_tickers.json"
FACTS_DIR = DATA_DIR / "companyfacts"

SEC_HEADERS = {
    "User-Agent": os.getenv("SEC_USER_AGENT", "autostock research support@example.com"),
    "Accept-Encoding": "gzip, deflate",
}

_DURATION_TAG_CANDIDATES: dict[str, tuple[str, ...]] = {
    "revenue": (
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "SalesRevenueNet",
        "Revenues",
    ),
    "net_income": ("NetIncomeLoss",),
    "operating_income": ("OperatingIncomeLoss",),
    "gross_profit": ("GrossProfit",),
    "eps_diluted": ("EarningsPerShareDiluted",),
}

_INSTANT_TAG_CANDIDATES: dict[str, tuple[str, ...]] = {
    "assets": ("Assets",),
    "liabilities": ("Liabilities",),
    "equity": ("StockholdersEquity", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"),
}

_SEC_FORMS = {"10-Q", "10-K", "10-Q/A", "10-K/A"}


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return default
        return out
    except Exception:
        return default


def _parse_date(value: Any) -> date | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return datetime.strptime(value[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def _cache_json(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")


def _load_json(path: Path) -> dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def _fetch_company_tickers() -> dict[str, Any]:
    if TICKERS_JSON.exists():
        cached = _load_json(TICKERS_JSON)
        if cached:
            return cached
    headers = dict(SEC_HEADERS)
    headers["Host"] = "www.sec.gov"
    resp = requests.get("https://www.sec.gov/files/company_tickers.json", headers=headers, timeout=30)
    resp.raise_for_status()
    obj = resp.json()
    if not isinstance(obj, dict):
        raise RuntimeError("SEC tickers response malformed")
    _cache_json(TICKERS_JSON, obj)
    return obj


def load_ticker_to_cik() -> dict[str, str]:
    raw = _fetch_company_tickers()
    out: dict[str, str] = {}
    for _, item in raw.items():
        if not isinstance(item, dict):
            continue
        ticker = str(item.get("ticker") or "").strip().upper()
        cik = str(int(_to_float(item.get("cik_str"), 0))).zfill(10) if item.get("cik_str") is not None else ""
        if ticker and cik:
            out[ticker] = cik
    return out


def _companyfacts_path(cik: str) -> Path:
    return FACTS_DIR / f"CIK{cik}.json"


def load_companyfacts(cik: str, refresh: bool = False) -> dict[str, Any]:
    path = _companyfacts_path(cik)
    if path.exists() and not refresh:
        cached = _load_json(path)
        if cached:
            return cached
    headers = dict(SEC_HEADERS)
    headers["Host"] = "data.sec.gov"
    resp = requests.get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json", headers=headers, timeout=30)
    resp.raise_for_status()
    obj = resp.json()
    if not isinstance(obj, dict):
        raise RuntimeError(f"SEC companyfacts malformed for {cik}")
    _cache_json(path, obj)
    return obj


@dataclass(frozen=True)
class PreparedCompanyFacts:
    revenue: tuple["DurationFact", ...]
    net_income: tuple["DurationFact", ...]
    operating_income: tuple["DurationFact", ...]
    gross_profit: tuple["DurationFact", ...]
    eps_diluted: tuple["DurationFact", ...]
    assets: tuple["InstantFact", ...]
    liabilities: tuple["InstantFact", ...]
    equity: tuple["InstantFact", ...]


@dataclass(frozen=True)
class DurationFact:
    filed: date
    end: date
    value: float
    form: str


@dataclass(frozen=True)
class InstantFact:
    filed: date
    end: date
    value: float
    form: str


def _duration_facts(obj: dict[str, Any], tags: tuple[str, ...], unit_keys: tuple[str, ...]) -> list[DurationFact]:
    facts = (((obj.get("facts") or {}).get("us-gaap") or {}) if isinstance(obj, dict) else {}) or {}
    out: list[DurationFact] = []
    for tag in tags:
        node = facts.get(tag)
        if not isinstance(node, dict):
            continue
        units = node.get("units") or {}
        if not isinstance(units, dict):
            continue
        for unit_key in unit_keys:
            rows = units.get(unit_key)
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                form = str(row.get("form") or "")
                if form not in _SEC_FORMS:
                    continue
                filed = _parse_date(row.get("filed"))
                start = _parse_date(row.get("start"))
                end = _parse_date(row.get("end"))
                if filed is None or start is None or end is None:
                    continue
                duration = (end - start).days
                if duration < 70 or duration > 120:
                    continue
                out.append(
                    DurationFact(
                        filed=filed,
                        end=end,
                        value=_to_float(row.get("val"), math.nan),
                        form=form,
                    )
                )
            if out:
                return sorted(out, key=lambda x: (x.filed, x.end))
    return sorted(out, key=lambda x: (x.filed, x.end))


def _instant_facts(obj: dict[str, Any], tags: tuple[str, ...]) -> list[InstantFact]:
    facts = (((obj.get("facts") or {}).get("us-gaap") or {}) if isinstance(obj, dict) else {}) or {}
    out: list[InstantFact] = []
    for tag in tags:
        node = facts.get(tag)
        if not isinstance(node, dict):
            continue
        units = node.get("units") or {}
        if not isinstance(units, dict):
            continue
        rows = units.get("USD")
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            form = str(row.get("form") or "")
            if form not in _SEC_FORMS:
                continue
            filed = _parse_date(row.get("filed"))
            end = _parse_date(row.get("end"))
            if filed is None or end is None:
                continue
            out.append(
                InstantFact(
                    filed=filed,
                    end=end,
                    value=_to_float(row.get("val"), math.nan),
                    form=form,
                )
            )
        if out:
            return sorted(out, key=lambda x: (x.filed, x.end))
    return sorted(out, key=lambda x: (x.filed, x.end))


def _latest_duration_asof(rows: tuple[DurationFact, ...] | list[DurationFact], asof: date) -> DurationFact | None:
    for row in reversed(rows):
        if row.filed <= asof and math.isfinite(row.value):
            return row
    return None


def _prior_year_duration(rows: list[DurationFact], latest: DurationFact) -> DurationFact | None:
    target = latest.end.toordinal() - 365
    for row in reversed(rows):
        if (
            row.end < latest.end
            and abs(row.end.toordinal() - target) <= 45
            and math.isfinite(row.value)
        ):
            return row
    return None


def _latest_instant_asof(rows: tuple[InstantFact, ...] | list[InstantFact], asof: date) -> InstantFact | None:
    for row in reversed(rows):
        if row.filed <= asof and math.isfinite(row.value):
            return row
    return None


def _prepare_companyfacts(obj: dict[str, Any]) -> PreparedCompanyFacts:
    return PreparedCompanyFacts(
        revenue=tuple(_duration_facts(obj, _DURATION_TAG_CANDIDATES["revenue"], ("USD",))),
        net_income=tuple(_duration_facts(obj, _DURATION_TAG_CANDIDATES["net_income"], ("USD",))),
        operating_income=tuple(_duration_facts(obj, _DURATION_TAG_CANDIDATES["operating_income"], ("USD",))),
        gross_profit=tuple(_duration_facts(obj, _DURATION_TAG_CANDIDATES["gross_profit"], ("USD",))),
        eps_diluted=tuple(_duration_facts(obj, _DURATION_TAG_CANDIDATES["eps_diluted"], ("USD/shares",))),
        assets=tuple(_instant_facts(obj, _INSTANT_TAG_CANDIDATES["assets"])),
        liabilities=tuple(_instant_facts(obj, _INSTANT_TAG_CANDIDATES["liabilities"])),
        equity=tuple(_instant_facts(obj, _INSTANT_TAG_CANDIDATES["equity"])),
    )


def _build_pit_features_from_prepared(prepared: PreparedCompanyFacts, asof: date) -> dict[str, float | int | bool | str | None]:
    latest_rev = _latest_duration_asof(prepared.revenue, asof)
    prev_rev = _prior_year_duration(prepared.revenue, latest_rev) if latest_rev else None
    latest_ni = _latest_duration_asof(prepared.net_income, asof)
    prev_ni = _prior_year_duration(prepared.net_income, latest_ni) if latest_ni else None
    latest_oi = _latest_duration_asof(prepared.operating_income, asof)
    latest_gp = _latest_duration_asof(prepared.gross_profit, asof)
    latest_eps = _latest_duration_asof(prepared.eps_diluted, asof)
    prev_eps = _prior_year_duration(prepared.eps_diluted, latest_eps) if latest_eps else None
    latest_assets = _latest_instant_asof(prepared.assets, asof)
    latest_liab = _latest_instant_asof(prepared.liabilities, asof)
    latest_equity = _latest_instant_asof(prepared.equity, asof)

    def _yoy(cur: DurationFact | None, prev: DurationFact | None) -> float:
        if cur is None or prev is None or not math.isfinite(cur.value) or not math.isfinite(prev.value) or abs(prev.value) < 1e-9:
            return float("nan")
        return float((cur.value / prev.value - 1.0) * 100.0)

    latest_filed = max(
        [x.filed for x in (latest_rev, latest_ni, latest_oi, latest_gp, latest_eps, latest_assets, latest_liab, latest_equity) if x is not None],
        default=None,
    )
    filing_age_days = (asof - latest_filed).days if latest_filed is not None else None

    revenue = latest_rev.value if latest_rev is not None else float("nan")
    op_margin = (latest_oi.value / revenue * 100.0) if latest_oi is not None and math.isfinite(revenue) and abs(revenue) > 1e-9 else float("nan")
    gross_margin = (latest_gp.value / revenue * 100.0) if latest_gp is not None and math.isfinite(revenue) and abs(revenue) > 1e-9 else float("nan")
    debt_to_assets = (
        latest_liab.value / latest_assets.value
        if latest_liab is not None and latest_assets is not None and abs(latest_assets.value) > 1e-9
        else float("nan")
    )
    equity_ratio = (
        latest_equity.value / latest_assets.value
        if latest_equity is not None and latest_assets is not None and abs(latest_assets.value) > 1e-9
        else float("nan")
    )

    return {
        "pit_has_data": bool(latest_filed is not None),
        "pit_filing_age_days": int(filing_age_days) if filing_age_days is not None else None,
        "pit_rev_yoy_pct": _yoy(latest_rev, prev_rev),
        "pit_ni_yoy_pct": _yoy(latest_ni, prev_ni),
        "pit_eps_yoy_pct": _yoy(latest_eps, prev_eps),
        "pit_op_margin_pct": float(op_margin),
        "pit_gross_margin_pct": float(gross_margin),
        "pit_debt_to_assets": float(debt_to_assets),
        "pit_equity_ratio": float(equity_ratio),
    }


def build_pit_features(obj: dict[str, Any], asof: date) -> dict[str, float | int | bool | str | None]:
    return _build_pit_features_from_prepared(_prepare_companyfacts(obj), asof)


class SecPointInTimeStore:
    def __init__(self, symbols: list[str]):
        self._ticker_to_cik = load_ticker_to_cik()
        self._companyfacts: dict[str, PreparedCompanyFacts] = {}
        for symbol in sorted({str(s).upper() for s in symbols if isinstance(s, str) and s.strip()}):
            cik = self._ticker_to_cik.get(symbol)
            if not cik:
                continue
            try:
                self._companyfacts[symbol] = _prepare_companyfacts(load_companyfacts(cik))
            except Exception:
                continue

    def features_asof(self, symbol: str, asof: date) -> dict[str, Any]:
        prepared = self._companyfacts.get(str(symbol).upper())
        if not prepared:
            return {"pit_has_data": False}
        try:
            return _build_pit_features_from_prepared(prepared, asof)
        except Exception:
            return {"pit_has_data": False}
