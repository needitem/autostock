
from __future__ import annotations

import io
import json
import math
import os
import re
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup

try:
    from pykrx import stock as pykrx_stock
except Exception:  # pragma: no cover
    pykrx_stock = None


REQUEST_TIMEOUT = 14
FRED_BASE = "https://fred.stlouisfed.org/graph/fredgraph.csv?id="
ECOS_BASE = "https://ecos.bok.or.kr/api/StatisticSearch/"
OPENDART_BASE = "https://opendart.fss.or.kr/api"


@dataclass
class SeriesPoint:
    date: str
    value: float


class FREDClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "autostock/2.0"})

    def fetch_series(self, series_id: str) -> pd.DataFrame:
        url = f"{FRED_BASE}{series_id}"
        resp = self.session.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text))
        col_date = "observation_date" if "observation_date" in df.columns else "DATE"
        val_col = [c for c in df.columns if c != col_date][0]
        df[col_date] = pd.to_datetime(df[col_date])
        df[val_col] = pd.to_numeric(df[val_col], errors="coerce")
        df = df.dropna().sort_values(col_date)
        return df.rename(columns={col_date: "date", val_col: "value"})


class ETFGIClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "autostock/2.0"})

    def _parse_inflow_value(self, text: str) -> float | None:
        if not text:
            return None
        lower = text.lower()
        if "net inflow" not in lower and "net inflows" not in lower:
            return None
        m = re.search(r"\$\s?([0-9,.]+)\s*(trillion|billion)", text, re.IGNORECASE)
        if not m:
            m = re.search(r"us\$\s*([0-9,.]+)\s*(trillion|billion)", text, re.IGNORECASE)
        if not m:
            return None
        val = float(m.group(1).replace(",", ""))
        unit = m.group(2).lower()
        return val * 1000.0 if unit.startswith("trillion") else val

    def fetch_recent_inflows(self, months_back: int = 6) -> dict[str, Any]:
        url = "https://etfgi.com/news/press-releases"
        try:
            resp = self.session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
        except Exception as exc:
            return {"error": f"etfgi fetch failed: {exc}", "items": []}

        soup = BeautifulSoup(resp.text, "html.parser")
        links = []
        for a in soup.select("a"):
            href = a.get("href") or ""
            if "/news/press-releases/" in href:
                if href.startswith("/"):
                    href = "https://etfgi.com" + href
                links.append(href)
        max_links = int(os.getenv("DEEP_ETFGI_MAX_LINKS", "30"))
        links = list(dict.fromkeys(links))[:max_links]

        items: list[dict[str, Any]] = []
        cutoff = pd.Timestamp.utcnow() - pd.DateOffset(months=months_back)

        for link in links:
            try:
                page = self.session.get(link, timeout=REQUEST_TIMEOUT)
                if page.status_code != 200:
                    continue
                psoup = BeautifulSoup(page.text, "html.parser")
                title = (psoup.find("h1") or psoup.find("title"))
                title_text = title.get_text(strip=True) if title else ""
                date_tag = psoup.find("time")
                date_text = date_tag.get("datetime") if date_tag else ""
                if not date_text:
                    meta_date = psoup.find("meta", {"property": "article:published_time"})
                    date_text = meta_date.get("content", "") if meta_date else ""
                if not date_text:
                    continue
                published = pd.to_datetime(date_text, errors="coerce")
                if published is pd.NaT or published < cutoff:
                    continue
                body = psoup.get_text(" ", strip=True)
                inflow = self._parse_inflow_value(body)
                items.append(
                    {
                        "date": published.date().isoformat(),
                        "title": title_text,
                        "inflow_usd_b": inflow,
                        "link": link,
                    }
                )
            except Exception:
                continue

        inflows = [i["inflow_usd_b"] for i in items if isinstance(i.get("inflow_usd_b"), (int, float))]
        total = float(sum(inflows)) if inflows else None
        return {"items": items, "total_usd_b": total}

class ECOSClient:
    def __init__(self, api_key: str | None) -> None:
        self.api_key = (api_key or "").strip()
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "autostock/2.0"})

    def fetch_series(
        self,
        stat_code: str,
        item_code: str,
        start: str,
        end: str,
        cycle: str = "M",
    ) -> pd.DataFrame | None:
        if not self.api_key:
            return None
        url = (
            f"{ECOS_BASE}{self.api_key}/json/kr/1/20000/"
            f"{stat_code}/{cycle}/{start}/{end}/{item_code}"
        )
        resp = self.session.get(url, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            return None
        payload = resp.json()
        data = payload.get("StatisticSearch") or {}
        rows = data.get("row") or []
        if not rows:
            return None
        df = pd.DataFrame(rows)
        date_col = "TIME" if "TIME" in df.columns else "TIME_PERIOD"
        val_col = "DATA_VALUE"
        df["date"] = pd.to_datetime(df[date_col], errors="coerce")
        df["value"] = pd.to_numeric(df[val_col], errors="coerce")
        df = df.dropna(subset=["date", "value"]).sort_values("date")
        return df[["date", "value"]]


class OpenDARTClient:
    def __init__(self, api_key: str | None) -> None:
        self.api_key = (api_key or "").strip()
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "autostock/2.0"})
        self._corp_map: dict[str, str] | None = None

    def has_key(self) -> bool:
        return bool(self.api_key)

    def _download_corp_codes(self) -> dict[str, str]:
        if not self.api_key:
            return {}
        url = f"{OPENDART_BASE}/corpCode.xml"
        resp = self.session.get(url, params={"crtfc_key": self.api_key}, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        with zf.open("CORPCODE.xml") as fh:
            xml = fh.read().decode("utf-8", errors="ignore")
        codes = {}
        for corp_code, stock_code in re.findall(
            r"<corp_code>(\d+)</corp_code>\s*<corp_name>.*?</corp_name>\s*<stock_code>(\d+)</stock_code>",
            xml,
            flags=re.S,
        ):
            if stock_code and stock_code != " " * len(stock_code):
                codes[stock_code] = corp_code
        return codes

    def get_corp_code(self, stock_code: str) -> str | None:
        if not self.api_key:
            return None
        if self._corp_map is None:
            self._corp_map = self._download_corp_codes()
        return (self._corp_map or {}).get(stock_code)

    def list_disclosures(self, corp_code: str, start: str, end: str) -> list[dict[str, Any]]:
        if not self.api_key:
            return []
        url = f"{OPENDART_BASE}/list.json"
        params = {
            "crtfc_key": self.api_key,
            "corp_code": corp_code,
            "bgn_de": start,
            "end_de": end,
            "page_no": 1,
            "page_count": 100,
        }
        resp = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        data = resp.json()
        return data.get("list") or []

    def fetch_fs(self, corp_code: str, year: str, report_code: str = "11011") -> list[dict[str, Any]]:
        if not self.api_key:
            return []
        url = f"{OPENDART_BASE}/fnlttSinglAcntAll.json"
        params = {
            "crtfc_key": self.api_key,
            "corp_code": corp_code,
            "bsns_year": year,
            "reprt_code": report_code,
        }
        resp = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        data = resp.json()
        return data.get("list") or []


class GDELTClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "autostock/2.0"})

    def count_mentions(self, query: str, timespan: str = "1month") -> int:
        url = "https://api.gdeltproject.org/api/v2/doc/doc"
        params = {
            "query": query,
            "mode": "artlist",
            "maxrecords": 250,
            "timespan": timespan,
            "format": "json",
        }
        resp = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            return 0
        data = resp.json()
        return len(data.get("articles", []))


def _six_month_change(df: pd.DataFrame) -> tuple[SeriesPoint, SeriesPoint, float | None]:
    if df.empty:
        return SeriesPoint("", math.nan), SeriesPoint("", math.nan), None
    latest = df.iloc[-1]
    latest_date = pd.to_datetime(latest["date"])
    target_date = latest_date - pd.DateOffset(months=6)
    base_df = df[df["date"] <= target_date]
    if base_df.empty:
        return (
            SeriesPoint(latest_date.date().isoformat(), float(latest["value"])),
            SeriesPoint("", math.nan),
            None,
        )
    base = base_df.iloc[-1]
    base_date = pd.to_datetime(base["date"])
    pct = None
    if float(base["value"]) != 0:
        pct = (float(latest["value"]) / float(base["value"]) - 1.0) * 100.0
    return (
        SeriesPoint(latest_date.date().isoformat(), float(latest["value"])),
        SeriesPoint(base_date.date().isoformat(), float(base["value"])),
        pct,
    )


def _score_risk_on_off(metrics: dict[str, Any]) -> dict[str, Any]:
    score = 0.0
    notes = []

    m2 = metrics.get("m2_6m_pct")
    if isinstance(m2, (int, float)):
        if m2 >= 3:
            score += 1.5
        elif m2 >= 1:
            score += 0.8
        elif m2 < 0:
            score -= 1.0

    real_rate = metrics.get("real_rate_10y")
    if isinstance(real_rate, (int, float)):
        if real_rate >= 2.0:
            score -= 1.5
        elif real_rate >= 1.5:
            score -= 1.0
        elif real_rate <= 0.5:
            score += 0.8

    curve = metrics.get("yield_curve_10y_2y")
    if isinstance(curve, (int, float)):
        if curve > 0.4:
            score += 0.6
        elif curve < 0:
            score -= 1.0

    usd = metrics.get("dollar_6m_pct")
    if isinstance(usd, (int, float)):
        if usd >= 2:
            score -= 0.8
        elif usd <= -2:
            score += 0.6

    etf = metrics.get("etf_flow_6m_total_usd_b")
    if isinstance(etf, (int, float)):
        if etf >= 200:
            score += 1.0
        elif etf <= 0:
            score -= 0.8
    else:
        notes.append("ETF flow missing")

    if score >= 1.5:
        label = "risk_on"
    elif score <= -1.5:
        label = "risk_off"
    else:
        label = "neutral"

    return {"score": round(score, 2), "label": label, "notes": notes}


def _scenario_probs(metrics: dict[str, Any]) -> dict[str, float]:
    real_rate = metrics.get("real_rate_10y")
    cpi_6m_ann = metrics.get("cpi_6m_annualized")
    unrate = metrics.get("unemployment_rate")
    gdp_yoy = metrics.get("real_gdp_yoy")

    probs = {
        "rate_cuts_accelerate": 0.25,
        "stagflation": 0.20,
        "recession": 0.15,
        "soft_landing": 0.40,
    }

    if isinstance(real_rate, (int, float)) and real_rate >= 1.8:
        probs["recession"] += 0.05
        probs["soft_landing"] -= 0.05

    if isinstance(cpi_6m_ann, (int, float)) and cpi_6m_ann >= 3.5:
        probs["stagflation"] += 0.05
        probs["soft_landing"] -= 0.05

    if isinstance(unrate, (int, float)) and unrate >= 4.5:
        probs["recession"] += 0.05
        probs["soft_landing"] -= 0.05

    if isinstance(gdp_yoy, (int, float)) and gdp_yoy <= 0.5:
        probs["recession"] += 0.05
        probs["soft_landing"] -= 0.05

    total = sum(probs.values())
    if total > 0:
        for k in probs:
            probs[k] = round(probs[k] / total, 3)

    return probs


def _to_json(value: Any) -> Any:
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    return value


def _latest_business_day() -> str:
    today = datetime.now().strftime("%Y%m%d")
    if pykrx_stock:
        try:
            return pykrx_stock.get_nearest_business_day_in_a_week(today)
        except Exception:
            return today
    return today


def _get_kospi_sector_indices() -> list[dict[str, str]]:
    if not pykrx_stock:
        return []
    indices = []
    for code in pykrx_stock.get_index_ticker_list(market="KOSPI"):
        name = pykrx_stock.get_index_ticker_name(code)
        if "코스피" in name and "업종" in name:
            indices.append({"code": code, "name": name})
    return indices


def _fetch_index_fundamental_10y(code: str) -> pd.DataFrame | None:
    if not pykrx_stock:
        return None
    end = _latest_business_day()
    start = (datetime.now() - timedelta(days=365 * 10)).strftime("%Y%m%d")
    try:
        df = pykrx_stock.get_index_fundamental_by_date(start, end, code)
        if df is None or df.empty:
            return None
        df = df.rename(columns=str.lower)
        df = df.reset_index().rename(columns={"날짜": "date", "date": "date"})
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        return df.dropna(subset=["date"])
    except Exception:
        return None


def _index_roe_from_per_pbr(per: float | None, pbr: float | None) -> float | None:
    if not per or not pbr:
        return None
    if per <= 0 or pbr <= 0:
        return None
    return round((pbr / per) * 100.0, 2)


def _get_index_constituents(index_code: str) -> list[str]:
    if not pykrx_stock:
        return []
    try:
        return list(pykrx_stock.get_index_portfolio_deposit_file(index_code))
    except Exception:
        return []


def _get_ticker_name(ticker: str) -> str:
    if not pykrx_stock:
        return ticker
    try:
        return pykrx_stock.get_market_ticker_name(ticker)
    except Exception:
        return ticker


def _market_cap_weights(date: str, tickers: list[str]) -> dict[str, float]:
    if not pykrx_stock or not tickers:
        return {}
    try:
        cap = pykrx_stock.get_market_cap_by_ticker(date)
        cap = cap.loc[cap.index.intersection(tickers)]
        cap = cap["시가총액"].astype(float)
        total = cap.sum()
        if total <= 0:
            return {}
        return {t: float(v) / total for t, v in cap.items()}
    except Exception:
        return {}


def _calc_sector_eps_bps(date: str, tickers: list[str]) -> dict[str, float]:
    if not pykrx_stock or not tickers:
        return {"eps": math.nan, "bps": math.nan}
    try:
        fund = pykrx_stock.get_market_fundamental_by_ticker(date)
        fund = fund.loc[fund.index.intersection(tickers)]
        eps = fund["EPS"].replace(0, math.nan).astype(float)
        bps = fund["BPS"].replace(0, math.nan).astype(float)
        return {"eps": float(eps.mean(skipna=True)), "bps": float(bps.mean(skipna=True))}
    except Exception:
        return {"eps": math.nan, "bps": math.nan}

def _score_transition(row: dict[str, Any]) -> float:
    score = 0.0
    if row.get("turnaround"):
        score += 0.4
    if row.get("volume_surge"):
        score += 0.3
    if row.get("short_decreasing"):
        score += 0.3
    return round(min(0.95, max(0.05, score)), 2)


def _volume_surge(ticker: str, window: int = 60) -> bool:
    if not pykrx_stock:
        return False
    end = _latest_business_day()
    start = (datetime.now() - timedelta(days=window * 2)).strftime("%Y%m%d")
    try:
        df = pykrx_stock.get_market_ohlcv_by_date(start, end, ticker)
        if df is None or df.empty:
            return False
        vol = df["거래량"].astype(float)
        if len(vol) < 10:
            return False
        avg = vol.tail(window).mean()
        latest = vol.iloc[-1]
        return latest >= avg * 2.0
    except Exception:
        return False


def _short_balance_decreasing(ticker: str) -> bool:
    if not pykrx_stock:
        return False
    end = _latest_business_day()
    start = (datetime.now() - timedelta(days=120)).strftime("%Y%m%d")
    try:
        df = pykrx_stock.get_shorting_balance_by_date(start, end, ticker)
        if df is None or df.empty:
            return False
        first = df["잔고수량"].astype(float).iloc[0]
        last = df["잔고수량"].astype(float).iloc[-1]
        return last < first * 0.9
    except Exception:
        return False


def _turnaround_proxy(ticker: str) -> bool:
    if not pykrx_stock:
        return False
    end = _latest_business_day()
    try:
        fund = pykrx_stock.get_market_fundamental_by_ticker(end)
        if ticker not in fund.index:
            return False
        eps = float(fund.loc[ticker]["EPS"])
        return eps > 0
    except Exception:
        return False


def _risk_label(prob: float) -> str:
    if prob >= 0.7:
        return "high"
    if prob >= 0.45:
        return "medium"
    return "low"


def _fetch_kr_macro(fred: FREDClient, ecos: ECOSClient) -> dict[str, Any]:
    data: dict[str, Any] = {}

    m2 = None
    cpi = None
    if ecos.api_key:
        stat_code = os.getenv("ECOS_M2_STAT", "101Y013")
        item_code = os.getenv("ECOS_M2_ITEM", "BBGA00")
        cpi_stat = os.getenv("ECOS_CPI_STAT", "901Y009")
        cpi_item = os.getenv("ECOS_CPI_ITEM", "0")
        start = (datetime.now() - timedelta(days=365 * 6)).strftime("%Y%m")
        end = datetime.now().strftime("%Y%m")
        m2 = ecos.fetch_series(stat_code, item_code, start, end, cycle="M")
        cpi = ecos.fetch_series(cpi_stat, cpi_item, start, end, cycle="M")

    if m2 is None:
        try:
            m2 = fred.fetch_series(os.getenv("FRED_KR_M2", "MYAGM2KRM189N"))
        except Exception:
            m2 = None

    if cpi is None:
        try:
            cpi = fred.fetch_series(os.getenv("FRED_KR_CPI", "KORCPIALLMINMEI"))
        except Exception:
            cpi = None

    try:
        kr_10y = fred.fetch_series(os.getenv("FRED_KR_10Y", "IRLTLT01KRM156N"))
    except Exception:
        kr_10y = None

    try:
        kr_2y = fred.fetch_series(os.getenv("FRED_KR_2Y", "IRLTLT02KRM156N"))
    except Exception:
        kr_2y = None

    data["m2"] = m2
    data["cpi"] = cpi
    data["kr10y"] = kr_10y
    data["kr2y"] = kr_2y
    return data


def _analyze_liquidity(us: dict[str, Any], kr: dict[str, Any], etfgi: dict[str, Any]) -> dict[str, Any]:
    us_metrics = {
        "m2_6m_pct": us.get("m2_6m_pct"),
        "real_rate_10y": us.get("real_rate_10y"),
        "yield_curve_10y_2y": us.get("yield_curve_10y_2y"),
        "dollar_6m_pct": us.get("dollar_6m_pct"),
        "etf_flow_6m_total_usd_b": etfgi.get("total_usd_b"),
    }

    kr_metrics = {
        "m2_6m_pct": kr.get("m2_6m_pct"),
        "real_rate_10y": kr.get("real_rate_10y"),
        "yield_curve_10y_2y": kr.get("yield_curve_10y_2y"),
        "dollar_6m_pct": us.get("dollar_6m_pct"),
        "etf_flow_6m_total_usd_b": etfgi.get("total_usd_b"),
    }

    us_risk = _score_risk_on_off(us_metrics)
    kr_risk = _score_risk_on_off(kr_metrics)

    return {
        "us": {"metrics": us_metrics, "risk": us_risk},
        "kr": {"metrics": kr_metrics, "risk": kr_risk},
    }


def _build_kr_valuation_report() -> dict[str, Any]:
    if not pykrx_stock:
        return {"error": "pykrx not available"}

    indices = _get_kospi_sector_indices()
    if not indices:
        return {"error": "no sector indices"}

    report = []
    for idx in indices:
        df = _fetch_index_fundamental_10y(idx["code"])
        if df is None or df.empty:
            continue
        per = df["per"].iloc[-1] if "per" in df.columns else None
        pbr = df["pbr"].iloc[-1] if "pbr" in df.columns else None
        per_mean = df["per"].mean() if "per" in df.columns else math.nan
        per_std = df["per"].std() if "per" in df.columns else math.nan
        pbr_mean = df["pbr"].mean() if "pbr" in df.columns else math.nan
        pbr_std = df["pbr"].std() if "pbr" in df.columns else math.nan

        per_z = None
        pbr_z = None
        if per is not None and per_std and per_std > 0:
            per_z = (per - per_mean) / per_std
        if pbr is not None and pbr_std and pbr_std > 0:
            pbr_z = (pbr - pbr_mean) / pbr_std

        roe = _index_roe_from_per_pbr(per, pbr)
        report.append(
            {
                "sector_index": idx["name"],
                "code": idx["code"],
                "per": per,
                "pbr": pbr,
                "per_z": round(per_z, 2) if per_z is not None else None,
                "pbr_z": round(pbr_z, 2) if pbr_z is not None else None,
                "roe_proxy": roe,
            }
        )

    outliers = [r for r in report if (abs(r.get("per_z", 0)) >= 2) or (abs(r.get("pbr_z", 0)) >= 2)]
    return {"sectors": report, "outliers": outliers}


def _build_flow_reversal_report() -> dict[str, Any]:
    if not pykrx_stock:
        return {"error": "pykrx not available"}
    if os.getenv("DEEP_SKIP_FLOW", "0").strip() in {"1", "true", "yes", "on"}:
        return {"skipped": True}

    end = _latest_business_day()
    start = (datetime.now() - timedelta(days=95)).strftime("%Y%m%d")

    cap = pykrx_stock.get_market_cap_by_ticker(end)
    cap = cap.sort_values(by="시가총액", ascending=False).head(int(os.getenv("FLOW_UNIVERSE_SIZE", "60")))
    tickers = list(cap.index)

    rows = []
    for ticker in tickers:
        try:
            df = pykrx_stock.get_market_trading_value_by_date(start, end, ticker)
            if df is None or df.empty:
                continue
            foreign = float(df["외국인합계"].sum())
            inst = float(df["기관합계"].sum())
            volume_surge = _volume_surge(ticker)
            short_down = _short_balance_decreasing(ticker)
            turnaround = _turnaround_proxy(ticker)
            prob = _score_transition(
                {
                    "turnaround": turnaround,
                    "volume_surge": volume_surge,
                    "short_decreasing": short_down,
                }
            )
            rows.append(
                {
                    "ticker": ticker,
                    "name": _get_ticker_name(ticker),
                    "foreign_net": foreign,
                    "institution_net": inst,
                    "turnaround": turnaround,
                    "volume_surge": volume_surge,
                    "short_decreasing": short_down,
                    "probability": prob,
                    "confidence": _risk_label(prob),
                }
            )
        except Exception:
            continue

    foreign_top = sorted(rows, key=lambda x: -x.get("foreign_net", 0))[:20]
    inst_top = sorted(rows, key=lambda x: -x.get("institution_net", 0))[:20]

    candidates = [r for r in rows if r["probability"] >= 0.5]
    candidates = sorted(candidates, key=lambda x: -x["probability"])[:25]

    return {
        "foreign_top20": foreign_top,
        "institution_top20": inst_top,
        "reversal_candidates": candidates,
    }

def _industry_cycle_report() -> dict[str, Any]:
    if not pykrx_stock:
        return {"error": "pykrx not available"}
    if os.getenv("DEEP_SKIP_INDUSTRY", "0").strip() in {"1", "true", "yes", "on"}:
        return {"skipped": True}
    indices = _get_kospi_sector_indices()
    keywords = {
        "Semiconductor": ["반도체", "전기전자"],
        "Auto": ["운송장비", "자동차"],
        "Insurance": ["보험"],
        "Shipbuilding": ["조선"],
        "Nuclear": ["전기가스", "유틸"],
    }

    out = []
    for name, keys in keywords.items():
        matched = [i for i in indices if any(k in i["name"] for k in keys)]
        if not matched:
            out.append({"industry": name, "stage": "unknown", "reason": "index not found"})
            continue
        idx = matched[0]
        df = _fetch_index_fundamental_10y(idx["code"])
        if df is None or df.empty:
            out.append({"industry": name, "stage": "unknown", "reason": "no data"})
            continue
        per = df["per"].iloc[-1] if "per" in df.columns else None
        per_mean = df["per"].tail(60).mean() if "per" in df.columns else None
        pbr = df["pbr"].iloc[-1] if "pbr" in df.columns else None
        pbr_mean = df["pbr"].tail(60).mean() if "pbr" in df.columns else None
        stage = "expansion"
        if per and per_mean and per < per_mean * 0.85 and pbr and pbr_mean and pbr < pbr_mean * 0.85:
            stage = "downturn"
        elif per and per_mean and per > per_mean * 1.15 and pbr and pbr_mean and pbr > pbr_mean * 1.15:
            stage = "overheat"
        else:
            stage = "recovery"
        out.append({"industry": name, "stage": stage, "index": idx["name"]})

    return {"industries": out}


def _narrative_vs_numbers_report(gdelt: GDELTClient, dart: OpenDARTClient) -> dict[str, Any]:
    if not pykrx_stock:
        return {"error": "pykrx not available"}

    if os.getenv("DEEP_SKIP_GDELT", "0").strip() in {"1", "true", "yes", "on"}:
        return {"skipped": True, "note": "GDELT skipped by config"}

    end = _latest_business_day()
    caps = pykrx_stock.get_market_cap_by_ticker(end)
    caps = caps.sort_values(by="시가총액", ascending=False).head(int(os.getenv("DEEP_GDELT_MAX", "20")))

    items = []
    for ticker in caps.index:
        name = _get_ticker_name(ticker)
        count = gdelt.count_mentions(name, timespan=os.getenv("GDELT_TIMESPAN", "1month"))
        items.append({"ticker": ticker, "name": name, "news_count": count})

    top = sorted(items, key=lambda x: -x["news_count"])[:20]

    return {"top_news": top, "note": "financial improvements require OPENDART_API_KEY"}


def _default_risk_report(dart: OpenDARTClient) -> dict[str, Any]:
    if not dart.has_key():
        return {"error": "OPENDART_API_KEY required"}
    if os.getenv("DEEP_SKIP_DEFAULT", "0").strip() in {"1", "true", "yes", "on"}:
        return {"skipped": True}

    return {"warning": "DART parsing not fully configured", "top10": []}


def run_deep_research_pipeline(output_dir: str = "data/reports") -> dict[str, Any]:
    os.makedirs(output_dir, exist_ok=True)

    region = os.getenv("DEEP_REGION", "both").strip().lower()

    fred = FREDClient()
    etfgi = ETFGIClient()
    ecos = ECOSClient(os.getenv("BOK_ECOS_API_KEY"))
    dart = OpenDARTClient(os.getenv("OPENDART_API_KEY"))
    gdelt = GDELTClient()

    us_series = {
        "m2": "M2SL",
        "real_rate_10y": "DFII10",
        "yield_curve_10y_2y": "T10Y2Y",
        "dollar_index": "DTWEXBGS",
        "cpi": "CPIAUCSL",
        "unemployment": "UNRATE",
        "real_gdp": "GDPC1",
    }
    us_data: dict[str, Any] = {}
    for key, series_id in us_series.items():
        try:
            us_data[key] = fred.fetch_series(series_id)
        except Exception as exc:
            us_data[key] = {"error": str(exc)}

    m2_latest, m2_base, m2_6m = _six_month_change(us_data["m2"]) if isinstance(us_data["m2"], pd.DataFrame) else (None, None, None)
    usd_latest, usd_base, usd_6m = _six_month_change(us_data["dollar_index"]) if isinstance(us_data["dollar_index"], pd.DataFrame) else (None, None, None)

    real_rate_latest = float(us_data["real_rate_10y"].iloc[-1]["value"]) if isinstance(us_data["real_rate_10y"], pd.DataFrame) and not us_data["real_rate_10y"].empty else None
    curve_latest = float(us_data["yield_curve_10y_2y"].iloc[-1]["value"]) if isinstance(us_data["yield_curve_10y_2y"], pd.DataFrame) and not us_data["yield_curve_10y_2y"].empty else None

    cpi_6m_ann = None
    if isinstance(us_data["cpi"], pd.DataFrame):
        _, _, cpi_6m = _six_month_change(us_data["cpi"])
        if isinstance(cpi_6m, (int, float)):
            cpi_6m_ann = round(cpi_6m * 2, 2)

    gdp_yoy = None
    if isinstance(us_data["real_gdp"], pd.DataFrame) and len(us_data["real_gdp"]) >= 5:
        latest = us_data["real_gdp"].iloc[-1]
        prior = us_data["real_gdp"].iloc[-5]
        if float(prior["value"]) != 0:
            gdp_yoy = round((float(latest["value"]) / float(prior["value"]) - 1.0) * 100.0, 2)

    unrate = None
    if isinstance(us_data["unemployment"], pd.DataFrame) and not us_data["unemployment"].empty:
        unrate = float(us_data["unemployment"].iloc[-1]["value"])

    us_metrics = {
        "m2_latest": m2_latest.__dict__ if m2_latest else None,
        "m2_base": m2_base.__dict__ if m2_base else None,
        "m2_6m_pct": round(m2_6m, 2) if isinstance(m2_6m, (int, float)) else None,
        "real_rate_10y": round(real_rate_latest, 2) if isinstance(real_rate_latest, (int, float)) else None,
        "yield_curve_10y_2y": round(curve_latest, 2) if isinstance(curve_latest, (int, float)) else None,
        "dollar_latest": usd_latest.__dict__ if usd_latest else None,
        "dollar_base": usd_base.__dict__ if usd_base else None,
        "dollar_6m_pct": round(usd_6m, 2) if isinstance(usd_6m, (int, float)) else None,
        "cpi_6m_annualized": cpi_6m_ann,
        "unemployment_rate": unrate,
        "real_gdp_yoy": gdp_yoy,
    }

    kr_metrics = {}
    if region in {"both", "kr", "korea"}:
        kr_data = _fetch_kr_macro(fred, ecos)
        kr_m2_latest, kr_m2_base, kr_m2_6m = _six_month_change(kr_data["m2"]) if isinstance(kr_data.get("m2"), pd.DataFrame) else (None, None, None)
        kr_10y = kr_data.get("kr10y")
        kr_2y = kr_data.get("kr2y")
        kr_cpi = kr_data.get("cpi")

        kr_real = None
        if (
            isinstance(kr_10y, pd.DataFrame)
            and isinstance(kr_cpi, pd.DataFrame)
            and not kr_10y.empty
            and not kr_cpi.empty
            and "value" in kr_cpi.columns
        ):
            cpi_change = kr_cpi["value"].pct_change().iloc[-1]
            if pd.notna(cpi_change):
                kr_real = float(kr_10y.iloc[-1]["value"]) - float(cpi_change * 100.0)

        kr_curve = None
        if isinstance(kr_10y, pd.DataFrame) and isinstance(kr_2y, pd.DataFrame) and not kr_10y.empty and not kr_2y.empty:
            kr_curve = float(kr_10y.iloc[-1]["value"]) - float(kr_2y.iloc[-1]["value"])

        kr_metrics = {
            "m2_latest": kr_m2_latest.__dict__ if kr_m2_latest else None,
            "m2_base": kr_m2_base.__dict__ if kr_m2_base else None,
            "m2_6m_pct": round(kr_m2_6m, 2) if isinstance(kr_m2_6m, (int, float)) else None,
            "real_rate_10y": round(kr_real, 2) if isinstance(kr_real, (int, float)) else None,
            "yield_curve_10y_2y": round(kr_curve, 2) if isinstance(kr_curve, (int, float)) else None,
        }

    etfgi_data = etfgi.fetch_recent_inflows(months_back=6)
    liquidity = _analyze_liquidity(us_metrics, kr_metrics, etfgi_data)
    scenario_us = _scenario_probs(us_metrics)

    if region in {"both", "kr", "korea"}:
        valuation = _build_kr_valuation_report()
        flows = _build_flow_reversal_report()
        industry = _industry_cycle_report()
        narrative = _narrative_vs_numbers_report(gdelt, dart)
        default_risk = _default_risk_report(dart)
    else:
        valuation = {"skipped": True, "reason": "region=us"}
        flows = {"skipped": True, "reason": "region=us"}
        industry = {"skipped": True, "reason": "region=us"}
        narrative = {"skipped": True, "reason": "region=us"}
        default_risk = {"skipped": True, "reason": "region=us"}

    report = {
        "as_of": datetime.utcnow().isoformat() + "Z",
        "module1_liquidity": {
            "us": us_metrics,
            "kr": kr_metrics,
            "etf_flows": etfgi_data,
            "risk_on_off": liquidity,
        },
        "module2_valuation_distortion": valuation,
        "module3_flow_reversal": flows,
        "module4_industry_cycle": industry,
        "module5_narrative_vs_numbers": narrative,
        "module6_macro_scenarios": scenario_us,
        "module7_default_risk": default_risk,
        "data_gaps": [
            "OPENDART_API_KEY required for full financial/credit analysis",
            "BOK_ECOS_API_KEY recommended for Korea M2/CPI",
            "Korea 2y yield series may require manual config (FRED or ECOS)",
        ],
    }

    date_tag = datetime.now().strftime("%Y-%m-%d")
    json_path = os.path.join(output_dir, f"deep_research_report_{date_tag}.json")
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, ensure_ascii=False, indent=2, default=_to_json)

    md_path = os.path.join(output_dir, f"deep_research_report_{date_tag}.md")
    with open(md_path, "w", encoding="utf-8") as fh:
        fh.write("# Deep Research Report (Korea + US)\n\n")
        fh.write(f"as_of: {report['as_of']}\n\n")
        fh.write("## Module 1: Liquidity\n")
        fh.write(json.dumps(report["module1_liquidity"], ensure_ascii=False, indent=2, default=_to_json))
        fh.write("\n\n## Module 2: Valuation Distortion\n")
        fh.write(json.dumps(valuation, ensure_ascii=False, indent=2, default=_to_json))
        fh.write("\n\n## Module 3: Flow Reversal\n")
        fh.write(json.dumps(flows, ensure_ascii=False, indent=2, default=_to_json))
        fh.write("\n\n## Module 4: Industry Cycle\n")
        fh.write(json.dumps(industry, ensure_ascii=False, indent=2, default=_to_json))
        fh.write("\n\n## Module 5: Narrative vs Numbers\n")
        fh.write(json.dumps(narrative, ensure_ascii=False, indent=2, default=_to_json))
        fh.write("\n\n## Module 6: Macro Scenarios\n")
        fh.write(json.dumps(scenario_us, ensure_ascii=False, indent=2, default=_to_json))
        fh.write("\n\n## Module 7: Default Risk\n")
        fh.write(json.dumps(default_risk, ensure_ascii=False, indent=2, default=_to_json))
        fh.write("\n\n## Data Gaps\n")
        for gap in report["data_gaps"]:
            fh.write(f"- {gap}\n")

    return {"report": report, "json_path": json_path, "md_path": md_path}


if __name__ == "__main__":
    run_deep_research_pipeline()
