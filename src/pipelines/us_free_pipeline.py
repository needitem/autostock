
from __future__ import annotations

import io
import json
import math
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup
from dotenv import load_dotenv


REQUEST_TIMEOUT = 12
FRED_BASE = "https://fred.stlouisfed.org/graph/fredgraph.csv?id="
SEC_13F_PAGE = "https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets"
FINRA_SI_PAGE = "https://www.finra.org/finra-data/browse-catalog/equity-short-interest/files"

CACHE_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "cache")

load_dotenv()


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

    def _detect_scope(self, title: str) -> str | None:
        t = (title or "").lower()
        if "global" in t or "world" in t:
            return "global"
        if "united states" in t or "u.s." in t or "us etf" in t or "u.s. etf" in t:
            return "us"
        if "europe" in t:
            return "europe"
        if "asia" in t or "pacific" in t:
            return "asia_pacific"
        if "canada" in t or "canadian" in t:
            return "canada"
        if "korea" in t or "korean" in t:
            return "korea"
        if "japan" in t:
            return "japan"
        return None

    def _detect_scope_from_body(self, body: str) -> str | None:
        t = (body or "").lower()
        if "in the united states" in t or "u.s. etf industry" in t:
            return "us"
        if "in europe" in t or "european etf industry" in t:
            return "europe"
        if "in canada" in t or "canadian etf industry" in t:
            return "canada"
        if "global etf industry" in t or "worldwide etf industry" in t:
            return "global"
        if "in korea" in t or "korean" in t:
            return "korea"
        return None

    def _parse_month_label(self, text: str) -> str | None:
        t = (text or "").lower()
        months = [
            "january",
            "february",
            "march",
            "april",
            "may",
            "june",
            "july",
            "august",
            "september",
            "october",
            "november",
            "december",
        ]
        for m in months:
            if m in t:
                return m
        return None

    def _detect_category(self, title: str) -> str | None:
        t = (title or "").lower()
        if "active" in t:
            return "active"
        if "equity" in t:
            return "equity"
        if "fixed income" in t or "bond" in t:
            return "fixed_income"
        if "commodity" in t:
            return "commodity"
        if "esg" in t or "sustainable" in t:
            return "esg"
        return None

    def _to_usd_b(self, val: float, unit: str) -> float:
        return val * 1000.0 if unit.startswith("tr") else val

    def _extract_structured(self, title: str, body: str) -> dict[str, Any]:
        out: dict[str, Any] = {
            "net_inflow_month_usd_b": None,
            "net_inflow_ytd_usd_b": None,
            "aum_usd_trn": None,
            "category_ytd_usd_b": None,
            "warnings": [],
        }
        title_l = (title or "").lower()
        scope = self._detect_scope(title) or self._detect_scope_from_body(body)
        out["scope"] = scope
        out["month_label"] = self._parse_month_label(title) or self._parse_month_label(body)
        category = self._detect_category(title)
        out["category"] = category
        out["industry_total"] = bool(("etf industry" in title_l or "etp industry" in title_l) or (scope and not category))

        # AUM: look for "assets ... reached ... US$X trillion"
        m = re.search(r"assets[^.]{0,80}reached[^.]{0,40}(us\$|\$)\s*([0-9,.]+)\s*(trillion|billion)", body, re.IGNORECASE)
        if m:
            val = float(m.group(2).replace(",", ""))
            unit = m.group(3).lower()
            out["aum_usd_trn"] = val if unit.startswith("tr") else val / 1000.0
        else:
            if "assets" not in title_l and "aum" not in title_l:
                out["warnings"].append("title missing assets keyword")

        # Monthly inflow
        m = re.search(
            r"during\s+(january|february|march|april|may|june|july|august|september|october|november|december)"
            r"[^.]{0,120}(gathered|attracted|recorded|saw|net inflows of)\s*(us\$|\$)\s*([0-9,.]+)\s*(trillion|billion|bn)",
            body,
            re.IGNORECASE,
        )
        if not m:
            m = re.search(
                r"(january|february|march|april|may|june|july|august|september|october|november|december)"
                r"[^.]{0,80}(gathered|attracted|recorded|saw|net inflows of)\s*(us\$|\$)\s*([0-9,.]+)\s*(trillion|billion|bn)",
                body,
                re.IGNORECASE,
            )
        if not m:
            m = re.search(r"net\s+inflows?\s+of\s*(us\$|\$)\s*([0-9,.]+)\s*(trillion|billion|bn)", body, re.IGNORECASE)
        if m:
            if m.lastindex and m.lastindex >= 5:
                month_label = m.group(1).lower()
                val = float(m.group(4).replace(",", ""))
                unit = m.group(5).lower()
                out["month_label"] = month_label
            else:
                val = float(m.group(m.lastindex-1).replace(",", ""))
                unit = m.group(m.lastindex).lower()
            out["net_inflow_month_usd_b"] = self._to_usd_b(val, unit)
        else:
            if "inflow" not in title_l and "inflows" not in title_l:
                out["warnings"].append("title missing inflow keyword")

        # YTD inflow (multiple patterns with sentence-aware filtering)
        ytd_patterns = [
            r"year[- ]to[- ]date[^.]{0,120}net\s+inflows?\s+to[^$]{0,20}(us\$|\$)\s*([0-9,.]+)\s*(trillion|billion|bn)",
            r"bringing\s+[^.]{0,60}net\s+inflows?\s+to[^$]{0,20}(us\$|\$)\s*([0-9,.]+)\s*(trillion|billion|bn)",
            r"net\s+inflows?\s+in\s+\d{4}\s+to[^$]{0,20}(us\$|\$)\s*([0-9,.]+)\s*(trillion|billion|bn)",
            r"bringing\s+\d{4}\s+net\s+inflows?\s+to[^$]{0,20}(us\$|\$)\s*([0-9,.]+)\s*(trillion|billion|bn)",
            r"(us\$|\$)\s*([0-9,.]+)\s*(trillion|billion|bn)[^.]{0,40}net\s+inflows?\s+in\s+\d{4}",
        ]
        sentences = re.split(r"(?<=[.!?])\s+", body)
        ytd_candidates: list[dict[str, Any]] = []
        category_words = (
            "equity",
            "fixed income",
            "bond",
            "active",
            "commodity",
            "esg",
            "sustainable",
            "crypto",
            "money market",
        )
        for sent in sentences:
            if not sent:
                continue
            sent_l = sent.lower()
            for pattern in ytd_patterns:
                m = re.search(pattern, sent, re.IGNORECASE)
                if not m:
                    continue
                val = float(m.group(2).replace(",", ""))
                unit = m.group(3).lower()
                is_category = any(word in sent_l for word in category_words)
                is_industry = (
                    "etf industry" in sent_l
                    or "etp industry" in sent_l
                    or "industry in" in sent_l
                    or "global etf industry" in sent_l
                    or "worldwide etf industry" in sent_l
                    or "canadian etf industry" in sent_l
                    or "u.s. etf industry" in sent_l
                    or "european etf industry" in sent_l
                )
                ytd_candidates.append(
                    {
                        "value_usd_b": self._to_usd_b(val, unit),
                        "is_category": is_category,
                        "is_industry": is_industry,
                    }
                )
        if ytd_candidates:
            industry_candidates = [c for c in ytd_candidates if c["is_industry"] and not c["is_category"]]
            non_category_candidates = [c for c in ytd_candidates if not c["is_category"]]
            chosen = None
            if industry_candidates:
                chosen = industry_candidates[0]
            elif non_category_candidates:
                chosen = non_category_candidates[0]
            if chosen:
                out["net_inflow_ytd_usd_b"] = chosen["value_usd_b"]
            else:
                out["category_ytd_usd_b"] = ytd_candidates[0]["value_usd_b"]
        if out["net_inflow_ytd_usd_b"] is None and out["category_ytd_usd_b"] is not None:
            out["warnings"].append("ytd_from_category_only")

        if category and not out["industry_total"]:
            out["warnings"].append("category_only")
            if out["net_inflow_ytd_usd_b"] is not None:
                out["category_ytd_usd_b"] = out["net_inflow_ytd_usd_b"]
                out["net_inflow_ytd_usd_b"] = None

        # Sanity checks
        if out["aum_usd_trn"] is not None and out["aum_usd_trn"] < 1 and "trillion" in title_l:
            out["warnings"].append("aum_trn<1 but title mentions trillion")
            out["aum_usd_trn"] = None
        if out["net_inflow_month_usd_b"] is not None and out["aum_usd_trn"] is not None:
            if abs(out["net_inflow_month_usd_b"] - out["aum_usd_trn"] * 1000.0) <= max(50.0, out["aum_usd_trn"] * 100.0):
                out["warnings"].append("inflow?aum*1000; likely misparse")
                out["net_inflow_month_usd_b"] = None

        return out

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
        max_links = int(os.getenv("US_ETFGI_MAX_LINKS", "10"))
        links = list(dict.fromkeys(links))[:max_links]

        items: list[dict[str, Any]] = []
        cutoff = pd.Timestamp.utcnow() - pd.DateOffset(months=months_back)

        def _infer_period_end(month_label: str | None, published: pd.Timestamp | None) -> str | None:
            if not month_label or published is None or published is pd.NaT:
                return None
            month_map = {
                "january": 1,
                "february": 2,
                "march": 3,
                "april": 4,
                "may": 5,
                "june": 6,
                "july": 7,
                "august": 8,
                "september": 9,
                "october": 10,
                "november": 11,
                "december": 12,
            }
            target_month = month_map.get(month_label.lower())
            if not target_month:
                return None
            year = published.year
            if published.month < target_month:
                year -= 1
            return f"{year:04d}-{target_month:02d}"

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
                parsed = self._extract_structured(title_text, body)
                period_end = _infer_period_end(parsed.get("month_label"), published)
                if parsed.get("net_inflow_month_usd_b") is not None and period_end is None:
                    parsed.setdefault("warnings", []).append("period_end_missing")
                items.append(
                    {
                        "date": published.date().isoformat(),
                        "title": title_text,
                        "scope": parsed.get("scope"),
                        "month_label": parsed.get("month_label"),
                        "period_end": period_end,
                        "category": parsed.get("category"),
                        "industry_total": parsed.get("industry_total"),
                        "net_inflow_month_usd_b": parsed.get("net_inflow_month_usd_b"),
                        "net_inflow_ytd_usd_b": parsed.get("net_inflow_ytd_usd_b"),
                        "category_ytd_usd_b": parsed.get("category_ytd_usd_b"),
                        "aum_usd_trn": parsed.get("aum_usd_trn"),
                        "warnings": parsed.get("warnings", []),
                        "link": link,
                    }
                )
            except Exception:
                continue

        items = sorted(items, key=lambda x: x.get("date", ""), reverse=True)
        latest_global = None
        latest_any = None
        latest_published = items[0] if items else None
        latest_by_scope: dict[str, dict[str, Any]] = {}
        for item in items:
            if not isinstance(item.get("net_inflow_month_usd_b"), (int, float)) or item.get("warnings"):
                continue
            if not item.get("industry_total"):
                continue
            if latest_any is None:
                latest_any = item
            if item.get("scope") == "global" and latest_global is None:
                latest_global = item
                break
            scope = item.get("scope") or "unknown"
            if scope not in latest_by_scope:
                latest_by_scope[scope] = item

        latest = latest_global or latest_any
        latest_inflow = latest.get("net_inflow_month_usd_b") if latest else None
        latest_scope = latest.get("scope") if latest else None
        latest_date = latest.get("date") if latest else None
        latest_title = latest.get("title") if latest else None
        latest_month_label = latest.get("month_label") if latest else None

        warnings = []
        if latest_scope and latest_scope != "global":
            warnings.append(f"latest inflow scope is {latest_scope}, not global")

        stats = {
            "items_total": len(items),
            "items_with_inflow": sum(1 for i in items if isinstance(i.get("net_inflow_month_usd_b"), (int, float))),
            "items_with_inflow_total": sum(
                1
                for i in items
                if isinstance(i.get("net_inflow_month_usd_b"), (int, float)) and i.get("industry_total")
            ),
            "items_with_aum": sum(1 for i in items if isinstance(i.get("aum_usd_trn"), (int, float))),
            "items_with_ytd": sum(1 for i in items if isinstance(i.get("net_inflow_ytd_usd_b"), (int, float))),
            "items_with_warnings": sum(1 for i in items if i.get("warnings")),
        }

        return {
            "items": items,
            "latest_net_inflow_month_usd_b": latest_inflow,
            "latest_scope": latest_scope,
            "latest_date": latest_date,
            "latest_title": latest_title,
            "latest_month_label": latest_month_label,
            "latest_kind": "global" if latest is latest_global else ("any" if latest else None),
            "latest_by_scope": latest_by_scope,
            "latest_by_published_date": latest_published,
            "warnings": warnings,
            "stats": stats,
        }


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
        try:
            resp = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code != 200:
                return 0
            data = resp.json()
            return len(data.get("articles", []))
        except Exception:
            return 0


class SEC13FClient:
    def __init__(self) -> None:
        self.user_agent = os.getenv("SEC_USER_AGENT", "").strip()
        self.session = requests.Session()
        headers = {"Accept-Encoding": "gzip, deflate"}
        if self.user_agent:
            headers["User-Agent"] = self.user_agent
        self.session.headers.update(headers)

    def _latest_zip_url(self) -> str | None:
        if not self.user_agent:
            return None
        try:
            resp = self.session.get(SEC_13F_PAGE, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
        except Exception:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        links = []
        for a in soup.select("a[href]"):
            href = a.get("href") or ""
            if href.endswith(".zip") and "13f" in href.lower():
                if href.startswith("/"):
                    href = "https://www.sec.gov" + href
                links.append(href)
        if not links:
            return None
        return links[0]

    def _cache_path(self) -> str:
        os.makedirs(CACHE_DIR, exist_ok=True)
        return os.path.join(CACHE_DIR, "sec_13f_latest.zip")

    def fetch_latest_zip(self, max_age_days: int = 7) -> str | None:
        if not self.user_agent:
            return None
        path = self._cache_path()
        if os.path.exists(path):
            age = datetime.utcnow() - datetime.utcfromtimestamp(os.path.getmtime(path))
            if age.days <= max_age_days:
                return path
        url = self._latest_zip_url()
        if not url:
            return None
        try:
            with self.session.get(url, timeout=REQUEST_TIMEOUT, stream=True) as resp:
                resp.raise_for_status()
                with open(path, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            fh.write(chunk)
        except Exception:
            return None
        return path

    def summarize_top_holdings(self, max_rows: int = 200000, top_n: int = 20) -> dict[str, Any]:
        path = self.fetch_latest_zip()
        if not path:
            return {"error": "13F zip not available"}

        try:
            import zipfile

            with zipfile.ZipFile(path, "r") as zf:
                name = None
                for candidate in zf.namelist():
                    lower = candidate.lower()
                    if "infotable" in lower and (lower.endswith(".tsv") or lower.endswith(".txt") or lower.endswith(".csv")):
                        name = candidate
                        break
                if not name:
                    return {"error": "infotable not found"}
                with zf.open(name) as fh:
                    raw = fh.read()
                text = raw.decode("utf-8", errors="ignore")
            sep = "\t" if name.lower().endswith((".tsv", ".txt")) else ","
            df = pd.read_csv(io.StringIO(text), sep=sep, nrows=max_rows)
        except Exception as exc:
            return {"error": f"parse failed: {exc}"}

        cols = {c.lower(): c for c in df.columns}
        value_col = cols.get("value") or cols.get("value$") or cols.get("value_")
        issuer_col = cols.get("issuername") or cols.get("issuer_name") or cols.get("nameofissuer") or cols.get("nameofissuer")
        cusip_col = cols.get("cusip")

        if not value_col:
            return {"error": "value column not found"}

        df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
        df = df.dropna(subset=[value_col])

        if cusip_col:
            df[cusip_col] = df[cusip_col].astype(str).str.strip()
        if issuer_col:
            df[issuer_col] = df[issuer_col].astype(str).str.strip()

        if cusip_col:
            agg = df.groupby(cusip_col, dropna=False)[value_col].sum().reset_index()
            if issuer_col:
                issuer_map = (
                    df.groupby(cusip_col)[issuer_col]
                    .agg(lambda s: s.value_counts().index[0] if len(s.value_counts()) else "")
                    .reset_index()
                )
                agg = agg.merge(issuer_map, on=cusip_col, how="left")
        else:
            agg = df[[value_col]].copy()

        agg = agg.sort_values(value_col, ascending=False).head(top_n)

        results = []
        for _, row in agg.iterrows():
            results.append(
                {
                    "issuer": str(row.get(issuer_col, "")).strip() if issuer_col else "",
                    "cusip": str(row.get(cusip_col, "")).strip() if cusip_col else "",
                    "value": float(row.get(value_col, 0)),
                }
            )

        dataset_id = os.path.basename(path)
        dataset_date = None
        m = re.search(r"(\\d{4}q\\d)", dataset_id, re.IGNORECASE)
        if m:
            dataset_date = m.group(1).lower()
        else:
            m = re.search(r"(\\d{8})", dataset_id)
            if m:
                dataset_date = m.group(1)

        return {
            "top_holdings": results,
            "dataset_id": dataset_id,
            "dataset_date": dataset_date,
            "note": "Top 13F holdings aggregated by CUSIP.",
        }


class FINRAShortInterestClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "autostock/2.0"})

    def _latest_file_url(self) -> str | None:
        try:
            resp = self.session.get(FINRA_SI_PAGE, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
        except Exception:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        links = []
        for a in soup.select("a[href]"):
            href = a.get("href") or ""
            if "short" in href.lower() and href.lower().endswith((".csv", ".zip", ".txt")):
                if href.startswith("/"):
                    href = "https://www.finra.org" + href
                links.append(href)
            if "cdn.finra.org" in href and href.lower().endswith((".csv", ".zip", ".txt")):
                links.append(href)
        if not links:
            return None
        return links[0]

    def _cache_path(self) -> str:
        os.makedirs(CACHE_DIR, exist_ok=True)
        return os.path.join(CACHE_DIR, "finra_short_interest_latest.csv")

    def fetch_latest(self, max_age_days: int = 7) -> str | None:
        path = self._cache_path()
        if os.path.exists(path):
            age = datetime.utcnow() - datetime.utcfromtimestamp(os.path.getmtime(path))
            if age.days <= max_age_days:
                return path
        url = self._latest_file_url()
        if not url:
            return None
        try:
            with self.session.get(url, timeout=REQUEST_TIMEOUT, stream=True) as resp:
                resp.raise_for_status()
                if url.lower().endswith(".zip"):
                    import zipfile
                    tmp = io.BytesIO(resp.content)
                    with zipfile.ZipFile(tmp, "r") as zf:
                        name = zf.namelist()[0]
                        with zf.open(name) as fh:
                            content = fh.read()
                    with open(path, "wb") as fh:
                        fh.write(content)
                else:
                    with open(path, "wb") as fh:
                        for chunk in resp.iter_content(chunk_size=1024 * 512):
                            if chunk:
                                fh.write(chunk)
        except Exception:
            return None
        return path

    def summarize(self, universe: list[str], top_n: int = 20) -> dict[str, Any]:
        path = self.fetch_latest()
        if not path:
            return {"error": "short interest file not available"}
        try:
            df = pd.read_csv(path, sep="|")
        except Exception:
            try:
                df = pd.read_csv(path)
            except Exception as exc:
                return {"error": f"parse failed: {exc}"}

        cols = {c.lower(): c for c in df.columns}
        sym_col = cols.get("symbol") or cols.get("symbolcode") or cols.get("issue_symbol") or cols.get("issue")
        si_col = (
            cols.get("shortinterest")
            or cols.get("short_interest")
            or cols.get("short_int")
            or cols.get("shortinterestquantity")
            or cols.get("currentshortpositionquantity")
        )
        ratio_col = cols.get("days_to_cover") or cols.get("daystocoverquantity") or cols.get("shortinterestratio") or cols.get("short_interest_ratio")

        if not sym_col or not si_col:
            return {"error": "required columns not found"}

        df[si_col] = pd.to_numeric(df[si_col], errors="coerce")
        df = df.dropna(subset=[si_col])
        df = df[df[sym_col].isin(universe)]
        df = df.sort_values(si_col, ascending=False).head(top_n)

        rows = []
        for _, row in df.iterrows():
            rows.append(
                {
                    "symbol": str(row.get(sym_col)),
                    "short_interest": float(row.get(si_col, 0)),
                    "days_to_cover": float(row.get(ratio_col)) if ratio_col and pd.notna(row.get(ratio_col)) else None,
                }
            )
        return {"top_short_interest": rows}


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
    components: list[dict[str, Any]] = []

    m2 = metrics.get("m2_6m_pct")
    if isinstance(m2, (int, float)):
        if m2 >= 3:
            score += 1.5
            components.append({"factor": "m2_6m_pct", "value": m2, "points": 1.5})
        elif m2 >= 1:
            score += 0.8
            components.append({"factor": "m2_6m_pct", "value": m2, "points": 0.8})
        elif m2 < 0:
            score -= 1.0
            components.append({"factor": "m2_6m_pct", "value": m2, "points": -1.0})
    elif m2 is not None:
        components.append({"factor": "m2_6m_pct", "value": m2, "points": 0.0})

    real_rate = metrics.get("real_rate_10y")
    if isinstance(real_rate, (int, float)):
        if real_rate >= 2.0:
            score -= 1.5
            components.append({"factor": "real_rate_10y", "value": real_rate, "points": -1.5})
        elif real_rate >= 1.5:
            score -= 1.0
            components.append({"factor": "real_rate_10y", "value": real_rate, "points": -1.0})
        elif real_rate <= 0.5:
            score += 0.8
            components.append({"factor": "real_rate_10y", "value": real_rate, "points": 0.8})
    elif real_rate is not None:
        components.append({"factor": "real_rate_10y", "value": real_rate, "points": 0.0})

    curve = metrics.get("yield_curve_10y_2y")
    if isinstance(curve, (int, float)):
        if curve > 0.4:
            score += 0.6
            components.append({"factor": "yield_curve_10y_2y", "value": curve, "points": 0.6})
        elif curve < 0:
            score -= 1.0
            components.append({"factor": "yield_curve_10y_2y", "value": curve, "points": -1.0})
    elif curve is not None:
        components.append({"factor": "yield_curve_10y_2y", "value": curve, "points": 0.0})

    usd = metrics.get("dollar_6m_pct")
    if isinstance(usd, (int, float)):
        if usd >= 2:
            score -= 0.8
            components.append({"factor": "dollar_6m_pct", "value": usd, "points": -0.8})
        elif usd <= -2:
            score += 0.6
            components.append({"factor": "dollar_6m_pct", "value": usd, "points": 0.6})
    elif usd is not None:
        components.append({"factor": "dollar_6m_pct", "value": usd, "points": 0.0})

    etf = metrics.get("etf_flow_latest_net_inflow_month_usd_b")
    etf_scope = metrics.get("etf_flow_latest_scope")
    etf_kind = metrics.get("etf_flow_latest_kind")
    if isinstance(etf, (int, float)):
        if etf_kind == "global":
            if etf >= 100:
                score += 1.0
                components.append(
                    {
                        "factor": "etf_flow_latest_net_inflow_month_usd_b",
                        "value": etf,
                        "points": 1.0,
                        "scope": etf_scope,
                        "kind": etf_kind,
                    }
                )
            elif etf <= 0:
                score -= 0.8
                components.append(
                    {
                        "factor": "etf_flow_latest_net_inflow_month_usd_b",
                        "value": etf,
                        "points": -0.8,
                        "scope": etf_scope,
                        "kind": etf_kind,
                    }
                )
        else:
            notes.append("ETF flow not global; excluded from score")
            components.append(
                {
                    "factor": "etf_flow_latest_net_inflow_month_usd_b",
                    "value": etf,
                    "points": 0.0,
                    "scope": etf_scope,
                    "kind": etf_kind,
                    "note": "excluded_non_global",
                }
            )
    else:
        notes.append("ETF flow missing")
        if etf is not None:
            components.append(
                {
                    "factor": "etf_flow_latest_net_inflow_month_usd_b",
                    "value": etf,
                    "points": 0.0,
                    "scope": etf_scope,
                    "kind": etf_kind,
                    "note": "missing_or_invalid",
                }
            )
    if etf_scope and etf_scope != "global":
        notes.append(f"ETF flow scope={etf_scope}")

    if score >= 1.5:
        label = "risk_on"
    elif score <= -1.5:
        label = "risk_off"
    else:
        label = "neutral"

    return {"score": round(score, 2), "label": label, "notes": notes, "components": components}


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


def _to_plain(obj: Any, seen: set[int] | None = None) -> Any:
    if seen is None:
        seen = set()
    oid = id(obj)
    if oid in seen:
        return None
    if isinstance(obj, (dict, list, tuple)):
        seen.add(oid)
    if isinstance(obj, dict):
        return {str(k): _to_plain(v, seen) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_plain(v, seen) for v in obj]
    if isinstance(obj, tuple):
        return [_to_plain(v, seen) for v in obj]
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    return str(obj)


def _load_us_universe(limit: int) -> list[str]:
    from config import load_sp500

    symbols = load_sp500()
    if limit > 0:
        return symbols[:limit]
    return symbols


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        if isinstance(value, str) and value.strip() in {"", "N/A", "None"}:
            return None
        return float(value)
    except Exception:
        return None


def _sector_etf_map() -> dict[str, str]:
    return {
        "Communication Services": "XLC",
        "Consumer Discretionary": "XLY",
        "Consumer Staples": "XLP",
        "Energy": "XLE",
        "Financials": "XLF",
        "Health Care": "XLV",
        "Industrials": "XLI",
        "Materials": "XLB",
        "Real Estate": "XLRE",
        "Technology": "XLK",
        "Utilities": "XLU",
    }

def _sector_price_band_proxy(etf: str, years: int = 10) -> dict[str, Any]:
    try:
        hist = yf.Ticker(etf).history(period=f"{years}y", interval="1mo", auto_adjust=False)
        if hist is None or hist.empty:
            return {"error": "no history"}
        close = hist["Close"].dropna()
        log_close = np.log(close.replace(0, np.nan)).dropna()
        if log_close.empty:
            return {"error": "no log history"}
        mean = float(log_close.mean())
        std = float(log_close.std())
        latest = float(log_close.iloc[-1])
        z = (latest - mean) / std if std > 0 else 0.0
        return {
            "method": "log_price_z",
            "latest_log": round(latest, 4),
            "mean_log": round(mean, 4),
            "std_log": round(std, 4),
            "z": round(z, 2),
        }
    except Exception as exc:
        return {"error": str(exc)}


def _price_trend_metrics(etf: str, years: int = 5) -> dict[str, Any]:
    try:
        hist = yf.Ticker(etf).history(period=f"{years}y", interval="1d", auto_adjust=False)
        if hist is None or hist.empty:
            return {"error": "no history"}
        close = hist["Close"].dropna()
        if len(close) < 260:
            return {"error": "insufficient history"}
        ma200 = close.rolling(200).mean()
        latest = float(close.iloc[-1])
        ma200_latest = float(ma200.iloc[-1])
        ret_63 = (latest / float(close.iloc[-64]) - 1.0) * 100.0 if len(close) >= 64 else None
        ret_252 = (latest / float(close.iloc[-253]) - 1.0) * 100.0 if len(close) >= 253 else None
        ma200_gap = (latest / ma200_latest - 1.0) * 100.0 if ma200_latest else None
        return {
            "latest": round(latest, 2),
            "ma200": round(ma200_latest, 2),
            "ma200_gap_pct": round(ma200_gap, 2) if isinstance(ma200_gap, (int, float)) else None,
            "ret_63d_pct": round(ret_63, 2) if isinstance(ret_63, (int, float)) else None,
            "ret_252d_pct": round(ret_252, 2) if isinstance(ret_252, (int, float)) else None,
        }
    except Exception as exc:
        return {"error": str(exc)}


def _collect_infos(universe: list[str]) -> dict[str, dict[str, Any]]:
    infos: dict[str, dict[str, Any]] = {}
    for symbol in universe:
        try:
            infos[symbol] = yf.Ticker(symbol).info or {}
        except Exception:
            infos[symbol] = {}
    return infos


def _sector_valuation_proxy(universe: list[str], infos: dict[str, dict[str, Any]]) -> dict[str, Any]:
    # Use current fundamentals across a limited universe; no true 10y PER/PBR series
    sector_map: dict[str, list[dict[str, Any]]] = {}
    for symbol in universe:
        info = infos.get(symbol) or {}
        sector = str(info.get("sector") or "Unknown")
        if sector == "Unknown":
            continue
        equity = _safe_float(info.get("totalStockholderEquity"))
        roe = _safe_float(info.get("returnOnEquity"))
        if isinstance(equity, (int, float)) and equity <= 0:
            roe = None
        if isinstance(roe, (int, float)) and abs(roe) > 2.0:
            roe = None
        row = {
            "pe": _safe_float(info.get("trailingPE")),
            "pb": _safe_float(info.get("priceToBook")),
            "roe": roe,
            "rev_growth": _safe_float(info.get("revenueGrowth")),
            "earn_growth": _safe_float(info.get("earningsGrowth")),
            "forward_eps_growth": _safe_float(info.get("forwardEps"))
            if _safe_float(info.get("trailingEps"))
            else None,
            "equity": equity,
        }
        sector_map.setdefault(sector, []).append(row)

    sectors = []
    for sector, rows in sector_map.items():
        pe_vals = [r["pe"] for r in rows if isinstance(r.get("pe"), (int, float)) and r["pe"] > 0]
        pb_vals = [r["pb"] for r in rows if isinstance(r.get("pb"), (int, float)) and r["pb"] > 0]
        roe_vals = [r["roe"] for r in rows if isinstance(r.get("roe"), (int, float))]
        eg_vals = [r["earn_growth"] for r in rows if isinstance(r.get("earn_growth"), (int, float))]
        rg_vals = [r["rev_growth"] for r in rows if isinstance(r.get("rev_growth"), (int, float))]

        pe = float(pd.Series(pe_vals).median()) if pe_vals else None
        pb = float(pd.Series(pb_vals).median()) if pb_vals else None
        roe = float(pd.Series(roe_vals).median()) if roe_vals else None
        eg = float(pd.Series(eg_vals).median()) if eg_vals else None
        rg = float(pd.Series(rg_vals).median()) if rg_vals else None

        sectors.append(
            {
                "sector": sector,
                "pe": pe,
                "pb": pb,
                "roe": roe,
                "earnings_growth": eg,
                "revenue_growth": rg,
                "sample_size": len(rows),
            }
        )

    return {"sectors": sectors, "note": "PE/PB are cross-sectional medians, not 10y bands. ROE filtered for equity<=0 or extreme values."}


def _valuation_distortion_report(universe: list[str], infos: dict[str, dict[str, Any]]) -> dict[str, Any]:
    etf_map = _sector_etf_map()
    sector_proxy = _sector_valuation_proxy(universe, infos)
    proxy_map = {s["sector"]: s for s in sector_proxy.get("sectors", [])}
    min_sample = int(os.getenv("US_VALUATION_MIN_SAMPLE", "5"))

    outliers = []
    details = []
    for sector, etf in etf_map.items():
        band = _sector_price_band_proxy(etf)
        trend = _price_trend_metrics(etf, years=5)
        row = {"sector": sector, "etf": etf, "price_band": band, "trend": trend}
        row.update(proxy_map.get(sector, {}))
        details.append(row)

        z = band.get("z") if isinstance(band, dict) else None
        eg = row.get("earnings_growth")
        if row.get("sample_size", 0) < min_sample:
            outliers.append({**row, "label": "insufficient_data", "confidence": "low"})
            continue
        if isinstance(z, (int, float)) and abs(z) >= 2:
            if isinstance(eg, (int, float)) and eg > 0:
                label = "fundamentals_improving"
            elif isinstance(eg, (int, float)) and eg < 0:
                label = "expectations_only"
            else:
                label = "unclear"
            outliers.append({**row, "label": label, "confidence": "medium"})

    return {
        "details": details,
        "outliers": outliers,
        "min_sample": min_sample,
        "note": sector_proxy.get("note"),
    }


def _volume_surge_us(symbol: str, window: int = 60) -> bool:
    try:
        hist = yf.Ticker(symbol).history(period="6mo", interval="1d")
        if hist is None or hist.empty:
            return False
        vol = hist["Volume"].dropna()
        if len(vol) < 10:
            return False
        avg = vol.tail(window).mean()
        latest = vol.iloc[-1]
        return latest >= avg * 2.0
    except Exception:
        return False


def _short_proxy(info: dict[str, Any]) -> bool:
    spf = _safe_float(info.get("shortPercentOfFloat"))
    if spf is None:
        return False
    return spf <= 5.0


def _turnaround_proxy_us(info: dict[str, Any]) -> bool:
    teps = _safe_float(info.get("trailingEps"))
    feps = _safe_float(info.get("forwardEps"))
    if teps is None or feps is None:
        return False
    return teps <= 0 and feps > 0


def _flow_reversal_us(universe: list[str], infos: dict[str, dict[str, Any]]) -> dict[str, Any]:
    rows = []
    for symbol in universe:
        info = infos.get(symbol) or {}
        turnaround = _turnaround_proxy_us(info)
        volume = _volume_surge_us(symbol)
        short_down = _short_proxy(info)
        score = 0.0
        if turnaround:
            score += 0.4
        if volume:
            score += 0.3
        if short_down:
            score += 0.3
        score = round(min(0.95, max(0.05, score)), 2)
        rows.append(
            {
                "symbol": symbol,
                "name": info.get("shortName", symbol),
                "turnaround": turnaround,
                "volume_surge": volume,
                "short_ok": short_down,
                "probability": score,
            }
        )

    candidates = sorted(rows, key=lambda x: -x["probability"])[:20]
    return {
        "candidates": candidates,
        "note": "Institutional/foreign flow is proxied by fundamentals + volume + short data.",
    }


def _industry_cycle_us() -> dict[str, Any]:
    proxies = {
        "Semiconductor": "SOXX",
        "Auto": "CARZ",
        "Insurance": "KIE",
        "Shipbuilding": "ITA",
        "Nuclear": "URA",
    }
    out = []
    for name, etf in proxies.items():
        band = _sector_price_band_proxy(etf, years=5)
        trend = _price_trend_metrics(etf, years=5)
        z = band.get("z") if isinstance(band, dict) else None
        ret_252 = trend.get("ret_252d_pct") if isinstance(trend, dict) else None
        ma200_gap = trend.get("ma200_gap_pct") if isinstance(trend, dict) else None
        stage = "expansion"
        if isinstance(ret_252, (int, float)) and isinstance(ma200_gap, (int, float)):
            if ret_252 < 0 and ma200_gap < 0:
                stage = "downturn"
            elif ret_252 > 10 and ma200_gap > 3:
                stage = "overheat"
            elif ret_252 > 0 and ma200_gap >= 0:
                stage = "expansion"
            else:
                stage = "recovery"
        elif isinstance(z, (int, float)):
            if z <= -1.0:
                stage = "recovery"
            elif z >= 1.5:
                stage = "overheat"
            else:
                stage = "expansion"
        out.append({"industry": name, "proxy_etf": etf, "stage": stage, "price_band": band, "trend": trend})
    return {"industries": out, "note": "Industry cycle uses ETF price-band + trend proxy."}


def _narrative_vs_numbers_us(
    universe: list[str],
    infos: dict[str, dict[str, Any]],
    gdelt: GDELTClient | None = None,
) -> dict[str, Any]:
    rows = []
    for symbol in universe:
        info = infos.get(symbol) or {}
        rev = _safe_float(info.get("revenueGrowth"))
        earn = _safe_float(info.get("earningsGrowth"))
        fcf = _safe_float(info.get("freeCashflow"))
        equity = _safe_float(info.get("totalStockholderEquity"))
        roe = _safe_float(info.get("returnOnEquity"))
        if isinstance(equity, (int, float)) and equity <= 0:
            roe = None
        if isinstance(roe, (int, float)) and abs(roe) > 2.0:
            roe = None

        positives = 0
        for val in (rev, earn, roe):
            if isinstance(val, (int, float)) and val > 0:
                positives += 1
        if isinstance(fcf, (int, float)) and fcf > 0:
            positives += 1

        label = "numbers_proving" if positives >= 3 else "narrative_only"
        rows.append(
            {
                "symbol": symbol,
                "name": info.get("shortName", symbol),
                "revenue_growth": rev,
                "earnings_growth": earn,
                "free_cash_flow": fcf,
                "roe": roe,
                "label": label,
            }
        )

    news_ranked = []
    if gdelt and os.getenv("US_FREE_SKIP_GDELT", "1").strip() not in {"1", "true", "yes", "on"}:
        top_for_news = rows[: int(os.getenv("US_FREE_GDELT_MAX", "8"))]
        for row in top_for_news:
            name = row.get("name") or row["symbol"]
            count = gdelt.count_mentions(name, timespan=os.getenv("GDELT_TIMESPAN", "1month"))
            news_ranked.append({**row, "news_count": count})
        news_ranked = sorted(news_ranked, key=lambda x: -x.get("news_count", 0))

    numbers = [r for r in rows if r["label"] == "numbers_proving"]
    narrative = [r for r in rows if r["label"] == "narrative_only"]

    return {
        "numbers": numbers[:20],
        "narrative": narrative[:20],
        "top_news": news_ranked[:15],
        "note": "News frequency uses GDELT (free). Fundamentals use yfinance. ROE filtered for equity<=0 or extreme values.",
    }


def _default_risk_us(universe: list[str], infos: dict[str, dict[str, Any]]) -> dict[str, Any]:
    rows = []
    for symbol in universe:
        info = infos.get(symbol) or {}
        fcf = _safe_float(info.get("freeCashflow"))
        equity = _safe_float(info.get("totalStockholderEquity"))
        debt_to_equity = _safe_float(info.get("debtToEquity"))
        if isinstance(equity, (int, float)) and equity <= 0:
            debt_to_equity = None
        current_ratio = _safe_float(info.get("currentRatio"))
        beta = _safe_float(info.get("beta"))

        score = 0.0
        if isinstance(fcf, (int, float)) and fcf < 0:
            score += 0.35
        if isinstance(debt_to_equity, (int, float)) and debt_to_equity > 200:
            score += 0.30
        if isinstance(current_ratio, (int, float)) and current_ratio < 1:
            score += 0.20
        if isinstance(beta, (int, float)) and beta > 1.8:
            score += 0.15

        rows.append(
            {
                "symbol": symbol,
                "name": info.get("shortName", symbol),
                "free_cash_flow": fcf,
                "debt_to_equity": debt_to_equity,
                "current_ratio": current_ratio,
                "beta": beta,
                "risk_score": round(min(1.0, score), 2),
            }
        )

    top = sorted(rows, key=lambda x: -x["risk_score"])[:10]
    return {
        "top10": top,
        "note": "Proxy risk score (no direct interest coverage). debt_to_equity filtered when equity<=0.",
    }


def run_us_free_pipeline(output_dir: str = "data/reports", write_outputs: bool = True) -> dict[str, Any]:
    os.makedirs(output_dir, exist_ok=True)

    fred = FREDClient()
    etfgi = ETFGIClient()
    gdelt = GDELTClient()
    sec13f = SEC13FClient()
    finra = FINRAShortInterestClient()

    series = {
        "m2": "M2SL",
        "real_rate_10y": "DFII10",
        "yield_curve_10y_2y": "T10Y2Y",
        "dollar_index": "DTWEXBGS",
        "cpi": "CPIAUCSL",
        "unemployment": "UNRATE",
        "real_gdp": "GDPC1",
    }

    data: dict[str, Any] = {}
    for key, series_id in series.items():
        try:
            data[key] = fred.fetch_series(series_id)
        except Exception as exc:
            data[key] = {"error": str(exc)}

    m2_latest, m2_base, m2_6m = _six_month_change(data["m2"]) if isinstance(data["m2"], pd.DataFrame) else (None, None, None)
    usd_latest, usd_base, usd_6m = _six_month_change(data["dollar_index"]) if isinstance(data["dollar_index"], pd.DataFrame) else (None, None, None)
    real_rate_latest = float(data["real_rate_10y"].iloc[-1]["value"]) if isinstance(data["real_rate_10y"], pd.DataFrame) and not data["real_rate_10y"].empty else None
    curve_latest = float(data["yield_curve_10y_2y"].iloc[-1]["value"]) if isinstance(data["yield_curve_10y_2y"], pd.DataFrame) and not data["yield_curve_10y_2y"].empty else None

    cpi_6m_ann = None
    if isinstance(data["cpi"], pd.DataFrame):
        _, _, cpi_6m = _six_month_change(data["cpi"])
        if isinstance(cpi_6m, (int, float)):
            cpi_6m_ann = round(cpi_6m * 2, 2)

    gdp_yoy = None
    if isinstance(data["real_gdp"], pd.DataFrame) and len(data["real_gdp"]) >= 5:
        latest = data["real_gdp"].iloc[-1]
        prior = data["real_gdp"].iloc[-5]
        if float(prior["value"]) != 0:
            gdp_yoy = round((float(latest["value"]) / float(prior["value"]) - 1.0) * 100.0, 2)

    unrate = None
    if isinstance(data["unemployment"], pd.DataFrame) and not data["unemployment"].empty:
        unrate = float(data["unemployment"].iloc[-1]["value"])

    metrics = {
        "m2_latest": m2_latest.__dict__ if m2_latest else None,
        "m2_base": m2_base.__dict__ if m2_base else None,
        "m2_6m_pct": round(m2_6m, 2) if isinstance(m2_6m, (int, float)) else None,
        "real_rate_10y": round(real_rate_latest, 2) if isinstance(real_rate_latest, (int, float)) else None,
        "yield_curve_10y_2y": round(curve_latest, 2) if isinstance(curve_latest, (int, float)) else None,
        "dollar_latest": usd_latest.__dict__ if usd_latest else None,
        "dollar_base": usd_base.__dict__ if usd_base else None,
        "dollar_6m_pct": round(usd_6m, 2) if isinstance(usd_6m, (int, float)) else None,
        "dollar_index_series": "DTWEXBGS",
        "cpi_6m_annualized": cpi_6m_ann,
        "unemployment_rate": unrate,
        "real_gdp_yoy": gdp_yoy,
    }

    etfgi_data = etfgi.fetch_recent_inflows(months_back=6)
    liquidity = _score_risk_on_off(
        {
            "m2_6m_pct": metrics.get("m2_6m_pct"),
            "real_rate_10y": metrics.get("real_rate_10y"),
            "yield_curve_10y_2y": metrics.get("yield_curve_10y_2y"),
            "dollar_6m_pct": metrics.get("dollar_6m_pct"),
            "etf_flow_latest_net_inflow_month_usd_b": etfgi_data.get("latest_net_inflow_month_usd_b"),
            "etf_flow_latest_scope": etfgi_data.get("latest_scope"),
            "etf_flow_latest_kind": etfgi_data.get("latest_kind"),
            "etf_flow_latest_date": etfgi_data.get("latest_date"),
            "etf_flow_latest_month_label": etfgi_data.get("latest_month_label"),
        }
    )

    scenario = _scenario_probs(metrics)

    limit = int(os.getenv("US_FREE_MAX_TICKERS", "40"))
    universe = _load_us_universe(limit)

    infos = _collect_infos(universe)
    valuation = _valuation_distortion_report(universe, infos)
    flow = _flow_reversal_us(universe, infos)
    industry = _industry_cycle_us()
    narrative = _narrative_vs_numbers_us(universe, infos, gdelt=gdelt)
    default_risk = _default_risk_us(universe, infos)
    if os.getenv("US_FREE_USE_13F", "1").strip() not in {"0", "false", "no", "off"}:
        if not os.getenv("SEC_USER_AGENT", "").strip():
            thirteen_f = {"error": "SEC_USER_AGENT required in .env for 13F download"}
        else:
            thirteen_f = sec13f.summarize_top_holdings(
                max_rows=int(os.getenv("US_13F_MAX_ROWS", "200000")),
                top_n=int(os.getenv("US_13F_TOP_N", "20")),
            )
    else:
        thirteen_f = {"skipped": True}
    short_interest = (
        finra.summarize(universe, top_n=int(os.getenv("US_FINRA_TOP_N", "20")))
        if os.getenv("US_FREE_USE_FINRA", "1").strip() not in {"0", "false", "no", "off"}
        else {"skipped": True}
    )

    valuation_errors = any(
        isinstance(row.get("price_band"), dict) and "error" in row.get("price_band", {})
        for row in valuation.get("details", [])
    )
    industry_errors = any(
        isinstance(row.get("price_band"), dict) and "error" in row.get("price_band", {})
        for row in industry.get("industries", [])
    )
    data_gaps = [
        "No paid sector PER/PBR 10y series; price-band proxy used (log-price).",
        "No institutional/foreign flow dataset; flow is proxy-based.",
        "ETF flow uses ETFGI press releases; latest scope may be non-global.",
        "No direct interest coverage or short-term debt trend; default risk is proxy-based.",
        "News frequency uses GDELT; may miss paywalled sources.",
    ]
    if valuation_errors:
        data_gaps.append("Valuation price-band proxy had errors for some sectors.")
    if industry_errors:
        data_gaps.append("Industry price-band proxy had errors for some industries.")

    etf_conf = "high"
    if etfgi_data.get("warnings") or etfgi_data.get("latest_net_inflow_month_usd_b") is None:
        etf_conf = "medium"
    stats = etfgi_data.get("stats") or {}
    if isinstance(stats.get("items_with_warnings"), int) and stats.get("items_with_warnings") > 0:
        etf_conf = "medium"
    if isinstance(stats.get("items_with_inflow_total"), int) and stats.get("items_with_inflow_total") == 0:
        etf_conf = "medium"
    if etfgi_data.get("latest_kind") != "global":
        etf_conf = "medium"

    report = {
        "as_of": datetime.utcnow().isoformat() + "Z",
        "module1_liquidity": {
            "metrics": metrics,
            "etf_flows": etfgi_data,
            "risk_on_off": liquidity,
            "confidence": "medium" if etf_conf != "high" else "high",
            "confidence_macro": "high",
            "confidence_flows": etf_conf,
        },
        "module2_valuation_distortion": {**valuation, "confidence": "low"},
        "module3_flow_reversal": {**flow, "confidence": "low"},
        "module3b_short_interest": {**short_interest, "confidence": "medium"},
        "module4_industry_cycle": {**industry, "confidence": "low"},
        "module5_narrative_vs_numbers": {**narrative, "confidence": "low"},
        "module6_macro_scenarios": {**scenario, "confidence": "medium"},
        "module7_default_risk": {**default_risk, "confidence": "low"},
        "module8_13f_top_holdings": {**thirteen_f, "confidence": "medium"},
        "data_gaps": data_gaps,
    }

    try:
        from ai.analyzer import ai

        ai_result = ai.analyze_research_report(report)
        report["ai_summary"] = ai_result
    except Exception as exc:
        report["ai_summary"] = {"error": str(exc)}

    date_tag = datetime.now().strftime("%Y-%m-%d")
    json_path = None
    md_path = None
    safe_report = _to_plain(report)
    if write_outputs:
        json_path = os.path.join(output_dir, f"us_free_report_{date_tag}.json")
        with open(json_path, "w", encoding="utf-8") as fh:
            json.dump(safe_report, fh, ensure_ascii=False, indent=2)

        md_path = os.path.join(output_dir, f"us_free_report_{date_tag}.md")
        with open(md_path, "w", encoding="utf-8") as fh:
            fh.write("# US Free Pipeline Report\n\n")
            fh.write("as_of: " + str(report.get("as_of", "")) + "\n\n")
            fh.write("## Module 1: Liquidity\n")
            fh.write(json.dumps(safe_report["module1_liquidity"], ensure_ascii=False, indent=2))
            fh.write("\n\n## Module 2: Valuation Distortion\n")
            fh.write(json.dumps(safe_report["module2_valuation_distortion"], ensure_ascii=False, indent=2))
            fh.write("\n\n## Module 3: Flow Reversal\n")
            fh.write(json.dumps(safe_report["module3_flow_reversal"], ensure_ascii=False, indent=2))
            fh.write("\n\n## Module 3b: Short Interest (FINRA)\n")
            fh.write(json.dumps(safe_report["module3b_short_interest"], ensure_ascii=False, indent=2))
            fh.write("\n\n## Module 4: Industry Cycle\n")
            fh.write(json.dumps(safe_report["module4_industry_cycle"], ensure_ascii=False, indent=2))
            fh.write("\n\n## Module 5: Narrative vs Numbers\n")
            fh.write(json.dumps(safe_report["module5_narrative_vs_numbers"], ensure_ascii=False, indent=2))
            fh.write("\n\n## Module 6: Macro Scenarios\n")
            fh.write(json.dumps(safe_report["module6_macro_scenarios"], ensure_ascii=False, indent=2))
            fh.write("\n\n## Module 7: Default Risk\n")
            fh.write(json.dumps(safe_report["module7_default_risk"], ensure_ascii=False, indent=2))
            fh.write("\n\n## Module 8: 13F Top Holdings\n")
            fh.write(json.dumps(safe_report["module8_13f_top_holdings"], ensure_ascii=False, indent=2))
            fh.write("\n\n## Data Gaps\n")
            for gap in safe_report["data_gaps"]:
                fh.write(f"- {gap}\n")

    return {"report": report, "json_path": json_path, "md_path": md_path}


if __name__ == "__main__":
    run_us_free_pipeline()
