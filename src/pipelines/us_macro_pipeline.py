from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import StringIO


FRED_BASE = "https://fred.stlouisfed.org/graph/fredgraph.csv?id="
REQUEST_TIMEOUT = 12


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
        df = pd.read_csv(StringIO(resp.text))
        col_date = "observation_date" if "observation_date" in df.columns else "DATE"
        val_col = [c for c in df.columns if c != col_date][0]
        df[col_date] = pd.to_datetime(df[col_date])
        df[val_col] = pd.to_numeric(df[val_col], errors="coerce")
        df = df.dropna().sort_values(col_date)
        df = df.rename(columns={col_date: "date", val_col: "value"})
        return df


class ETFGIClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "autostock/2.0"})

    def _parse_inflow_value(self, text: str) -> float | None:
        # Returns USD billions if possible.
        if not text:
            return None
        lower = text.lower()
        if "net inflow" not in lower and "net inflows" not in lower:
            return None

        import re

        # Examples: US$330.78 billion, $330.78 billion
        m = re.search(r"\$\s?([0-9,.]+)\s*(trillion|billion)", text, re.IGNORECASE)
        if not m:
            m = re.search(r"us\$\s*([0-9,.]+)\s*(trillion|billion)", text, re.IGNORECASE)
        if not m:
            return None
        val = float(m.group(1).replace(",", ""))
        unit = m.group(2).lower()
        if unit.startswith("trillion"):
            return val * 1000.0
        return val

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
        links = list(dict.fromkeys(links))[:60]

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
                # Date hint
                date_tag = psoup.find("time")
                date_text = date_tag.get("datetime") if date_tag else ""
                if not date_text:
                    # try common meta
                    meta_date = psoup.find("meta", {"property": "article:published_time"})
                    date_text = meta_date.get("content", "") if meta_date else ""
                if not date_text:
                    continue
                published = pd.to_datetime(date_text, errors="coerce")
                if published is pd.NaT:
                    continue
                if published < cutoff:
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

        # Sum inflows where value exists
        inflows = [i["inflow_usd_b"] for i in items if isinstance(i.get("inflow_usd_b"), (int, float))]
        total = float(sum(inflows)) if inflows else None
        return {"items": items, "total_usd_b": total}


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
    if float(base["value"]) == 0:
        pct = None
    else:
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


def _scenario_probs(metrics: dict[str, Any]) -> dict[str, Any]:
    # Simple heuristic model
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

    # Normalize
    total = sum(probs.values())
    if total > 0:
        for k in probs:
            probs[k] = round(probs[k] / total, 3)

    return probs


def _to_json_serializable(value: Any) -> Any:
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    return value


def run_us_macro_pipeline(output_dir: str = "data/reports") -> dict[str, Any]:
    fred = FREDClient()
    etfgi = ETFGIClient()

    os.makedirs(output_dir, exist_ok=True)

    series_map = {
        "m2": "M2SL",
        "real_rate_10y": "DFII10",
        "yield_curve_10y_2y": "T10Y2Y",
        "dollar_index": "DTWEXBGS",
        "cpi": "CPIAUCSL",
        "unemployment": "UNRATE",
        "real_gdp": "GDPC1",
    }

    data: dict[str, Any] = {}
    for key, series_id in series_map.items():
        try:
            df = fred.fetch_series(series_id)
            data[key] = df
        except Exception as exc:
            data[key] = {"error": str(exc)}

    # Core macro metrics
    m2_latest, m2_base, m2_6m = _six_month_change(data["m2"]) if isinstance(data["m2"], pd.DataFrame) else (None, None, None)
    usd_latest, usd_base, usd_6m = _six_month_change(data["dollar_index"]) if isinstance(data["dollar_index"], pd.DataFrame) else (None, None, None)

    real_rate_latest = float(data["real_rate_10y"].iloc[-1]["value"]) if isinstance(data["real_rate_10y"], pd.DataFrame) and not data["real_rate_10y"].empty else None
    curve_latest = float(data["yield_curve_10y_2y"].iloc[-1]["value"]) if isinstance(data["yield_curve_10y_2y"], pd.DataFrame) and not data["yield_curve_10y_2y"].empty else None

    # CPI 6m annualized
    cpi_6m_ann = None
    if isinstance(data["cpi"], pd.DataFrame):
        cpi_latest, cpi_base, cpi_6m = _six_month_change(data["cpi"])
        if isinstance(cpi_6m, (int, float)):
            cpi_6m_ann = round(cpi_6m * 2, 2)

    # GDP YoY
    gdp_yoy = None
    if isinstance(data["real_gdp"], pd.DataFrame) and len(data["real_gdp"]) >= 5:
        latest = data["real_gdp"].iloc[-1]
        prior = data["real_gdp"].iloc[-5]
        if float(prior["value"]) != 0:
            gdp_yoy = (float(latest["value"]) / float(prior["value"]) - 1.0) * 100.0
            gdp_yoy = round(gdp_yoy, 2)

    # Unemployment latest
    unrate = None
    if isinstance(data["unemployment"], pd.DataFrame) and not data["unemployment"].empty:
        unrate = float(data["unemployment"].iloc[-1]["value"])

    # ETF flows
    etfgi_data = etfgi.fetch_recent_inflows(months_back=6)

    metrics = {
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
        "etf_flow_6m_total_usd_b": etfgi_data.get("total_usd_b"),
        "etf_flow_items": etfgi_data.get("items", []),
    }

    risk = _score_risk_on_off(metrics)
    scenarios = _scenario_probs(metrics)

    report = {
        "as_of": datetime.utcnow().isoformat() + "Z",
        "metrics": metrics,
        "risk_on_off": risk,
        "scenarios": scenarios,
        "data_gaps": [
            "Sector 10y PER/PBR bands require paid datasets",
            "Institutional/foreign flow and short interest trends require paid datasets",
            "Industry cycle inputs need specialized data (orders, inventories, capex)",
            "Narrative vs numbers requires news volume datasets",
            "Default risk screen needs detailed debt/CB/BW data",
        ],
    }

    date_tag = datetime.now().strftime("%Y-%m-%d")
    json_path = os.path.join(output_dir, f"us_macro_report_{date_tag}.json")
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, ensure_ascii=False, indent=2, default=_to_json_serializable)

    md_path = os.path.join(output_dir, f"us_macro_report_{date_tag}.md")
    with open(md_path, "w", encoding="utf-8") as fh:
        fh.write("# US Macro Pipeline Report\n\n")
        fh.write(f"as_of: {report['as_of']}\n\n")
        fh.write("## Risk On/Off\n")
        fh.write(f"label: {risk['label']}\n")
        fh.write(f"score: {risk['score']}\n\n")
        fh.write("## Key Metrics\n")
        fh.write(json.dumps(metrics, ensure_ascii=False, indent=2, default=_to_json_serializable))
        fh.write("\n\n")
        fh.write("## Scenario Probabilities\n")
        fh.write(json.dumps(scenarios, ensure_ascii=False, indent=2))
        fh.write("\n\n")
        fh.write("## Data Gaps\n")
        for gap in report["data_gaps"]:
            fh.write(f"- {gap}\n")

    return {"report": report, "json_path": json_path, "md_path": md_path}


if __name__ == "__main__":
    run_us_macro_pipeline()
