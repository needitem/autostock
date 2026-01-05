"""
재무제표 데이터 수집 모듈
- yfinance에서 재무지표 가져오기
- 핵심 재무 비율 계산

참고:
- 피터 린치: PEG < 1 저평가
- 워렌 버핏: ROE > 15%, 부채비율 낮음, 이익 성장
- 벤저민 그레이엄: P/E < 15, P/B < 1.5, 유동비율 > 2
"""
import yfinance as yf
from functools import lru_cache
from datetime import datetime, timedelta


@lru_cache(maxsize=200)
def get_financial_data(symbol: str) -> dict:
    """
    종목의 재무제표 데이터 가져오기
    
    Returns:
        dict: 핵심 재무지표
    """
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        
        # 기본 정보
        data = {
            "symbol": symbol,
            "name": info.get("shortName", symbol),
            "sector": info.get("sector", "N/A"),
            "industry": info.get("industry", "N/A"),
            "market_cap": info.get("marketCap", 0),
            "currency": info.get("currency", "USD"),
        }
        
        # === 수익성 지표 ===
        data["roe"] = info.get("returnOnEquity", 0)  # 자기자본이익률
        data["roa"] = info.get("returnOnAssets", 0)  # 총자산이익률
        data["profit_margin"] = info.get("profitMargins", 0)  # 순이익률
        data["operating_margin"] = info.get("operatingMargins", 0)  # 영업이익률
        data["gross_margin"] = info.get("grossMargins", 0)  # 매출총이익률
        
        # === 밸류에이션 지표 ===
        data["pe_trailing"] = info.get("trailingPE", 0)  # 후행 P/E
        data["pe_forward"] = info.get("forwardPE", 0)  # 선행 P/E
        data["peg"] = info.get("pegRatio", 0)  # PEG 비율
        data["pb"] = info.get("priceToBook", 0)  # P/B
        data["ps"] = info.get("priceToSalesTrailing12Months", 0)  # P/S
        data["ev_ebitda"] = info.get("enterpriseToEbitda", 0)  # EV/EBITDA
        data["ev_revenue"] = info.get("enterpriseToRevenue", 0)  # EV/Revenue
        
        # === 성장성 지표 ===
        data["revenue_growth"] = info.get("revenueGrowth", 0)  # 매출 성장률
        data["earnings_growth"] = info.get("earningsGrowth", 0)  # 이익 성장률
        data["earnings_quarterly_growth"] = info.get("earningsQuarterlyGrowth", 0)  # 분기 이익 성장
        
        # === 재무 건전성 지표 ===
        data["debt_to_equity"] = info.get("debtToEquity", 0)  # 부채비율
        data["current_ratio"] = info.get("currentRatio", 0)  # 유동비율
        data["quick_ratio"] = info.get("quickRatio", 0)  # 당좌비율
        data["total_debt"] = info.get("totalDebt", 0)  # 총부채
        data["total_cash"] = info.get("totalCash", 0)  # 총현금
        data["free_cash_flow"] = info.get("freeCashflow", 0)  # 잉여현금흐름
        data["operating_cash_flow"] = info.get("operatingCashflow", 0)  # 영업현금흐름
        
        # === 배당 지표 ===
        data["dividend_yield"] = info.get("dividendYield", 0)  # 배당수익률
        data["dividend_rate"] = info.get("dividendRate", 0)  # 배당금
        data["payout_ratio"] = info.get("payoutRatio", 0)  # 배당성향
        data["ex_dividend_date"] = info.get("exDividendDate", None)  # 배당락일
        
        # === 애널리스트 추정 ===
        data["target_mean"] = info.get("targetMeanPrice", 0)  # 목표가 평균
        data["target_high"] = info.get("targetHighPrice", 0)  # 목표가 최고
        data["target_low"] = info.get("targetLowPrice", 0)  # 목표가 최저
        data["recommendation"] = info.get("recommendationKey", "N/A")  # 추천
        data["num_analysts"] = info.get("numberOfAnalystOpinions", 0)  # 애널리스트 수
        
        # === 주가 정보 ===
        data["current_price"] = info.get("currentPrice", 0) or info.get("regularMarketPrice", 0)
        data["52w_high"] = info.get("fiftyTwoWeekHigh", 0)
        data["52w_low"] = info.get("fiftyTwoWeekLow", 0)
        data["50d_avg"] = info.get("fiftyDayAverage", 0)
        data["200d_avg"] = info.get("twoHundredDayAverage", 0)
        data["beta"] = info.get("beta", 1)  # 베타 (시장 대비 변동성)
        
        # === 기타 ===
        data["shares_outstanding"] = info.get("sharesOutstanding", 0)
        data["float_shares"] = info.get("floatShares", 0)
        data["short_ratio"] = info.get("shortRatio", 0)  # 공매도 비율
        data["insider_ownership"] = info.get("heldPercentInsiders", 0)  # 내부자 지분
        data["institution_ownership"] = info.get("heldPercentInstitutions", 0)  # 기관 지분
        
        return data
        
    except Exception as e:
        print(f"재무 데이터 가져오기 실패 ({symbol}): {e}")
        return {"symbol": symbol, "error": str(e)}


def calculate_financial_score(data: dict) -> dict:
    """
    재무지표 기반 종합 점수 계산
    
    기준:
    - 피터 린치: PEG < 1
    - 워렌 버핏: ROE > 15%, 낮은 부채
    - 벤저민 그레이엄: P/E < 15, P/B < 1.5
    """
    scores = {
        "profitability": 0,
        "valuation": 0,
        "growth": 0,
        "financial_health": 0,
        "dividend": 0,
    }
    
    # === 수익성 점수 (0-100) ===
    roe = (data.get("roe") or 0) * 100
    roa = (data.get("roa") or 0) * 100
    profit_margin = (data.get("profit_margin") or 0) * 100
    
    prof_score = 50
    # ROE (워렌 버핏: 15% 이상)
    if roe >= 25:
        prof_score += 25
    elif roe >= 20:
        prof_score += 20
    elif roe >= 15:
        prof_score += 15
    elif roe >= 10:
        prof_score += 10
    elif roe < 0:
        prof_score -= 20
    
    # ROA
    if roa >= 15:
        prof_score += 15
    elif roa >= 10:
        prof_score += 10
    elif roa >= 5:
        prof_score += 5
    elif roa < 0:
        prof_score -= 10
    
    # 순이익률
    if profit_margin >= 20:
        prof_score += 10
    elif profit_margin >= 10:
        prof_score += 5
    elif profit_margin < 0:
        prof_score -= 10
    
    scores["profitability"] = max(0, min(100, prof_score))
    
    # === 밸류에이션 점수 (0-100) ===
    pe = data.get("pe_trailing") or 0
    pb = data.get("pb") or 0
    peg = data.get("peg") or 0
    
    val_score = 50
    # P/E (그레이엄: 15 이하)
    if 0 < pe <= 10:
        val_score += 25
    elif 10 < pe <= 15:
        val_score += 20
    elif 15 < pe <= 20:
        val_score += 10
    elif 20 < pe <= 30:
        val_score += 0
    elif pe > 40:
        val_score -= 15
    
    # P/B (그레이엄: 1.5 이하)
    if 0 < pb <= 1:
        val_score += 15
    elif 1 < pb <= 1.5:
        val_score += 10
    elif 1.5 < pb <= 3:
        val_score += 5
    elif pb > 5:
        val_score -= 10
    
    # PEG (피터 린치: 1 이하)
    if 0 < peg <= 0.5:
        val_score += 15
    elif 0.5 < peg <= 1:
        val_score += 10
    elif 1 < peg <= 2:
        val_score += 5
    elif peg > 3:
        val_score -= 10
    
    scores["valuation"] = max(0, min(100, val_score))
    
    # === 성장성 점수 (0-100) ===
    rev_growth = (data.get("revenue_growth") or 0) * 100
    earn_growth = (data.get("earnings_growth") or 0) * 100
    
    growth_score = 50
    # 매출 성장률
    if rev_growth >= 30:
        growth_score += 20
    elif rev_growth >= 20:
        growth_score += 15
    elif rev_growth >= 10:
        growth_score += 10
    elif rev_growth >= 5:
        growth_score += 5
    elif rev_growth < 0:
        growth_score -= 10
    
    # 이익 성장률
    if earn_growth >= 30:
        growth_score += 20
    elif earn_growth >= 20:
        growth_score += 15
    elif earn_growth >= 10:
        growth_score += 10
    elif earn_growth < -10:
        growth_score -= 15
    
    scores["growth"] = max(0, min(100, growth_score))
    
    # === 재무 건전성 점수 (0-100) ===
    debt_eq = (data.get("debt_to_equity") or 0) / 100  # 퍼센트를 비율로
    current = data.get("current_ratio") or 0
    fcf = data.get("free_cash_flow") or 0
    
    health_score = 50
    # 부채비율 (낮을수록 좋음)
    if debt_eq <= 0.3:
        health_score += 20
    elif debt_eq <= 0.5:
        health_score += 15
    elif debt_eq <= 1:
        health_score += 10
    elif debt_eq > 2:
        health_score -= 15
    
    # 유동비율 (그레이엄: 2 이상)
    if current >= 2:
        health_score += 15
    elif current >= 1.5:
        health_score += 10
    elif current >= 1:
        health_score += 5
    elif current < 1:
        health_score -= 10
    
    # 잉여현금흐름 (양수면 좋음)
    if fcf > 0:
        health_score += 15
    else:
        health_score -= 10
    
    scores["financial_health"] = max(0, min(100, health_score))
    
    # === 배당 점수 (0-100) ===
    div_yield = (data.get("dividend_yield") or 0) * 100
    payout = (data.get("payout_ratio") or 0) * 100
    
    div_score = 50
    # 배당수익률
    if div_yield >= 4:
        div_score += 20
    elif div_yield >= 2:
        div_score += 15
    elif div_yield >= 1:
        div_score += 10
    elif div_yield > 0:
        div_score += 5
    
    # 배당성향 (30-60%가 이상적)
    if 30 <= payout <= 60:
        div_score += 15
    elif 20 <= payout < 30 or 60 < payout <= 80:
        div_score += 10
    elif payout > 100:
        div_score -= 10  # 이익보다 많이 배당 (위험)
    
    scores["dividend"] = max(0, min(100, div_score))
    
    # === 종합 점수 ===
    # 가중치: 수익성 25%, 밸류에이션 25%, 성장성 20%, 재무건전성 20%, 배당 10%
    composite = (
        scores["profitability"] * 0.25 +
        scores["valuation"] * 0.25 +
        scores["growth"] * 0.20 +
        scores["financial_health"] * 0.20 +
        scores["dividend"] * 0.10
    )
    
    # 등급
    if composite >= 70:
        grade = "A"
    elif composite >= 60:
        grade = "B"
    elif composite >= 50:
        grade = "C"
    elif composite >= 40:
        grade = "D"
    else:
        grade = "F"
    
    return {
        "symbol": data.get("symbol", ""),
        "financial_score": round(composite, 1),
        "financial_grade": grade,
        "scores": scores,
        "key_metrics": {
            "roe": f"{roe:.1f}%",
            "pe": f"{pe:.1f}" if pe else "N/A",
            "pb": f"{pb:.1f}" if pb else "N/A",
            "peg": f"{peg:.1f}" if peg else "N/A",
            "debt_equity": f"{debt_eq*100:.0f}%",
            "revenue_growth": f"{rev_growth:.1f}%",
            "dividend_yield": f"{div_yield:.1f}%",
        }
    }


def get_financial_summary(symbol: str) -> dict:
    """재무 데이터 + 점수 종합"""
    data = get_financial_data(symbol)
    if "error" in data:
        return data
    
    score = calculate_financial_score(data)
    
    return {
        **data,
        **score,
    }


def format_financial_report(data: dict) -> str:
    """재무 분석 리포트 포맷팅"""
    if "error" in data:
        return f"❌ {data['symbol']}: {data['error']}"
    
    report = f"""
📊 {data['symbol']} ({data.get('name', '')}) 재무 분석
{'='*50}

💰 수익성
  • ROE: {data.get('key_metrics', {}).get('roe', 'N/A')}
  • 순이익률: {(data.get('profit_margin') or 0)*100:.1f}%
  • 영업이익률: {(data.get('operating_margin') or 0)*100:.1f}%

📈 밸류에이션
  • P/E: {data.get('key_metrics', {}).get('pe', 'N/A')}
  • P/B: {data.get('key_metrics', {}).get('pb', 'N/A')}
  • PEG: {data.get('key_metrics', {}).get('peg', 'N/A')}

🚀 성장성
  • 매출 성장률: {data.get('key_metrics', {}).get('revenue_growth', 'N/A')}
  • 이익 성장률: {(data.get('earnings_growth') or 0)*100:.1f}%

🏦 재무 건전성
  • 부채비율: {data.get('key_metrics', {}).get('debt_equity', 'N/A')}
  • 유동비율: {data.get('current_ratio', 0):.2f}
  • 잉여현금흐름: ${data.get('free_cash_flow', 0)/1e9:.1f}B

💵 배당
  • 배당수익률: {data.get('key_metrics', {}).get('dividend_yield', 'N/A')}
  • 배당성향: {(data.get('payout_ratio') or 0)*100:.0f}%

{'='*50}
📊 종합 점수: {data.get('financial_score', 0)}/100 ({data.get('financial_grade', 'N/A')})

세부 점수:
  • 수익성: {data.get('scores', {}).get('profitability', 0)}/100
  • 밸류에이션: {data.get('scores', {}).get('valuation', 0)}/100
  • 성장성: {data.get('scores', {}).get('growth', 0)}/100
  • 재무건전성: {data.get('scores', {}).get('financial_health', 0)}/100
  • 배당: {data.get('scores', {}).get('dividend', 0)}/100
"""
    return report


if __name__ == "__main__":
    # 테스트
    symbols = ["AAPL", "MSFT", "NVDA"]
    
    for symbol in symbols:
        data = get_financial_summary(symbol)
        print(format_financial_report(data))
        print()
