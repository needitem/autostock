"""
Groq AI 기반 나스닥 100 종합 분석 모듈
- 전체 종목 분석 후 매수/매도 추천
"""
import os
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"

# 사용 가능한 모델들 (똑똑한 순)
GROQ_MODELS = {
    "llama4-maverick": "meta-llama/llama-4-maverick-17b-128e-instruct",  # Llama 4 최신 (가장 똑똑)
    "llama4-scout": "meta-llama/llama-4-scout-17b-16e-instruct",  # Llama 4 빠름
    "kimi-k2": "moonshotai/kimi-k2-instruct-0905",  # Kimi K2 (262K 컨텍스트)
    "qwen3-32b": "qwen/qwen3-32b",  # Qwen 3 32B
    "llama3.3-70b": "llama-3.3-70b-versatile",  # Llama 3.3 70B
    "gpt-oss-120b": "openai/gpt-oss-120b",  # GPT OSS 120B
}

DEFAULT_MODEL = "llama4-maverick"  # 기본값: Llama 4 Maverick (가장 똑똑)


def _call_groq(prompt: str, max_tokens: int = 4000, model: str = None) -> str | None:
    """Groq API 호출"""
    if not GROQ_API_KEY:
        print("GROQ_API_KEY가 설정되지 않았습니다.")
        return None
    
    # 모델 선택
    model_name = GROQ_MODELS.get(model or DEFAULT_MODEL, GROQ_MODELS[DEFAULT_MODEL])
    
    try:
        response = requests.post(
            GROQ_URL,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {GROQ_API_KEY}"
            },
            json={
                "model": model_name,
                "messages": [
                    {"role": "system", "content": "당신은 미국 주식 전문 애널리스트입니다. 반드시 한국어로 답변하세요."},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.3,
                "max_tokens": max_tokens
            },
            timeout=120
        )
        
        if response.status_code != 200:
            print(f"Groq API 오류: {response.status_code} - {response.text[:200]}")
            return None
        
        data = response.json()
        return data["choices"][0]["message"]["content"]
        
    except Exception as e:
        print(f"Groq 호출 실패: {e}")
        return None


def collect_all_stock_data() -> list[dict]:
    """나스닥 100 전체 종목 데이터 수집"""
    from config import NASDAQ_100
    from analyzer import analyze_single_stock
    from market_data import get_finviz_stock_data
    from factor_model import calculate_composite_score, DEFAULT_WEIGHTS
    from financial_data import get_financial_summary, calculate_financial_score
    
    all_data = []
    
    print(f"📊 {len(NASDAQ_100)}개 종목 데이터 수집 중...")
    
    def analyze_one(symbol):
        try:
            # 기술적 분석
            tech = analyze_single_stock(symbol)
            if not tech:
                return None
            
            # Finviz 데이터 (밸류에이션)
            fv = get_finviz_stock_data(symbol)
            
            # 재무제표 데이터 (yfinance)
            fin = get_financial_summary(symbol)
            
            stock_data = {
                "symbol": symbol,
                "price": tech.get("price"),
                "risk_score": tech.get("risk_score"),
                "risk_grade": tech.get("risk_grade"),
                "rsi": tech.get("rsi"),
                "bb_position": tech.get("bb_position"),
                "position_52w": tech.get("position_52w"),
                "ma50_gap": tech.get("ma50_gap"),
                "change_5d": tech.get("change_5d"),
                "strategies": tech.get("strategies_matched", []),
                "warnings": tech.get("warnings", []),
                # Finviz
                "pe": fv.get("pe", "N/A") if fv else "N/A",
                "forward_pe": fv.get("forward_pe", "N/A") if fv else "N/A",
                "peg": fv.get("peg", "N/A") if fv else "N/A",
                "roe": fv.get("roe", "N/A") if fv else "N/A",
                "roa": fv.get("roa", "N/A") if fv else "N/A",
                "debt_eq": fv.get("debt_eq", "N/A") if fv else "N/A",
                "dividend": fv.get("dividend", "N/A") if fv else "N/A",
                "target_price": fv.get("target_price", "N/A") if fv else "N/A",
                "sector": fv.get("sector", "N/A") if fv else "N/A",
            }
            
            # 재무제표 데이터 추가
            if fin and "error" not in fin:
                stock_data["financial_score"] = fin.get("financial_score", 50)
                stock_data["financial_grade"] = fin.get("financial_grade", "C")
                stock_data["profit_margin"] = fin.get("profit_margin", 0)
                stock_data["operating_margin"] = fin.get("operating_margin", 0)
                stock_data["revenue_growth"] = fin.get("revenue_growth", 0)
                stock_data["earnings_growth"] = fin.get("earnings_growth", 0)
                stock_data["current_ratio"] = fin.get("current_ratio", 0)
                stock_data["free_cash_flow"] = fin.get("free_cash_flow", 0)
                stock_data["beta"] = fin.get("beta", 1)
                stock_data["financial_scores"] = fin.get("scores", {})
            else:
                stock_data["financial_score"] = 50
                stock_data["financial_grade"] = "C"
            
            # 팩터 점수 계산
            factor_result = calculate_composite_score(stock_data, DEFAULT_WEIGHTS)
            stock_data["factor_score"] = factor_result["composite_score"]
            stock_data["factor_grade"] = factor_result["grade"]
            stock_data["factor_recommendation"] = factor_result["recommendation"]
            stock_data["factors"] = factor_result["factors"]
            
            # 종합 점수 (팩터 60% + 재무 40%)
            stock_data["total_score"] = round(
                stock_data["factor_score"] * 0.6 + 
                stock_data.get("financial_score", 50) * 0.4, 1
            )
            
            return stock_data
        except Exception as e:
            print(f"  {symbol} 분석 실패: {e}")
            return None
    
    # 병렬 처리 (10개씩)
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(analyze_one, s): s for s in NASDAQ_100}
        
        for i, future in enumerate(as_completed(futures)):
            result = future.result()
            if result:
                all_data.append(result)
            if (i + 1) % 20 == 0:
                print(f"  {i + 1}/{len(NASDAQ_100)} 완료...")
    
    print(f"✅ {len(all_data)}개 종목 데이터 수집 완료")
    return all_data


def analyze_with_groq(stock_data: list[dict], fear_greed: dict = None, model: str = None) -> dict:
    """Groq으로 전체 종목 분석 및 매수/매도 추천"""
    if not GROQ_API_KEY:
        return {"error": "GROQ_API_KEY가 설정되지 않았습니다."}
    
    # 매수/매도 후보 필터링 (종합 점수 기반: 팩터 60% + 재무 40%)
    buy_candidates = []
    sell_candidates = []
    
    for s in stock_data:
        total_score = s.get("total_score", 50)
        factor_grade = s.get("factor_grade", "C")
        financial_grade = s.get("financial_grade", "C")
        
        # 매수 후보: 종합 점수 높음 (55+) + 위험도 낮음
        if total_score >= 55 and s["risk_score"] <= 35:
            buy_candidates.append(s)
        
        # 매도 후보: 종합 점수 낮음 또는 위험도 높음
        if total_score <= 45 or s["risk_score"] >= 50 or s["rsi"] >= 70:
            sell_candidates.append(s)
    
    # 정렬 (종합 점수 기반)
    buy_candidates.sort(key=lambda x: (-x.get("total_score", 0), x["risk_score"]))
    sell_candidates.sort(key=lambda x: (x.get("total_score", 100), -x["risk_score"]))
    
    # 상위 15개씩만
    buy_candidates = buy_candidates[:15]
    sell_candidates = sell_candidates[:15]
    
    # 데이터 요약 (토큰 절약)
    def summarize(s):
        total = s.get("total_score", 50)
        factor = s.get("factor_score", 50)
        fin = s.get("financial_score", 50)
        f_grade = s.get("factor_grade", "C")
        fin_grade = s.get("financial_grade", "C")
        rev_growth = s.get("revenue_growth", 0)
        rev_growth_pct = f"{rev_growth*100:.0f}%" if rev_growth else "N/A"
        return f"{s['symbol']}:${s['price']},T{total:.0f},F{factor:.0f}{f_grade},FIN{fin:.0f}{fin_grade},risk{s['risk_score']},rsi{s['rsi']:.0f},52w{s['position_52w']:.0f},pe{s['pe']},roe{s.get('roe','N/A')},revG{rev_growth_pct}"
    
    buy_text = "\n".join([summarize(s) for s in buy_candidates])
    sell_text = "\n".join([summarize(s) for s in sell_candidates])
    
    fg_text = ""
    if fear_greed:
        fg_text = f"시장심리: {fear_greed.get('score', 'N/A')}/100 ({fear_greed.get('rating', '')})"
    
    prompt = f"""나스닥100 분석 데이터입니다. {fg_text}

형식: 심볼:$가격,T종합점수,F팩터점수+등급,FIN재무점수+등급,risk위험도,rsi값,52w52주위치,peP/E,roeROE,revG매출성장률

점수 체계:
- 종합점수(T): 팩터 60% + 재무 40%
- 팩터점수(F): 학술연구 기반 (수익성25%+모멘텀20%+가치15%+퀄리티15%+저변동성10%)
- 재무점수(FIN): 피터린치/버핏/그레이엄 기준 (수익성25%+밸류에이션25%+성장성20%+재무건전성20%+배당10%)
- 등급: A(70+적극매수), B(60+매수), C(50+관망), D(40+매도고려), F(매도)

[매수 후보 - 종합점수 높음]
{buy_text}

[매도/관망 후보 - 위험도 높음]
{sell_text}

분석해주세요:

## 📈 매수 추천 TOP 5
각 종목: 심볼, 가격, 종합등급(팩터+재무), 추천이유(팩터+재무 기반 2줄), 주의점

## 📉 매도/관망 TOP 5  
각 종목: 심볼, 가격, 이유(2줄), 리스크

## 💡 종합 전략
팩터+재무 분석 기반 투자 조언 (초보자용 3-4줄)"""

    result = _call_groq(prompt, max_tokens=2000, model=model)
    
    if result:
        return {
            "analysis": result,
            "total_analyzed": len(stock_data),
        }
    return {"error": "Groq 분석 실패"}


def run_full_analysis(model: str = None) -> dict:
    """전체 분석 실행"""
    from market_data import get_fear_greed_index
    from financial_data import get_financial_summary
    
    model_name = GROQ_MODELS.get(model or DEFAULT_MODEL, GROQ_MODELS[DEFAULT_MODEL])
    print(f"🚀 나스닥 100 전체 분석 시작... (모델: {model or DEFAULT_MODEL})")
    print()
    
    # 1. 공포탐욕 지수
    print("[1/3] 시장 심리 확인...")
    fear_greed = get_fear_greed_index()
    print(f"  {fear_greed['emoji']} {fear_greed['score']}/100 - {fear_greed['rating']}")
    print()
    
    # 2. 전체 종목 데이터 수집
    print("[2/3] 종목 데이터 수집...")
    stock_data = collect_all_stock_data()
    print()
    
    # 3. Groq 분석
    print("[3/3] AI 분석 중... (30초~1분 소요)")
    result = analyze_with_groq(stock_data, fear_greed, model)
    
    if "error" in result:
        print(f"❌ 오류: {result['error']}")
        return result
    
    print("✅ 분석 완료!")
    print()
    
    # 4. 매수 추천 TOP 5 재무 데이터 추출
    top_buy_stocks = []
    buy_candidates = sorted(
        [s for s in stock_data if s.get("total_score", 0) >= 55 and s.get("risk_score", 100) <= 35],
        key=lambda x: -x.get("total_score", 0)
    )[:5]
    
    for s in buy_candidates:
        fin = get_financial_summary(s["symbol"])
        roe = fin.get("roe", 0)
        roe_str = f"{roe*100:.1f}%" if isinstance(roe, float) and roe else "N/A"
        pe = fin.get("pe_trailing", 0)
        pe_str = f"{pe:.1f}" if pe else "N/A"
        rev_growth = fin.get("revenue_growth", 0)
        growth_str = f"{rev_growth*100:.1f}%" if isinstance(rev_growth, float) and rev_growth else "N/A"
        
        top_buy_stocks.append({
            "symbol": s["symbol"],
            "price": s.get("price", 0),
            "total_score": s.get("total_score", 0),
            "factor_grade": s.get("factor_grade", "C"),
            "financial_grade": s.get("financial_grade", "C"),
            "roe": roe_str,
            "pe": pe_str,
            "growth": growth_str,
        })
    
    return {
        "fear_greed": fear_greed,
        "total_stocks": len(stock_data),
        "analysis": result.get("analysis", ""),
        "model": model or DEFAULT_MODEL,
        "top_buy_stocks": top_buy_stocks,
    }


def get_quick_recommendations() -> dict:
    """빠른 추천 (데이터 기반, AI 없이)"""
    from config import NASDAQ_100
    from analyzer import analyze_single_stock
    
    buy_candidates = []
    sell_candidates = []
    
    print("📊 빠른 스캔 중...")
    
    def analyze_one(symbol):
        try:
            return analyze_single_stock(symbol)
        except:
            return None
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(analyze_one, s): s for s in NASDAQ_100}
        
        for future in as_completed(futures):
            symbol = futures[future]
            result = future.result()
            if not result:
                continue
            
            # 매수 후보: 위험도 낮음 + 전략 매칭 + RSI 적정
            if (result["risk_score"] <= 25 and 
                result["strategies_matched"] and 
                35 <= result["rsi"] <= 65):
                buy_candidates.append({
                    "symbol": symbol,
                    "price": result["price"],
                    "risk_score": result["risk_score"],
                    "rsi": result["rsi"],
                    "strategies": result["strategies_matched"],
                    "ma50_gap": result["ma50_gap"],
                })
            
            # 매도 후보: 위험도 높음 또는 과매수
            if (result["risk_score"] >= 50 or 
                result["rsi"] >= 70 or 
                result["position_52w"] >= 95):
                sell_candidates.append({
                    "symbol": symbol,
                    "price": result["price"],
                    "risk_score": result["risk_score"],
                    "rsi": result["rsi"],
                    "warnings": result["warnings"],
                    "position_52w": result["position_52w"],
                })
    
    # 정렬
    buy_candidates.sort(key=lambda x: (x["risk_score"], -len(x["strategies"])))
    sell_candidates.sort(key=lambda x: -x["risk_score"])
    
    return {
        "buy": buy_candidates[:10],
        "sell": sell_candidates[:10],
    }


if __name__ == "__main__":
    import sys
    
    # 사용법 출력
    if len(sys.argv) > 1 and sys.argv[1] in ["--help", "-h"]:
        print("""
사용법: python groq_analyzer.py [옵션] [모델]

옵션:
  --quick       빠른 스캔 (AI 없이, 데이터 기반)
  --help, -h    도움말

모델 (기본값: deepseek-r1):
  deepseek-r1      DeepSeek R1 70B (가장 똑똑, 추론/수학 최강)
  llama4-maverick  Llama 4 Maverick (최신)
  llama4-scout     Llama 4 Scout (빠름)
  llama3.3-70b     Llama 3.3 70B
  qwen-qwq         Qwen QwQ 32B (추론)

예시:
  python groq_analyzer.py                    # DeepSeek R1로 분석
  python groq_analyzer.py deepseek-r1        # DeepSeek R1로 분석
  python groq_analyzer.py llama4-maverick    # Llama 4로 분석
  python groq_analyzer.py --quick            # 빠른 스캔 (AI 없이)
""")
        sys.exit(0)
    
    if len(sys.argv) > 1 and sys.argv[1] == "--quick":
        # 빠른 추천 (AI 없이)
        result = get_quick_recommendations()
        
        print("\n" + "=" * 50)
        print("📈 매수 추천 (위험도 낮음 + 전략 매칭)")
        print("=" * 50)
        for i, s in enumerate(result["buy"], 1):
            strategies = ", ".join([st.split()[0] for st in s["strategies"]])
            print(f"{i}. {s['symbol']} ${s['price']} | 위험도:{s['risk_score']} RSI:{s['rsi']:.0f} | {strategies}")
        
        print("\n" + "=" * 50)
        print("📉 매도/관망 추천 (위험도 높음)")
        print("=" * 50)
        for i, s in enumerate(result["sell"], 1):
            print(f"{i}. {s['symbol']} ${s['price']} | 위험도:{s['risk_score']} RSI:{s['rsi']:.0f} 52w:{s['position_52w']}%")
    else:
        # 모델 선택
        model = None
        if len(sys.argv) > 1 and sys.argv[1] in GROQ_MODELS:
            model = sys.argv[1]
        
        # 전체 Groq 분석
        result = run_full_analysis(model)
        
        if "analysis" in result:
            print("=" * 60)
            print(result["analysis"])
            print("=" * 60)
