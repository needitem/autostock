"""
AI 뉴스 분석 모듈 (Groq - Llama 3)
"""
import os
import requests
from dotenv import load_dotenv

load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"


def _call_groq(prompt: str, system_prompt: str = "You are a helpful AI assistant.") -> str | None:
    """Groq API 호출"""
    if not GROQ_API_KEY:
        return None
    
    try:
        response = requests.post(
            GROQ_URL,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {GROQ_API_KEY}"
            },
            json={
                "model": "llama-3.1-8b-instant",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.3,
                "max_tokens": 800
            },
            timeout=30
        )
        
        if response.status_code != 200:
            print(f"Groq API 오류: {response.status_code} - {response.text}")
            return None
        
        data = response.json()
        return data["choices"][0]["message"]["content"]
        
    except Exception as e:
        print(f"Groq 호출 실패: {e}")
        return None


def analyze_news_with_ai(symbol: str, news_list: list) -> dict:
    """뉴스를 AI로 분석"""
    if not GROQ_API_KEY:
        return {"error": "GROQ_API_KEY가 설정되지 않았습니다."}
    
    if not news_list:
        return {"error": "분석할 뉴스가 없습니다."}
    
    news_text = "\n".join([f"- {n['headline']}" for n in news_list[:7]])
    
    prompt = f"""아래 {symbol} 관련 뉴스를 분석해주세요.

뉴스 목록:
{news_text}

다음 형식으로 간결하게 한국어로 답변해주세요:

1. 감성 분석: (긍정/중립/부정) (확신도 %)
2. 핵심 요약: (2-3문장)
3. 주가 영향: (상승/하락/중립 예상, 이유)
4. 주의할 점: (있다면)
5. 투자 의견: (매수/관망/매도 중 하나, 간단한 이유)

주식 초보자도 이해할 수 있게 쉽게 설명해주세요."""

    system = "당신은 주식 투자 전문가입니다. 반드시 한국어로 답변하세요."
    
    result = _call_groq(prompt, system)
    if result:
        return {"symbol": symbol, "analysis": result, "news_count": len(news_list)}
    return {"error": "AI 분석 실패"}


def analyze_stock_with_ai(symbol: str, stock_data: dict, news_list: list = None, market_data: dict = None) -> dict:
    """종목 종합 AI 분석 (외부 데이터 소스 포함)"""
    if not GROQ_API_KEY:
        return {"error": "GROQ_API_KEY가 설정되지 않았습니다."}
    
    news_text = ""
    if news_list:
        news_text = "최근 뉴스:\n" + "\n".join([f"- {n['headline']}" for n in news_list[:5]])
    
    # 외부 데이터 소스 정보 추가
    external_data = ""
    if market_data:
        sources = market_data.get("sources", {})
        
        # Finviz 데이터
        fv = sources.get("finviz", {})
        if fv:
            external_data += f"\nFinviz 데이터:\n"
            external_data += f"- P/E: {fv.get('pe', 'N/A')}, Forward P/E: {fv.get('forward_pe', 'N/A')}\n"
            external_data += f"- PEG: {fv.get('peg', 'N/A')}, ROE: {fv.get('roe', 'N/A')}\n"
            external_data += f"- 목표가: ${fv.get('target_price', 'N/A')}\n"
            external_data += f"- 공매도비율: {fv.get('short_float', 'N/A')}\n"
        
        # TipRanks 데이터
        tr = sources.get("tipranks", {})
        if tr:
            external_data += f"\nTipRanks 애널리스트:\n"
            external_data += f"- 컨센서스: {tr.get('consensus', 'N/A')}\n"
            external_data += f"- 매수/보유/매도: {tr.get('buy', 0)}/{tr.get('hold', 0)}/{tr.get('sell', 0)}\n"
            external_data += f"- 평균 목표가: ${tr.get('price_target_avg', 0):.2f}\n"
    
    prompt = f"""{symbol} 종목을 분석해주세요.

기술적 지표:
- 현재가: ${stock_data.get('price', 'N/A')}
- RSI: {stock_data.get('rsi', 'N/A')}
- 50일선 대비: {stock_data.get('ma50_gap', 'N/A')}%
- 52주 범위 위치: {stock_data.get('position_52w', 'N/A')}%
- 5일 수익률: {stock_data.get('change_5d', 'N/A')}%
- 위험도 점수: {stock_data.get('risk_score', 'N/A')}/100
{external_data}
{news_text}

다음 형식으로 간결하게 한국어로 답변해주세요:

📊 종합 평가: (한 문장)
🎯 매매 전략: (구체적인 진입/손절/목표가)
⚠️ 리스크: (주의할 점)
💡 초보자 조언: (쉬운 설명)

주식 초보자도 이해할 수 있게 쉽게 설명해주세요."""

    system = "당신은 주식 투자 전문가입니다. 반드시 한국어로 답변하세요."
    
    result = _call_groq(prompt, system)
    if result:
        return {"symbol": symbol, "analysis": result}
    return {"error": "AI 분석 실패"}


def get_market_sentiment(news_list: list, fear_greed: dict = None) -> dict:
    """시장 전체 감성 분석 (공포탐욕 지수 포함)"""
    if not GROQ_API_KEY:
        return {"error": "GROQ_API_KEY가 설정되지 않았습니다."}
    
    if not news_list:
        return {"error": "분석할 뉴스가 없습니다."}
    
    news_text = "\n".join([f"- {n['headline']}" for n in news_list[:10]])
    
    fg_text = ""
    if fear_greed:
        fg_text = f"\nCNN 공포탐욕 지수: {fear_greed.get('score', 'N/A')}/100 ({fear_greed.get('rating', '')})"
    
    prompt = f"""아래 시장 뉴스를 분석해주세요.

뉴스 목록:
{news_text}
{fg_text}

다음 형식으로 간결하게 한국어로 답변해주세요:

🌡️ 시장 분위기: (탐욕/낙관/중립/공포 중 하나)
📈 주요 이슈: (2-3개 핵심 이슈)
💡 오늘의 전략: (초보자를 위한 조언)

주식 초보자도 이해할 수 있게 쉽게 설명해주세요."""

    system = "당신은 주식 시장 전문가입니다. 반드시 한국어로 답변하세요."
    
    result = _call_groq(prompt, system)
    if result:
        return {"analysis": result}
    return {"error": "AI 분석 실패"}
