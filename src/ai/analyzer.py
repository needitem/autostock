"""
AI 분석 모듈 (OpenRouter / Z.ai)
"""
import os
import requests
from dotenv import load_dotenv

load_dotenv()


class AIAnalyzer:
    """AI 분석기"""
    
    # OpenRouter 모델
    OPENROUTER_MODELS = {
        "deepseek": "deepseek/deepseek-r1-0528:free",
        "kimi": "moonshotai/kimi-k2:free",
        "qwen": "qwen/qwen3-4b:free",
        "gemma": "google/gemma-3n-e4b-it:free",
    }
    
    # Z.ai 모델
    ZAI_MODELS = {
        "glm-4.7": "glm-4.7",
        "glm-4.6": "glm-4.6",
        "glm-4.5": "glm-4.5",
    }
    
    def __init__(self, provider: str = "auto", model: str = None):
        """
        provider: "openrouter", "zai", "auto" (auto는 Z.ai 우선)
        """
        self.openrouter_key = os.getenv("OPENROUTER_API_KEY")
        self.zai_key = os.getenv("ZAI_API_KEY")
        
        # 자동 선택: Z.ai 키가 있으면 Z.ai 우선
        if provider == "auto":
            if self.zai_key:
                provider = "zai"
            elif self.openrouter_key:
                provider = "openrouter"
            else:
                provider = None
        
        self.provider = provider
        
        if provider == "zai":
            self.api_key = self.zai_key
            self.base_url = "https://api.z.ai/api/coding/paas/v4/chat/completions"  # Coding Plan용
            self.model = model or "glm-4.7"
        elif provider == "openrouter":
            self.api_key = self.openrouter_key
            self.base_url = "https://openrouter.ai/api/v1/chat/completions"
            self.model = self.OPENROUTER_MODELS.get(model, self.OPENROUTER_MODELS["deepseek"])
        else:
            self.api_key = None
            self.base_url = None
            self.model = None
    
    def _call(self, prompt: str, max_tokens: int = 2000) -> str | None:
        """API 호출"""
        if not self.api_key:
            print("AI 호출 실패: API 키 없음")
            return None
        
        try:
            print(f"[AI] {self.provider} 호출 중... (모델: {self.model})")
            response = requests.post(
                self.base_url,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": self.model,
                    "messages": [
                        {"role": "system", "content": "당신은 미국 주식 전문 애널리스트입니다. 반드시 한국어로만 답변하세요. 생각 과정 없이 결과만 간결하게 출력하세요."},
                        {"role": "user", "content": prompt}
                    ],
                    "temperature": 0.3,
                    "max_tokens": max_tokens
                },
                timeout=120
            )
            
            print(f"[AI] 응답 코드: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print(f"[AI] 응답 데이터 키: {data.keys()}")
                
                choice = data.get("choices", [{}])[0]
                message = choice.get("message", {})
                
                # Z.ai는 reasoning_content와 content가 분리될 수 있음
                # content만 사용 (reasoning은 생각 과정이므로 무시)
                content = message.get("content", "")
                
                if content:
                    print(f"[AI] 성공 - 응답 길이: {len(content)}")
                    return content
                else:
                    # content가 없으면 reasoning_content 사용 (폴백)
                    reasoning = message.get("reasoning_content", "")
                    if reasoning:
                        print(f"[AI] reasoning_content 사용 - 길이: {len(reasoning)}")
                        return reasoning
                    print(f"[AI] 응답 내용 없음 - message: {message}")
            else:
                print(f"[AI] 호출 실패 ({self.provider}): HTTP {response.status_code} - {response.text[:500]}")
                
                # Z.ai 실패 시 OpenRouter로 폴백
                if self.provider == "zai" and self.openrouter_key:
                    print("[AI] OpenRouter로 폴백...")
                    self.provider = "openrouter"
                    self.api_key = self.openrouter_key
                    self.base_url = "https://openrouter.ai/api/v1/chat/completions"
                    self.model = self.OPENROUTER_MODELS["deepseek"]
                    return self._call(prompt, max_tokens)
                    
        except Exception as e:
            print(f"[AI] 호출 예외: {e}")
        
        return None
    
    def analyze_stock(self, symbol: str, data: dict) -> dict:
        """개별 종목 AI 분석"""
        # 재무 데이터
        finviz = data.get("finviz", {})
        
        prompt = f"""{symbol} 종목 분석 데이터:

📊 가격 정보:
- 현재가: ${data.get('price', 0)}
- 52주 고가: ${data.get('high_52w', 0)} / 저가: ${data.get('low_52w', 0)}
- 52주 내 위치: {data.get('position_52w', 50):.0f}%

📈 기술적 지표:
- RSI(14): {data.get('rsi', 50):.0f}
- MACD: {data.get('macd', 0):.3f} (시그널: {data.get('macd_signal', 0):.3f})
- 볼린저밴드 위치: {data.get('bb_position', 50):.0f}%
- 50일선 대비: {data.get('ma50_gap', 0):+.1f}%
- 200일선 대비: {data.get('ma200_gap', 0):+.1f}%
- 5일 변화율: {data.get('change_5d', 0):+.1f}%

💰 재무 지표:
- P/E: {finviz.get('P/E', data.get('pe', 'N/A'))}
- Forward P/E: {finviz.get('Forward P/E', 'N/A')}
- PEG: {finviz.get('PEG', 'N/A')}
- ROE: {finviz.get('ROE', data.get('roe', 'N/A'))}
- ROA: {finviz.get('ROA', 'N/A')}
- Profit Margin: {finviz.get('Profit Margin', 'N/A')}
- Debt/Eq: {finviz.get('Debt/Eq', 'N/A')}

📋 종합점수: {data.get('total_score', 50)}/100

위 데이터를 바탕으로 분석해주세요:
1. 현재 기술적 상태 (2줄)
2. 재무 건전성 평가 (1줄)
3. 매수/매도/관망 의견과 근거 (2줄)
4. 주요 리스크 (1줄)"""

        result = self._call(prompt, 800)
        return {"analysis": result} if result else {"error": "AI 분석 실패"}
    
    def analyze_recommendations(self, stocks: list[dict]) -> dict:
        """추천 종목 AI 분석"""
        if not stocks:
            return {"error": "분석할 종목 없음"}
        
        # score 딕셔너리에서 값 추출
        def get_score(s):
            score = s.get("score", {})
            return score.get("total_score", 0) if isinstance(score, dict) else 0
        
        def get_risk(s):
            score = s.get("score", {})
            risk = score.get("risk", {}) if isinstance(score, dict) else {}
            return risk.get("score", 0) if isinstance(risk, dict) else 0
        
        def get_grade(s):
            score = s.get("score", {})
            return score.get("grade", "C") if isinstance(score, dict) else "C"
        
        # 상위 15개만
        stocks = sorted(stocks, key=lambda x: -get_score(x))[:15]
        
        stock_text = "\n".join([
            f"• {s['symbol']}: ${s.get('price',0):.0f}, 점수 {get_score(s):.0f}({get_grade(s)}), "
            f"RSI {s.get('rsi',50):.0f}, BB {s.get('bb_position',50):.0f}%, "
            f"50MA {s.get('ma50_gap',0):+.1f}%, 위험 {get_risk(s)}"
            for s in stocks
        ])
        
        prompt = f"""나스닥 100 종목 스캔 결과입니다.

{stock_text}

지표 설명:
- 점수: 종합 투자 매력도 (100점 만점, A~F 등급)
- RSI: 과매수(>70)/과매도(<30) 지표
- BB: 볼린저밴드 위치 (0%=하단, 100%=상단)
- 50MA: 50일 이동평균선 대비 괴리율
- 위험: 위험도 점수 (높을수록 위험)

분석해주세요:

## 📈 매수 추천 TOP 5
각 종목별로:
- 심볼 ($가격)
- 추천 이유 (기술적/재무적 근거 1줄)

## 📉 주의 종목
위험도가 높거나 과매수 구간인 종목 (있다면)

## 💡 투자 전략
현재 시장 상황을 고려한 초보자용 조언 (2-3줄)"""

        result = self._call(prompt, 1500)
        return {"analysis": result, "total": len(stocks)} if result else {"error": "AI 분석 실패"}
    
    def analyze_category(self, category: str, stocks: list[dict]) -> dict:
        """카테고리별 AI 분석"""
        if not stocks:
            return {"error": "분석할 종목 없음"}
        
        def get_score(s):
            score = s.get("score", {})
            return score.get("total_score", 0) if isinstance(score, dict) else 0
        
        stocks = sorted(stocks, key=lambda x: -get_score(x))[:10]
        
        stock_text = "\n".join([
            f"{s['symbol']}:${s.get('price',0):.0f},점수{get_score(s):.0f},RSI{s.get('rsi',50):.0f}"
            for s in stocks
        ])
        
        prompt = f"""{category} 섹터 분석 데이터입니다.

{stock_text}

분석해주세요:

## 📈 매수 추천 TOP 3
각 종목: 심볼, 가격, 이유(1줄)

## 💡 {category} 투자 전략
이 섹터 투자 시 고려사항 (2줄)"""

        result = self._call(prompt, 800)
        return {"analysis": result, "category": category} if result else {"error": "AI 분석 실패"}


# 싱글톤
ai = AIAnalyzer()
