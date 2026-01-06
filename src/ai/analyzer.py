# -*- coding: utf-8 -*-
import os
import requests
from dotenv import load_dotenv
load_dotenv()

class AIAnalyzer:
    def __init__(self, provider="auto", model=None):
        self.openrouter_key = os.getenv("OPENROUTER_API_KEY")
        self.zai_key = os.getenv("ZAI_API_KEY")
        if provider == "auto":
            provider = "zai" if self.zai_key else ("openrouter" if self.openrouter_key else None)
        self.provider = provider
        if provider == "zai":
            self.api_key = self.zai_key
            self.base_url = "https://api.z.ai/api/coding/paas/v4/chat/completions"
            self.model = model or "glm-4.7"
        elif provider == "openrouter":
            self.api_key = self.openrouter_key
            self.base_url = "https://openrouter.ai/api/v1/chat/completions"
            self.model = "deepseek/deepseek-r1-0528:free"
        else:
            self.api_key = self.base_url = self.model = None

    def _call(self, prompt, max_tokens=8000):
        if not self.api_key:
            return None
        try:
            print(f"[AI] {self.provider} calling...")
            r = requests.post(self.base_url, headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
                json={"model": self.model, "messages": [{"role": "system", "content": "US stock analyst. Korean only. Be thorough."}, {"role": "user", "content": prompt}], "temperature": 0.3, "max_tokens": max_tokens}, timeout=300)
            if r.status_code == 200:
                c = r.json().get("choices", [{}])[0].get("message", {})
                result = c.get("content") or c.get("reasoning_content")
                if result:
                    print(f"[AI] OK len={len(result)}")
                return result
            print(f"[AI] Failed: {r.status_code}")
        except Exception as e:
            print(f"[AI] Error: {e}")
        return None

    def _fmt_stock(self, s, news_list=None):
        sc = s.get("score", {})
        ts = sc.get("total_score", 0) if isinstance(sc, dict) else 0
        gr = sc.get("grade", "C") if isinstance(sc, dict) else "C"
        line = f"{s['symbol']} $${s.get('price',0):.1f} | 점수{ts:.0f}({gr}) | RSI{s.get('rsi',50):.0f} | BB{s.get('bb_position',50):.0f}% | 50MA{s.get('ma50_gap',0):+.1f}%"
        if news_list:
            for n in news_list[:3]:
                headline = n.get('headline', '')[:80]
                summary = n.get('summary', '')[:100]
                sentiment = "긍정" if any(w in headline.lower() for w in ['up','rise','gain','beat','strong']) else "부정" if any(w in headline.lower() for w in ['down','fall','drop','miss','weak','cut']) else "중립"
                line += f"\n  [{n.get('datetime','')}] {headline} ({sentiment})"
                if summary:
                    line += f"\n    > {summary}"
        return line

    def _cat_section(self, cat, info, stocks, news):
        cs = [s for s in stocks if s["symbol"] in info["stocks"]]
        if not cs:
            return ""
        cs.sort(key=lambda x: -(x.get("score",{}).get("total_score",0) if isinstance(x.get("score"),dict) else 0))
        sec = f"\n### {info['emoji']} {cat} ({len(cs)}개 종목)\n"
        for s in cs:
            sec += f"{self._fmt_stock(s, news.get(s['symbol']))}\n"
        return sec

    def analyze_stock(self, symbol, data):
        news = data.get("news", [])
        news_text = ""
        if news:
            news_text = "\n\n 최근 뉴스:\n"
            for n in news[:5]:
                headline = n.get('headline', '')
                summary = n.get('summary', '')[:150]
                sentiment = "긍정" if any(w in headline.lower() for w in ['up','rise','gain','beat','strong']) else "부정" if any(w in headline.lower() for w in ['down','fall','drop','miss','weak','cut']) else "중립"
                news_text += f"- [{n.get('datetime','')}] {headline} ({sentiment})\n"
                if summary:
                    news_text += f"  요약: {summary}\n"
        prompt = f"""{symbol} 종목 분석

 기술적 지표:
- 현재가: $${data.get('price',0)}
- RSI: {data.get('rsi',50):.0f}
- 볼린저밴드 위치: {data.get('bb_position',50):.0f}%
- 50일선 대비: {data.get('ma50_gap',0):+.1f}%
- 200일선 대비: {data.get('ma200_gap',0):+.1f}%
- 종합점수: {data.get('total_score',50)}/100
{news_text}

한국어로 분석해주세요:
1. 기술적 상태 (2줄)
2. 뉴스 영향 분석 - 각 뉴스가 주가에 미치는 영향 (3줄)
3. 매수/매도/관망 의견과 근거 (2줄)
4. 주요 리스크 (1줄)"""
        r = self._call(prompt, 2000)
        return {"analysis": r} if r else {"error": "AI failed"}

    def analyze_full_market(self, stocks, news_data, market_data, categories):
        if not stocks:
            return {"error": "No stocks"}
        n = len(stocks)
        avg_rsi = sum(s.get("rsi",50) for s in stocks) / n
        get_sc = lambda s: s.get("score",{}).get("total_score",0) if isinstance(s.get("score"),dict) else 0
        avg_score = sum(get_sc(s) for s in stocks) / n
        gd = {"A":0,"B":0,"C":0,"D":0,"F":0}
        for s in stocks:
            g = s.get("score",{}).get("grade","C") if isinstance(s.get("score"),dict) else "C"
            if g in gd: gd[g] += 1
        oversold = sum(1 for s in stocks if s.get("rsi",50) < 30)
        overbought = sum(1 for s in stocks if s.get("rsi",50) > 70)
        fg = market_data.get("fear_greed",{})
        mc = market_data.get("market_condition",{})
        mn = market_data.get("market_news",[])
        
        mkt = f"""
 시장 현황:
- 공포탐욕지수: {fg.get('score','?')} ({fg.get('rating','?')})
- 시장 추세: {mc.get('message','?')}
- QQQ: $${mc.get('price',0):.2f}
"""
        if mn:
            mkt += "\n 시장 주요 뉴스:\n"
            for x in mn[:5]:
                mkt += f"- {x.get('headline','')}\n"
                if x.get('summary'):
                    mkt += f"  > {x.get('summary','')[:100]}\n"
        
        cat_sec = "".join([self._cat_section(c, i, stocks, news_data) for c, i in categories.items()])
        
        prompt = f"""전체 시장 종합 분석 ({n}개 종목)

 시장 통계:
- 평균 RSI: {avg_rsi:.0f}
- 평균 점수: {avg_score:.0f}/100
- 등급 분포: A등급 {gd['A']}개, B등급 {gd['B']}개, C등급 {gd['C']}개, D등급 {gd['D']}개, F등급 {gd['F']}개
- 과매도(RSI<30): {oversold}개
- 과매수(RSI>70): {overbought}개
{mkt}

## 카테고리별 전체 종목 데이터 (기술적 지표 + 뉴스)
{cat_sec}

위 모든 데이터(기술적 지표 + 뉴스)를 종합 분석해서 한국어로 작성해주세요:

##  카테고리별 TOP 5 (각 카테고리에서 5개씩)
각 카테고리별로 가장 매력적인 종목 5개 선정
- 종목명 () - 선정 이유 (기술적 지표 + 뉴스 근거 포함, 2줄)

##  전체 시장 TOP 5
모든 카테고리 통틀어 가장 추천하는 5개 종목
- 종목명 () [카테고리] - 추천 이유 (기술적 + 뉴스 분석, 3줄)

##  주의 종목
위험도가 높거나 악재 뉴스가 있는 종목들 (뉴스 근거 포함)

##  시장 분석
- 현재 시장 전체 상태 평가 (2줄)
- 강세 섹터 vs 약세 섹터 (1줄)

##  투자 전략
현재 시장 상황, 뉴스, 기술적 지표를 종합한 투자 조언 (4줄)"""

        r = self._call(prompt, 8000)
        return {"analysis": r, "total": n, "stats": {"avg_rsi": avg_rsi, "avg_score": avg_score, "grade_dist": gd, "oversold": oversold, "overbought": overbought}} if r else {"error": "AI failed"}

    def analyze_category(self, category, stocks, news_data=None):
        if not stocks:
            return {"error": "No stocks"}
        news_data = news_data or {}
        stocks = sorted(stocks, key=lambda x: -(x.get("score",{}).get("total_score",0) if isinstance(x.get("score"),dict) else 0))
        st = "\n".join([self._fmt_stock(s, news_data.get(s['symbol'])) for s in stocks])
        prompt = f"""{category} 섹터 분석 ({len(stocks)}개 종목)

{st}

한국어로 분석:
## TOP 5 추천
각 종목: 심볼 (), 이유 (기술적 + 뉴스 근거 2줄)

## 주의 종목
위험하거나 악재 뉴스가 있는 종목

## {category} 투자 전략 (3줄)"""
        r = self._call(prompt, 3000)
        return {"analysis": r, "category": category, "total": len(stocks)} if r else {"error": "AI failed"}

    def analyze_recommendations(self, stocks, news_data=None, market_data=None):
        from config import STOCK_CATEGORIES
        return self.analyze_full_market(stocks, news_data or {}, market_data or {}, STOCK_CATEGORIES)

ai = AIAnalyzer()
