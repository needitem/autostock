# -*- coding: utf-8 -*-
import os
import requests
from dotenv import load_dotenv
load_dotenv()

class AIAnalyzer:
    # 지표 해석 가이드 (초보자용)
    INDICATOR_GUIDE = {
        "rsi": {
            "name": "RSI (상대강도지수)",
            "desc": "주가의 과매수/과매도 상태를 0~100으로 표시",
            "ranges": [(30, "과매도 - 매수 기회 가능"), (50, "중립"), (70, "과매수 근접"), (100, "과매수 - 매도 고려")]
        },
        "stoch": {
            "name": "스토캐스틱",
            "desc": "단기 모멘텀 지표, RSI보다 민감하게 반응",
            "ranges": [(20, "과매도 - 반등 기대"), (50, "중립"), (80, "과매수 - 조정 가능")]
        },
        "adx": {
            "name": "ADX (추세강도)",
            "desc": "추세의 강도를 측정 (방향은 알 수 없음)",
            "ranges": [(20, "추세 약함/횡보"), (25, "추세 형성 중"), (40, "강한 추세"), (100, "매우 강한 추세")]
        },
        "volume": {
            "name": "거래량",
            "desc": "평균 대비 거래량 비율",
            "ranges": [(0.5, "거래 부진"), (1.0, "평균 수준"), (1.5, "관심 증가"), (2.0, "급등/급락 주의")]
        },
        "bb": {
            "name": "볼린저밴드 위치",
            "desc": "현재가가 밴드 내 어디에 있는지 (0%=하단, 100%=상단)",
            "ranges": [(20, "하단 근접 - 반등 기대"), (50, "중앙"), (80, "상단 근접 - 조정 가능")]
        }
    }

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

    def _call(self, prompt, max_tokens=4096):
        if not self.api_key:
            return None
        try:
            print(f"[AI] {self.provider} calling...")
            system_prompt = """미국 주식 전문 애널리스트입니다. 한국어로만 답변합니다.

중요 규칙:
1. 모든 기술적 용어는 초보자도 이해할 수 있게 쉽게 설명
2. 추천/의견을 제시할 때는 반드시 구체적인 지표 수치를 근거로 제시
3. 예시: "RSI가 28로 과매도 구간이라 반등 가능성 있음", "ADX 35로 강한 상승 추세 진행 중"
4. 지지선/저항선은 매수/매도 타이밍 판단에 활용
5. 캔들 패턴과 크로스 신호는 단기 방향성 판단에 중요"""
            
            # Z.ai용 요청 본문
            body = {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.3,
                "max_tokens": max_tokens
            }
            
            # Z.ai는 thinking 모드 비활성화 (더 빠른 응답)
            if self.provider == "zai":
                body["thinking"] = {"type": "disabled"}
            
            r = requests.post(self.base_url, headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
                json=body, timeout=300)
            if r.status_code == 200:
                c = r.json().get("choices", [{}])[0].get("message", {})
                result = c.get("content") or c.get("reasoning_content")
                if result:
                    print(f"[AI] OK len={len(result)}")
                return result
            print(f"[AI] Failed: {r.status_code} - {r.text[:200]}")
        except Exception as e:
            print(f"[AI] Error: {e}")
        return None

    def _interpret_indicator(self, name, value):
        """지표 값을 초보자가 이해할 수 있는 설명으로 변환"""
        guide = self.INDICATOR_GUIDE.get(name)
        if not guide:
            return f"{value}"
        for threshold, desc in guide["ranges"]:
            if value <= threshold:
                return f"{value:.1f} ({desc})"
        return f"{value:.1f}"

    def _fmt_stock(self, s, news_list=None):
        sc = s.get("score", {})
        ts = sc.get("total_score", 0) if isinstance(sc, dict) else 0
        gr = sc.get("grade", "C") if isinstance(sc, dict) else "C"
        
        # 기본 지표 + 해석
        rsi = s.get('rsi', 50)
        rsi_status = "과매도" if rsi < 30 else ("과매수" if rsi > 70 else "중립")
        line = f"{s['symbol']} ${s.get('price',0):.1f} | 점수{ts:.0f}({gr}) | RSI{rsi:.0f}({rsi_status})"
        
        # 스토캐스틱 + 해석
        stoch_k = s.get('stoch_k')
        if stoch_k:
            stoch_status = "과매도" if stoch_k < 20 else ("과매수" if stoch_k > 80 else "중립")
            line += f" | 스토캐스틱{stoch_k:.0f}({stoch_status})"
        
        # ADX (추세강도) + 해석
        adx = s.get('adx')
        if adx:
            adx_status = "횡보" if adx < 20 else ("추세형성" if adx < 25 else "강한추세")
            line += f" | ADX{adx:.0f}({adx_status})"
        
        # 거래량 + 해석
        vol_ratio = s.get('volume_ratio')
        if vol_ratio:
            vol_status = "부진" if vol_ratio < 0.7 else ("평균" if vol_ratio < 1.3 else "활발")
            line += f" | 거래량{vol_ratio:.1f}배({vol_status})"
        
        # 캔들 패턴 (신호 포함)
        patterns = s.get('candle_patterns', [])
        if patterns:
            pattern_str = ",".join([f"{p['pattern']}({p['signal']})" for p in patterns[:2]])
            line += f" | 캔들:{pattern_str}"
        
        # 크로스 신호 (신호 포함)
        crosses = s.get('crosses', [])
        if crosses:
            cross_str = ",".join([f"{c['type']}({c['signal']})" for c in crosses])
            line += f" | {cross_str}"
        
        # 지지/저항
        support = s.get('support', [])
        resistance = s.get('resistance', [])
        if support:
            sup_val = support[0]
            line += f" | 지지${sup_val:.1f}" if isinstance(sup_val, (int, float)) else f" | 지지${sup_val}"
        if resistance:
            res_val = resistance[0]
            line += f" | 저항${res_val:.1f}" if isinstance(res_val, (int, float)) else f" | 저항${res_val}"
        
        # 뉴스
        if news_list:
            line += f"\n  뉴스: {'/'.join([n.get('headline','')[:40] for n in news_list[:2]])}"
        
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
        patterns = data.get("candle_patterns", [])
        crosses = data.get("crosses", [])
        support = data.get("support", [])
        resistance = data.get("resistance", [])
        vol_signal = data.get("volume_signal", {})
        fib = data.get("fib_levels", {})
        
        # 뉴스 텍스트
        news_text = ""
        if news:
            news_text = "\n\n📰 최근 뉴스:\n" + "\n".join([f"- {n.get('headline','')}" for n in news[:5]])
        
        # 캔들 패턴 텍스트
        pattern_text = ""
        if patterns:
            pattern_text = "\n\n🕯️ 캔들 패턴:\n" + "\n".join([f"- {p['pattern']} ({p['signal']}): {p['desc']}" for p in patterns])
        
        # 크로스 신호 텍스트
        cross_text = ""
        if crosses:
            cross_text = "\n\n✨ 크로스 신호:\n" + "\n".join([f"- {c['type']} ({c['signal']}): {c['detail']}" for c in crosses])
        
        # 지지/저항 포맷
        support_str = ', '.join([f'${x:.2f}' for x in support[:3]]) if support else '없음'
        resistance_str = ', '.join([f'${x:.2f}' for x in resistance[:3]]) if resistance else '없음'
        
        prompt = f"""{symbol} 종목 분석

📊 기본 지표:
- 현재가: ${data.get('price',0):.2f} (전일대비 {data.get('change_5d',0):+.1f}% 5일)
- RSI: {data.get('rsi',50):.0f} (30이하=과매도/매수신호, 70이상=과매수/매도신호)
- 스토캐스틱: K{data.get('stoch_k',50):.0f}/D{data.get('stoch_d',50):.0f} (20이하=과매도, 80이상=과매수)
- MACD: {data.get('macd',0):.3f} (시그널: {data.get('macd_signal',0):.3f})
- 볼린저밴드 위치: {data.get('bb_position',50):.0f}% (0%=하단/매수, 100%=상단/매도)

📈 추세 지표:
- ADX: {data.get('adx',0):.0f} (25이상=강한추세, 20이하=횡보)
- 50일선 대비: {data.get('ma50_gap',0):+.1f}% (양수=상승추세, 음수=하락추세)
- 200일선 대비: {data.get('ma200_gap',0):+.1f}%

📉 변동성:
- ATR: {data.get('atr_pct',0):.1f}% (높을수록 변동성 큼)

📊 거래량:
- 거래량 비율: {data.get('volume_ratio',1):.1f}배 (평균 대비)
- 거래량 신호: {vol_signal.get('signal','중립')} - {vol_signal.get('desc','')}

🎯 지지/저항선:
- 지지선: {support_str}
- 저항선: {resistance_str}

📐 피보나치 되돌림:
- 38.2%: ${fib.get('0.382',0):.2f} | 50%: ${fib.get('0.5',0):.2f} | 61.8%: ${fib.get('0.618',0):.2f}
{pattern_text}{cross_text}{news_text}

🏆 종합점수: {data.get('total_score',50)}/100

위 모든 지표를 종합해서 초보자도 이해할 수 있게 분석해주세요:

1. 현재 상태 요약 (각 지표가 의미하는 바를 쉽게 설명, 3줄)
2. 캔들/차트 패턴 분석 (있다면, 2줄)
3. 거래량 분석 (1줄)
4. 뉴스 영향 (2줄)
5. 매수/매도/관망 의견 (근거와 함께, 3줄)
6. 목표가/손절가 제안 (지지/저항선 기반)
7. 주요 리스크 (1줄)"""

        r = self._call(prompt, 2500)
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
        
        # 스토캐스틱 과매도/과매수
        stoch_oversold = sum(1 for s in stocks if s.get("stoch_k",50) < 20)
        stoch_overbought = sum(1 for s in stocks if s.get("stoch_k",50) > 80)
        
        # 강한 추세 종목
        strong_trend = sum(1 for s in stocks if s.get("adx",0) > 25)
        
        # 크로스 신호 있는 종목
        cross_stocks = [(s['symbol'], s.get('crosses',[])) for s in stocks if s.get('crosses')]
        
        # 캔들 패턴 있는 종목
        pattern_stocks = [(s['symbol'], s.get('candle_patterns',[])) for s in stocks if s.get('candle_patterns')]
        
        fg = market_data.get("fear_greed",{})
        mc = market_data.get("market_condition",{})
        mn = market_data.get("market_news",[])
        
        mkt = f"""
📊 시장 현황:
- 공포탐욕지수: {fg.get('score','?')} ({fg.get('rating','?')}) - 0~25:극단공포(매수기회), 75~100:극단탐욕(매도고려)
- 시장 추세: {mc.get('message','?')}
- QQQ: ${mc.get('price',0):.2f}
"""
        if mn:
            mkt += "\n📰 시장 주요 뉴스:\n" + "\n".join([f"- {x.get('headline','')}" for x in mn[:5]])
        
        # 크로스/패턴 요약 (신호 포함)
        signal_summary = ""
        if cross_stocks:
            cross_info = [f"{s}({c[0]['type']}-{c[0]['signal']})" for s, c in cross_stocks[:10]]
            signal_summary += f"\n\n✨ 크로스 신호 발생 종목: {', '.join(cross_info)}"
        if pattern_stocks:
            pattern_info = [f"{s}({p[0]['pattern']}-{p[0]['signal']})" for s, p in pattern_stocks[:10]]
            signal_summary += f"\n🕯️ 캔들 패턴 발생 종목: {', '.join(pattern_info)}"
        
        cat_sec = "".join([self._cat_section(c, i, stocks, news_data) for c, i in categories.items()])
        
        prompt = f"""전체 시장 종합 분석 ({n}개 종목)

📊 시장 통계:
- 평균 RSI: {avg_rsi:.0f} (50 기준, 높으면 과열/낮으면 침체)
- 평균 점수: {avg_score:.0f}/100
- 등급 분포: A등급 {gd['A']}개, B등급 {gd['B']}개, C등급 {gd['C']}개, D등급 {gd['D']}개, F등급 {gd['F']}개
- RSI 과매도(RSI<30): {oversold}개 (매수 기회 가능)
- RSI 과매수(RSI>70): {overbought}개 (매도 고려)
- 스토캐스틱 과매도(<20): {stoch_oversold}개
- 스토캐스틱 과매수(>80): {stoch_overbought}개
- 강한 추세(ADX>25): {strong_trend}개
{mkt}{signal_summary}

## 카테고리별 전체 종목 데이터
(지표 해석: RSI/스토캐스틱 - 과매도=반등기대, 과매수=조정가능 | ADX - 횡보=방향없음, 강한추세=추세지속 | 거래량 - 활발=관심증가)
{cat_sec}

위 모든 데이터를 종합해서 초보자도 이해할 수 있게 한국어로 작성해주세요.
각 추천에 대해 왜 그 종목을 선택했는지 지표 근거를 쉽게 설명해주세요.

## 🏆 카테고리별 TOP 5 (각 카테고리에서 5개씩)
각 종목: 심볼 (현재가)
- 선정 이유: 어떤 지표가 좋은지 쉽게 설명 (예: "RSI 35로 과매도 구간이라 반등 기대", "골든크로스 발생으로 상승 추세 시작")

## 🌟 전체 시장 TOP 5
종목 (현재가) [카테고리]
- 추천 이유: 기술적 지표 + 뉴스 근거를 초보자도 이해하게 설명

## ⚠️ 주의 종목
위험한 종목과 그 이유 (어떤 지표가 위험 신호인지 설명)

## 📈 시장 분석
- 현재 시장 상태를 초보자도 이해하게 설명 (2줄)
- 강세/약세 섹터 (1줄)

## 💡 투자 전략
초보자를 위한 구체적인 조언 (4줄)
- 지금 사야 할지, 기다려야 할지
- 어떤 종목을 얼마나 살지
- 손절/익절 기준"""

        r = self._call(prompt, 8000)
        return {"analysis": r, "total": n, "stats": {"avg_rsi": avg_rsi, "avg_score": avg_score, "grade_dist": gd, "oversold": oversold, "overbought": overbought, "stoch_oversold": stoch_oversold, "stoch_overbought": stoch_overbought, "strong_trend": strong_trend}} if r else {"error": "AI failed"}

    def analyze_category(self, category, stocks, news_data=None):
        if not stocks:
            return {"error": "No stocks"}
        news_data = news_data or {}
        stocks = sorted(stocks, key=lambda x: -(x.get("score",{}).get("total_score",0) if isinstance(x.get("score"),dict) else 0))
        st = "\n".join([self._fmt_stock(s, news_data.get(s['symbol'])) for s in stocks])
        
        # 섹터 통계
        avg_rsi = sum(s.get("rsi",50) for s in stocks) / len(stocks)
        oversold = sum(1 for s in stocks if s.get("rsi",50) < 30)
        overbought = sum(1 for s in stocks if s.get("rsi",50) > 70)
        strong_trend = sum(1 for s in stocks if s.get("adx",0) > 25)
        
        prompt = f"""{category} 섹터 분석 ({len(stocks)}개 종목)

📊 섹터 통계:
- 평균 RSI: {avg_rsi:.0f}
- 과매도 종목: {oversold}개 (매수 기회)
- 과매수 종목: {overbought}개 (매도 고려)
- 강한 추세 종목(ADX>25): {strong_trend}개

📋 종목 데이터:
(지표 해석: RSI/스토캐스틱 - 과매도=반등기대, 과매수=조정가능 | ADX - 횡보=방향없음, 강한추세=추세지속)
{st}

초보자도 이해할 수 있게 한국어로 분석해주세요:

## 🏆 TOP 5 추천
각 종목: 심볼 (현재가)
- 이유: 어떤 지표가 좋은지 쉽게 설명 (예: "RSI 28로 과매도라 반등 기대", "ADX 35로 강한 상승 추세")

## ⚠️ 주의 종목
위험한 종목과 그 이유 (어떤 지표가 위험 신호인지)

## 💡 {category} 투자 전략 (3줄)
초보자를 위한 구체적 조언"""
        r = self._call(prompt, 3000)
        return {"analysis": r, "category": category, "total": len(stocks)} if r else {"error": "AI failed"}

    def analyze_recommendations(self, stocks, news_data=None, market_data=None):
        from config import STOCK_CATEGORIES
        return self.analyze_full_market(stocks, news_data or {}, market_data or {}, STOCK_CATEGORIES)

ai = AIAnalyzer()
