# -*- coding: utf-8 -*-
"""
관심종목 실시간 모니터링 모듈
30분마다 체크하여 중요 변화 시 알림
"""
import os
import sys
import json
from datetime import datetime
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class StockMonitor:
    """관심종목 모니터링"""
    
    # 알림 조건 설정
    ALERT_CONDITIONS = {
        "price_change_pct": 3.0,      # 가격 ±3% 변동
        "rsi_oversold": 30,            # RSI 과매도
        "rsi_overbought": 70,          # RSI 과매수
        "stoch_oversold": 20,          # 스토캐스틱 과매도
        "stoch_overbought": 80,        # 스토캐스틱 과매수
        "adx_strong_trend": 25,        # 강한 추세
        "volume_spike": 2.0,           # 거래량 2배 이상
    }
    
    def __init__(self):
        self.cache_file = os.path.join(os.path.dirname(__file__), "..", "..", "data", "monitor_cache.json")
        self._cache = None
    
    def _load_cache(self) -> dict:
        """이전 상태 캐시 로드"""
        if self._cache is None:
            if os.path.exists(self.cache_file):
                try:
                    with open(self.cache_file, "r", encoding="utf-8") as f:
                        self._cache = json.load(f)
                except:
                    self._cache = {"stocks": {}, "last_check": None}
            else:
                self._cache = {"stocks": {}, "last_check": None}
        return self._cache
    
    def _save_cache(self):
        """캐시 저장"""
        os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
        with open(self.cache_file, "w", encoding="utf-8") as f:
            json.dump(self._cache, f, indent=2, ensure_ascii=False)
    
    def check_stock(self, symbol: str) -> dict:
        """단일 종목 체크 - 현재 상태와 알림 조건"""
        from core.indicators import get_full_analysis
        
        analysis = get_full_analysis(symbol)
        if analysis is None:
            return {"error": f"{symbol} 데이터 없음"}
        
        cache = self._load_cache()
        prev = cache["stocks"].get(symbol, {})
        
        alerts = []
        current = {
            "price": analysis.get("price", 0),
            "rsi": analysis.get("rsi", 50),
            "stoch_k": analysis.get("stoch_k", 50),
            "adx": analysis.get("adx", 0),
            "volume_ratio": analysis.get("volume_ratio", 1),
            "support": analysis.get("support", []),
            "resistance": analysis.get("resistance", []),
            "candle_patterns": analysis.get("candle_patterns", []),
            "crosses": analysis.get("crosses", []),
            "ma50_gap": analysis.get("ma50_gap", 0),
            "bb_position": analysis.get("bb_position", 50),
            "checked_at": datetime.now().isoformat(),
        }
        
        # 1. 가격 변동 체크
        if prev.get("price"):
            change_pct = (current["price"] - prev["price"]) / prev["price"] * 100
            if abs(change_pct) >= self.ALERT_CONDITIONS["price_change_pct"]:
                direction = "📈 급등" if change_pct > 0 else "📉 급락"
                alerts.append({
                    "type": "price_change",
                    "emoji": "🚨",
                    "title": direction,
                    "detail": f"{change_pct:+.1f}% (${prev['price']:.2f} → ${current['price']:.2f})",
                    "signal": "매도고려" if change_pct > 0 else "매수기회",
                    "priority": "high"
                })
        
        # 2. RSI 과매도 진입
        if current["rsi"] <= self.ALERT_CONDITIONS["rsi_oversold"]:
            if not prev.get("rsi") or prev["rsi"] > self.ALERT_CONDITIONS["rsi_oversold"]:
                alerts.append({
                    "type": "rsi_oversold",
                    "emoji": "💰",
                    "title": "RSI 과매도 진입",
                    "detail": f"RSI {current['rsi']:.0f} (30 이하 = 반등 기대)",
                    "signal": "매수기회",
                    "priority": "high"
                })
        
        # 3. RSI 과매수 진입
        if current["rsi"] >= self.ALERT_CONDITIONS["rsi_overbought"]:
            if not prev.get("rsi") or prev["rsi"] < self.ALERT_CONDITIONS["rsi_overbought"]:
                alerts.append({
                    "type": "rsi_overbought",
                    "emoji": "⚠️",
                    "title": "RSI 과매수 진입",
                    "detail": f"RSI {current['rsi']:.0f} (70 이상 = 조정 가능)",
                    "signal": "매도고려",
                    "priority": "medium"
                })
        
        # 4. 스토캐스틱 과매도
        if current["stoch_k"] <= self.ALERT_CONDITIONS["stoch_oversold"]:
            if not prev.get("stoch_k") or prev["stoch_k"] > self.ALERT_CONDITIONS["stoch_oversold"]:
                alerts.append({
                    "type": "stoch_oversold",
                    "emoji": "💰",
                    "title": "스토캐스틱 과매도",
                    "detail": f"K={current['stoch_k']:.0f} (20 이하 = 단기 반등 기대)",
                    "signal": "매수기회",
                    "priority": "medium"
                })
        
        # 5. 스토캐스틱 과매수
        if current["stoch_k"] >= self.ALERT_CONDITIONS["stoch_overbought"]:
            if not prev.get("stoch_k") or prev["stoch_k"] < self.ALERT_CONDITIONS["stoch_overbought"]:
                alerts.append({
                    "type": "stoch_overbought",
                    "emoji": "⚠️",
                    "title": "스토캐스틱 과매수",
                    "detail": f"K={current['stoch_k']:.0f} (80 이상 = 단기 조정 가능)",
                    "signal": "매도고려",
                    "priority": "medium"
                })
        
        # 6. 거래량 급증
        if current["volume_ratio"] >= self.ALERT_CONDITIONS["volume_spike"]:
            if not prev.get("volume_ratio") or prev["volume_ratio"] < self.ALERT_CONDITIONS["volume_spike"]:
                alerts.append({
                    "type": "volume_spike",
                    "emoji": "📊",
                    "title": "거래량 급증",
                    "detail": f"평균 대비 {current['volume_ratio']:.1f}배 (관심 집중)",
                    "signal": "주목",
                    "priority": "medium"
                })
        
        # 7. 지지선 돌파 (하락)
        if prev.get("price") and current["support"]:
            nearest_support = current["support"][0]
            if prev["price"] > nearest_support and current["price"] <= nearest_support:
                alerts.append({
                    "type": "support_break",
                    "emoji": "🔻",
                    "title": "지지선 하향 돌파",
                    "detail": f"${nearest_support:.2f} 지지선 붕괴 (추가 하락 주의)",
                    "signal": "손절고려",
                    "priority": "high"
                })
        
        # 8. 저항선 돌파 (상승)
        if prev.get("price") and current["resistance"]:
            nearest_resistance = current["resistance"][0]
            if prev["price"] < nearest_resistance and current["price"] >= nearest_resistance:
                alerts.append({
                    "type": "resistance_break",
                    "emoji": "🚀",
                    "title": "저항선 상향 돌파",
                    "detail": f"${nearest_resistance:.2f} 저항선 돌파 (추가 상승 기대)",
                    "signal": "보유/추매",
                    "priority": "high"
                })
        
        # 9. 캔들 패턴 발생
        if current["candle_patterns"]:
            for pattern in current["candle_patterns"]:
                alerts.append({
                    "type": "candle_pattern",
                    "emoji": "🕯️",
                    "title": f"캔들 패턴: {pattern['pattern']}",
                    "detail": pattern.get("desc", ""),
                    "signal": pattern.get("signal", "중립"),
                    "priority": "low"
                })
        
        # 10. 크로스 신호 발생
        if current["crosses"]:
            for cross in current["crosses"]:
                priority = "high" if "골든" in cross["type"] or "데드" in cross["type"] else "medium"
                alerts.append({
                    "type": "cross_signal",
                    "emoji": "✨",
                    "title": cross["type"],
                    "detail": cross.get("detail", ""),
                    "signal": cross.get("signal", "중립"),
                    "priority": priority
                })
        
        # 캐시 업데이트
        cache["stocks"][symbol] = current
        cache["last_check"] = datetime.now().isoformat()
        self._save_cache()
        
        return {
            "symbol": symbol,
            "current": current,
            "alerts": alerts,
            "has_alerts": len(alerts) > 0,
        }
    
    def check_all_watchlist(self) -> list[dict]:
        """관심종목 전체 체크"""
        from trading.watchlist import watchlist
        
        data = watchlist.get_all()
        results = []
        
        for symbol in data["stocks"].keys():
            result = self.check_stock(symbol)
            if result.get("has_alerts"):
                results.append(result)
        
        return results
    
    def format_alert_message(self, results: list[dict]) -> str:
        """알림 메시지 포맷"""
        if not results:
            return ""
        
        text = "🔔 <b>관심종목 알림</b>\n"
        text += f"━━━━━━━━━━━━━━━━━━\n\n"
        
        for result in results:
            symbol = result["symbol"]
            current = result["current"]
            alerts = result["alerts"]
            
            # 우선순위 높은 것 먼저
            alerts.sort(key=lambda x: {"high": 0, "medium": 1, "low": 2}.get(x["priority"], 3))
            
            text += f"<b>{symbol}</b> ${current['price']:.2f}\n"
            
            for alert in alerts[:3]:  # 최대 3개 알림
                text += f"  {alert['emoji']} {alert['title']}\n"
                text += f"     {alert['detail']}\n"
                text += f"     → <b>{alert['signal']}</b>\n"
            
            text += "\n"
        
        text += f"⏰ {datetime.now().strftime('%H:%M')} 기준"
        return text
    
    def get_summary(self, symbol: str) -> str:
        """종목 현재 상태 요약"""
        from core.indicators import get_full_analysis
        
        analysis = get_full_analysis(symbol)
        if analysis is None:
            return f"❌ {symbol} 데이터 없음"
        
        rsi = analysis.get("rsi", 50)
        stoch_k = analysis.get("stoch_k", 50)
        adx = analysis.get("adx", 0)
        
        # RSI 해석
        rsi_status = "과매도🟢" if rsi < 30 else ("과매수🔴" if rsi > 70 else "중립⚪")
        
        # 스토캐스틱 해석
        stoch_status = "과매도🟢" if stoch_k < 20 else ("과매수🔴" if stoch_k > 80 else "중립⚪")
        
        # ADX 해석
        adx_status = "강한추세📈" if adx > 25 else "횡보↔️"
        
        text = f"<b>{symbol}</b> ${analysis.get('price', 0):.2f}\n"
        text += f"├ RSI: {rsi:.0f} ({rsi_status})\n"
        text += f"├ 스토캐스틱: {stoch_k:.0f} ({stoch_status})\n"
        text += f"├ ADX: {adx:.0f} ({adx_status})\n"
        text += f"├ 거래량: {analysis.get('volume_ratio', 1):.1f}배\n"
        
        support = analysis.get("support", [])
        resistance = analysis.get("resistance", [])
        if support:
            text += f"├ 지지선: ${support[0]:.2f}\n"
        if resistance:
            text += f"└ 저항선: ${resistance[0]:.2f}\n"
        
        return text


# 싱글톤
monitor = StockMonitor()
