"""
포트폴리오 관리 모듈
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from trading.kis_api import kis
from trading.watchlist import watchlist
from core.signals import check_exit_signal


class Portfolio:
    """포트폴리오 관리"""
    
    def __init__(self):
        self.kis = kis
        self.watchlist = watchlist
    
    def get_status(self) -> dict:
        """포트폴리오 현황"""
        balance = self.kis.get_balance()
        if "error" in balance:
            return balance
        
        holdings = balance.get("holdings", [])
        
        # 청산 신호 체크
        for h in holdings:
            exit_signal = check_exit_signal(h["symbol"], h["avg_price"])
            if "error" not in exit_signal:
                h["exit_signal"] = exit_signal["is_exit"]
                h["exit_reason"] = exit_signal["reason"]
                h["exit_urgency"] = exit_signal["urgency"]
            else:
                h["exit_signal"] = False
                h["exit_reason"] = ""
                h["exit_urgency"] = ""
        
        return {
            "holdings": holdings,
            "available_cash": balance.get("available_cash", 0),
            "total_eval": balance.get("total_eval", 0),
            "exit_candidates": [h for h in holdings if h.get("exit_signal")],
        }
    
    def auto_buy_signals(self, max_amount: float = 200) -> list[dict]:
        """저점 신호 자동매수"""
        if not self.watchlist.is_auto_buy():
            return [{"error": "자동매수 비활성화"}]
        
        balance = self.kis.get_balance()
        if "error" in balance:
            return [{"error": balance["error"]}]
        
        available = balance.get("available_cash", 0)
        if available < 50:
            return [{"error": f"잔고 부족: ${available:.2f}"}]
        
        signals = self.watchlist.scan_signals()
        if not signals:
            return [{"message": "매수 신호 없음"}]
        
        # AI 매수 계획
        plan = self._get_buy_plan(signals, available, max_amount)
        
        results = []
        for order in plan:
            symbol = order["symbol"]
            amount = order["amount"]
            price = order["price"]
            
            qty = int(amount / price)
            if qty < 1:
                continue
            
            buy_price = round(price * 1.005, 2)  # 슬리피지
            result = self.kis.buy(symbol, qty, buy_price)
            
            if result.get("success"):
                self.watchlist.mark_bought(symbol, buy_price, qty)
                result["amount"] = round(qty * buy_price, 2)
                result["reason"] = order.get("reason", "")
            
            results.append(result)
        
        return results
    
    def auto_sell_losers(self, threshold: float = -7) -> list[dict]:
        """손절 자동매도"""
        balance = self.kis.get_balance()
        if "error" in balance:
            return [{"error": balance["error"]}]
        
        holdings = balance.get("holdings", [])
        losers = [h for h in holdings if h.get("pnl_pct", 0) <= threshold]
        
        if not losers:
            return [{"message": f"손절 대상 없음 ({threshold}% 이하)"}]
        
        results = []
        for h in losers:
            price_info = self.kis.get_price(h["symbol"])
            if not price_info:
                continue
            
            sell_price = round(price_info["price"] * 0.995, 2)  # 슬리피지
            result = self.kis.sell(h["symbol"], h["qty"], sell_price)
            result["reason"] = f"손절 ({h['pnl_pct']:.1f}%)"
            results.append(result)
        
        return results
    
    def _get_buy_plan(self, signals: list, available: float, max_per_stock: float) -> list[dict]:
        """매수 계획 (AI 또는 기본 로직)"""
        import os
        import requests
        import re
        import json
        
        api_key = os.getenv("OPENROUTER_API_KEY")
        
        if api_key:
            # AI 매수 계획
            try:
                signal_text = "\n".join([
                    f"{s['symbol']}: ${s['price']}, RSI:{s['rsi']}, 강도:{s['strength']}"
                    for s in signals[:5]
                ])
                
                prompt = f"""저점 매수 신호 종목의 매수 계획을 세워주세요.

가용 자금: ${available:.2f}

신호 종목:
{signal_text}

규칙:
- 총 매수금액은 가용자금의 50% 이내
- 종목당 최소 $50, 최대 ${max_per_stock}
- 신호 강도가 "강함"이면 더 많이 배분
- 최대 3종목

JSON 형식으로만 답변:
{{"orders": [{{"symbol": "NVDA", "amount": 200, "reason": "RSI 과매도"}}]}}"""

                response = requests.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers={"Authorization": f"Bearer {api_key}"},
                    json={
                        "model": "deepseek/deepseek-chat:free",
                        "messages": [{"role": "user", "content": prompt}],
                        "max_tokens": 500
                    },
                    timeout=30
                )
                
                if response.status_code == 200:
                    content = response.json()["choices"][0]["message"]["content"]
                    match = re.search(r'\{.*\}', content, re.DOTALL)
                    if match:
                        plan = json.loads(match.group())
                        for order in plan.get("orders", []):
                            sig = next((s for s in signals if s["symbol"] == order["symbol"]), None)
                            if sig:
                                order["price"] = sig["price"]
                        return plan.get("orders", [])
            except:
                pass
        
        # 기본 로직
        max_total = available * 0.5
        per_stock = min(max_per_stock, max_total / len(signals)) if signals else 0
        
        orders = []
        for s in signals[:3]:
            amount = per_stock if s["strength"] == "강함" else per_stock * 0.7
            amount = max(50, min(amount, max_per_stock))
            orders.append({
                "symbol": s["symbol"],
                "amount": round(amount, 2),
                "price": s["price"],
                "reason": f"신호 {s['strength']}, RSI {s['rsi']}"
            })
        
        return orders


# 싱글톤
portfolio = Portfolio()
