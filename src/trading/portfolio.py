"""
Portfolio management module.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.signals import check_exit_signal
from trading.kis_api import kis
from trading.watchlist import watchlist


class Portfolio:
    """Portfolio manager."""

    def __init__(self):
        self.kis = kis
        self.watchlist = watchlist

    def get_status(self) -> dict:
        """Return holdings + exit signal status."""
        balance = self.kis.get_balance()
        if "error" in balance:
            return balance

        holdings = balance.get("holdings", [])

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
        """Auto-buy by watchlist signals."""
        if not self.watchlist.is_auto_buy():
            return [{"error": "자동매수 비활성화"}]

        balance = self.kis.get_balance()
        if "error" in balance:
            return [{"error": balance["error"]}]

        available = balance.get("available_cash", 0)
        if available < 50:
            return [{"error": f"잔고 부족 ${available:.2f}"}]

        signals = self.watchlist.scan_signals()
        if not signals:
            return [{"message": "매수 신호 없음"}]

        plan = self._get_buy_plan(signals, available, max_amount)

        results = []
        for order in plan:
            symbol = order["symbol"]
            amount = order["amount"]
            price = order["price"]

            qty = int(amount / price) if price > 0 else 0
            if qty < 1:
                continue

            buy_price = round(price * 1.005, 2)
            result = self.kis.buy(symbol, qty, buy_price)

            if result.get("success"):
                self.watchlist.mark_bought(symbol, buy_price, qty)
                result["amount"] = round(qty * buy_price, 2)
                result["reason"] = order.get("reason", "")

            results.append(result)

        return results

    def auto_sell_losers(self, threshold: float = -7) -> list[dict]:
        """Auto sell losing positions under threshold."""
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

            sell_price = round(price_info["price"] * 0.995, 2)
            result = self.kis.sell(h["symbol"], h["qty"], sell_price)
            result["reason"] = f"손절 ({h['pnl_pct']:.1f}%)"
            results.append(result)

        return results

    def _get_buy_plan(self, signals: list, available: float, max_per_stock: float) -> list[dict]:
        """Create buy plan using scan trade-plan first, then fallback to signal rule."""
        if not signals:
            return []

        ranked = []
        try:
            from core.signals import scan_stocks

            symbols = [str(s.get("symbol", "")).strip().upper() for s in signals if s.get("symbol")]
            symbols = [s for s in symbols if s]
            if symbols:
                scan = scan_stocks(symbols, fundamental_limit=len(symbols))
                ranked = sorted(
                    scan.get("results", []),
                    key=lambda x: -x.get(
                        "investability_score",
                        x.get("quality_score", x.get("score", {}).get("total_score", 0)),
                    ),
                )
        except Exception:
            ranked = []

        signal_by_symbol = {str(s.get("symbol", "")).upper(): s for s in signals}
        max_total = available * 0.5
        remaining = max_total
        orders: list[dict] = []

        if ranked:
            for row in ranked[:5]:
                if remaining < 50:
                    break

                symbol = str(row.get("symbol", "")).upper()
                if not symbol:
                    continue

                plan = row.get("trade_plan", {})
                if not plan.get("tradeable", False):
                    continue

                position_pct = float(plan.get("execution", {}).get("position_pct", row.get("position_size_pct", 0)) or 0)
                if position_pct <= 0:
                    continue

                desired = available * (position_pct / 100.0)
                amount = min(max_per_stock, desired, remaining)
                if amount < 50:
                    continue

                rr2 = float(plan.get("risk_reward", {}).get("rr2", 0) or 0)
                stage = plan.get("positioning", {}).get("stage", "")
                liq = float(row.get("liquidity_score", 0) or 0)
                signal = signal_by_symbol.get(symbol, {})
                strength = signal.get("strength", "보통")

                orders.append(
                    {
                        "symbol": symbol,
                        "amount": round(amount, 2),
                        "price": float(row.get("price", signal.get("price", 0)) or 0),
                        "reason": f"trade_plan({strength}) stage={stage} rr2={rr2:.2f} liq={liq:.0f}",
                    }
                )
                remaining -= amount
                if len(orders) >= 3:
                    break

        if orders:
            return orders

        prioritized = sorted(
            signals,
            key=lambda s: (1 if s.get("strength") == "강함" else 0, -abs(float(s.get("rsi", 50) - 30))),
            reverse=True,
        )
        per_stock = min(max_per_stock, max_total / len(prioritized)) if prioritized else 0

        fallback: list[dict] = []
        for s in prioritized[:3]:
            weight = 1.0 if s.get("strength") == "강함" else 0.7
            if s.get("rsi", 50) <= 30:
                weight += 0.15
            amount = max(50, min(per_stock * weight, max_per_stock))
            fallback.append(
                {
                    "symbol": s["symbol"],
                    "amount": round(amount, 2),
                    "price": s["price"],
                    "reason": f"신호기반({s.get('strength', '보통')}), RSI {s.get('rsi', 50)}",
                }
            )

        return fallback


portfolio = Portfolio()
