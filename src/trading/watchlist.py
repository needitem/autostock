"""Watchlist management module."""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class Watchlist:
    """Watchlist manager."""

    def __init__(self):
        self.file = os.path.join(os.path.dirname(__file__), "..", "..", "data", "watchlist.json")
        self._data = None

    def _load(self) -> dict:
        if self._data is None:
            if os.path.exists(self.file):
                try:
                    with open(self.file, "r", encoding="utf-8") as f:
                        self._data = json.load(f)
                except Exception:
                    self._data = {"stocks": {}, "settings": {"auto_buy": False}}
            else:
                self._data = {"stocks": {}, "settings": {"auto_buy": False}}
        return self._data

    def _save(self):
        os.makedirs(os.path.dirname(self.file), exist_ok=True)
        with open(self.file, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2, ensure_ascii=False)

    def add(self, symbol: str, target_price: float = 0, memo: str = "") -> dict:
        """Add symbol to watchlist."""
        from core.indicators import calculate_indicators
        from core.stock_data import get_stock_data

        df = get_stock_data(symbol)
        if df is None:
            return {"error": f"{symbol} data not found"}

        ind = calculate_indicators(df)
        if ind is None:
            return {"error": "indicator calculation failed"}

        price = ind["price"]
        if target_price <= 0:
            target_price = round(min(ind["bb_lower"], price * 0.95), 2)

        data = self._load()
        data["stocks"][symbol] = {
            "added_date": datetime.now().isoformat(),
            "added_price": price,
            "target_price": target_price,
            "memo": memo,
            "status": "watching",
        }
        self._save()

        return {
            "success": True,
            "symbol": symbol,
            "price": price,
            "target_price": target_price,
        }

    def remove(self, symbol: str) -> dict:
        """Remove symbol from watchlist."""
        data = self._load()
        if symbol in data["stocks"]:
            del data["stocks"][symbol]
            self._save()
            return {"success": True}
        return {"error": "symbol not found"}

    def get_all(self) -> dict:
        """Return full watchlist data."""
        return self._load()

    def get_status(self) -> list[dict]:
        """Return current watchlist status (including latest price)."""
        from core.signals import check_entry_signal

        data = self._load()
        result = []

        for symbol, info in data["stocks"].items():
            signal = check_entry_signal(symbol, info.get("target_price", 0))
            if "error" in signal:
                continue

            added_price = info.get("added_price", signal["price"])
            change_pct = round((signal["price"] - added_price) / added_price * 100, 1)

            result.append(
                {
                    "symbol": symbol,
                    "status": info.get("status", "watching"),
                    "price": signal["price"],
                    "added_price": added_price,
                    "target_price": info.get("target_price", 0),
                    "change_pct": change_pct,
                    "is_signal": signal["is_signal"],
                    "strength": signal["strength"],
                    "rsi": signal["rsi"],
                    "bb_position": signal["bb_position"],
                    "met_count": signal["met_count"],
                    "memo": info.get("memo", ""),
                }
            )

        return result

    def scan_signals(self) -> list[dict]:
        """Scan watchlist for dip-entry signals."""
        status = self.get_status()
        return [s for s in status if s["is_signal"]]

    def set_auto_buy(self, enabled: bool):
        """Set auto-buy option."""
        data = self._load()
        data["settings"]["auto_buy"] = enabled
        self._save()

    def is_auto_buy(self) -> bool:
        """Return whether auto-buy is enabled."""
        return self._load()["settings"].get("auto_buy", False)

    def mark_bought(self, symbol: str, price: float, qty: int):
        """Mark a symbol as bought."""
        data = self._load()
        if symbol in data["stocks"]:
            data["stocks"][symbol]["status"] = "bought"
            data["stocks"][symbol]["bought_price"] = price
            data["stocks"][symbol]["bought_qty"] = qty
            data["stocks"][symbol]["bought_date"] = datetime.now().isoformat()
            self._save()


# Singleton instance.
watchlist = Watchlist()
