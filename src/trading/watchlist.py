"""
관심종목 관리 모듈
"""
import os
import sys
import json
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class Watchlist:
    """관심종목 관리"""
    
    def __init__(self):
        self.file = os.path.join(os.path.dirname(__file__), "..", "..", "data", "watchlist.json")
        self._data = None
    
    def _load(self) -> dict:
        if self._data is None:
            if os.path.exists(self.file):
                try:
                    with open(self.file, "r", encoding="utf-8") as f:
                        self._data = json.load(f)
                except:
                    self._data = {"stocks": {}, "settings": {"auto_buy": False}}
            else:
                self._data = {"stocks": {}, "settings": {"auto_buy": False}}
        return self._data
    
    def _save(self):
        os.makedirs(os.path.dirname(self.file), exist_ok=True)
        with open(self.file, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2, ensure_ascii=False)
    
    def add(self, symbol: str, target_price: float = 0, memo: str = "") -> dict:
        """관심종목 추가"""
        from core.stock_data import get_stock_data
        from core.indicators import calculate_indicators
        
        df = get_stock_data(symbol)
        if df is None:
            return {"error": f"{symbol} 데이터 없음"}
        
        ind = calculate_indicators(df)
        if ind is None:
            return {"error": "지표 계산 실패"}
        
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
        """관심종목 제거"""
        data = self._load()
        if symbol in data["stocks"]:
            del data["stocks"][symbol]
            self._save()
            return {"success": True}
        return {"error": "종목 없음"}
    
    def get_all(self) -> dict:
        """전체 목록"""
        return self._load()
    
    def get_status(self) -> list[dict]:
        """현재 상태 (현재가 포함)"""
        from core.signals import check_entry_signal
        
        data = self._load()
        result = []
        
        for symbol, info in data["stocks"].items():
            signal = check_entry_signal(symbol, info.get("target_price", 0))
            if "error" in signal:
                continue
            
            result.append({
                "symbol": symbol,
                "status": info.get("status", "watching"),
                "price": signal["price"],
                "added_price": info.get("added_price", 0),
                "target_price": info.get("target_price", 0),
                "change_pct": round((signal["price"] - info.get("added_price", signal["price"])) / info.get("added_price", signal["price"]) * 100, 1),
                "is_signal": signal["is_signal"],
                "strength": signal["strength"],
                "rsi": signal["rsi"],
                "bb_position": signal["bb_position"],
                "met_count": signal["met_count"],
                "memo": info.get("memo", ""),
            })
        
        return result
    
    def scan_signals(self) -> list[dict]:
        """저점 신호 스캔"""
        status = self.get_status()
        return [s for s in status if s["is_signal"]]
    
    def set_auto_buy(self, enabled: bool):
        """자동매수 설정"""
        data = self._load()
        data["settings"]["auto_buy"] = enabled
        self._save()
    
    def is_auto_buy(self) -> bool:
        """자동매수 여부"""
        return self._load()["settings"].get("auto_buy", False)
    
    def mark_bought(self, symbol: str, price: float, qty: int):
        """매수 완료 표시"""
        data = self._load()
        if symbol in data["stocks"]:
            data["stocks"][symbol]["status"] = "bought"
            data["stocks"][symbol]["bought_price"] = price
            data["stocks"][symbol]["bought_qty"] = qty
            data["stocks"][symbol]["bought_date"] = datetime.now().isoformat()
            self._save()


# 싱글톤
watchlist = Watchlist()
