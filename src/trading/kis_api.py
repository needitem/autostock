"""
한국투자증권 해외주식 API
"""
import os
import json
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()


class KISApi:
    """한국투자증권 API 클래스"""
    
    def __init__(self):
        self.app_key = os.getenv("KIS_APP_KEY", "")
        self.app_secret = os.getenv("KIS_APP_SECRET", "")
        self.account_no = os.getenv("KIS_ACCOUNT_NO", "")
        self.account_prod = os.getenv("KIS_ACCOUNT_PROD", "01")
        self.is_paper = os.getenv("KIS_IS_PAPER", "true").lower() == "true"
        
        self.base_url = (
            "https://openapivts.koreainvestment.com:29443" if self.is_paper 
            else "https://openapi.koreainvestment.com:9443"
        )
        self.token_file = os.path.join(os.path.dirname(__file__), "..", "..", "data", "kis_token.json")
        self._token = None
    
    def _get_token(self) -> str | None:
        """토큰 발급/캐시"""
        if not self.app_key or not self.app_secret:
            return None
        
        # 캐시 확인
        if os.path.exists(self.token_file):
            try:
                with open(self.token_file, "r") as f:
                    cache = json.load(f)
                    if datetime.now() < datetime.fromisoformat(cache["expires_at"]):
                        return cache["access_token"]
            except:
                pass
        
        # 새 토큰 발급
        try:
            response = requests.post(
                f"{self.base_url}/oauth2/tokenP",
                headers={"Content-Type": "application/json"},
                json={
                    "grant_type": "client_credentials",
                    "appkey": self.app_key,
                    "appsecret": self.app_secret
                },
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                token = data["access_token"]
                expires = datetime.now() + timedelta(hours=23)
                
                os.makedirs(os.path.dirname(self.token_file), exist_ok=True)
                with open(self.token_file, "w") as f:
                    json.dump({"access_token": token, "expires_at": expires.isoformat()}, f)
                
                return token
        except Exception as e:
            print(f"토큰 발급 실패: {e}")
        
        return None
    
    def _headers(self, tr_id: str, hashkey: str = "") -> dict:
        """API 헤더"""
        token = self._get_token()
        if not token:
            return {}
        
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "authorization": f"Bearer {token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": tr_id,
            "custtype": "P"
        }
        if hashkey:
            headers["hashkey"] = hashkey
        return headers
    
    def _hashkey(self, data: dict) -> str:
        """해시키 생성"""
        try:
            response = requests.post(
                f"{self.base_url}/uapi/hashkey",
                headers={"Content-Type": "application/json", "appkey": self.app_key, "appsecret": self.app_secret},
                json=data,
                timeout=10
            )
            return response.json().get("HASH", "") if response.status_code == 200 else ""
        except:
            return ""
    
    def check_status(self) -> dict:
        """API 상태 확인"""
        if not self.app_key or not self.app_secret:
            return {"connected": False, "error": "API 키 미설정"}
        
        token = self._get_token()
        if token:
            return {
                "connected": True,
                "is_paper": self.is_paper,
                "account": f"{self.account_no}-{self.account_prod}" if self.account_no else "미설정",
                "mode": "모의투자" if self.is_paper else "실전투자"
            }
        return {"connected": False, "error": "토큰 발급 실패"}
    
    def get_price(self, symbol: str, exchange: str = "NAS") -> dict | None:
        """현재가 조회"""
        headers = self._headers("HHDFS00000300")
        if not headers:
            return None
        
        try:
            response = requests.get(
                f"{self.base_url}/uapi/overseas-price/v1/quotations/price",
                headers=headers,
                params={"AUTH": "", "EXCD": exchange, "SYMB": symbol},
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get("rt_cd") == "0":
                    out = data.get("output", {})
                    return {
                        "symbol": symbol,
                        "price": float(out.get("last", 0)),
                        "change": float(out.get("diff", 0)),
                        "change_pct": float(out.get("rate", 0)),
                        "volume": int(out.get("tvol", 0)),
                    }
        except:
            pass
        return None
    
    def get_balance(self) -> dict:
        """잔고 조회"""
        tr_id = "VTTS3012R" if self.is_paper else "TTTS3012R"
        headers = self._headers(tr_id)
        if not headers:
            return {"error": "인증 실패"}
        
        try:
            response = requests.get(
                f"{self.base_url}/uapi/overseas-stock/v1/trading/inquire-balance",
                headers=headers,
                params={
                    "CANO": self.account_no,
                    "ACNT_PRDT_CD": self.account_prod,
                    "OVRS_EXCG_CD": "NASD",
                    "TR_CRCY_CD": "USD",
                    "CTX_AREA_FK200": "",
                    "CTX_AREA_NK200": ""
                },
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get("rt_cd") == "0":
                    holdings = []
                    for item in data.get("output1", []):
                        qty = float(item.get("ovrs_cblc_qty", 0))
                        if qty > 0:
                            holdings.append({
                                "symbol": item.get("ovrs_pdno"),
                                "name": item.get("ovrs_item_name"),
                                "qty": int(qty),
                                "avg_price": float(item.get("pchs_avg_pric", 0)),
                                "eval_amt": float(item.get("ovrs_stck_evlu_amt", 0)),
                                "pnl": float(item.get("frcr_evlu_pfls_amt", 0)),
                                "pnl_pct": float(item.get("evlu_pfls_rt", 0)),
                            })
                    
                    out2 = data.get("output2", {})
                    return {
                        "holdings": holdings,
                        "available_cash": float(out2.get("frcr_ord_psbl_amt1", 0)) if out2 else 0,
                        "total_eval": float(out2.get("tot_evlu_pfls_amt", 0)) if out2 else 0,
                    }
                return {"error": data.get("msg1", "조회 실패")}
        except Exception as e:
            return {"error": str(e)}
        
        return {"error": "API 오류"}
    
    def buy(self, symbol: str, qty: int, price: float, exchange: str = "NASD") -> dict:
        """매수 주문"""
        tr_id = "VTTT1002U" if self.is_paper else "TTTT1002U"
        
        body = {
            "CANO": self.account_no,
            "ACNT_PRDT_CD": self.account_prod,
            "OVRS_EXCG_CD": exchange,
            "PDNO": symbol,
            "ORD_QTY": str(qty),
            "OVRS_ORD_UNPR": str(price),
            "ORD_SVR_DVSN_CD": "0",
            "ORD_DVSN": "00"
        }
        
        headers = self._headers(tr_id, self._hashkey(body))
        if not headers:
            return {"error": "인증 실패"}
        
        try:
            response = requests.post(
                f"{self.base_url}/uapi/overseas-stock/v1/trading/order",
                headers=headers,
                json=body,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get("rt_cd") == "0":
                    return {
                        "success": True,
                        "order_no": data.get("output", {}).get("ODNO"),
                        "symbol": symbol,
                        "qty": qty,
                        "price": price,
                    }
                return {"error": data.get("msg1", "주문 실패")}
        except Exception as e:
            return {"error": str(e)}
        
        return {"error": "API 오류"}
    
    def sell(self, symbol: str, qty: int, price: float, exchange: str = "NASD") -> dict:
        """매도 주문"""
        tr_id = "VTTT1001U" if self.is_paper else "TTTT1006U"
        
        body = {
            "CANO": self.account_no,
            "ACNT_PRDT_CD": self.account_prod,
            "OVRS_EXCG_CD": exchange,
            "PDNO": symbol,
            "ORD_QTY": str(qty),
            "OVRS_ORD_UNPR": str(price),
            "ORD_SVR_DVSN_CD": "0",
            "ORD_DVSN": "00"
        }
        
        headers = self._headers(tr_id, self._hashkey(body))
        if not headers:
            return {"error": "인증 실패"}
        
        try:
            response = requests.post(
                f"{self.base_url}/uapi/overseas-stock/v1/trading/order",
                headers=headers,
                json=body,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get("rt_cd") == "0":
                    return {
                        "success": True,
                        "order_no": data.get("output", {}).get("ODNO"),
                        "symbol": symbol,
                        "qty": qty,
                        "price": price,
                    }
                return {"error": data.get("msg1", "주문 실패")}
        except Exception as e:
            return {"error": str(e)}
        
        return {"error": "API 오류"}
    
    def get_orders(self) -> dict:
        """미체결 주문 조회"""
        tr_id = "VTTS3018R" if self.is_paper else "TTTS3018R"
        headers = self._headers(tr_id)
        if not headers:
            return {"error": "인증 실패"}
        
        try:
            response = requests.get(
                f"{self.base_url}/uapi/overseas-stock/v1/trading/inquire-nccs",
                headers=headers,
                params={
                    "CANO": self.account_no,
                    "ACNT_PRDT_CD": self.account_prod,
                    "OVRS_EXCG_CD": "NASD",
                    "SORT_SQN": "DS",
                    "CTX_AREA_FK200": "",
                    "CTX_AREA_NK200": ""
                },
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get("rt_cd") == "0":
                    orders = []
                    for item in data.get("output", []):
                        orders.append({
                            "order_no": item.get("odno"),
                            "symbol": item.get("pdno"),
                            "side": "매수" if item.get("sll_buy_dvsn_cd") == "02" else "매도",
                            "qty": int(item.get("ft_ord_qty", 0)),
                            "filled": int(item.get("ft_ccld_qty", 0)),
                            "price": float(item.get("ft_ord_unpr3", 0)),
                        })
                    return {"orders": orders}
        except Exception as e:
            return {"error": str(e)}
        
        return {"error": "API 오류"}


# 싱글톤 인스턴스
kis = KISApi()
