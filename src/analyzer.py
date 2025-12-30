from concurrent.futures import ThreadPoolExecutor, as_completed
from data_fetcher import get_stock_data, check_market_condition
from strategies import add_all_indicators, ALL_STRATEGIES, analyze_risk_level
from config import NASDAQ_100


def analyze_stock_all_strategies(symbol: str) -> list[dict]:
    """개별 종목에 모든 전략 적용"""
    df = get_stock_data(symbol)
    if df is None:
        return []
    
    df = add_all_indicators(df)
    if df is None:
        return []
    
    results = []
    for emoji, name, strategy_func in ALL_STRATEGIES:
        try:
            result = strategy_func(df, symbol)
            if result:
                result["emoji"] = emoji
                # 위험도 분석 추가
                risk = analyze_risk_level(df, symbol)
                result["risk_grade"] = risk["risk_grade"]
                result["risk_score"] = risk["risk_score"]
                results.append(result)
        except Exception as e:
            pass  # 개별 전략 실패는 무시
    
    return results


def analyze_single_stock(symbol: str) -> dict | None:
    """단일 종목 상세 분석"""
    df = get_stock_data(symbol)
    if df is None:
        return None
    
    df = add_all_indicators(df)
    if df is None:
        return None
    
    # 위험도 분석
    risk = analyze_risk_level(df, symbol)
    
    # 적용되는 전략들
    strategies_matched = []
    for emoji, name, strategy_func in ALL_STRATEGIES:
        try:
            result = strategy_func(df, symbol)
            if result:
                strategies_matched.append(f"{emoji} {name}")
        except:
            pass
    
    risk["strategies_matched"] = strategies_matched
    return risk


def scan_all_stocks() -> dict:
    """나스닥 100 전체 스캔 (병렬 처리)"""
    market = check_market_condition()
    
    # 전략별 결과 저장
    strategy_results = {name: [] for _, name, _ in ALL_STRATEGIES}
    total_scanned = 0
    
    # 병렬로 종목 분석 (10개씩 동시 처리)
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(analyze_stock_all_strategies, symbol): symbol for symbol in NASDAQ_100}
        
        for future in as_completed(futures):
            results = future.result()
            if results:
                total_scanned += 1
                for result in results:
                    strategy_name = result["strategy"]
                    for _, name, _ in ALL_STRATEGIES:
                        if name in strategy_name or strategy_name in name:
                            strategy_results[name].append(result)
                            break
            elif results == []:
                total_scanned += 1
    
    return {
        "market": market,
        "strategy_results": strategy_results,
        "total_scanned": total_scanned,
    }


def get_recommendations() -> dict:
    """나스닥 100 중 매수 추천 종목 선별"""
    recommendations = []
    
    # 병렬로 전체 분석
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(analyze_single_stock, symbol): symbol for symbol in NASDAQ_100}
        
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                result = future.result()
                if result and result["strategies_matched"]:
                    # 전략 매칭 + 위험도 낮은 종목만
                    if result["risk_score"] <= 30:
                        recommendations.append({
                            "symbol": symbol,
                            "price": result["price"],
                            "risk_score": result["risk_score"],
                            "risk_grade": result["risk_grade"],
                            "strategies": result["strategies_matched"],
                            "rsi": result["rsi"],
                            "ma50_gap": result["ma50_gap"],
                            "change_5d": result["change_5d"],
                        })
            except:
                pass
    
    # 위험도 낮은 순 + 전략 많은 순 정렬
    recommendations.sort(key=lambda x: (x["risk_score"], -len(x["strategies"])))
    
    # 전부 반환
    return {
        "recommendations": recommendations,
        "total_analyzed": len(NASDAQ_100),
    }
