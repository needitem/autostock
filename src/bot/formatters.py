"""
텔레그램 메시지 포맷터 모듈
"""

# 구분선
LINE = "━" * 20


def header(title: str, emoji: str = "") -> str:
    """헤더 생성"""
    return f"{emoji} <b>{title}</b>\n{LINE}\n"


def section(title: str) -> str:
    """섹션 제목"""
    return f"\n<b>{title}</b>\n"


def item(label: str, value, suffix: str = "") -> str:
    """항목 (라벨: 값)"""
    return f"• {label}: {value}{suffix}\n"


def pct(value: float, with_sign: bool = True) -> str:
    """퍼센트 포맷"""
    if with_sign:
        return f"{value:+.1f}%"
    return f"{value:.1f}%"


def usd(value: float, decimals: int = 2) -> str:
    """달러 포맷"""
    if decimals == 0:
        return f"${value:,.0f}"
    return f"${value:,.{decimals}f}"


def emoji_pnl(value: float) -> str:
    """손익 이모지"""
    return "🟢" if value >= 0 else "🔴"


def grade_emoji(grade: str) -> str:
    """등급 이모지"""
    return {"A": "🏆", "B": "✅", "C": "⚪", "D": "⚠️", "F": "❌"}.get(grade, "")


# ===== 종목 분석 =====
def format_analysis(data: dict) -> str:
    """종목 분석 결과"""
    symbol = data.get("symbol", "")
    price = data.get("price", 0)
    score = data.get("score", {})
    risk = score.get("risk", {})
    
    text = header(f"{symbol}", "📊")
    text += f"<b>{usd(price)}</b>\n"
    
    # 점수
    total = score.get("total_score", 0)
    grade = score.get("grade", "C")
    text += f"\n{grade_emoji(grade)} 종합점수: <b>{total:.0f}</b>/100 ({grade}등급)\n"
    text += f"└ {score.get('recommendation', '')}\n"
    
    # 위험도
    risk_score = risk.get("score", 0)
    text += f"\n⚠️ 위험도: {risk_score}/100 {risk.get('grade', '')}\n"
    if risk.get("warnings"):
        for w in risk["warnings"][:2]:
            text += f"  └ {w}\n"
    
    # 기술적 지표
    text += section("📉 기술적 지표")
    text += f"RSI: {data.get('rsi', 50):.0f} │ "
    text += f"BB: {data.get('bb_position', 50):.0f}% │ "
    text += f"52주: {data.get('position_52w', 50):.0f}%\n"
    text += f"50일선: {pct(data.get('ma50_gap', 0))}\n"
    
    return text


# ===== 추천 종목 =====
def format_recommendations(stocks: list, total: int) -> str:
    """추천 종목 목록"""
    text = header("추천 종목", "📈")
    
    if not stocks:
        text += "\n😢 조건에 맞는 종목이 없습니다."
        return text
    
    for i, s in enumerate(stocks[:20], 1):  # 최대 20개
        score = s.get("score", {})
        grade = score.get("grade", "C")
        risk = score.get("risk", {}).get("score", 0)
        
        text += f"\n<b>{i}. {s['symbol']}</b> {usd(s.get('price', 0))}\n"
        text += f"   {grade_emoji(grade)} {score.get('total_score', 0):.0f}점 │ "
        text += f"RSI {s.get('rsi', 50):.0f} │ "
        text += f"위험 {risk}\n"
    
    text += f"\n{LINE}\n"
    text += f"📌 {total}개 분석 → 상위 {len(stocks)}개"
    return text


# ===== 잔고 =====
def format_balance(balance: dict) -> str:
    """잔고 현황"""
    text = header("보유 현황", "💰")
    
    available = balance.get("available_cash", 0)
    text += f"주문가능: <b>{usd(available)}</b>\n"
    
    holdings = balance.get("holdings", [])
    if not holdings:
        text += "\n보유 종목이 없습니다."
        return text
    
    total_pnl = 0
    text += section("보유 종목")
    
    for h in holdings:
        pnl_pct = h.get("pnl_pct", 0)
        emoji = emoji_pnl(pnl_pct)
        
        text += f"\n{emoji} <b>{h['symbol']}</b> {h['qty']}주\n"
        text += f"   {usd(h.get('avg_price', 0))} → {usd(h.get('eval_amt', 0) / h['qty'] if h['qty'] else 0)}\n"
        text += f"   평가: {usd(h.get('eval_amt', 0))} ({pct(pnl_pct)})\n"
        
        if h.get("exit_signal"):
            text += f"   ⚠️ {h.get('exit_reason', '')}\n"
        
        total_pnl += h.get("pnl", 0)
    
    text += f"\n{LINE}\n"
    text += f"{emoji_pnl(total_pnl)} 총 손익: <b>{usd(total_pnl)}</b>"
    return text


# ===== 미체결 주문 =====
def format_orders(orders: list) -> str:
    """미체결 주문"""
    text = header("미체결 주문", "📋")
    
    if not orders:
        text += "\n미체결 주문이 없습니다."
        return text
    
    for o in orders:
        emoji = "🟢" if o.get("side") == "매수" else "🔴"
        text += f"\n{emoji} <b>{o['symbol']}</b> {o['side']}\n"
        text += f"   {o.get('qty', 0)}주 @ {usd(o.get('price', 0))}\n"
        text += f"   체결: {o.get('filled', 0)}주\n"
    
    return text


# ===== 관심종목 =====
def format_watchlist(stocks: list, auto_buy: bool) -> str:
    """관심종목 현황"""
    text = header(f"관심종목 ({len(stocks)})", "👀")
    
    if not stocks:
        text += "\n등록된 종목이 없습니다.\n➕ 종목추가 버튼을 눌러주세요."
        return text
    
    for s in stocks:
        # 상태 이모지
        if s.get("is_signal"):
            emoji = "🚨"
        elif s.get("status") == "bought":
            emoji = "✅"
        else:
            emoji = "👀"
        
        text += f"\n{emoji} <b>{s['symbol']}</b>\n"
        text += f"   현재: {usd(s.get('price', 0))} ({pct(s.get('change_pct', 0))})\n"
        text += f"   목표: {usd(s.get('target_price', 0))}\n"
        text += f"   RSI {s.get('rsi', 50):.0f} │ BB {s.get('bb_position', 50):.0f}% │ 조건 {s.get('met_count', 0)}/4\n"
    
    text += f"\n{LINE}\n"
    text += f"자동매수: {'✅ ON' if auto_buy else '❌ OFF'}"
    return text


def format_watchlist_signals(signals: list, total: int) -> str:
    """저점 신호 스캔"""
    text = header("저점 스캔", "🔍")
    text += f"관심종목 {total}개 스캔 완료\n"
    
    if not signals:
        text += "\n저점 신호 없음 ✓\n"
        text += "\n<b>신호 조건:</b>\n"
        text += "• RSI ≤ 35\n• BB 하단 근처\n• 5일선 -3% 이하\n• 3일 연속 하락"
        return text
    
    text += f"\n🚨 <b>신호 {len(signals)}개 발생!</b>\n"
    
    for s in signals:
        text += f"\n<b>{s['symbol']}</b> - {s.get('strength', '보통')}\n"
        text += f"   {usd(s.get('price', 0))} │ RSI {s.get('rsi', 50):.0f}\n"
        text += f"   조건 {s.get('met_count', 0)}/4 충족\n"
    
    return text


# ===== 매매 결과 =====
def format_trade_result(action: str, results: list) -> str:
    """매매 결과"""
    text = header(f"자동{action} 결과", "🤖")
    
    success = fail = 0
    
    for r in results:
        if r.get("success"):
            success += 1
            text += f"\n✅ <b>{r.get('symbol', '?')}</b>\n"
            text += f"   {r.get('qty', 0)}주 @ {usd(r.get('price', 0))}\n"
            if r.get("reason"):
                text += f"   └ {r['reason']}\n"
        elif r.get("error"):
            fail += 1
            text += f"\n❌ {r.get('symbol', '?')}: {r['error']}\n"
        elif r.get("message"):
            text += f"\nℹ️ {r['message']}\n"
    
    text += f"\n{LINE}\n"
    text += f"성공 {success} │ 실패 {fail}"
    return text



# ===== API 상태 =====
def format_api_status(status: dict) -> str:
    """API 상태"""
    text = header("API 상태", "⚙️")
    
    if status.get("connected"):
        mode = "🧪 모의투자" if status.get("is_paper") else "💰 실전투자"
        text += f"\n✅ 연결됨\n"
        text += f"\n모드: {mode}\n"
        text += f"계좌: {status.get('account', '미설정')}"
    else:
        text += f"\n❌ 연결 안됨\n"
        text += f"\n오류: {status.get('error', '알 수 없음')}\n"
        text += "\n<b>설정 방법:</b>\n"
        text += "1. 한투 앱에서 API 신청\n"
        text += "2. .env 파일에 키 입력"
    
    return text


# ===== 공포탐욕 =====
def format_fear_greed(fg: dict) -> str:
    """공포탐욕 지수"""
    score = fg.get("score", 50)
    
    text = header("시장 심리", "😱")
    text += f"\n{fg.get('emoji', '😐')} <b>{score}</b>/100\n"
    text += f"상태: {fg.get('rating', 'N/A')}\n"
    text += f"\n💡 {fg.get('advice', '')}\n"
    
    text += section("지수 구간")
    text += "0-25: 극단적 공포 🔴\n"
    text += "25-45: 공포 🟠\n"
    text += "45-55: 중립 🟡\n"
    text += "55-75: 탐욕 🟢\n"
    text += "75-100: 극단적 탐욕 🔵"
    
    return text
