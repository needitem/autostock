# -*- coding: utf-8 -*-
"""나스닥 100 전체 분석 (새 지표 포함)"""
import sys
sys.path.insert(0, 'src')

from core.indicators import get_full_analysis
from core.scoring import calculate_score
from config import NASDAQ_100

print('=' * 80)
print('📊 나스닥 100 전체 분석 (새 지표 포함)')
print('=' * 80)
print(f'분석 대상: {len(NASDAQ_100)}개 종목\n')

results = []
errors = []

for i, symbol in enumerate(NASDAQ_100, 1):
    print(f'[{i}/{len(NASDAQ_100)}] {symbol} 분석 중...', end=' ')
    try:
        analysis = get_full_analysis(symbol)
        if analysis is None:
            print('❌ 데이터 없음')
            errors.append(symbol)
            continue
        
        score = calculate_score(analysis)
        analysis['symbol'] = symbol
        analysis['score'] = score
        results.append(analysis)
        
        # 간단 상태 표시
        rsi = analysis.get('rsi', 50)
        status = '🟢과매도' if rsi < 30 else ('🔴과매수' if rsi > 70 else '⚪')
        print(f'${analysis.get("price", 0):.2f} RSI:{rsi:.0f}{status}')
    except Exception as e:
        print(f'❌ 에러: {e}')
        errors.append(symbol)

# 점수순 정렬
results.sort(key=lambda x: -x['score'].get('total_score', 0))

print('\n' + '=' * 80)
print('📈 분석 결과 요약')
print('=' * 80)

# 통계
avg_rsi = sum(r.get('rsi', 50) for r in results) / len(results) if results else 0
oversold = [r for r in results if r.get('rsi', 50) < 30]
overbought = [r for r in results if r.get('rsi', 50) > 70]
strong_trend = [r for r in results if r.get('adx', 0) > 25]

print(f'\n📊 시장 통계:')
print(f'  • 분석 완료: {len(results)}개 / 실패: {len(errors)}개')
print(f'  • 평균 RSI: {avg_rsi:.1f}')
print(f'  • 과매도 (RSI<30): {len(oversold)}개 - 매수 기회')
print(f'  • 과매수 (RSI>70): {len(overbought)}개 - 매도 고려')
print(f'  • 강한 추세 (ADX>25): {len(strong_trend)}개')

# 과매도 종목 (매수 기회)
print(f'\n💰 과매도 종목 (RSI<30) - 반등 기대:')
if oversold:
    for r in oversold[:10]:
        print(f"  {r['symbol']:6} ${r.get('price',0):>8.2f} | RSI:{r.get('rsi',50):>5.1f} | 스토캐스틱:{r.get('stoch_k',50):>5.1f} | ADX:{r.get('adx',0):>5.1f}")
else:
    print('  없음')

# 과매수 종목 (매도 고려)
print(f'\n⚠️ 과매수 종목 (RSI>70) - 조정 가능:')
if overbought:
    for r in overbought[:10]:
        print(f"  {r['symbol']:6} ${r.get('price',0):>8.2f} | RSI:{r.get('rsi',50):>5.1f} | 스토캐스틱:{r.get('stoch_k',50):>5.1f} | ADX:{r.get('adx',0):>5.1f}")
else:
    print('  없음')

# 캔들 패턴 발생 종목
patterns = [(r['symbol'], r.get('candle_patterns', [])) for r in results if r.get('candle_patterns')]
print(f'\n🕯️ 캔들 패턴 발생 ({len(patterns)}개):')
for symbol, pats in patterns[:15]:
    pat_str = ', '.join([f"{p['pattern']}({p['signal']})" for p in pats])
    print(f"  {symbol}: {pat_str}")

# 크로스 신호 발생 종목
crosses = [(r['symbol'], r.get('crosses', [])) for r in results if r.get('crosses')]
print(f'\n✨ 크로스 신호 발생 ({len(crosses)}개):')
for symbol, crs in crosses[:15]:
    cr_str = ', '.join([f"{c['type']}({c['signal']})" for c in crs])
    print(f"  {symbol}: {cr_str}")

# TOP 20 종목
print(f'\n🏆 종합 점수 TOP 20:')
print(f"{'순위':^4} {'종목':^6} {'가격':^10} {'점수':^6} {'RSI':^6} {'스토캐':^6} {'ADX':^6} {'거래량':^8} {'상태'}")
print('-' * 75)
for i, r in enumerate(results[:20], 1):
    rsi = r.get('rsi', 50)
    stoch = r.get('stoch_k', 50)
    adx = r.get('adx', 0)
    vol = r.get('volume_ratio', 1)
    score = r['score'].get('total_score', 0)
    
    # 상태 판단
    status = []
    if rsi < 30: status.append('과매도')
    if rsi > 70: status.append('과매수')
    if stoch < 20: status.append('스토과매도')
    if stoch > 80: status.append('스토과매수')
    if adx > 25: status.append('강추세')
    if vol > 2: status.append('거래량↑')
    status_str = ', '.join(status) if status else '중립'
    
    print(f"{i:^4} {r['symbol']:^6} ${r.get('price',0):>8.2f} {score:>5.0f} {rsi:>6.1f} {stoch:>6.1f} {adx:>6.1f} {vol:>6.2f}x  {status_str}")

print('\n' + '=' * 80)
print('분석 완료!')
