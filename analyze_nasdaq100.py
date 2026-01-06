"""나스닥 100 전체 분석 스크립트"""
import sys
sys.path.insert(0, 'src')
from groq_analyzer import collect_all_stock_data

print('나스닥 100 전체 분석 중...')
data = collect_all_stock_data()

# 종합 점수 기준 정렬
data.sort(key=lambda x: -x.get('total_score', 0))

print()
print('=' * 110)
print('나스닥 100 전체 평가 (종합 점수 순)')
print('=' * 110)
header = f"{'순위':^4} {'종목':^6} {'가격':^9} {'종합':^6} {'팩터':^6} {'재무':^6} {'Mom':^5} {'Qua':^5} {'Val':^5} {'Pro':^5} {'Vol':^5} {'위험':^5} {'등급':^4}"
print(header)
print('-' * 110)

for i, s in enumerate(data, 1):
    symbol = s.get('symbol', '')
    price = s.get('price', 0)
    total = s.get('total_score', 0)
    factor = s.get('factor_score', 0)
    fin = s.get('financial_score', 50)
    factors = s.get('factors', {})
    mom = factors.get('momentum', 0)
    qua = factors.get('quality', 0)
    val = factors.get('value', 0)
    pro = factors.get('profitability', 0)
    vol = factors.get('low_volatility', factors.get('volatility', 0))
    risk = s.get('risk_score', 0)
    grade = s.get('factor_grade', 'C')
    
    row = f"{i:^4} {symbol:^6} ${price:>7.2f} {total:>6.1f} {factor:>6.1f} {fin:>6.1f} {mom:>5.0f} {qua:>5.0f} {val:>5.0f} {pro:>5.0f} {vol:>5.0f} {risk:>5} {grade:^4}"
    print(row)

print('=' * 110)
print()
print('컬럼 설명:')
print('  Mom=Momentum(30%), Qua=Quality(25%), Val=Value(20%), Pro=Profitability(15%), Vol=LowVol(10%)')
print('  종합 = 팩터(60%) + 재무(40%)')
print('  등급: A(70+) B(60+) C(50+) D(40+) F(40-)')
print()

# 통계
a_grade = len([s for s in data if s.get('factor_grade') == 'A'])
b_grade = len([s for s in data if s.get('factor_grade') == 'B'])
c_grade = len([s for s in data if s.get('factor_grade') == 'C'])
d_grade = len([s for s in data if s.get('factor_grade') == 'D'])
f_grade = len([s for s in data if s.get('factor_grade') == 'F'])

print(f'등급 분포: A={a_grade}, B={b_grade}, C={c_grade}, D={d_grade}, F={f_grade}')
