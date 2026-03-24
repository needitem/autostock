# Weekly Rebalance Trade Journal (Codex Decision)

- source run: `data/runs/ai_portfolio_backtest_results_trend10y_qld_gld125_20260305_v2.csv`
- decision mode: `AI_DECISION_ENGINE=ai` (Codex weekly buy/sell)
- features used for decision: chart indicators + market regime/context (VIX, breadth 등)
- execution: weekly next-open
- period: 2016-03-07 ~ 2026-03-02

## Headline Performance

- Strategy: CAGR **33.67%**, total **1731.51%**, MDD **-32.08%**, Sharpe **1.11**
- QQQ: CAGR **18.96%**, total **469.64%**, MDD **-34.47%**, Sharpe **0.94**
- Diff (Strategy - QQQ): CAGR **+14.71%p**, Total **+1261.87%p**, MDD **+2.39%p**

## Trade Summary

- Closed trades: **43**
- Win rate: **65.12%**
- Avg trade PnL: **+9.33%**
- Median trade PnL: **+1.00%**
- Avg alpha vs QQQ (same holding window): **+4.57%p**

## Yearly Comparison (Strategy vs QQQ)

```text
 year  weeks  strategy_total_pct  qqq_total_pct  diff_pctp
 2016     43               -3.59          13.45     -17.04
 2017     52               69.73          31.27      38.46
 2018     53               17.71           0.04      17.67
 2019     52               25.74          35.68      -9.94
 2020     52               95.27          48.29      46.98
 2021     52               54.28          26.64      27.64
 2022     52              -13.61         -32.68      19.07
 2023     52               72.20          51.07      21.13
 2024     53               11.95          29.12     -17.17
 2025     52               48.56          18.19      30.38
 2026      8                1.45          -3.30       4.76
```

## Trade Journal (buy/sell price & PnL)

```text
 trade_id symbol    buy_day  buy_price   sell_day  sell_price  holding_weeks  pnl_pct  qqq_same_period_pct  alpha_vs_qqq_pctp    exit_reason
        1    GLD 2016-03-07    121.180 2016-04-04     116.670              4   -3.722                4.975             -8.697 rebalanced_out
        2    QLD 2016-04-04      4.705 2016-05-02       4.327              4   -8.037               -3.960             -4.077 rebalanced_out
        3    GLD 2016-05-02    123.780 2016-05-31     115.760              4   -6.479                4.048            -10.527 rebalanced_out
        4    QLD 2016-05-31      4.680 2016-06-20       4.484              3   -4.180               -2.312             -1.868 rebalanced_out
        5    GLD 2016-06-20    122.260 2016-07-05     128.800              2    5.349               -0.204              5.553 rebalanced_out
        6    QLD 2016-07-05      4.449 2016-11-07       5.104             18   14.721                7.423              7.299 rebalanced_out
        7    GLD 2016-11-07    122.660 2016-11-14     116.120              1   -5.332                0.294             -5.626 rebalanced_out
        8    QLD 2016-11-14      5.136 2018-03-26       9.735             71   89.558               39.618             49.940 rebalanced_out
        9    GLD 2018-03-26    128.050 2018-04-02     126.650              1   -1.093               -1.688              0.595 rebalanced_out
       10    QLD 2018-04-02      9.386 2018-04-09       9.246              1   -1.492               -0.635             -0.856 rebalanced_out
       11    GLD 2018-04-09    126.450 2018-04-16     127.740              1    1.020                2.861             -1.841 rebalanced_out
       12    QLD 2018-04-16      9.761 2018-04-30       9.738              2   -0.243               -0.018             -0.225 rebalanced_out
       13    GLD 2018-04-30    124.410 2018-05-07     124.500              1    0.072                1.951             -1.879 rebalanced_out
       14    QLD 2018-05-07     10.109 2018-10-15      10.946             23    8.285                4.872              3.413 rebalanced_out
       15    GLD 2018-10-15    116.120 2019-02-25     125.780             19    8.319                0.288              8.031 rebalanced_out
       16    QLD 2019-02-25     10.579 2019-03-11      10.280              2   -2.824               -1.337             -1.487 rebalanced_out
       17    GLD 2019-03-11    122.520 2019-03-18     123.300              1    0.637                3.532             -2.895 rebalanced_out
       18    QLD 2019-03-18     11.052 2019-06-03      10.364             11   -6.232               -2.512             -3.720 rebalanced_out
       19    GLD 2019-06-03    124.090 2019-06-10     125.460              1    1.104                5.055             -3.951 rebalanced_out
       20    QLD 2019-06-10     11.403 2019-08-26      11.516             11    0.998                1.037             -0.039 rebalanced_out
       21    GLD 2019-08-26    144.350 2019-09-03     144.960              1    0.423                1.130             -0.707 rebalanced_out
       22    QLD 2019-09-03     11.770 2019-09-30      11.958              4    1.593                0.843              0.750 rebalanced_out
       23    GLD 2019-09-30    139.770 2019-10-07     141.160              1    0.994                0.256              0.739 rebalanced_out
       24    QLD 2019-10-07     12.009 2020-03-02      14.496             21   20.714               10.947              9.767 rebalanced_out
       25    GLD 2020-03-02    150.000 2020-05-11     160.340             10    6.893                6.994             -0.101 rebalanced_out
       26    QLD 2020-05-11     15.169 2022-01-10      39.485             87  160.305               67.712             92.593 rebalanced_out
       27    GLD 2022-01-10    167.360 2022-08-15     165.990             31   -0.819              -12.112             11.294 rebalanced_out
       28    QLD 2022-08-15     28.250 2022-08-22      26.360              1   -6.690               -3.315             -3.375 rebalanced_out
       29    GLD 2022-08-22    161.660 2023-01-30     179.290             23   10.906               -8.028             18.934 rebalanced_out
       30    QLD 2023-01-30     21.000 2023-10-23      28.650             38   36.429               20.595             15.833 rebalanced_out
       31    GLD 2023-10-23    183.510 2023-11-06     184.140              2    0.343                4.311             -3.968 rebalanced_out
       32    QLD 2023-11-06     31.050 2024-04-22      38.415             24   23.720               13.249             10.471 rebalanced_out
       33    GLD 2024-04-22    216.350 2024-04-29     216.020              1   -0.153                3.791             -3.943 rebalanced_out
       34    QLD 2024-04-29     41.300 2024-08-05      38.595             14   -6.550               -1.944             -4.606 rebalanced_out
       35    GLD 2024-08-05    220.560 2024-08-19     230.150              2    4.348               11.881             -7.533 rebalanced_out
       36    QLD 2024-08-19     48.205 2024-09-09      43.520              3   -9.719               -4.653             -5.066 rebalanced_out
       37    GLD 2024-09-09    231.260 2024-09-16     238.730              1    3.230                4.443             -1.213 rebalanced_out
       38    QLD 2024-09-16     47.380 2025-03-03      53.330             24   12.558                8.100              4.458 rebalanced_out
       39    GLD 2025-03-03    265.070 2025-05-19     297.850             11   12.367                0.532             11.835 rebalanced_out
       40    QLD 2025-05-19     51.540 2025-05-27      51.820              1    0.543                0.371              0.172 rebalanced_out
       41    GLD 2025-05-27    303.810 2025-06-02     309.210              1    1.777                0.211              1.566 rebalanced_out
       42    QLD 2025-06-02     51.975 2026-02-09      68.300             36   31.409               17.458             13.951 rebalanced_out
       43    GLD 2026-02-09    461.390 2026-03-02     490.100              3    6.222               -1.429              7.651    end_of_test
```
