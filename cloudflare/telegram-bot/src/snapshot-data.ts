export const SNAPSHOT = {
  "generatedAt": "2026-03-26 23:36:17.563429+00:00",
  "strategies": {
    "v2": {
      "label": "Strategy V2",
      "cagr": 31.24,
      "benchmarkCagr": 18.96,
      "drawdown": -39.93,
      "turnover": 0.272,
      "pAlphaGt0": 0.957
    },
    "v14": {
      "label": "Strategy V14",
      "cagr": 29.18,
      "benchmarkCagr": 18.96,
      "drawdown": -36.64,
      "turnover": 0.289,
      "pAlphaGt0": 0.929
    }
  },
  "rebalance": {
    "v2": {
      "latestMarketDay": "2026-03-26",
      "signalDay": "2026-03-20",
      "entryDay": "2026-03-23",
      "regimeState": "risk_off",
      "regimeReason": "risk_off",
      "positions": [
        {
          "symbol": "GLD",
          "weight_pct": 100.0
        }
      ],
      "positionPriceRefs": [
        {
          "symbol": "GLD",
          "weightPct": 100.0,
          "entryDay": "2026-03-23",
          "entryDayOpen": 405.12,
          "latestMarketDay": "2026-03-26",
          "latestClose": 400.64
        }
      ],
      "signalQqqClose": 582.06,
      "signalQqqMa200Gap": -1.8165,
      "signalQqqReturn21d": -3.5478,
      "signalQqqReturn63d": -3.0562,
      "signalVixClose": 26.78,
      "latestQqqClose": 573.79,
      "latestQqqMa200Gap": -3.3903,
      "latestQqqReturn21d": -6.955,
      "latestQqqReturn63d": -7.7671,
      "latestVixClose": 27.44
    },
    "v14": {
      "latestMarketDay": "2026-03-26",
      "signalDay": "2026-03-20",
      "entryDay": "2026-03-23",
      "regimeState": "risk_off",
      "regimeReason": "risk_off:fallback",
      "positions": [
        {
          "symbol": "GLD",
          "weight_pct": 100.0
        }
      ],
      "positionPriceRefs": [
        {
          "symbol": "GLD",
          "weightPct": 100.0,
          "entryDay": "2026-03-23",
          "entryDayOpen": 405.12,
          "latestMarketDay": "2026-03-26",
          "latestClose": 400.64
        }
      ],
      "signalQqqClose": 582.06,
      "signalQqqMa200Gap": -1.8165,
      "signalQqqReturn21d": -3.5478,
      "signalQqqReturn63d": -3.0562,
      "signalVixClose": 26.78,
      "latestQqqClose": 573.79,
      "latestQqqMa200Gap": -3.3903,
      "latestQqqReturn21d": -6.955,
      "latestQqqReturn63d": -7.7671,
      "latestVixClose": 27.44
    }
  }
} as const;
