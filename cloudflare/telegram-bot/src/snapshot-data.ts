export const SNAPSHOT = {
  "generatedAt": "2026-04-17T02:14:08.438879+00:00",
  "strategies": {
    "v4": {
      "label": "Strategy V4",
      "cagr": 49.5,
      "benchmarkCagr": 18.96,
      "drawdown": -29.36,
      "turnover": 0.197,
      "pAlphaGt0": 0.997
    }
  },
  "rebalance": {
    "v4": {
      "latestMarketDay": "2026-04-16",
      "signalDay": "2026-04-16",
      "entryDay": "2026-04-16",
      "regimeState": "neutral",
      "regimeReason": "Neutral: balanced exposure; keep core trend bias. Flows confidence=medium; scale exposure x0.90.",
      "cashPct": 74.22,
      "positions": [
        {
          "symbol": "ETR",
          "weight_pct": 8.0693
        },
        {
          "symbol": "LYB",
          "weight_pct": 5.4795
        },
        {
          "symbol": "GLW",
          "weight_pct": 3.78
        },
        {
          "symbol": "CIEN",
          "weight_pct": 3.376
        },
        {
          "symbol": "LITE",
          "weight_pct": 2.7172
        },
        {
          "symbol": "DELL",
          "weight_pct": 2.3625
        }
      ],
      "positionPriceRefs": [
        {
          "symbol": "ETR",
          "weightPct": 8.0693,
          "entryDay": "2026-04-16",
          "entryDayOpen": 112.08,
          "latestMarketDay": "2026-04-16",
          "latestClose": 114.95
        },
        {
          "symbol": "LYB",
          "weightPct": 5.4795,
          "entryDay": "2026-04-16",
          "entryDayOpen": 70.98,
          "latestMarketDay": "2026-04-16",
          "latestClose": 73.13
        },
        {
          "symbol": "GLW",
          "weightPct": 3.78,
          "entryDay": "2026-04-16",
          "entryDayOpen": 128.75,
          "latestMarketDay": "2026-04-16",
          "latestClose": 168.27
        },
        {
          "symbol": "CIEN",
          "weightPct": 3.376,
          "entryDay": "2026-04-16",
          "entryDayOpen": 368.11,
          "latestMarketDay": "2026-04-16",
          "latestClose": 475.76
        },
        {
          "symbol": "LITE",
          "weightPct": 2.7172,
          "entryDay": "2026-04-16",
          "entryDayOpen": 656.58,
          "latestMarketDay": "2026-04-16",
          "latestClose": 824.01
        },
        {
          "symbol": "DELL",
          "weightPct": 2.3625,
          "entryDay": "2026-04-16",
          "entryDayOpen": 176.42,
          "latestMarketDay": "2026-04-16",
          "latestClose": 177.28
        }
      ],
      "liveSignalDay": "2026-04-16",
      "liveRegimeState": "neutral",
      "liveRegimeReason": "Neutral: balanced exposure; keep core trend bias. Flows confidence=medium; scale exposure x0.90.",
      "liveCashPct": 74.22,
      "livePositions": [
        {
          "symbol": "ETR",
          "weight_pct": 8.0693
        },
        {
          "symbol": "LYB",
          "weight_pct": 5.4795
        },
        {
          "symbol": "GLW",
          "weight_pct": 3.78
        },
        {
          "symbol": "CIEN",
          "weight_pct": 3.376
        },
        {
          "symbol": "LITE",
          "weight_pct": 2.7172
        },
        {
          "symbol": "DELL",
          "weight_pct": 2.3625
        }
      ],
      "livePositionPriceRefs": [
        {
          "symbol": "ETR",
          "weightPct": 8.0693,
          "latestMarketDay": "2026-04-16",
          "latestClose": 114.95
        },
        {
          "symbol": "LYB",
          "weightPct": 5.4795,
          "latestMarketDay": "2026-04-16",
          "latestClose": 73.13
        },
        {
          "symbol": "GLW",
          "weightPct": 3.78,
          "latestMarketDay": "2026-04-16",
          "latestClose": 168.27
        },
        {
          "symbol": "CIEN",
          "weightPct": 3.376,
          "latestMarketDay": "2026-04-16",
          "latestClose": 475.76
        },
        {
          "symbol": "LITE",
          "weightPct": 2.7172,
          "latestMarketDay": "2026-04-16",
          "latestClose": 824.01
        },
        {
          "symbol": "DELL",
          "weightPct": 2.3625,
          "latestMarketDay": "2026-04-16",
          "latestClose": 177.28
        }
      ],
      "signalQqqClose": 637.4,
      "signalQqqMa200Gap": 6.8405,
      "signalQqqReturn21d": 6.3,
      "signalQqqReturn63d": 1.91,
      "signalVixClose": null,
      "latestQqqClose": 637.4,
      "latestQqqMa200Gap": 6.8405,
      "latestQqqReturn21d": 6.3,
      "latestQqqReturn63d": 1.91,
      "latestVixClose": null
    }
  },
  "eventRuntime": {
    "tsla": {
      "profile": "tsla",
      "generatedAt": "2026-04-17T02:12:53.732053+00:00",
      "symbol": "TSLA",
      "action": "WATCH",
      "confidence": 0.55,
      "eventSignal": "bearish",
      "eventStrength": "moderate",
      "macroMode": "risk_off",
      "macroReason": "macro_risk_event",
      "price": 388.9,
      "chartState": "mixed",
      "volumeRatio": 2.73,
      "reasons": [
        "실적 발표를 앞두고 부정적 프리뷰와 대형 투자은행의 하방 전망이 심리를 압박하고 있습니다.",
        "장중 거래는 VWAP 상회와 거래량 확대가 보이지만 차트 구조가 혼조라 강한 상방 확인으로 이어지지 않았습니다."
      ],
      "nextKnownEvents": [
        {
          "symbol": "TSLA",
          "type": "earnings",
          "priority": "high",
          "days_until": 5,
          "expected_date": "2026-04-22",
          "headline": "Tesla Q1 2026 earnings release and Q&A webcast",
          "source": "calendar"
        },
        {
          "symbol": "TSLA",
          "type": "earnings",
          "priority": "high",
          "days_until": 6,
          "expected_date": "2026-04-23",
          "headline": "TSLA earnings in 6 day(s)",
          "source": "calendar"
        }
      ],
      "rawEvents": [
        {
          "symbol": "TSLA",
          "scope": "stock",
          "sentiment": "neutral",
          "category": "product",
          "source": "wire",
          "magnitude": 1.0,
          "confirmed": true,
          "headline": "Tesla Earnings Preview: Why The Dream Is Breaking Down (NASDAQ:TSLA) - Seeking Alpha",
          "published_at": "2026-04-16T14:16:59+00:00",
          "tags": [],
          "link": "https://news.google.com/rss/articles/CBMimgFBVV95cUxQU2kwZ0dORHkxRUNSZHBfM2xsd2VidWNOdFU5bFFOVUZ3dUVxYlNNOEdFVHRxLVdFRmxtNXNwUkpCVkllOTRHVXRjaGp3Ni0zUmZWOEFkWm1xX3JEQm5GRHNKSlBvM3BUNENzMFo3VVlpdTNrZUZ3aXo5SWhQeFp3bVJ0bVJVb1ZxWnpGTk45cVVRUnZLYWpCeEhn?oc=5"
        },
        {
          "symbol": "TSLA",
          "scope": "stock",
          "sentiment": "neutral",
          "category": "product",
          "source": "wire",
          "magnitude": 1.0,
          "confirmed": true,
          "headline": "Tesla (TSLA) Earnings Expected to Grow: Should You Buy? - Yahoo Finance",
          "published_at": "2026-04-15T14:00:04+00:00",
          "tags": [],
          "link": "https://news.google.com/rss/articles/CBMinwFBVV95cUxOUFd3Rmo5eFVfa3NCZkx4N1ZEdkRUNENvSkpnZzI5UmZ3MEFSRTdMX1NsV2M0ZldsLWFkUmx2STBOUkpuQ29JSUNxSGVZdTdDRWoxbGVOZjJ5UGN0Vkwxdy1Fejd1ZmlKSXB0cUR4ZVRjNi1WeS03TlJrWGFpSVBJaDVFVDJmejBaQjZZRlpuTmJCS3d6SjY4UFliY1FOZWM?oc=5"
        },
        {
          "symbol": "TSLA",
          "scope": "stock",
          "sentiment": "neutral",
          "category": "product",
          "source": "social",
          "magnitude": 1.0,
          "confirmed": true,
          "headline": "Elon Musk says Tesla has taped out AI5 chip (TSLA:NASDAQ) - Seeking Alpha",
          "published_at": "2026-04-15T09:33:38+00:00",
          "tags": [],
          "link": "https://news.google.com/rss/articles/CBMiiAFBVV95cUxQcUJQbHlJN2VSY1NOTlVfRjhYSlJXSEZ5VTlTVjBxLXU0Mm5xMTJXVHVjaE5zOUxnVEVUZ0dVV29Jb1FtMkw2SVhJcEhrMWxqb1VWV3ZPeDk5V0dOT01UZ1VVY3NJZ0tRQXZLT3JHVTRFV2lRV1RmSi14dTJoQUZHS2xRNl96eGFx?oc=5"
        },
        {
          "symbol": "TSLA",
          "scope": "stock",
          "sentiment": "neutral",
          "category": "product",
          "source": "wire",
          "magnitude": 1.0,
          "confirmed": true,
          "headline": "Tesla (TSLA) down 20% in 2026 — JPMorgan sees another 60% downside - Electrek",
          "published_at": "2026-04-08T07:00:00+00:00",
          "tags": [],
          "link": "https://news.google.com/rss/articles/CBMiiwFBVV95cUxPNDhYcng4SVNLQXVlakl4bUx4Y29US3ZCVVdOakFETXZ4Ykg2Y0Z1QzB1MzdCaG1TSXB1UlZkLVZyVzZCSm9WWFdXcXFjRERpb29jSVdmcFNocEE5MFlJREU2RnB5Y2lERVFNN1BoeHhwbkVSQm9xUUFxSmItNm9rRkJYbjliQXNBTWtN?oc=5"
        },
        {
          "symbol": "TSLA",
          "scope": "stock",
          "sentiment": "neutral",
          "category": "analyst",
          "source": "analyst",
          "magnitude": 1.0,
          "confirmed": true,
          "headline": "Tesla (TSLA) down 20% in 2026 — JPMorgan sees another 60% downside - Electrek",
          "published_at": "2026-04-08T07:00:00+00:00",
          "tags": [],
          "link": "https://news.google.com/rss/articles/CBMiiwFBVV95cUxPNDhYcng4SVNLQXVlakl4bUx4Y29US3ZCVVdOakFETXZ4Ykg2Y0Z1QzB1MzdCaG1TSXB1UlZkLVZyVzZCSm9WWFdXcXFjRERpb29jSVdmcFNocEE5MFlJREU2RnB5Y2lERVFNN1BoeHhwbkVSQm9xUUFxSmItNm9rRkJYbjliQXNBTWtN?oc=5"
        }
      ],
      "state": {
        "lastRunAt": "2026-04-17T02:13:13.347203+00:00",
        "lastAction": "WATCH",
        "seenEventCount": 7
      }
    }
  }
} as const;
