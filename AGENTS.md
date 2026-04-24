# Repository Guidance

This file supplements higher-priority operator, system, and runtime instructions.

## Stock Recommendation Response Contract

- When the user asks which US stocks are worth entering now, asks for a broad-screen news review, or asks for a ranking of actionable names, do not stop at news summary or rank order alone.
- The default response must include the basic trading block for each actionable top pick:
  - reference price
  - exact market data cutoff date
  - entry plan
  - stop-loss
  - first target price
  - target portfolio weight or priority weight
- Do not defer those basics to a follow-up. A follow-up can expand with TP2, runner policy, scenario splits, or alternate entries, but not the base trade block above.
- For concise Korean outputs, prefer this order:
  - conclusion
  - market/date basis
  - top picks
  - per-symbol trade block
  - portfolio weight summary
  - avoid/watchlist

## Data Source Preference

- When local rebalance artifacts exist under `data/rebalance/rebalance_recommendation_*.json`, use them as the first source for execution levels and sizing.
- Prefer these fields before inferring your own numbers:
  - `final_selected_symbols`
  - `orders`
  - `execution_plans`
  - `executed_weights_pct`
  - `candidates`
- If news interpretation materially changes the rank order suggested by local artifacts, state that explicitly and cite the concrete date of the new information.
- If reliable execution levels are unavailable for a symbol, say that they are unavailable instead of fabricating entry, stop, or target numbers.

## Minimum Coverage

- A complete actionable recommendation should usually cover at least:
  - the strongest candidates worth entering now
  - one or more lower-priority or defensive alternatives
  - one clear avoid/hold-off name when the data supports it
- If the user asks for "all news" or "analyze every name carefully", summarize the full universe, but still end with a practical shortlist that includes entry/stop/target/weight for the names you would actually act on.
