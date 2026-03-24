# Research Journal - 2026-03-24

## Goal

Keep parallel strategy research moving after the stock-first local search plateaued below the promotion bar.

## What Ran

- Stock-first promotion loop, full window:
  - `data/runs/stock_hypothesis_promotion_loop_20260323T015101Z.md`
- Stock-first targeted promotion loop:
  - `data/runs/stock_hypothesis_promotion_loop_targeted_20260323T031000Z.md`
- Stock-first structural promotion loop:
  - `data/runs/stock_hypothesis_promotion_loop_structural_20260324T014500Z.md`
- `V10 regime dynamic defense`:
  - `data/runs/promotion_check_strategy_v10_regime_dynamic_defense_20260324.json`
- `V11 chart/regime blend`:
  - `data/runs/promotion_check_strategy_v11_chart_regime_blend_20260324.json`
- `V13 regime defensive stock static`:
  - `data/runs/promotion_check_strategy_v13_regime_defensive_stock_static_20260324.json`
- Targeted `V10` variants:
  - `data/runs/promotion_check_strategy_v10a_regime_dynamic_defense_neutral_qld_20260324.json`
  - `data/runs/promotion_check_strategy_v10b_regime_dynamic_defense_sticky_20260324.json`
  - `data/runs/promotion_check_strategy_v10c_regime_dynamic_defense_qld_sticky_20260324.json`
  - `data/runs/promotion_check_strategy_v10d_regime_dynamic_defense_neutral_qld_hyst01_20260324.json`
  - `data/runs/promotion_check_strategy_v10e_regime_dynamic_defense_neutral_qld_filterqld_20260324.json`
- Dedicated `V10` sweep:
  - `data/runs/strategy_v10_dynamic_defense_sweep_20260324T000708Z.md`
- Dedicated `V10` risk-off sweep:
  - `data/runs/strategy_v10_dynamic_defense_riskoff_sweep_20260324T001527Z.md`
- Named promotion candidate:
  - `data/runs/promotion_check_strategy_v14_regime_gld_dynamic_defense_20260324.json`

## Main Findings

- The stock-first family is still alive, but local and structural tuning both failed to cross the promotion bar.
- The best stock-first region remained the `entryfreeze1_pb007` family.
- The local best stock-first variants reached `4/7` promotion criteria, but the 3-year, 5-year, and 7-year Newey-West p-values remained stuck around `0.17` to `0.24`.
- Adding portfolio-structure axes (`weight_mode`, `max_per_sector`, `min_overlap`) pushed the stock-first line backward to `3/7`.

- `V11 chart/regime blend` was rejected quickly.
  - Full-window CAGR diff was negative.
- `V13 regime defensive stock static` was also rejected.
  - It lost too much return and drawdown worsened.

- The only new family with any continuing value is `V10 regime dynamic defense`.
- The baseline `V10` result was not promotion-ready:
  - full-window CAGR diff `+0.97pp`
  - `p(alpha > 0) = 0.633`
  - turnover `0.327`

- The best `V10` region now is:
  - `neutral = QLD`
  - `filter_safe = QQQ`
  - `hysteresis = 0.0`
  - `crash_dynamic = 1`
- This is the top sweep variant:
  - `neutral_qld__filter_qqq__h0p0__crashdyn_1`
- Its current promotion profile:
  - criteria passed: `3/7`
  - full-window CAGR diff: `+2.72pp`
  - 3-year horizon: pass
  - 5-year horizon: fail
  - 7-year horizon: fail
  - full-window alpha: fail
  - turnover guardrail: fail

## Breakthrough

The next narrow sweep found the actual blocker and crossed the promotion bar.

- Sweep:
  - `data/runs/strategy_v10_dynamic_defense_riskoff_sweep_20260324T001527Z.md`
- Winning risk-off design:
  - `risk_off_pool = GLD`
  - `risk_off_top_n = 1`
  - `risk_off_weight_mode = inv_vol` or `equal`
  - `risk_off_fallback = GLD`
- Interpretation:
  - the multi-asset `IEF/TLT/GLD` defensive sleeve was the problem
  - once the `risk_off` sleeve collapsed to pure `GLD`, turnover dropped below the guardrail and long-window alpha became strong enough

Named runner and clean artifacts:

- Runner:
  - `scripts/run_strategy_v14_regime_gld_dynamic_defense.py`
- Verification:
  - `data/runs/ai_portfolio_backtest_verification_strategy_v14_regime_gld_dynamic_defense_20260324.md`
- Promotion check:
  - `data/runs/promotion_check_strategy_v14_regime_gld_dynamic_defense_20260324.md`

Promotion result for `V14`:

- full-window CAGR diff: `+10.21pp`
- full-window `P(alpha > 0)`: `0.929`
- turnover mean: `0.289`
- drawdown diff: `-2.16pp`
- 3-year horizon: `pass` with NW p(two-sided) `0.007`
- 5-year horizon: `pass` with NW p(two-sided) `0.018`
- 7-year horizon: `pass` with NW p(two-sided) `0.045`
- overall promotion check: `True`

## Current Production Ranking Check

To avoid overreacting to a single breakthrough, `V14` was rechecked against current leading deterministic families on the same promotion format.

- `V2 baseline`
  - promotion check: `True`
  - full-window CAGR diff: `+12.28pp`
  - turnover: `0.272`
  - drawdown diff: `-5.45pp`
- `V14 regime GLD dynamic defense`
  - promotion check: `True`
  - full-window CAGR diff: `+10.21pp`
  - turnover: `0.289`
  - drawdown diff: `-2.16pp`
- `V8 levered trend best`
  - promotion check: `False`
  - full-window CAGR diff: `+13.58pp`
  - turnover: `0.083`
  - drawdown diff: `+2.13pp`
  - blocker: 3-year horizon NW p(two-sided) stayed at `0.332`

Interpretation:

- `V2` is still the strongest passing regime-family baseline on this exact rule set.
- `V14` is now a real passing challenger, not just a research curiosity.
- `V8` remains the strongest raw performer, but is still blocked by the 3-year promotion rule.

## Decision

- Drop `event/filing drift` as a main production research path for now.
- Drop `V11 chart/regime blend` for now.
- Drop `V13 regime defensive stock static` for now.
- Promote the winning narrowed `V10` design as `V14 regime GLD dynamic defense`.
- Keep the remaining `V10` family only as a research branch if we want follow-up robustness checks.

## Next Research Step

The immediate next step is no longer to rescue stock-first. It is to stress-test `V14` as the new leading production candidate.

- Compare `V14` directly against `V2` and `levered trend` on the same verification and promotion format.
- Check whether `GLD-only risk_off` is robust to nearby perturbations, or whether it is an unstable knife-edge.
- If the result holds, treat `V14` as the new regime-family promotion candidate for default-path discussion.

## Code Added For This Round

- `scripts/run_strategy_v10_dynamic_defense_sweep.py`
- `scripts/run_strategy_v10_dynamic_defense_riskoff_sweep.py`
- `scripts/run_strategy_v14_regime_gld_dynamic_defense.py`
- `tests/test_strategy_v10_dynamic_defense_sweep_runner.py`
- `tests/test_strategy_v10_dynamic_defense_riskoff_sweep_runner.py`
- `tests/test_strategy_v14_regime_gld_dynamic_defense_runner.py`
- `scripts/run_stock_hypothesis_promotion_loop.py`
- `tests/test_run_stock_hypothesis_promotion_loop.py`
