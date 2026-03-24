# Strategy V2 Regime Core Plan

Status: baseline implemented and refined
Date: 2026-03-11

## Requirements Summary

- Replace the current primary thesis of weekly AI stock picking with a regime-first ETF baseline.
- Reuse the existing backtest, verification, and delivery infrastructure rather than rewriting the repo.
- Make the default path deterministic, cost-aware, and runnable without an LLM.
- Keep AI available for commentary and research, but not as the main execution engine.

## Grounding

- `docs/reports/validation_full10y_ai10y_codex_weekly_report_20260305.md:16` shows a 10-year AI result with higher CAGR than QQQ but much worse max drawdown and weak significance.
- `docs/reports/codex_weekly_tuning_ladder_20260304.md:27` shows the tuned 1-year AI setup does not remain robust through 2-year to 10-year expansions.
- `data/runs/verify_ai20y_ndx100_bydate_weekly_ai_ro1_dynblend_v2_20260306.md:13` shows the 20-year AI edge is modest and cost-sensitive.
- `docs/reports/weekly_broad_strategy_search_20260304.md:8` shows simple trend or leveraged ETF filters dominate the broader search.
- `scripts/backtest_ai_portfolio_selector.py:1611` already supports `ai`, `chart`, `trend`, and now `regime` engines.
- `scripts/backtest_ai_portfolio_selector.py:1853` already contains safe-mode filtering, target sizing, and the execution loop.
- `scripts/research_weekly_regime_allocator.py:450` already contains a robustness-first regime search and ranking pipeline.

## Acceptance Criteria

1. The checked-in V2 runner uses weekly next-open execution and includes at least `20 bps` total cost.
2. The default V2 path runs with `AI_DECISION_ENGINE=regime` and a small ETF universe only.
3. The default V2 path does not require Codex CLI login or any LLM call.
4. Documentation clearly marks AI stock picking as research-only, not the default production path.
5. The verification workflow remains the source of truth for CAGR, drawdown, turnover, and cost sensitivity.
6. The README links directly to the V2 strategy document and baseline entry point.
7. The baseline allocator must be allowed to deploy full weight rather than inheriting the generic 40 percent cap.

## Implementation Steps

1. Add a strategy document.
   - File: `docs/strategy-v2.md`
   - Purpose: freeze the new thesis, the baseline profile, and the keep-versus-demote decisions.

2. Add a checked-in baseline runner.
   - File: `scripts/run_strategy_v2_baseline.py`
   - Use the existing backtest engine in `scripts/backtest_ai_portfolio_selector.py`.
   - Set deterministic env defaults before import so the output is tagged and repeatable.

3. Link the new direction from the main entry point docs.
   - File: `README.md`
   - Add a short "Current Direction" section and a runnable baseline command.

4. Keep the next code phase small.
   - First implementation should promote the research allocator logic into a checked-in `regime` engine.
   - After the initial baseline is stable, allow only robustness-driven refinements.
   - The first approved refinement is a dedicated recovery state that routes shallow below-`MA200` rebounds into `QQQ` instead of forcing immediate `GLD` risk-off.

5. Delay mainline bot changes until the baseline is frozen.
   - Future files: `src/main.py`, `src/bot/handlers.py`, `src/bot/keyboards.py`, `src/bot/bot.py`
   - Goal: promote V2 to the default user path only after the baseline command and verification are stable.

## Risks And Mitigations

- Risk: the checked-in `regime` engine may still drift from the research allocator over time.
  - Mitigation: compare baseline outputs directly against `data/runs/regime_rotation_best_rr_v5_grid_20260305.json` whenever regime logic changes.

- Risk: broad universe defaults could leak back into the production path.
  - Mitigation: freeze `AI_SYMBOLS=TQQQ,QLD,QQQ,GLD,BIL` in the checked-in baseline runner.

- Risk: AI experiments continue to dominate attention because they look strong over short windows.
  - Mitigation: documentation must define robustness and cost criteria before headline CAGR.

- Risk: future README edits may drift from the frozen V2 thesis.
  - Mitigation: keep the README short and treat `docs/strategy-v2.md` as the canonical strategy reference.

## Verification Steps

1. Run `python -m py_compile scripts/run_strategy_v2_baseline.py`.
2. Run `python scripts/run_strategy_v2_baseline.py` when a full backtest is desired.
3. Run `python scripts/verify_ai_portfolio_backtest.py` against the produced outputs.
4. Compare the verification report against the V2 acceptance criteria before any bot promotion.
