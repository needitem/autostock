from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from core.daily_defense import daily_defense_state
from pipelines.us_rebalance import _daily_defense_overlay


def test_daily_defense_enters_hard_mode_below_ma200() -> None:
    out = daily_defense_state(
        ma50_gap_pct=-1.0,
        ma200_gap_pct=-0.5,
        return21d_pct=-3.0,
        vix_close=26.0,
        soft_exposure_pct=60.0,
        hard_exposure_pct=25.0,
        vix_soft=24.0,
        vix_hard=32.0,
        return21d_soft=-2.0,
    )

    assert out["state"] == "hard_defense"
    assert out["target_exposure_pct"] == 25.0
    assert "qqq_below_ma200" in out["reasons"]


def test_daily_defense_enters_soft_mode_on_weak_ma50_or_vix() -> None:
    out = daily_defense_state(
        ma50_gap_pct=-0.1,
        ma200_gap_pct=2.0,
        return21d_pct=1.0,
        vix_close=24.5,
        soft_exposure_pct=60.0,
        hard_exposure_pct=25.0,
        vix_soft=24.0,
        vix_hard=32.0,
        return21d_soft=-2.0,
    )

    assert out["state"] == "soft_defense"
    assert out["target_exposure_pct"] == 60.0
    assert "qqq_below_ma50" in out["reasons"] or "vix_elevated" in out["reasons"]


def test_daily_defense_stays_normal_when_trend_is_healthy() -> None:
    out = daily_defense_state(
        ma50_gap_pct=2.5,
        ma200_gap_pct=6.0,
        return21d_pct=3.0,
        vix_close=18.0,
        soft_exposure_pct=60.0,
        hard_exposure_pct=25.0,
        vix_soft=24.0,
        vix_hard=32.0,
        return21d_soft=-2.0,
    )

    assert out["state"] == "normal"
    assert out["target_exposure_pct"] == 100.0
    assert out["reasons"] == []


def test_us_rebalance_daily_defense_overlay_caps_exposure(monkeypatch) -> None:
    monkeypatch.setenv("AI_DAILY_DEFENSE_OVERLAY", "1")
    monkeypatch.setenv("AI_DAILY_DEFENSE_SOFT_EXPOSURE_PCT", "60")
    monkeypatch.setenv("AI_DAILY_DEFENSE_HARD_EXPOSURE_PCT", "25")
    monkeypatch.setenv("AI_DAILY_DEFENSE_VIX_SOFT", "24")
    monkeypatch.setenv("AI_DAILY_DEFENSE_VIX_HARD", "32")
    monkeypatch.setenv("AI_DAILY_DEFENSE_RETURN21_SOFT", "-2")

    out = _daily_defense_overlay(
        {
            "price": 90.0,
            "ma50": 100.0,
            "ma200": 105.0,
            "benchmark_return_21d": -5.0,
            "vix_close": 33.0,
        }
    )

    assert out["enabled"] is True
    assert out["state"] == "hard_defense"
    assert out["target_exposure_pct"] == 25.0


def test_us_rebalance_daily_defense_enabled_by_default(monkeypatch) -> None:
    monkeypatch.delenv("AI_DAILY_DEFENSE_OVERLAY", raising=False)

    out = _daily_defense_overlay(
        {
            "price": 110.0,
            "ma50": 100.0,
            "ma200": 95.0,
            "benchmark_return_21d": 4.0,
            "vix_close": 18.0,
        }
    )

    assert out["enabled"] is True
    assert out["state"] == "normal"
