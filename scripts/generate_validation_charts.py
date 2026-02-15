"""Generate walk-forward validation charts for README."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
INPUT_CSV = ROOT / "data" / "walkforward_10y_comparison_after_refactor.csv"
OUT_DIR = ROOT / "docs" / "charts"


def _require_input() -> pd.DataFrame:
    if not INPUT_CSV.exists():
        raise FileNotFoundError(f"Missing input CSV: {INPUT_CSV}")
    df = pd.read_csv(INPUT_CSV)
    if df.empty:
        raise ValueError("Input CSV is empty")
    return df.sort_values("year").reset_index(drop=True)


def _save_alpha_chart(df: pd.DataFrame) -> Path:
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(df["year"], df["total_alpha"], marker="o", linewidth=2, label="Total Score Alpha vs QQQ")
    ax.plot(df["year"], df["annual_alpha"], marker="o", linewidth=2, label="Annual Edge Alpha vs QQQ")
    ax.axhline(0, color="black", linestyle="--", linewidth=1)
    ax.set_title("10Y Walk-Forward: Top-Bucket Alpha vs QQQ")
    ax.set_xlabel("Year")
    ax.set_ylabel("Alpha (%)")
    ax.grid(alpha=0.25)
    ax.legend()
    out = OUT_DIR / "alpha_vs_qqq_10y.png"
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)
    return out


def _save_spread_chart(df: pd.DataFrame) -> Path:
    x = np.arange(len(df))
    width = 0.38

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(x - width / 2, df["total_spread"], width=width, label="Total Score Spread")
    ax.bar(x + width / 2, df["annual_spread"], width=width, label="Annual Edge Spread")
    ax.axhline(0, color="black", linestyle="--", linewidth=1)
    ax.set_title("10Y Walk-Forward: Top vs Bottom Spread")
    ax.set_xlabel("Year")
    ax.set_ylabel("Spread (%)")
    ax.set_xticks(x)
    ax.set_xticklabels(df["year"].astype(int).astype(str).tolist())
    ax.grid(axis="y", alpha=0.25)
    ax.legend()
    out = OUT_DIR / "top_bottom_spread_10y.png"
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)
    return out


def _save_cumulative_alpha_chart(df: pd.DataFrame) -> Path:
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(
        df["year"],
        df["total_alpha"].cumsum(),
        marker="o",
        linewidth=2,
        label="Total Score Cumulative Alpha",
    )
    ax.plot(
        df["year"],
        df["annual_alpha"].cumsum(),
        marker="o",
        linewidth=2,
        label="Annual Edge Cumulative Alpha",
    )
    ax.axhline(0, color="black", linestyle="--", linewidth=1)
    ax.set_title("10Y Walk-Forward: Cumulative Alpha vs QQQ")
    ax.set_xlabel("Year")
    ax.set_ylabel("Cumulative Alpha (%)")
    ax.grid(alpha=0.25)
    ax.legend()
    out = OUT_DIR / "cumulative_alpha_10y.png"
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)
    return out


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df = _require_input()
    outputs = [
        _save_alpha_chart(df),
        _save_spread_chart(df),
        _save_cumulative_alpha_chart(df),
    ]
    for output in outputs:
        print(f"saved: {output.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
