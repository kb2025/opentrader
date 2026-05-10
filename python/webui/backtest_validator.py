"""
Statistical validation layer for OpenTrader backtests.

Three methods — each independently callable, all run synchronously inside a
ProcessPoolExecutor worker (no asyncio dependencies).

  walk_forward(params)           — Split history into N equal windows, backtest each.
                                   Tests strategy consistency across market regimes.

  monte_carlo_permutation(curve) — Shuffle daily returns 1 000x, build Sharpe
                                   distribution. p_value < 0.05 = statistically
                                   significant edge over random.

  bootstrap_sharpe_ci(curve)     — Resample daily returns 1 000x with replacement.
                                   Returns 90 % confidence interval on Sharpe ratio.

Entry point: run_validation(params) -> dict
"""
from __future__ import annotations

import math
import time
from typing import Any

import numpy as np
import pandas as pd

from webui.backtest_runner import _fetch_ohlcv, _run_on_df

_ANN = 252  # trading days per year for Sharpe annualisation


# ── Internal helpers ───────────────────────────────────────────────────────────

def _returns_from_curve(equity_curve: list[float]) -> np.ndarray:
    """Convert an equity curve list to an array of daily simple returns."""
    eq = np.asarray(equity_curve, dtype=float)
    if len(eq) < 2:
        return np.array([])
    return np.diff(eq) / (eq[:-1] + 1e-12)


def _sharpe(returns: np.ndarray) -> float:
    """Annualised Sharpe ratio from a daily-return array. Returns 0.0 if flat."""
    if len(returns) < 2:
        return 0.0
    std = float(np.std(returns, ddof=1))
    if std < 1e-10:
        return 0.0
    return float(np.mean(returns)) / std * math.sqrt(_ANN)


# ── Walk-Forward Validation ────────────────────────────────────────────────────

def walk_forward(params: dict, n_splits: int = 0) -> dict[str, Any]:
    """Run the EMA strategy on N equal, non-overlapping time windows.

    Data is fetched once and sliced — no duplicate downloads.

    Args:
        params:   Same dict passed to run_backtest (ticker, period, stop_pct …).
        n_splits: Number of windows. 0 = auto-size (target ~125 bars/window,
                  min 2, max 6).

    Returns:
        {windows: [...], summary: {...}}
        Each window entry: start, end, bars, sharpe, total_return,
                           max_drawdown, win_rate, total_trades.
        Summary: n_splits, mean_sharpe, std_sharpe, pct_positive_sharpe,
                 best_window_sharpe, worst_window_sharpe, consistent.
    """
    df = _fetch_ohlcv(params["ticker"], params.get("period", "2y"))
    n  = len(df)

    if n_splits <= 0:
        # Target ~125 bars per window; clamp to [2, 6]
        n_splits = max(2, min(6, n // 125))

    window_size = n // n_splits
    min_bars    = 30   # EMA-21 warmup + a few trade opportunities

    if window_size < min_bars:
        return {
            "error":   (f"Not enough data for {n_splits} windows "
                        f"({window_size} bars each, need ≥ {min_bars})."),
            "windows": [],
            "summary": {},
        }

    windows: list[dict] = []
    for i in range(n_splits):
        start    = i * window_size
        end      = start + window_size if i < n_splits - 1 else n
        slice_df = df.iloc[start:end].copy()
        try:
            r = _run_on_df(slice_df, params)
            windows.append({
                "window":       i + 1,
                "start":        slice_df.index[0].strftime("%Y-%m-%d"),
                "end":          slice_df.index[-1].strftime("%Y-%m-%d"),
                "bars":         len(slice_df),
                "sharpe":       r["sharpe"],
                "total_return": r["total_return"],
                "max_drawdown": r["max_drawdown"],
                "win_rate":     r["win_rate"],
                "total_trades": r["total_trades"],
            })
        except Exception as exc:
            windows.append({
                "window": i + 1,
                "start":  slice_df.index[0].strftime("%Y-%m-%d"),
                "end":    slice_df.index[-1].strftime("%Y-%m-%d"),
                "bars":   len(slice_df),
                "error":  str(exc),
            })

    valid   = [w for w in windows if "sharpe" in w]
    sharpes = [w["sharpe"] for w in valid]

    summary: dict[str, Any] = {
        "n_splits":            n_splits,
        "windows_with_trades": len(valid),
        "mean_sharpe":         round(float(np.mean(sharpes)), 4)           if sharpes else 0.0,
        "std_sharpe":          round(float(np.std(sharpes, ddof=1)), 4)    if len(sharpes) > 1 else 0.0,
        "pct_positive_sharpe": round(sum(s > 0 for s in sharpes) / len(sharpes) * 100, 1) if sharpes else 0.0,
        "best_window_sharpe":  round(max(sharpes), 4)                      if sharpes else 0.0,
        "worst_window_sharpe": round(min(sharpes), 4)                      if sharpes else 0.0,
        "consistent":          all(s > 0 for s in sharpes),
    }
    return {"windows": windows, "summary": summary}


# ── Monte Carlo Permutation Test ───────────────────────────────────────────────

def monte_carlo_permutation(
    equity_curve: list[float],
    n_perms: int = 1000,
) -> dict[str, Any]:
    """Permutation test: is the real Sharpe significantly better than random?

    Shuffles the daily-return sequence n_perms times, building a null distribution
    of Sharpe ratios achievable by a strategy with the same return days in a
    random order.  p_value = fraction of permuted Sharpes ≥ real Sharpe.

    Interpretation:
      p_value < 0.05  → significant edge (unlikely to be luck)
      p_value < 0.10  → marginal
      p_value ≥ 0.10  → not significant

    Returns:
        n_permutations, real_sharpe, p_value, significant (bool), verdict (str),
        perm_sharpe_p5 / _median / _p95 (percentiles of null distribution).
    """
    returns = _returns_from_curve(equity_curve)
    if len(returns) < 10:
        return {"error": "Equity curve too short for Monte Carlo (need ≥ 10 bars)"}

    real_sharpe = _sharpe(returns)

    rng          = np.random.default_rng(seed=42)
    perm_sharpes = np.empty(n_perms)
    for i in range(n_perms):
        perm_sharpes[i] = _sharpe(rng.permutation(returns))

    p_value = float(np.mean(perm_sharpes >= real_sharpe))

    if p_value < 0.05:
        verdict = "significant"
    elif p_value < 0.10:
        verdict = "marginal"
    else:
        verdict = "not significant"

    return {
        "n_permutations":     n_perms,
        "real_sharpe":        round(real_sharpe, 4),
        "p_value":            round(p_value, 4),
        "significant":        p_value < 0.05,
        "verdict":            verdict,
        "perm_sharpe_p5":     round(float(np.percentile(perm_sharpes, 5)),  4),
        "perm_sharpe_median": round(float(np.percentile(perm_sharpes, 50)), 4),
        "perm_sharpe_p95":    round(float(np.percentile(perm_sharpes, 95)), 4),
    }


# ── Bootstrap Confidence Interval ─────────────────────────────────────────────

def bootstrap_sharpe_ci(
    equity_curve: list[float],
    n_samples: int = 1000,
    alpha: float = 0.10,
) -> dict[str, Any]:
    """Bootstrap confidence interval on the Sharpe ratio.

    Resamples daily returns with replacement n_samples times. Reports the
    (alpha/2) and (1 - alpha/2) percentiles as the CI bounds.

    Default alpha=0.10 produces a 90 % confidence interval.

    Key result: positive_ci=True means the entire CI lies above zero —
    the strategy shows a robust positive edge even in the worst bootstrap samples.

    Returns:
        n_samples, ci_level, real_sharpe, ci_lower, ci_median, ci_upper,
        positive_ci (bool).
    """
    returns = _returns_from_curve(equity_curve)
    if len(returns) < 10:
        return {"error": "Equity curve too short for bootstrap (need ≥ 10 bars)"}

    real_sharpe  = _sharpe(returns)
    n            = len(returns)
    rng          = np.random.default_rng(seed=0)
    boot_sharpes = np.empty(n_samples)

    for i in range(n_samples):
        sample          = rng.choice(returns, size=n, replace=True)
        boot_sharpes[i] = _sharpe(sample)

    lower  = float(np.percentile(boot_sharpes, alpha / 2 * 100))
    upper  = float(np.percentile(boot_sharpes, (1 - alpha / 2) * 100))
    median = float(np.median(boot_sharpes))

    return {
        "n_samples":   n_samples,
        "ci_level":    f"{int((1 - alpha) * 100)}%",
        "real_sharpe": round(real_sharpe, 4),
        "ci_lower":    round(lower,  4),
        "ci_median":   round(median, 4),
        "ci_upper":    round(upper,  4),
        "positive_ci": lower > 0,
    }


# ── Probability of Loss by Holding Period ─────────────────────────────────────

_PERIOD_LABELS: dict[int, str] = {1: "1d", 5: "1w", 10: "2w", 21: "1mo", 63: "1q"}


def probability_of_loss_by_holding_period(
    trade_log: list[dict],
    holding_periods: list[int] | None = None,
) -> dict[str, Any]:
    """Compute probability of loss for each holding-period bucket.

    For each threshold in `holding_periods`, selects trades whose calendar-day
    hold duration meets or exceeds that threshold and computes the fraction that
    ended as a loss. Mirrors the S&P 500 research insight that longer holding
    periods reduce the probability of loss.

    Args:
        trade_log:       Trade dicts from backtest_runner (entry_date, exit_date,
                         pnl_pct, …).
        holding_periods: Calendar-day thresholds. Defaults to [1, 5, 10, 21, 63].

    Returns:
        {"results": [{holding_days, label, eligible_trades, loss_trades,
                      probability_of_loss, avg_pnl_pct, median_pnl_pct}, …]}
    """
    if holding_periods is None:
        holding_periods = [1, 5, 10, 21, 63]

    durations: list[int] = []
    for t in trade_log:
        try:
            delta = (pd.Timestamp(t["exit_date"]) - pd.Timestamp(t["entry_date"])).days
            durations.append(max(0, int(delta)))
        except Exception:
            durations.append(0)

    results: list[dict] = []
    for period in holding_periods:
        eligible = [t for t, d in zip(trade_log, durations) if d >= period]
        label    = _PERIOD_LABELS.get(period, f"{period}d")

        if not eligible:
            results.append({
                "holding_days":        period,
                "label":               label,
                "eligible_trades":     0,
                "loss_trades":         0,
                "probability_of_loss": None,
                "avg_pnl_pct":         None,
                "median_pnl_pct":      None,
            })
            continue

        pnl_pcts = [float(t.get("pnl_pct", 0.0)) for t in eligible]
        losses   = [p for p in pnl_pcts if p < 0]

        results.append({
            "holding_days":        period,
            "label":               label,
            "eligible_trades":     len(eligible),
            "loss_trades":         len(losses),
            "probability_of_loss": round(len(losses) / len(eligible), 4),
            "avg_pnl_pct":         round(float(np.mean(pnl_pcts)),   2),
            "median_pnl_pct":      round(float(np.median(pnl_pcts)), 2),
        })

    return {"results": results}


# ── Main Entry Point ───────────────────────────────────────────────────────────

def run_validation(params: dict) -> dict[str, Any]:
    """Run all three validation methods and return combined results.

    Called synchronously inside a ProcessPoolExecutor worker.
    Runs a full base backtest first so the equity curve is available for
    Monte Carlo and Bootstrap without re-downloading data.

    Args:
        params: Same dict as run_backtest — ticker, period, stop_pct, tp_pct,
                confidence, direction, max_pos, initial_capital.

    Returns:
        {base, walk_forward, monte_carlo, bootstrap, elapsed_seconds}
        base: core metrics (no chart PNG or trade log — kept small for JSON).
    """
    t0 = time.perf_counter()

    from webui.backtest_runner import run_backtest
    base = run_backtest(params)

    equity_curve = base.get("equity_curve", [])

    wf  = walk_forward(params, n_splits=params.get("n_splits", 0))
    mc  = monte_carlo_permutation(equity_curve, n_perms=params.get("n_perms", 1000))
    bs  = bootstrap_sharpe_ci(equity_curve,     n_samples=params.get("n_bootstrap", 1000))
    pol = probability_of_loss_by_holding_period(base.get("trade_log", []))

    # Strip large binary fields from the base summary
    _omit = {"chart_png_b64", "trade_log", "equity_curve", "monthly_returns"}
    base_summary = {k: v for k, v in base.items() if k not in _omit}
    base_summary["trade_count"] = len(base.get("trade_log", []))

    return {
        "base":                base_summary,
        "walk_forward":        wf,
        "monte_carlo":         mc,
        "bootstrap":           bs,
        "probability_of_loss": pol,
        "elapsed_seconds":     round(time.perf_counter() - t0, 1),
    }
