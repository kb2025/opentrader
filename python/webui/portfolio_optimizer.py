"""
Portfolio Optimizer
Implements four classic allocation strategies from a list of tickers:

  max_sharpe      — Mean-Variance Optimization (Markowitz): maximize Sharpe ratio
  min_variance    — Minimum-variance portfolio on the efficient frontier
  risk_parity     — Equal Risk Contribution: each asset contributes equally to portfolio vol
  equal_vol       — Inverse-Volatility weighting (simpler, no covariance needed)
  max_div         — Maximum Diversification: maximize weighted-avg-vol / portfolio-vol ratio

All strategies are long-only (0 ≤ w ≤ 1, Σw = 1).
An optional max_weight cap per asset is supported.
Historical returns are fetched from Polygon.io (daily, auto-adjusted).
scipy.optimize.minimize with SLSQP is used for the three constrained problems.
"""
import logging
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

log = logging.getLogger("portfolio_optimizer")

# ── Data fetch ────────────────────────────────────────────────────────────────

def _fetch_returns(tickers: list[str], lookback_days: int) -> pd.DataFrame:
    """
    Download daily adjusted close prices for all tickers via Polygon.io and return
    a DataFrame of daily log-returns.  Tickers that fail are dropped.
    """
    import os
    import json
    import urllib.request
    from datetime import date, timedelta as _td

    api_key = os.getenv("MASSIVE_API_KEY", "")
    if not api_key:
        raise ValueError("MASSIVE_API_KEY not set — cannot fetch OHLCV")
    to_date  = date.today().isoformat()
    frm_date = (date.today() - _td(days=lookback_days + 10)).isoformat()

    closes_dict: dict[str, dict[str, float]] = {}
    for ticker in tickers:
        try:
            url = (
                f"https://api.polygon.io/v2/aggs/ticker/{ticker.upper()}/range/1/day"
                f"/{frm_date}/{to_date}?adjusted=true&limit=500&apiKey={api_key}"
            )
            with urllib.request.urlopen(url, timeout=15) as resp:
                data = json.loads(resp.read())
            for bar in data.get("results", []):
                d = date.fromtimestamp(bar["t"] / 1000).isoformat()
                closes_dict.setdefault(d, {})[ticker] = bar["c"]
        except Exception as e:
            log.warning("portfolio_optimizer: polygon fetch failed for %s: %s", ticker, e)

    if not closes_dict:
        raise ValueError("No price data returned for any ticker")

    closes = pd.DataFrame.from_dict(closes_dict, orient="index")
    closes.index = pd.to_datetime(closes.index)
    closes = closes.sort_index()
    closes = closes.dropna(how="all")

    # Drop columns (tickers) where more than 20% of rows are NaN
    min_rows = int(len(closes) * 0.80)
    closes = closes.dropna(thresh=min_rows, axis=1)

    if closes.empty or len(closes) < 30:
        raise ValueError("Insufficient price history — need at least 30 trading days")

    log_ret = np.log(closes / closes.shift(1)).dropna()
    return log_ret


# ── Covariance & mean helpers ─────────────────────────────────────────────────

def _annualized(ret: pd.DataFrame):
    """Return (annualized mean vector, annualized covariance matrix)."""
    mu    = ret.mean() * 252
    sigma = ret.cov()  * 252
    return mu.values, sigma.values


# ── Strategy implementations ──────────────────────────────────────────────────

def _equal_vol(ret: pd.DataFrame, max_weight: float) -> np.ndarray:
    """Inverse-volatility weights — closed form, no optimizer needed."""
    vols    = ret.std() * np.sqrt(252)
    inv_vol = 1.0 / vols.values
    w       = inv_vol / inv_vol.sum()
    return _apply_cap(w, max_weight)


def _apply_cap(w: np.ndarray, cap: float) -> np.ndarray:
    """
    Iteratively clip weights to `cap` and redistribute excess to uncapped assets.
    Converges in O(n) passes.
    """
    w = w.copy()
    for _ in range(len(w) + 2):
        over   = w > cap
        if not over.any():
            break
        excess = (w[over] - cap).sum()
        w[over] = cap
        under   = ~over
        if not under.any():
            break
        w[under] += excess * (w[under] / w[under].sum())
    return w / w.sum()


def _slsqp(objective, n: int, max_weight: float, extra_constraints=None):
    """Shared SLSQP runner with sum-to-1 and bounds."""
    from scipy.optimize import minimize

    w0          = np.ones(n) / n
    bounds      = [(0.0, max_weight) for _ in range(n)]
    constraints = [{"type": "eq", "fun": lambda w: w.sum() - 1.0}]
    if extra_constraints:
        constraints.extend(extra_constraints)

    res = minimize(
        objective, w0,
        method      = "SLSQP",
        bounds      = bounds,
        constraints = constraints,
        options     = {"maxiter": 1000, "ftol": 1e-9},
    )
    w = np.clip(res.x, 0, None)
    w /= w.sum()
    return w


def _max_sharpe(mu, sigma, rf: float, max_weight: float) -> np.ndarray:
    n = len(mu)

    def neg_sharpe(w):
        ret = w @ mu
        vol = np.sqrt(w @ sigma @ w + 1e-12)
        return -(ret - rf) / vol

    return _slsqp(neg_sharpe, n, max_weight)


def _min_variance(sigma, max_weight: float) -> np.ndarray:
    n = sigma.shape[0]

    def port_var(w):
        return w @ sigma @ w

    return _slsqp(port_var, n, max_weight)


def _risk_parity(sigma, max_weight: float) -> np.ndarray:
    """Minimize sum of squared differences from equal risk contribution."""
    n = sigma.shape[0]
    target = np.ones(n) / n

    def rp_loss(w):
        pv   = w @ sigma @ w + 1e-12
        mrc  = sigma @ w
        rc   = w * mrc / pv          # risk contribution fractions
        return float(np.sum((rc - target) ** 2))

    # Risk parity needs strictly positive weights
    from scipy.optimize import minimize
    w0     = np.ones(n) / n
    bounds = [(1e-4, max_weight) for _ in range(n)]
    cons   = [{"type": "eq", "fun": lambda w: w.sum() - 1.0}]
    res    = minimize(rp_loss, w0, method="SLSQP", bounds=bounds, constraints=cons,
                      options={"maxiter": 2000, "ftol": 1e-10})
    w = np.clip(res.x, 0, None)
    return w / w.sum()


def _max_div(mu, sigma, max_weight: float) -> np.ndarray:
    """Maximize diversification ratio = (w · σᵢ) / √(wᵀΣw)."""
    n      = sigma.shape[0]
    vols   = np.sqrt(np.diag(sigma))

    def neg_div(w):
        weighted_vol = w @ vols
        port_vol     = np.sqrt(w @ sigma @ w + 1e-12)
        return -weighted_vol / port_vol

    return _slsqp(neg_div, n, max_weight)


# ── Portfolio statistics ──────────────────────────────────────────────────────

def _portfolio_stats(w: np.ndarray, mu: np.ndarray, sigma: np.ndarray, rf: float) -> dict:
    exp_ret = float(w @ mu)
    exp_vol = float(np.sqrt(w @ sigma @ w))
    sharpe  = (exp_ret - rf) / exp_vol if exp_vol > 0 else 0.0
    return {
        "expected_return": round(exp_ret, 6),
        "volatility":      round(exp_vol, 6),
        "sharpe":          round(sharpe, 4),
    }


def _risk_contributions(w: np.ndarray, sigma: np.ndarray) -> np.ndarray:
    """Marginal risk contribution fraction for each asset (sums to 1)."""
    pv  = w @ sigma @ w
    mrc = sigma @ w
    rc  = w * mrc / (pv + 1e-12)
    return rc


def _correlation(sigma: np.ndarray) -> np.ndarray:
    """Convert covariance matrix to correlation matrix."""
    vols = np.sqrt(np.diag(sigma))
    outer = np.outer(vols, vols)
    corr = sigma / (outer + 1e-12)
    return np.clip(corr, -1, 1)


# ── Public API ────────────────────────────────────────────────────────────────

METHODS = ("max_sharpe", "min_variance", "risk_parity", "equal_vol", "max_div")


def optimize(
    tickers:      list[str],
    method:       str  = "max_sharpe",
    total_capital:float = 10_000.0,
    lookback_days:int   = 252,
    risk_free_rate:float= 0.045,
    max_weight:   float = 1.0,
) -> dict:
    """
    Run portfolio optimization and return a result dict suitable for JSON serialization.

    Parameters
    ----------
    tickers        : list of ticker symbols (3–30 recommended)
    method         : one of METHODS
    total_capital  : total dollars to allocate
    lookback_days  : trading days of history to use (≥ 60)
    risk_free_rate : annualized risk-free rate (e.g. 0.045 for 4.5%)
    max_weight     : maximum weight per asset (e.g. 0.40 = 40% cap)

    Returns
    -------
    dict with keys: weights, allocations, portfolio, assets, correlation, tickers, method
    """
    if method not in METHODS:
        raise ValueError(f"Unknown method '{method}'. Choose from: {METHODS}")
    if len(tickers) < 2:
        raise ValueError("Need at least 2 tickers")

    tickers = [t.upper().strip() for t in tickers]
    max_weight = float(np.clip(max_weight, 1.0 / len(tickers), 1.0))

    # ── Fetch data ────────────────────────────────────────────────────────────
    ret = _fetch_returns(tickers, lookback_days)
    available = list(ret.columns)
    if len(available) < 2:
        raise ValueError(f"Only {len(available)} ticker(s) had sufficient data: {available}")

    mu, sigma = _annualized(ret)

    # ── Run chosen strategy ───────────────────────────────────────────────────
    if method == "max_sharpe":
        w = _max_sharpe(mu, sigma, risk_free_rate, max_weight)
    elif method == "min_variance":
        w = _min_variance(sigma, max_weight)
    elif method == "risk_parity":
        w = _risk_parity(sigma, max_weight)
    elif method == "equal_vol":
        w = _equal_vol(ret, max_weight)
    elif method == "max_div":
        w = _max_div(mu, sigma, max_weight)

    # ── Stats ─────────────────────────────────────────────────────────────────
    stats   = _portfolio_stats(w, mu, sigma, risk_free_rate)
    rc      = _risk_contributions(w, sigma)
    corr    = _correlation(sigma)
    vols    = np.sqrt(np.diag(sigma))
    ind_ret = mu

    assets = {}
    for i, ticker in enumerate(available):
        assets[ticker] = {
            "weight":       round(float(w[i]), 6),
            "allocation":   round(float(w[i]) * total_capital, 2),
            "annual_vol":   round(float(vols[i]), 6),
            "annual_return":round(float(ind_ret[i]), 6),
            "risk_contrib": round(float(rc[i]), 6),
        }

    return {
        "method":      method,
        "tickers":     available,
        "weights":     {t: round(float(w[i]), 6) for i, t in enumerate(available)},
        "allocations": {t: round(float(w[i]) * total_capital, 2) for i, t in enumerate(available)},
        "portfolio":   stats,
        "assets":      assets,
        "correlation": [[round(float(corr[i, j]), 4) for j in range(len(available))]
                        for i in range(len(available))],
        "lookback_days":  lookback_days,
        "total_capital":  total_capital,
        "risk_free_rate": risk_free_rate,
        "max_weight":     max_weight,
        "data_rows":      len(ret),
    }
