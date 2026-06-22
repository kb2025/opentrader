"""
OpenTrader Factor IC Research Module
Computes Information Coefficient (IC) and related statistics for a set of
price-derived factors against forward 5-day returns.

Used by the /api/research/factors/{ticker} WebUI endpoint.
No pandas — pure stdlib math + aiohttp.
"""
import math
import statistics
from datetime import datetime, timezone
from typing import Optional

import aiohttp


# ── Factor helpers ────────────────────────────────────────────────────────────

def _safe_div(a: float, b: float, fallback: float = 0.0) -> float:
    if b == 0 or math.isnan(b) or math.isnan(a):
        return fallback
    return a / b


def _mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _std(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    try:
        return statistics.stdev(values)
    except Exception:
        return 0.0


def _pearson(xs: list[float], ys: list[float]) -> float:
    """Pearson correlation coefficient between two equal-length lists."""
    n = len(xs)
    if n < 2:
        return 0.0
    mx = _mean(xs)
    my = _mean(ys)
    num   = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    denom = math.sqrt(
        sum((x - mx) ** 2 for x in xs) * sum((y - my) ** 2 for y in ys)
    )
    return _safe_div(num, denom)


def _rolling_mean(series: list[float], window: int) -> list[Optional[float]]:
    result: list[Optional[float]] = [None] * len(series)
    for i in range(window - 1, len(series)):
        result[i] = _mean(series[i - window + 1: i + 1])
    return result


def _rolling_std(series: list[float], window: int) -> list[Optional[float]]:
    result: list[Optional[float]] = [None] * len(series)
    for i in range(window - 1, len(series)):
        chunk = series[i - window + 1: i + 1]
        result[i] = _std(chunk)
    return result


# ── Factor computations (all return list of Optional[float]) ─────────────────

def _momentum_20(closes: list[float]) -> list[Optional[float]]:
    """20-day price momentum: (close[i] / close[i-20]) - 1."""
    out: list[Optional[float]] = [None] * len(closes)
    for i in range(20, len(closes)):
        base = closes[i - 20]
        out[i] = _safe_div(closes[i] - base, base) if base != 0 else None
    return out


def _rsi_14(closes: list[float]) -> list[Optional[float]]:
    """14-period Wilder RSI."""
    out: list[Optional[float]] = [None] * len(closes)
    if len(closes) < 15:
        return out

    gains, losses = [], []
    for i in range(1, 15):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0.0))
        losses.append(max(-diff, 0.0))

    avg_gain = _mean(gains)
    avg_loss = _mean(losses)

    def _rsi_val(ag: float, al: float) -> float:
        if al == 0:
            return 100.0
        rs = _safe_div(ag, al)
        return 100.0 - (100.0 / (1.0 + rs))

    out[14] = _rsi_val(avg_gain, avg_loss)

    for i in range(15, len(closes)):
        diff = closes[i] - closes[i - 1]
        g    = max(diff, 0.0)
        l    = max(-diff, 0.0)
        avg_gain = (avg_gain * 13.0 + g)  / 14.0
        avg_loss = (avg_loss * 13.0 + l)  / 14.0
        out[i]   = _rsi_val(avg_gain, avg_loss)

    return out


def _vol_ratio(volumes: list[float]) -> list[Optional[float]]:
    """volume[i] / avg_volume(20 days)."""
    out: list[Optional[float]] = [None] * len(volumes)
    roll = _rolling_mean(volumes, 20)
    for i, avg in enumerate(roll):
        if avg and avg != 0:
            out[i] = _safe_div(volumes[i], avg)
    return out


def _bb_pct(closes: list[float], window: int = 20, k: float = 2.0) -> list[Optional[float]]:
    """Bollinger Band %B: (price - lower) / (upper - lower)."""
    out: list[Optional[float]] = [None] * len(closes)
    rm = _rolling_mean(closes, window)
    rs = _rolling_std(closes, window)
    for i in range(window - 1, len(closes)):
        mid = rm[i]
        std = rs[i]
        if mid is None or std is None or std == 0:
            continue
        upper = mid + k * std
        lower = mid - k * std
        band  = upper - lower
        out[i] = _safe_div(closes[i] - lower, band)
    return out


def _mean_rev_5(closes: list[float]) -> list[Optional[float]]:
    """5-day mean reversion: negative of 5-day return (negative momentum)."""
    out: list[Optional[float]] = [None] * len(closes)
    for i in range(5, len(closes)):
        base = closes[i - 5]
        if base != 0:
            ret    = _safe_div(closes[i] - base, base)
            out[i] = -ret
    return out


def _price_to_sma50(closes: list[float]) -> list[Optional[float]]:
    """price / 50-day SMA ratio."""
    out: list[Optional[float]] = [None] * len(closes)
    rm = _rolling_mean(closes, 50)
    for i in range(49, len(closes)):
        sma = rm[i]
        if sma and sma != 0:
            out[i] = _safe_div(closes[i], sma)
    return out


def _fwd_returns_5(closes: list[float]) -> list[Optional[float]]:
    """Forward 5-bar return: (close[i+5] / close[i]) - 1."""
    out: list[Optional[float]] = [None] * len(closes)
    for i in range(len(closes) - 5):
        base = closes[i]
        if base != 0:
            out[i] = _safe_div(closes[i + 5] - base, base)
    return out


# ── IC statistics ──────────────────────────────────────────────────────────────

def _factor_stats(
    factor_vals: list[Optional[float]],
    fwd_rets: list[Optional[float]],
    ic_window: int = 20,
) -> dict:
    """
    Compute IC, IR, mean, std, hit_rate for a factor series.

    IC   = Pearson(factor[i], fwd_return[i]) over all valid pairs.
    IR   = IC / std(rolling 20-bar IC) — information ratio.
    hit_rate = pct where sign(factor) == sign(fwd_return).
    """
    # Paired valid observations
    pairs = [
        (f, r)
        for f, r in zip(factor_vals, fwd_rets)
        if f is not None and r is not None
        and not math.isnan(f) and not math.isnan(r)
    ]
    if len(pairs) < 5:
        return {"ic": 0.0, "ir": 0.0, "hit_rate": 0.0, "mean": 0.0, "std": 0.0}

    fs = [p[0] for p in pairs]
    rs = [p[1] for p in pairs]

    ic = _pearson(fs, rs)

    # Rolling 20-window IC for IR
    rolling_ics: list[float] = []
    for j in range(ic_window, len(pairs) + 1):
        chunk_f = [pairs[k][0] for k in range(j - ic_window, j)]
        chunk_r = [pairs[k][1] for k in range(j - ic_window, j)]
        rolling_ics.append(_pearson(chunk_f, chunk_r))

    ic_std = _std(rolling_ics) if rolling_ics else 0.0
    ir     = _safe_div(ic, ic_std)

    # Hit rate
    hits  = sum(1 for f, r in pairs if (f > 0) == (r > 0))
    hit_rate = hits / len(pairs) if pairs else 0.0

    return {
        "ic":       round(ic, 6),
        "ir":       round(ir, 4),
        "hit_rate": round(hit_rate, 4),
        "mean":     round(_mean(fs), 6),
        "std":      round(_std(fs), 6),
    }


# ── Public entry point ────────────────────────────────────────────────────────

async def compute_factor_ic(
    ticker: str,
    lookback_days: int = 90,
    market_data_url: str = "http://ot-market-data:8090",
) -> dict:
    """
    Fetch OHLCV history and compute factor IC statistics.

    Parameters
    ----------
    ticker          : instrument symbol
    lookback_days   : how many days of history to request
    market_data_url : base URL of the market data gateway

    Returns
    -------
    dict with factor stats + raw time series suitable for charting.
    """
    url = f"{market_data_url}/ohlcv/{ticker}?days={lookback_days}"

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    raise ValueError(
                        f"Market data gateway returned HTTP {resp.status} for {ticker}: {body[:200]}"
                    )
                payload = await resp.json()
        except aiohttp.ClientError as e:
            raise ValueError(f"Failed to reach market data gateway for {ticker}: {e}") from e

    # Expect {"bars": [{"date": "...", "open": ..., "high": ..., "low": ...,
    #                   "close": ..., "volume": ...}, ...]}
    bars = payload.get("bars") or payload.get("data") or payload.get("ohlcv") or []
    if not bars:
        raise ValueError(f"No OHLCV data returned for {ticker} (lookback={lookback_days}d)")

    # Sort bars chronologically
    try:
        bars = sorted(bars, key=lambda b: b.get("date", ""))
    except Exception:
        pass

    dates   = [b.get("date", "") for b in bars]
    closes  = [float(b.get("close", 0) or 0) for b in bars]
    volumes = [float(b.get("volume", 0) or 0) for b in bars]

    if len(closes) < 21:
        raise ValueError(
            f"Insufficient data for {ticker}: only {len(closes)} bars, need at least 21"
        )

    # Compute all factor series
    f_momentum_20    = _momentum_20(closes)
    f_rsi_14         = _rsi_14(closes)
    f_vol_ratio      = _vol_ratio(volumes)
    f_bb_pct         = _bb_pct(closes)
    f_mean_rev_5     = _mean_rev_5(closes)
    f_price_to_sma50 = _price_to_sma50(closes)
    fwd_rets         = _fwd_returns_5(closes)

    factor_map = {
        "momentum_20":    f_momentum_20,
        "rsi_14":         f_rsi_14,
        "vol_ratio":      f_vol_ratio,
        "bb_pct":         f_bb_pct,
        "mean_rev_5":     f_mean_rev_5,
        "price_to_sma50": f_price_to_sma50,
    }

    factor_stats: dict[str, dict] = {}
    for name, series in factor_map.items():
        factor_stats[name] = _factor_stats(series, fwd_rets)

    # Build serialisable raw series (replace None with null-equivalent for JSON)
    def _clean(series: list[Optional[float]]) -> list:
        return [round(v, 6) if v is not None else None for v in series]

    return {
        "ticker":        ticker,
        "lookback_days": lookback_days,
        "bar_count":     len(bars),
        "factors":       factor_stats,
        "dates":         dates,
        "factor_series": {name: _clean(series) for name, series in factor_map.items()},
        "fwd_returns":   _clean(fwd_rets),
        "computed_at":   datetime.now(timezone.utc).isoformat(),
    }
