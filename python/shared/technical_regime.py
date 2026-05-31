"""
Technical Regime Classifier
Computes a 5-class overbought/oversold regime from SPY OHLCV bars using
consensus oscillator voting (RSI, Stochastic, Williams %R, Stochastic RSI,
MFI, Ultimate Oscillator).

Regime classes (mirrors article's 5-class schema):
  STRONG_OVERBOUGHT  consensus score >= 4
  OVERBOUGHT         score in [2, 3]
  NEUTRAL            score in [-1, 1]
  OVERSOLD           score in [-3, -2]
  STRONG_OVERSOLD    score <= -4

Returns a dict that is merged into the macro_regime snapshot.
"""
import logging

log = logging.getLogger("shared.technical_regime")

try:
    import numpy as np
    import pandas as pd
    _PANDAS_OK = True
except ImportError:
    _PANDAS_OK = False
    log.warning("technical_regime: pandas not available — regime disabled")


# ── Oscillator helpers ────────────────────────────────────────────────────────

def _to_series(bars: list, key: str) -> "pd.Series":
    return pd.Series([float(b[key]) for b in bars], dtype=float)


def _compute_rsi(close: "pd.Series", period: int = 14) -> "pd.Series":
    delta = close.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / loss.replace(0, float("nan"))
    return 100 - (100 / (1 + rs))


def _compute_mfi(close: "pd.Series", high: "pd.Series", low: "pd.Series",
                 volume: "pd.Series", period: int = 14) -> "pd.Series":
    typical = (close + high + low) / 3
    raw_mf  = typical * volume
    pos_mf  = raw_mf.where(typical > typical.shift(1), 0.0)
    neg_mf  = raw_mf.where(typical < typical.shift(1), 0.0)
    pos_sum = pos_mf.rolling(period).sum()
    neg_sum = neg_mf.rolling(period).sum()
    total   = (pos_sum + neg_sum).replace(0, float("nan"))
    return (pos_sum / total) * 100


def _stochastic(high: "pd.Series", low: "pd.Series", close: "pd.Series",
                k: int = 14, d: int = 3) -> "tuple[pd.Series, pd.Series]":
    """Return (%K, %D) stochastic oscillator (0–100)."""
    lowest_low   = low.rolling(k).min()
    highest_high = high.rolling(k).max()
    denom = (highest_high - lowest_low).replace(0, float("nan"))
    pct_k = (close - lowest_low) / denom * 100
    pct_d = pct_k.rolling(d).mean()
    return pct_k, pct_d


def _williams_r(high: "pd.Series", low: "pd.Series", close: "pd.Series",
                period: int = 14) -> "pd.Series":
    """Return Williams %R on 0–100 scale (0=overbought, 100=oversold)."""
    highest_high = high.rolling(period).max()
    lowest_low   = low.rolling(period).min()
    denom = (highest_high - lowest_low).replace(0, float("nan"))
    wr    = (highest_high - close) / denom * 100   # 0=OB, 100=OS
    return wr


def _stoch_rsi(close: "pd.Series", rsi_period: int = 14,
               stoch_period: int = 14) -> "pd.Series":
    """Return Stochastic RSI on 0–100 scale (100=OB, 0=OS)."""
    rsi  = _compute_rsi(close, rsi_period)
    low  = rsi.rolling(stoch_period).min()
    high = rsi.rolling(stoch_period).max()
    denom = (high - low).replace(0, float("nan"))
    return (rsi - low) / denom * 100


def _ultimate_oscillator(high: "pd.Series", low: "pd.Series", close: "pd.Series",
                         p1: int = 7, p2: int = 14, p3: int = 28) -> "pd.Series":
    """Return Ultimate Oscillator on 0–100 scale (>70=OB, <30=OS)."""
    prev_close = close.shift(1)
    true_low   = pd.concat([low, prev_close], axis=1).min(axis=1)
    bp         = close - true_low                          # buying pressure
    true_range = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs(),
    ], axis=1).max(axis=1)

    def _avg(period: int) -> "pd.Series":
        return bp.rolling(period).sum() / true_range.rolling(period).sum().replace(0, float("nan"))

    a1 = _avg(p1)
    a2 = _avg(p2)
    a3 = _avg(p3)
    uo = (4 * a1 + 2 * a2 + a3) / 7 * 100
    return uo


# ── Voting logic ──────────────────────────────────────────────────────────────

def _vote(value: float, ob_strong: float, ob_moderate: float,
          os_moderate: float, os_strong: float) -> int:
    """
    Assign a vote from {-2, -1, 0, +1, +2} based on where value falls
    relative to overbought/oversold thresholds.
    +2 = strongly overbought, -2 = strongly oversold.
    """
    if value >= ob_strong:
        return 2
    if value >= ob_moderate:
        return 1
    if value <= os_strong:
        return -2
    if value <= os_moderate:
        return -1
    return 0


def _classify(score: float) -> str:
    if score >= 4:
        return "STRONG_OVERBOUGHT"
    if score >= 2:
        return "OVERBOUGHT"
    if score <= -4:
        return "STRONG_OVERSOLD"
    if score <= -2:
        return "OVERSOLD"
    return "NEUTRAL"


# ── Public API ────────────────────────────────────────────────────────────────

def compute_technical_regime(bars: list) -> dict:
    """
    Compute a 5-class technical regime from OHLCV bars.

    bars: list of dicts with keys c, h, l, o, v (Polygon-style).
          At least 30 bars required; returns NEUTRAL on insufficient data.

    Returns:
        technical_regime: str  — STRONG_OVERBOUGHT | OVERBOUGHT | NEUTRAL | OVERSOLD | STRONG_OVERSOLD
        technical_score:  float — raw consensus vote sum (-14 to +14)
        oscillators:      dict  — latest value for each oscillator
    """
    _null = {
        "technical_regime": "NEUTRAL",
        "technical_score":  0.0,
        "oscillators":      {},
    }

    if not _PANDAS_OK or not bars or len(bars) < 30:
        return _null

    try:
        close  = _to_series(bars, "c")
        high   = _to_series(bars, "h")
        low    = _to_series(bars, "l")
        volume = _to_series(bars, "v")

        # Compute all oscillators
        rsi    = _compute_rsi(close, 14)
        mfi    = _compute_mfi(close, high, low, volume, 14)
        stk, _ = _stochastic(high, low, close, k=14, d=3)
        wr     = _williams_r(high, low, close, 14)
        srsi   = _stoch_rsi(close, 14, 14)
        uo     = _ultimate_oscillator(high, low, close)

        # Latest values (last non-NaN)
        def _last(s: "pd.Series") -> float:
            valid = s.dropna()
            return float(valid.iloc[-1]) if len(valid) > 0 else float("nan")

        rsi_v  = _last(rsi)
        mfi_v  = _last(mfi)
        stk_v  = _last(stk)
        wr_v   = _last(wr)
        srsi_v = _last(srsi)
        uo_v   = _last(uo)

        # Vote for each oscillator (thresholds from the article)
        votes = [
            _vote(rsi_v,  ob_strong=80, ob_moderate=70, os_moderate=30, os_strong=20),
            _vote(mfi_v,  ob_strong=90, ob_moderate=80, os_moderate=20, os_strong=10),
            _vote(stk_v,  ob_strong=90, ob_moderate=80, os_moderate=20, os_strong=10),
            # Williams %R: lower = overbought (invert: map 0=OB → vote +2)
            _vote(100 - wr_v, ob_strong=90, ob_moderate=80, os_moderate=20, os_strong=10),
            _vote(srsi_v, ob_strong=90, ob_moderate=80, os_moderate=20, os_strong=10),
            _vote(uo_v,   ob_strong=70, ob_moderate=60, os_moderate=40, os_strong=30),
        ]

        # Filter out NaN contributions
        valid_votes = [v for v, val in zip(votes, [rsi_v, mfi_v, stk_v, wr_v, srsi_v, uo_v])
                       if val == val]  # nan != nan
        score  = float(sum(valid_votes))
        regime = _classify(score)

        return {
            "technical_regime": regime,
            "technical_score":  round(score, 2),
            "oscillators": {
                "rsi_14":     round(rsi_v, 2)  if rsi_v  == rsi_v  else None,
                "stoch_k":    round(stk_v, 2)  if stk_v  == stk_v  else None,
                "williams_r": round(wr_v,  2)  if wr_v   == wr_v   else None,
                "stoch_rsi":  round(srsi_v, 2) if srsi_v == srsi_v else None,
                "mfi_14":     round(mfi_v, 2)  if mfi_v  == mfi_v  else None,
                "ultimate_osc": round(uo_v, 2) if uo_v   == uo_v   else None,
            },
        }

    except Exception as e:
        log.warning("technical_regime.compute_error", error=str(e))
        return _null
