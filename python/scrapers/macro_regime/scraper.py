"""
Macro Regime Scraper
Computes a macro regime signal from: SPY/QQQ momentum, TLT trend,
DXY (via UUP ETF), VIX proxy (VXX), and OVTLYR market breadth.
Returns regime: risk_on | risk_off | neutral
"""
import logging
from datetime import date, timedelta

import aiohttp

log = logging.getLogger("scraper.macro_regime")

MASSIVE_BASE = "https://api.massive.com"

MACRO_TICKERS = {
    "SPY":  "equity",
    "QQQ":  "equity",
    "TLT":  "bond",
    "GLD":  "commodity",
    "UUP":  "dollar",    # USD index proxy ETF
    "VXX":  "volatility",
    "HYG":  "credit",
}


async def _fetch_bars(session: aiohttp.ClientSession, api_key: str, ticker: str, days: int = 210) -> list:
    today    = date.today()
    from_dt  = (today - timedelta(days=days)).isoformat()
    to_dt    = today.isoformat()
    url      = f"{MASSIVE_BASE}/v2/aggs/ticker/{ticker}/range/1/day/{from_dt}/{to_dt}"
    headers  = {"Authorization": f"Bearer {api_key}"}
    try:
        async with session.get(url, params={"adjusted": "true", "sort": "asc", "limit": days + 10},
                               headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                return []
            data = await resp.json()
            return data.get("results", [])
    except Exception as e:
        log.warning("macro_regime.fetch_error", ticker=ticker, error=str(e))
        return []


def _sma(bars: list, period: int) -> float | None:
    closes = [float(b["c"]) for b in bars if "c" in b]
    if len(closes) < period:
        return None
    return sum(closes[-period:]) / period


def _trend_signal(bars: list) -> str:
    """above_200sma | below_200sma based on most recent close vs 200-day SMA."""
    if not bars:
        return "unknown"
    price = float(bars[-1].get("c", 0))
    sma200 = _sma(bars, 200)
    if sma200 is None:
        sma50 = _sma(bars, 50)
        if sma50 is None:
            return "unknown"
        return "above_200sma" if price > sma50 else "below_200sma"
    return "above_200sma" if price > sma200 else "below_200sma"


def _momentum(bars: list, period: int = 20) -> float:
    """Return % change over `period` days."""
    closes = [float(b["c"]) for b in bars if "c" in b]
    if len(closes) < period + 1:
        return 0.0
    return (closes[-1] - closes[-period]) / closes[-period] * 100


async def compute_macro_regime(api_key: str, breadth_pct: float | None = None) -> dict:
    """Fetch macro data and return a regime snapshot dict."""
    bars = {}
    async with aiohttp.ClientSession() as session:
        for ticker in MACRO_TICKERS:
            bars[ticker] = await _fetch_bars(session, api_key, ticker)

    # Bull/bear signals
    bull = 0
    bear = 0
    signals = {}

    # SPY trend
    spy_trend = _trend_signal(bars.get("SPY", []))
    signals["spy_trend"] = spy_trend
    if spy_trend == "above_200sma":
        bull += 1
    else:
        bear += 1

    # QQQ momentum (20d)
    qqq_mom = _momentum(bars.get("QQQ", []), 20)
    signals["qqq_momentum_20d"] = round(qqq_mom, 2)
    if qqq_mom > 2:
        bull += 1
    elif qqq_mom < -2:
        bear += 1

    # TLT trend (bonds up = risk-off)
    tlt_trend = "unknown"
    tlt_bars  = bars.get("TLT", [])
    if tlt_bars:
        tlt_mom = _momentum(tlt_bars, 20)
        signals["tlt_momentum_20d"] = round(tlt_mom, 2)
        if tlt_mom > 1:
            tlt_trend = "rising"
            bear += 1  # flight to safety = risk-off
        elif tlt_mom < -1:
            tlt_trend = "falling"
            bull += 1  # yields rising = risk-on
        else:
            tlt_trend = "neutral"

    # DXY proxy (UUP): strong dollar = risk-off for equities
    dxy_trend = "neutral"
    uup_bars  = bars.get("UUP", [])
    if uup_bars:
        dxy_mom = _momentum(uup_bars, 20)
        signals["dxy_momentum_20d"] = round(dxy_mom, 2)
        if dxy_mom > 1:
            dxy_trend = "rising"
            bear += 1
        elif dxy_mom < -1:
            dxy_trend = "falling"
            bull += 1
        else:
            dxy_trend = "neutral"

    # VIX proxy (VXX): low = risk-on
    vix_level = None
    vxx_bars  = bars.get("VXX", [])
    if vxx_bars:
        vix_level = float(vxx_bars[-1].get("c", 0))
        signals["vix_proxy"] = round(vix_level, 2)
        if vix_level < 20:
            bull += 1
        elif vix_level > 30:
            bear += 1

    # Credit spread (HYG): high = risk-on
    hyg_bars = bars.get("HYG", [])
    if hyg_bars:
        hyg_mom = _momentum(hyg_bars, 20)
        signals["hyg_momentum_20d"] = round(hyg_mom, 2)
        if hyg_mom > 0.5:
            bull += 1
        elif hyg_mom < -0.5:
            bear += 1

    # OVTLYR market breadth
    if breadth_pct is not None:
        signals["breadth_pct"] = round(breadth_pct, 1)
        if breadth_pct > 60:
            bull += 1
        elif breadth_pct < 40:
            bear += 1

    total = bull + bear
    score = round((bull - bear) / total, 3) if total > 0 else 0.0

    if score >= 0.3:
        regime = "risk_on"
    elif score <= -0.3:
        regime = "risk_off"
    else:
        regime = "neutral"

    return {
        "regime":       regime,
        "bull_signals": bull,
        "bear_signals": bear,
        "total_signals": total,
        "regime_score": score,
        "spy_trend":    spy_trend,
        "vix_level":    vix_level,
        "dxy_trend":    dxy_trend,
        "tlt_trend":    tlt_trend,
        "breadth_pct":  breadth_pct,
        "raw":          signals,
    }
