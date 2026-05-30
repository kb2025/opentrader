"""
OpenTrader Options Monitor
==========================
Runs at EOD (16:05 ET) and during market hours (every 5 min) to:
  1. Scan all broker accounts for open option contracts
  2. Calculate 14-period ATR on the underlying stock
  3. Compute Emergency Exit / Exit Alert / Roll levels
  4. Log all data to option_positions + option_trade_log DB tables
  5. Fire alerts via Redis for threshold crossings
  6. Generate per-position matplotlib charts accessible from the WebUI

ATR level definitions (measured on the underlying stock price):
  Emergency Exit  : underlying_entry - 3 * ATR  → immediate close signal
  Exit Alert      : underlying_entry - 2 * ATR  → soft warning
  1st Roll        : underlying_entry + 0.5 * ATR
  2nd Roll        : underlying_entry + 1 * ATR
  3rd Roll        : underlying_entry + 2 * ATR
  (Extra rolls added at +3, +4, +5 … ATR as trade progresses)
"""
import asyncio
import base64
import io
import json
import os
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import asyncpg
import structlog

from shared.base_agent import BaseAgent
from shared.redis_client import STREAMS, get_redis
from shared.data_client import DataClient

log = structlog.get_logger("options-monitor")

DB_URL               = os.getenv("DB_URL", "")
POLYGON_API_KEY      = os.getenv("MASSIVE_API_KEY", "")
BROKER_GATEWAY_TIMEOUT = int(os.getenv("BROKER_GATEWAY_TIMEOUT_SEC", "20"))
SCAN_INTERVAL_MIN    = int(os.getenv("OPTIONS_SCAN_INTERVAL_MIN", "5"))
# How many consecutive scans a position must be absent before it is marked closed.
# At 5-min intervals, 3 = 15 minutes — enough to survive transient Webull dropouts
# while still catching genuine closes/rolls within one extra scan cycle.
MISS_THRESHOLD       = int(os.getenv("OPTIONS_MISS_THRESHOLD", "3"))
ATR_PERIOD           = 14
# Maximum extra roll levels to pre-compute beyond roll_3
MAX_EXTRA_ROLLS      = 7   # gives rolls at +3 … +9 ATR
# Default option type for Webull non-OCC positions where type isn't in raw data.
# Set to "put" if you hold puts, or "unknown" to search both (may misidentify).
WEBULL_DEFAULT_OPTION_TYPE = os.getenv("WEBULL_DEFAULT_OPTION_TYPE", "call")
# OI wall detector — minimum OI at a strike to qualify as a wall, and drop threshold
OI_WALL_MIN_OI         = int(os.getenv("OI_WALL_MIN_OI", "3000"))
OI_WALL_DROP_THRESHOLD = float(os.getenv("OI_WALL_DROP_PCT", "0.40"))
# TTL for OI wall Redis cache — 2× the scan interval so one missed scan doesn't clear state
OI_WALL_CACHE_TTL      = SCAN_INTERVAL_MIN * 60 * 2
# Unusual flow scanner thresholds
FLOW_MIN_VOL_DELTA     = int(os.getenv("FLOW_MIN_VOL_DELTA", "10"))    # min new contracts per scan
FLOW_MIN_NOTIONAL      = float(os.getenv("FLOW_MIN_NOTIONAL", "5000")) # min $ notional (vol_delta×price×100)
FLOW_TOP_N             = int(os.getenv("FLOW_TOP_N", "25"))            # top hits to surface
FLOW_CACHE_TTL         = 90 * 60   # 90 min Redis TTL for flow snapshot
VOL_SNAP_TTL           = 8 * 3600  # 8 h Redis TTL for per-contract volume baseline


def _parse_option_expiry(raw) -> Optional[date]:
    """Parse a Webull expiry value into a date. Handles ISO strings, YYYYMMDD, and Unix timestamps."""
    if not raw:
        return None
    s = str(raw).strip()
    if len(s) >= 10 and s[4] == "-":
        try:
            return date.fromisoformat(s[:10])
        except Exception:
            return None
    if len(s) == 8 and s.isdigit():
        try:
            return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
        except Exception:
            return None
    if s.isdigit() and len(s) == 13:
        try:
            return datetime.fromtimestamp(int(s) / 1000, tz=timezone.utc).date()
        except Exception:
            return None
    if s.isdigit() and len(s) == 10:
        try:
            return datetime.fromtimestamp(int(s), tz=timezone.utc).date()
        except Exception:
            return None
    return None


# ── DB helpers ────────────────────────────────────────────────────────────────

_pool: Optional[asyncpg.Pool] = None

async def _get_pool() -> asyncpg.Pool:
    global _pool
    if _pool:
        return _pool
    from urllib.parse import urlparse, unquote
    p = urlparse(DB_URL)
    _pool = await asyncpg.create_pool(
        host=p.hostname, port=p.port or 5432,
        user=p.username,
        password=unquote(p.password) if p.password else None,
        database=p.path.lstrip("/"),
        min_size=1, max_size=4,
    )
    return _pool


# ── ATR calculation ───────────────────────────────────────────────────────────

def _compute_atr(candles: list[dict], period: int = ATR_PERIOD) -> Optional[float]:
    """
    Compute ATR-14 from a list of OHLCV candle dicts.
    Each dict must have keys: open, high, low, close.
    Returns None if not enough data.
    """
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        h   = float(candles[i]["high"])
        lo  = float(candles[i]["low"])
        pc  = float(candles[i - 1]["close"])
        tr  = max(h - lo, abs(h - pc), abs(lo - pc))
        trs.append(tr)
    if len(trs) < period:
        return None
    # Simple moving average for first ATR, then Wilder's smoothing
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return round(atr, 4)


async def _fetch_atr_and_price(ticker: str) -> tuple[Optional[float], Optional[float]]:
    """
    Fetch 30 daily candles via the Market Data Gateway.
    Returns (atr_14, last_close_price).  Both may be None if unavailable.
    """
    try:
        data = await DataClient().bars(ticker, days=30)
        if not data:
            return None, None
        candles = data if isinstance(data, list) else (
            data.get("candles") or data.get("bars") or data.get("results") or []
        )
        if not candles:
            return None, None
        atr        = _compute_atr(candles)
        last_close = float(candles[-1]["close"]) if candles else None
        return atr, last_close
    except Exception as e:
        log.warning("options_monitor.atr_fetch_failed", ticker=ticker, error=str(e))
        return None, None


# Keep backward-compat alias used in _refresh_chart
async def _fetch_atr(ticker: str) -> Optional[float]:
    atr, _ = await _fetch_atr_and_price(ticker)
    return atr


# Cash-settled index tickers that lack standard bid/ask quote streams.
# Polygon returns no last-trade quote for these; fall back to daily aggs.
_INDEX_TICKERS = frozenset({"VIX", "SPX", "SPXW", "NDX", "RUT", "XSP", "DJX"})
# Polygon symbol overrides for cash indices (prefixed with "I:")
_INDEX_POLY_SYM = {
    "VIX": "I:VIX", "SPX": "I:SPX", "SPXW": "I:SPX",
    "NDX": "I:NDX", "RUT": "I:RUT", "XSP":  "I:XSP",
    "DJX": "I:DJI",
}


async def _fetch_underlying_price(ticker: str) -> Optional[float]:
    """
    Fetch latest underlying price with Quote → Trade (prev-close agg) fallback.

    Equity tickers: Massive MCP get_quote (real-time last trade).
    Index tickers (VIX, SPX, NDX, RUT, SPXW …): get_quote returns nothing
    useful for cash indices, so fall back to Polygon daily aggs prev-close.
    This mirrors tasty-agent's stream_quotes_with_trade_fallback() pattern.
    """
    sym = ticker.upper()

    # ── Tier 1: standard quote via gateway ────────────────────────────────
    data = await DataClient().quote(sym)
    if data:
        price = data.get("last") or data.get("close") or data.get("prev_close")
        if price:
            return float(price)

    # ── Tier 2: prev-close agg (index fallback) ────────────────────────────
    api_key  = os.getenv("MASSIVE_API_KEY", "")
    if not api_key:
        return None

    poly_sym = _INDEX_POLY_SYM.get(sym, sym)
    from datetime import timedelta
    today    = date.today()
    from_str = (today - timedelta(days=7)).isoformat()

    try:
        import aiohttp
        url = (
            f"https://api.polygon.io/v2/aggs/ticker/{poly_sym}/range/1/day"
            f"/{from_str}/{today.isoformat()}?adjusted=true&sort=desc&limit=1&apiKey={api_key}"
        )
        async with aiohttp.ClientSession() as sess:
            async with sess.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status == 200:
                    d    = await resp.json()
                    bars = d.get("results") or []
                    if bars:
                        log.debug("options_monitor.price_trade_fallback",
                                  ticker=sym, poly_sym=poly_sym)
                        return float(bars[0]["c"])
    except Exception as e:
        log.warning("options_monitor.price_fallback_failed", ticker=sym, error=str(e))

    return None


async def _fetch_earnings_date(ticker: str) -> Optional[date]:
    """Fetch next earnings date via the Market Data Gateway."""
    try:
        data = await DataClient().earnings(ticker)
        if not data:
            return None
        records = data if isinstance(data, list) else (
            data.get("earnings") or data.get("results") or []
        )
        today_str = date.today().isoformat()
        upcoming = [e for e in records if e.get("date") and e["date"] >= today_str]
        if upcoming:
            return date.fromisoformat(upcoming[-1]["date"])
    except Exception:
        pass
    return None


async def _fetch_ex_dividend_date(ticker: str) -> Optional[date]:
    """Fetch next ex-dividend date via the Market Data Gateway."""
    try:
        data = await DataClient().dividends(ticker)
        if not data:
            return None
        records = data if isinstance(data, list) else (
            data.get("dividends") or data.get("results") or []
        )
        today_str = date.today().isoformat()
        upcoming = [r for r in records if r.get("ex_dividend_date", "") >= today_str]
        if upcoming:
            return date.fromisoformat(upcoming[0]["ex_dividend_date"])
    except Exception:
        pass
    return None


def _check_early_assignment_risk(
    option_type: str,
    expiration_date: Optional[date],
    ex_div_date: Optional[date],
) -> bool:
    """
    Return True if an ITM call is at risk of early assignment due to an ex-dividend
    date falling before expiration. Puts are not at risk for dividend-driven assignment.
    From option_screener find_roll_outs.py: ex-date within 10 days of expiry = high risk.
    """
    if option_type != "call":
        return False
    if not expiration_date or not ex_div_date:
        return False
    return ex_div_date <= expiration_date


async def _score_roll_candidates(
    underlying: str,
    option_type: str,
    current_strike: Optional[float],
    current_expiry: Optional[date],
    current_contract_bid: float,
    days_to_exp: int,
) -> list[dict]:
    """
    Fetch option chain for higher-DTE expirations and rank roll candidates.

    Scoring formula (adapted from option_screener find_roll_outs.py:109-150):
      score = credit_pct + buy_up_pct
      + 0.5 bonus if credit > buy_up (prefer credit rolls over debit rolls)
      + (10 - (new_dte / 7) * 3)     duration factor — penalise rolling too far out
      - 2.0 if ex-div within 10 days of new expiry (assignment risk)

    Returns up to 5 scored candidates sorted descending by score.
    """
    if not POLYGON_API_KEY or not current_strike or not current_expiry:
        return []
    today = date.today()
    min_new_exp = current_expiry + timedelta(days=7)
    max_new_exp = current_expiry + timedelta(days=56)   # max 8-week look-ahead
    ex_div      = await _fetch_ex_dividend_date(underlying)

    try:
        import aiohttp as _aiohttp
        url = (
            f"https://api.polygon.io/v3/snapshot/options/{underlying.upper()}"
            f"?option_type={option_type[0]}"
            f"&expiration_date.gte={min_new_exp.isoformat()}"
            f"&expiration_date.lte={max_new_exp.isoformat()}"
            f"&limit=100&apiKey={POLYGON_API_KEY}"
        )
        async with _aiohttp.ClientSession() as sess:
            async with sess.get(url, timeout=_aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
        snaps = data.get("results") or []
    except Exception as e:
        log.debug("options_monitor.roll_chain_fetch_failed", underlying=underlying, error=str(e))
        return []

    candidates = []
    for snap in snaps:
        details = snap.get("details") or {}
        quote   = snap.get("last_quote") or {}
        strike  = float(details.get("strike_price") or 0)
        exp_str = details.get("expiration_date", "")
        bid     = float(quote.get("bid_price") or 0)
        if not strike or not exp_str or bid <= 0:
            continue
        try:
            new_exp = date.fromisoformat(exp_str)
        except Exception:
            continue
        new_bid = bid
        new_dte = (new_exp - today).days
        if new_dte <= 0:
            continue

        # credit_pct: what we receive rolling vs current contract
        credit    = new_bid - current_contract_bid
        credit_pct = credit / max(current_contract_bid, 0.01)

        # buy_up_pct: upside from rolling to a higher strike (calls only)
        buy_up     = max(0.0, strike - current_strike) if current_strike else 0.0
        buy_up_pct = buy_up / max(current_strike or strike, 1.0)

        score = credit_pct + buy_up_pct
        if credit > buy_up:
            score += 0.5                              # credit preference bonus
        score += max(0, 10 - (new_dte / 7) * 3)     # duration factor
        if ex_div and (ex_div - new_exp).days >= -10 and (ex_div - new_exp).days <= 0:
            score -= 2.0                              # ex-div risk penalty

        candidates.append({
            "strike":         round(strike, 2),
            "expiry":         exp_str,
            "new_dte":        new_dte,
            "new_bid":        round(new_bid, 2),
            "credit":         round(credit, 2),
            "credit_pct":     round(credit_pct * 100, 2),
            "buy_up_pct":     round(buy_up_pct * 100, 2),
            "score":          round(score, 3),
            "ex_div_risk":    bool(ex_div and ex_div <= new_exp),
        })

    candidates.sort(key=lambda c: -c["score"])
    return candidates[:5]


def _bs_greeks(S: float, K: float, T: float, sigma: float,
               r: float = 0.05, option_type: str = "call",
               q: float = 0.0) -> dict:
    """
    Black-Scholes Greeks using stdlib math only (no scipy).
    Includes continuous dividend yield q (default 0).
    Returns delta, gamma, theta (per calendar day), vega (per 1% IV),
            rho (per 1% rate move), volga (vega convexity),
            vanna (∂Δ/∂σ), charm (∂Δ/∂t per day), pop (risk-neutral ITM prob).
    """
    import math
    out = {"delta": None, "gamma": None, "theta": None, "vega": None,
           "rho": None, "volga": None, "vanna": None, "charm": None, "pop": None}
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return out
    try:
        sqrt_T  = math.sqrt(T)
        eq_T    = math.exp(-q * T)
        er_T    = math.exp(-r * T)
        d1 = (math.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * sqrt_T)
        d2 = d1 - sigma * sqrt_T
        def ncdf(x):
            return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0
        def npdf(x):
            return math.exp(-0.5 * x ** 2) / math.sqrt(2.0 * math.pi)
        n_d1  = ncdf(d1)
        n_d2  = ncdf(d2)
        np_d1 = npdf(d1)
        vega_raw = S * eq_T * np_d1 * sqrt_T          # raw vega (per unit vol)
        if option_type == "call":
            delta   = eq_T * n_d1
            theta   = ((-S * eq_T * np_d1 * sigma / (2.0 * sqrt_T))
                       + q * S * eq_T * n_d1
                       - r * K * er_T * n_d2) / 365.0
            rho_raw = K * T * er_T * n_d2              # per unit rate
            pop     = n_d2
        else:
            delta   = -eq_T * ncdf(-d1)
            theta   = ((-S * eq_T * np_d1 * sigma / (2.0 * sqrt_T))
                       - q * S * eq_T * ncdf(-d1)
                       + r * K * er_T * ncdf(-d2)) / 365.0
            rho_raw = -K * T * er_T * ncdf(-d2)
            pop     = ncdf(-d2)
        gamma = eq_T * np_d1 / (S * sigma * sqrt_T)
        vega  = vega_raw / 100.0                       # per 1% change in IV
        rho   = rho_raw  / 100.0                       # per 1% change in rate
        volga = vega * d1 * d2 / sigma                 # vega convexity (Vomma)
        # vanna: sensitivity of delta to vol (∂Δ/∂σ = ∂²V/∂S∂σ)
        vanna = -eq_T * np_d1 * d2 / sigma
        # charm: delta decay per calendar day (∂Δ/∂t)
        charm = -eq_T * np_d1 * (2.0 * (r - q) * T - d2 * sigma * sqrt_T) / (2.0 * T * sigma * sqrt_T) / 365.0
        out["delta"] = round(delta, 4)
        out["gamma"] = round(gamma, 6)
        out["theta"] = round(theta, 4)
        out["vega"]  = round(vega,  4)
        out["rho"]   = round(rho,   4)
        out["volga"] = round(volga, 4)
        out["vanna"] = round(vanna, 4)
        out["charm"] = round(charm, 6)
        out["pop"]   = round(pop,   4)
    except Exception:
        pass
    return out


def _bs_delta(S: float, K: float, T: float, sigma: float,
              r: float = 0.05, option_type: str = "call") -> Optional[float]:
    """Black-Scholes delta — thin wrapper around _bs_greeks for backward compat."""
    return _bs_greeks(S, K, T, sigma, r, option_type)["delta"]


def _intrinsic_value(S: float, K: float, option_type: str) -> float:
    """Intrinsic value: floor of option worth. Never negative."""
    if option_type == "call":
        return max(0.0, S - K)
    if option_type == "put":
        return max(0.0, K - S)
    return 0.0


def _bs_price(S: float, K: float, T: float, sigma: float,
              r: float = 0.045, option_type: str = "call",
              q: float = 0.0) -> Optional[float]:
    """Black-Scholes theoretical price including continuous dividend yield q."""
    import math
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return _intrinsic_value(S, K, option_type)
    try:
        sqrt_T = math.sqrt(T)
        d1 = (math.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * sqrt_T)
        d2 = d1 - sigma * sqrt_T
        def ncdf(x):
            return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0
        if option_type == "call":
            price = S * math.exp(-q * T) * ncdf(d1) - K * math.exp(-r * T) * ncdf(d2)
        else:
            price = K * math.exp(-r * T) * ncdf(-d2) - S * math.exp(-q * T) * ncdf(-d1)
        return round(max(price, _intrinsic_value(S, K, option_type)), 4)
    except Exception:
        return None


def _binomial_american_price(S: float, K: float, T: float, sigma: float,
                             r: float = 0.045, option_type: str = "call",
                             q: float = 0.0, steps: int = 100) -> Optional[float]:
    """
    CRR binomial tree price for American-style options.
    Supports early exercise via backward induction at each node.
    """
    import math
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return _intrinsic_value(S, K, option_type)
    try:
        dt = T / steps
        u  = math.exp(sigma * math.sqrt(dt))
        d  = 1.0 / u
        p  = (math.exp((r - q) * dt) - d) / (u - d)
        disc = math.exp(-r * dt)
        # Terminal node prices
        prices = [S * (u ** (steps - 2 * j)) for j in range(steps + 1)]
        if option_type == "call":
            values = [max(0.0, px - K) for px in prices]
        else:
            values = [max(0.0, K - px) for px in prices]
        # Backward induction with early-exercise check
        for i in range(steps - 1, -1, -1):
            for j in range(i + 1):
                hold = disc * (p * values[j] + (1 - p) * values[j + 1])
                spot = S * (u ** (i - 2 * j))
                exercise = max(0.0, spot - K) if option_type == "call" else max(0.0, K - spot)
                values[j] = max(hold, exercise)
        return round(values[0], 4)
    except Exception:
        return None


def _binomial_greeks(S: float, K: float, T: float, sigma: float,
                     r: float = 0.045, option_type: str = "call",
                     q: float = 0.0, steps: int = 50) -> dict:
    """
    Finite-difference Greeks using the American CRR binomial tree.
    delta/gamma/theta/vega/rho are computed via central differences — more accurate
    than closed-form B-S for American options that carry early-exercise premium.
    volga/vanna/charm/pop are taken from the B-S analytical result (negligible
    early-exercise correction for these higher-order Greeks).
    """
    out = _bs_greeks(S, K, T, sigma, r, option_type, q)
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return out
    try:
        h  = S * 0.01            # ±1% spot shift for delta/gamma
        dv = 0.001               # ±0.1% vol shift for vega
        dr = 0.0001              # ±0.01% rate shift for rho
        dt = 1.0 / 365.0         # 1 calendar day for theta

        def _p(s=S, t=T, v=sigma, ri=r):
            return _binomial_american_price(s, K, max(t, 1e-4), v, ri, option_type, q, steps) or 0.0

        p0   = _p()
        p_su = _p(s=S + h)
        p_sd = _p(s=S - h)
        p_t  = _p(t=T - dt)     # price 1 day closer to expiry
        p_vu = _p(v=sigma + dv)
        p_vd = _p(v=max(sigma - dv, 0.001))
        p_ru = _p(ri=r + dr)
        p_rd = _p(ri=max(r - dr, 0.0))

        out["delta"] = round((p_su - p_sd) / (2.0 * h), 4)
        out["gamma"] = round((p_su - 2.0 * p0 + p_sd) / (h ** 2), 6)
        out["theta"] = round(p_t - p0, 4)               # Δ price per calendar day (negative = decay)
        out["vega"]  = round((p_vu - p_vd) / (2.0 * dv) * 0.01, 4)  # per 1% vol
        out["rho"]   = round((p_ru - p_rd) / (2.0 * dr) * 0.01, 4)  # per 1% rate
    except Exception:
        pass
    return out


async def _compute_option_mark(
    broker_price: float,
    contract_symbol: str,
    underlying: str,
    option_type: str,
    strike: Optional[float],
    expiration_date,           # date | None
    current_underlying: Optional[float],
) -> tuple[float, str]:
    """
    Market-aware option valuation with graceful fallback (BYE pattern).

    Returns (mark_price, source) where source is one of:
      "broker"    — live broker quote (preferred)
      "polygon"   — Polygon snapshot mid-price
      "bs_model"  — Black-Scholes using VIX as sigma estimate
      "intrinsic" — max(0, S-K) floor value
      "zero"      — no data available

    Called when broker_price == 0 (Webull scan dropout or after-hours gap).
    """
    # Tier 1: broker has a live price — nothing to do
    if broker_price and broker_price > 0:
        return broker_price, "broker"

    # Tier 2: Polygon option snapshot (OCC ticker format O:...)
    if POLYGON_API_KEY and contract_symbol:
        poly_sym = contract_symbol if contract_symbol.startswith("O:") else f"O:{contract_symbol}"
        try:
            import aiohttp as _ah
            url = (
                f"https://api.polygon.io/v3/snapshot/options/{underlying}/{poly_sym}"
                f"?apiKey={POLYGON_API_KEY}"
            )
            async with _ah.ClientSession() as sess:
                async with sess.get(url, timeout=_ah.ClientTimeout(total=6)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        snap = (data.get("results") or {})
                        day  = snap.get("day") or {}
                        bid   = float(snap.get("last_quote", {}).get("bid") or 0)
                        close = float(day.get("close") or 0)
                        _ask  = float(snap.get("last_quote", {}).get("ask") or 0)
                        if bid > 0 and _ask > 0:
                            mid = round((bid + _ask) / 2.0, 4)
                            log.info("options_monitor.mark_polygon_mid",
                                     contract=contract_symbol, mid=mid, bid=bid, ask=_ask)
                            return mid, "polygon"
                        if close > 0:
                            log.info("options_monitor.mark_polygon_close",
                                     contract=contract_symbol, close=close)
                            return close, "polygon"
        except Exception as _e:
            log.debug("options_monitor.mark_polygon_failed",
                      contract=contract_symbol, error=str(_e))

    # Tier 3: Black-Scholes using VIX as sigma, requires S, K, T
    if current_underlying and strike and expiration_date and option_type in ("call", "put"):
        T = max(0.0, (expiration_date - date.today()).days) / 365.0
        # Fetch VIX from Redis cache (set by market_tone module, 30m TTL)
        sigma = 0.25  # default 25% vol if VIX unavailable
        try:
            from shared.redis_client import get_redis as _gr
            _r = await _gr()
            vix_raw = await _r.get("market:vix:latest")
            if vix_raw:
                vix = float(vix_raw)
                sigma = vix / 100.0
        except Exception:
            pass
        bs = _bs_price(current_underlying, float(strike), T, sigma, option_type=option_type)
        if bs is not None and bs > 0:
            log.info("options_monitor.mark_bs_model",
                     contract=contract_symbol, bs_price=bs, sigma=round(sigma, 3), T=round(T, 3))
            return bs, "bs_model"

    # Tier 4: intrinsic value floor
    if current_underlying and strike and option_type in ("call", "put"):
        intrinsic = _intrinsic_value(current_underlying, float(strike), option_type)
        log.info("options_monitor.mark_intrinsic",
                 contract=contract_symbol, intrinsic=intrinsic)
        return intrinsic, "intrinsic"

    return 0.0, "zero"


async def _fetch_option_chain_details(
    underlying: str,
    current_option_price: float,
    hint_option_type: str = "unknown",
    current_underlying_price: float = 0.0,
    entry_date: Optional[date] = None,
) -> Optional[dict]:
    """
    Look up option contract details (strike, type, expiry, delta) via Polygon.io snapshots.
    Uses the v3/snapshot/options REST endpoint directly via aiohttp (no polygon SDK required).
    - When current_option_price > 0, matches by bid/ask midpoint proximity.
    - When current_option_price == 0, matches by nearest-ATM strike.
    - entry_date: if provided, skips expiry dates that would have been < 14 DTE when
      the position was opened (prevents ITM calls from matching near-weekly expiries).
    Returns dict with strike, option_type, expiration_date, delta — or None.
    """
    import os
    import aiohttp as _aiohttp

    api_key = os.getenv("MASSIVE_API_KEY", "")
    if not api_key:
        return None

    opt_type_filter = hint_option_type if hint_option_type in ("call", "put") else "call"
    MAX_ENTRY_TO_EXPIRY_DAYS = 90
    contracts: list = []

    try:
        async with _aiohttp.ClientSession() as session:
            url    = f"https://api.polygon.io/v3/snapshot/options/{underlying.upper()}"
            params = {"contract_type": opt_type_filter, "limit": 250, "apiKey": api_key}
            while len(contracts) < 500:
                async with session.get(url, params=params,
                                       timeout=_aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status != 200:
                        break
                    data     = await resp.json()
                    results  = data.get("results") or []
                    contracts.extend(results)
                    next_url = data.get("next_url")
                    if not next_url or not results:
                        break
                    url    = next_url
                    params = {"apiKey": api_key}
    except Exception as e:
        log.warning("options_monitor.polygon_chain_error", ticker=underlying, error=str(e))
        return None

    if not contracts:
        return None

    best: Optional[dict] = None
    best_score = float("inf")

    for snap in contracts:
        details = snap.get("details") or {}
        if not details:
            continue
        try:
            exp_d = date.fromisoformat(str(details.get("expiration_date", ""))[:10])
        except Exception:
            continue

        # Cap expiry to entry_date + 90 days
        if entry_date and (exp_d - entry_date).days > MAX_ENTRY_TO_EXPIRY_DAYS:
            continue
        if entry_date and (exp_d - entry_date).days < 14:
            continue

        contract_strike = float(details.get("strike_price") or 0)
        opt_type        = details.get("contract_type") or opt_type_filter

        # Greeks from Polygon snapshot
        greeks_snap = snap.get("greeks") or {}
        delta = float(greeks_snap["delta"]) if greeks_snap.get("delta") is not None else None
        gamma = float(greeks_snap["gamma"]) if greeks_snap.get("gamma") is not None else None
        theta = float(greeks_snap["theta"]) if greeks_snap.get("theta") is not None else None
        vega  = float(greeks_snap["vega"])  if greeks_snap.get("vega")  is not None else None

        # Fallback: compute B-S Greeks from IV if Polygon didn't return them
        rho   = None
        volga = None
        vanna = None
        charm = None
        pop   = None
        if delta is None and current_underlying_price > 0 and contract_strike > 0:
            iv = float(snap.get("implied_volatility") or 0)
            if iv > 0:
                T = max((exp_d - date.today()).days, 1) / 365.0
                g = _binomial_greeks(current_underlying_price, contract_strike, T, iv,
                                    option_type=opt_type)
                delta, gamma, theta, vega = g["delta"], g["gamma"], g["theta"], g["vega"]
                rho, volga = g["rho"], g["volga"]
                vanna, charm, pop = g["vanna"], g["charm"], g["pop"]

        # Scoring
        day_snap = snap.get("day") or {}
        quote    = snap.get("last_quote") or {}
        if current_option_price > 0:
            bid  = float(quote.get("bid_price") or 0)
            ask  = float(quote.get("ask_price") or 0)
            last = float(day_snap.get("close") or 0)
            if bid > 0 and ask > 0:
                ref = (bid + ask) / 2.0
            elif bid > 0:
                ref = bid
            elif last > 0:
                ref = last
            else:
                continue
            score = abs(ref - current_option_price) / current_option_price
            threshold = 0.25
        elif current_underlying_price > 0 and contract_strike > 0:
            score = abs(contract_strike - current_underlying_price) / current_underlying_price
            threshold = 0.25
        else:
            oi = float(snap.get("open_interest") or 1)
            score = 1.0 / max(oi, 1)
            threshold = 1.0

        improvement_required = best_score * 0.50
        if score < improvement_required and score < threshold:
            best_score = score
            best = {
                "strike":          contract_strike,
                "option_type":     opt_type,
                "expiration_date": exp_d,
                "delta":           delta,
                "gamma":           gamma,
                "theta":           theta,
                "vega":            vega,
                "rho":             rho,
                "volga":           volga,
                "vanna":           vanna,
                "charm":           charm,
                "pop":             pop,
                "iv":              float(snap.get("implied_volatility") or 0) or None,
            }

    return best


# ── ATR level builder ─────────────────────────────────────────────────────────

def _build_levels(entry_price: float, atr: float) -> dict:
    """Return all ATR price levels given underlying entry price and ATR value."""
    return {
        "level_emergency":  round(entry_price - 3.0 * atr, 4),
        "level_exit_alert": round(entry_price - 2.0 * atr, 4),
        "level_roll_1":     round(entry_price + 0.5 * atr, 4),
        "level_roll_2":     round(entry_price + 1.0 * atr, 4),
        "level_roll_3":     round(entry_price + 2.0 * atr, 4),
        "extra_roll_levels": [
            {"label": f"Roll {i+4}", "multiplier": float(i + 3),
             "price": round(entry_price + (i + 3) * atr, 4)}
            for i in range(MAX_EXTRA_ROLLS)
        ],
    }


# ── Chart generation ──────────────────────────────────────────────────────────

async def _generate_chart(pos: dict, candles: list[dict]) -> Optional[str]:
    """
    Generate a matplotlib chart for an option position.
    Returns base64-encoded PNG string or None.
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        underlying    = pos["underlying"]
        entry_price   = float(pos["underlying_entry"] or 0)
        atr           = float(pos["atr_14"] or 0)
        entry_date    = pos["entry_date"]
        exp_date      = pos["expiration_date"]

        # Build price series from candles (last 60 days)
        prices = [float(c["close"]) for c in candles[-60:]]
        dates  = list(range(len(prices)))

        fig, ax = plt.subplots(figsize=(12, 6), facecolor="#0d1117")
        ax.set_facecolor("#0d1117")

        # Price line
        ax.plot(dates, prices, color="#58a6ff", linewidth=1.5, label=underlying, zorder=3)

        if entry_price and atr:
            levels = _build_levels(entry_price, atr)

            # Entry price (buy point)
            ax.axhline(entry_price, color="#3fb950", linewidth=1.5, linestyle="--",
                       label=f"Entry ${entry_price:.2f}", zorder=4)

            # Emergency exit
            ax.axhline(levels["level_emergency"], color="#f85149", linewidth=1.5,
                       linestyle="-", label=f"Emergency Exit ${levels['level_emergency']:.2f}", zorder=4)
            ax.axhspan(levels["level_emergency"] - atr * 0.3, levels["level_emergency"],
                       alpha=0.08, color="#f85149")

            # Exit alert
            ax.axhline(levels["level_exit_alert"], color="#d29922", linewidth=1.2,
                       linestyle="--", label=f"Exit Alert ${levels['level_exit_alert']:.2f}", zorder=4)

            # Roll levels
            roll_colors = ["#79c0ff", "#a5d6ff", "#cae8ff"]
            for i, (key, color) in enumerate(zip(
                ["level_roll_1", "level_roll_2", "level_roll_3"], roll_colors
            )):
                roll_n = ["1st", "2nd", "3rd"][i]
                ax.axhline(levels[key], color=color, linewidth=1.0, linestyle=":",
                           label=f"{roll_n} Roll ${levels[key]:.2f}", zorder=4)

        ax.set_title(f"{underlying} — Options Trade | Entry: {entry_date} | Exp: {exp_date}",
                     color="#e6edf3", fontsize=12, pad=10)
        ax.set_xlabel("Days", color="#8b949e")
        ax.set_ylabel("Price ($)", color="#8b949e")
        ax.tick_params(colors="#8b949e")
        for spine in ax.spines.values():
            spine.set_edgecolor("#30363d")
        ax.grid(True, color="#21262d", linewidth=0.5, zorder=0)
        ax.legend(loc="upper left", framealpha=0.4,
                  facecolor="#161b22", edgecolor="#30363d",
                  labelcolor="#e6edf3", fontsize=8)

        buf = io.BytesIO()
        plt.tight_layout()
        plt.savefig(buf, format="png", dpi=120, facecolor=fig.get_facecolor())
        plt.close(fig)
        buf.seek(0)
        return base64.b64encode(buf.read()).decode()
    except Exception as e:
        log.warning("options_monitor.chart_failed", error=str(e))
        return None


# ── Option detection ──────────────────────────────────────────────────────────

def _is_option_position(pos: dict) -> bool:
    """
    Detect whether a normalised position dict is an option contract.
    Handles all three brokers:
      - Webull:  raw.instrument_type == "OPTION"
      - Alpaca:  raw.asset_class == "us_option"
      - Tradier: OCC-format symbol (e.g. AAPL250418C00200000)
    """
    import re
    raw = pos.get("raw", {})
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            raw = {}

    # Webull: instrument_type field in raw
    if raw.get("instrument_type", "").upper() == "OPTION":
        return True

    # Alpaca: asset_class field in raw
    if raw.get("asset_class", "").lower() in ("us_option", "option"):
        return True

    # Normalised asset_class on the position itself
    asset_cls = pos.get("asset_class", "").lower()
    if asset_cls in ("option", "options", "us_option"):
        return True

    # OCC symbol format: 1-6 uppercase letters + 6 digits + C/P + 8 digits
    symbol = pos.get("symbol", "")
    if re.match(r'^[A-Z]{1,6}\d{6}[CP]\d{8}$', symbol.upper()):
        return True

    return False


def _normalise_option_position(pos: dict, acct_label: str, broker: str, mode: str) -> dict:
    """
    Build a normalised option position dict from a raw broker position.
    For Webull (non-OCC), uses instrument_id as part of the synthetic contract symbol,
    and attempts to extract option-specific fields from the raw API response.
    """
    raw = pos.get("raw", {})
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            raw = {}

    symbol = pos.get("symbol", "")

    # Attempt OCC parse
    parsed = _parse_occ_symbol(symbol)
    if parsed:
        contract_symbol  = symbol
        underlying       = parsed["underlying"]
        option_type      = parsed["option_type"]
        strike           = parsed["strike"]
        expiration_date  = parsed["expiration_date"]
    else:
        # Non-OCC (Webull): use instrument_id to make a unique synthetic symbol
        instrument_id    = raw.get("instrument_id", "")
        underlying       = symbol   # symbol IS the underlying ticker for Webull options
        contract_symbol  = f"WBL:{instrument_id}" if instrument_id else f"OPT:{symbol}"

        # Try to extract option-specific fields from Webull raw data
        # Webull API uses various field name conventions across versions
        raw_otype = (
            raw.get("option_type") or raw.get("optionType") or
            raw.get("contract_type") or raw.get("contractType") or
            raw.get("put_call") or raw.get("putCall") or ""
        )
        if str(raw_otype).upper() in ("CALL", "C"):
            option_type = "call"
        elif str(raw_otype).upper() in ("PUT", "P"):
            option_type = "put"
        else:
            # Default to WEBULL_DEFAULT_OPTION_TYPE when not in raw data
            option_type = WEBULL_DEFAULT_OPTION_TYPE

        raw_strike = (
            raw.get("strike_price") or raw.get("strikePrice") or
            raw.get("strike") or raw.get("exercise_price") or
            raw.get("exercisePrice")
        )
        try:
            strike = float(raw_strike) if raw_strike else None
        except (ValueError, TypeError):
            strike = None

        raw_expiry = (
            raw.get("expiry_date") or raw.get("expiryDate") or
            raw.get("expiration_date") or raw.get("expirationDate") or
            raw.get("expire_date") or raw.get("expireDate") or
            raw.get("maturity_date") or raw.get("maturityDate")
        )
        expiration_date = _parse_option_expiry(raw_expiry)

    return {
        "contract_symbol":  contract_symbol,
        "underlying":       underlying,
        "option_type":      option_type,
        "strike":           strike,
        "expiration_date":  expiration_date,
        "account_label":    acct_label,
        "broker":           broker,
        "mode":             mode,
        "qty":              float(pos.get("qty", pos.get("quantity", 0)) or 0),
        "current_price":    float(pos.get("current_price", pos.get("last_price", 0)) or 0),
        "entry_price":      float(pos.get("avg_entry_price", pos.get("unit_cost",
                                  pos.get("avg_cost", pos.get("cost_basis", 0)))) or 0),
        "market_value":     float(pos.get("market_value", 0) or 0),
        "raw":              pos,
    }


# ── Broker position fetcher ───────────────────────────────────────────────────

async def _fetch_option_positions(redis) -> list[dict]:
    """
    Send get_positions to all accounts via broker gateway and extract option contracts.
    Returns list of normalised position dicts.
    """
    request_id = str(uuid.uuid4())
    await redis.xadd(
        STREAMS["broker_commands"],
        {
            "command":       "get_positions",
            "request_id":    request_id,
            "account_label": "",    # empty = all accounts
            "mode":          "",
            "issued_by":     "options-monitor",
        },
        maxlen=10_000,
    )
    reply_raw = await redis.blpop(
        f"broker:reply:{request_id}", timeout=BROKER_GATEWAY_TIMEOUT
    )
    if reply_raw is None:
        log.warning("options_monitor.gateway_timeout")
        return []

    _, reply_json = reply_raw
    try:
        results = json.loads(reply_json)
    except Exception:
        return []
    if not isinstance(results, list):
        results = [results]

    positions = []
    for account_result in results:
        if account_result.get("status") == "error":
            log.debug("options_monitor.account_error",
                      account=account_result.get("account_label"),
                      error=account_result.get("error", "")[:80])
            continue

        acct_label = account_result.get("account_label", "unknown")
        broker     = account_result.get("broker", "")
        mode       = account_result.get("mode", "live")

        # Gateway wraps list results in {"items": [...]} — check both keys
        data    = account_result.get("data", {})
        raw_pos = data.get("items") or data.get("positions") or []
        if not isinstance(raw_pos, list):
            raw_pos = []

        for pos in raw_pos:
            if not _is_option_position(pos):
                continue
            norm = _normalise_option_position(pos, acct_label, broker, mode)
            log.info("options_monitor.option_detected",
                     contract=norm["contract_symbol"],
                     underlying=norm["underlying"],
                     account=acct_label)
            positions.append(norm)

    return positions


# ── OCC symbol parser ─────────────────────────────────────────────────────────

def _parse_occ_symbol(symbol: str) -> Optional[dict]:
    """
    Parse an OCC-format option symbol: AAPL250418C00200000
    Returns dict with underlying, expiration_date, option_type, strike or None.
    """
    import re
    m = re.match(
        r'^([A-Z]{1,6})(\d{2})(\d{2})(\d{2})([CP])(\d{8})$',
        symbol.upper()
    )
    if not m:
        return None
    underlying, yy, mm, dd, otype, strike_raw = m.groups()
    try:
        exp_date  = date(2000 + int(yy), int(mm), int(dd))
        strike    = int(strike_raw) / 1000.0
        opt_type  = "call" if otype == "C" else "put"
        return {
            "underlying":      underlying,
            "expiration_date": exp_date,
            "option_type":     opt_type,
            "strike":          strike,
        }
    except Exception:
        return None


# ── Account name mapping ──────────────────────────────────────────────────────

def _load_account_names() -> dict[str, str]:
    """
    Load account label → friendly display name.
    Priority: {LABEL_UPPER}_DISPLAY_NAME env var → accounts.toml notes → formatted label.
    Matches the same convention used by the webui broker/positions pages.
    """
    try:
        import tomllib
        with open("/app/config/accounts.toml", "rb") as f:
            cfg = tomllib.load(f)
        mapping = {}
        for acct in cfg.get("accounts", []):
            label = acct.get("label", "")
            if not label:
                continue
            # Check env var first: WEBULL_LIVE_2_DISPLAY_NAME etc.
            env_key = label.upper().replace("-", "_") + "_DISPLAY_NAME"
            env_name = os.getenv(env_key, "").strip()
            if env_name:
                mapping[label] = env_name
                continue
            # Fall back to notes field
            notes = acct.get("notes", "").strip()
            if notes:
                name = notes.split("—")[0].strip() if "—" in notes else notes
            else:
                name = label.replace("-", " ").title()
            mapping[label] = name
        return mapping
    except Exception:
        return {}


# ── Main agent ────────────────────────────────────────────────────────────────

class OptionsMonitor(BaseAgent):

    def __init__(self):
        super().__init__("options-monitor")
        self._account_names: dict[str, str] = {}

    async def run(self):
        await self.setup()
        self.redis = await get_redis()
        self._account_names = _load_account_names()
        log.info("options_monitor.starting")
        await asyncio.gather(
            self.heartbeat_loop(),
            self._scan_loop(),
        )

    async def _scan_loop(self):
        """
        Scan logic:
        - Every SCAN_INTERVAL_MIN during market hours + 1 h after close
        - Always run once at startup (EOD import on first boot)
        """
        # Run initial scan immediately
        await self._run_scan()
        while self._running:
            await asyncio.sleep(SCAN_INTERVAL_MIN * 60)
            await self._run_scan()

    async def _run_scan(self):
        log.info("options_monitor.scan_start")
        pool = await _get_pool()

        # ── 1. Fetch all option positions from brokers ─────────────────────────
        broker_positions = await _fetch_option_positions(self.redis)
        if not broker_positions:
            log.info("options_monitor.no_option_positions")
            return

        log.info("options_monitor.positions_found", count=len(broker_positions))

        for bp in broker_positions:
            try:
                await self._process_position(pool, bp)
            except Exception as e:
                log.error("options_monitor.position_error",
                           contract=bp.get("contract_symbol"), error=str(e))

        # ── 2. Mark any DB-active positions not seen in this scan ─────────────
        seen_keys = {
            f"{p['contract_symbol']}:{p['account_label']}"
            for p in broker_positions
        }
        active_rows = await pool.fetch(
            """SELECT id, contract_symbol, account_label, underlying,
                      entry_price, qty, expiration_date
               FROM option_positions WHERE status='active'"""
        )
        for row in active_rows:
            key = f"{row['contract_symbol']}:{row['account_label']}"
            if key not in seen_keys:
                # Require MISS_THRESHOLD consecutive absent scans before closing.
                # A single Webull API dropout must not create a phantom position.
                miss_key   = f"options:miss:{row['id']}"
                miss_count = await self.redis.incr(miss_key)
                # TTL: auto-expire if the position reappears and misses reset
                await self.redis.expire(miss_key, SCAN_INTERVAL_MIN * 60 * (MISS_THRESHOLD + 3))

                if miss_count < MISS_THRESHOLD:
                    log.info("options_monitor.position_absent",
                             contract=row["contract_symbol"],
                             account=row["account_label"],
                             misses=miss_count, threshold=MISS_THRESHOLD)
                    continue

                # Threshold reached — position is genuinely gone, close it
                await self.redis.delete(miss_key)

                # Fetch last known contract price from most recent scan event
                last_scan = await pool.fetchrow(
                    """SELECT contract_price FROM option_trade_log
                       WHERE position_id=$1 AND event_type='scan'
                         AND contract_price IS NOT NULL
                       ORDER BY ts DESC LIMIT 1""",
                    row["id"],
                )
                last_cp = float(last_scan["contract_price"]) if last_scan else None
                ep      = float(row["entry_price"]) if row["entry_price"] else None
                qty     = float(row["qty"]) if row["qty"] else None

                # If no price captured but option is past expiration, treat as expired worthless
                if last_cp is None and row["expiration_date"] and row["expiration_date"] < date.today():
                    last_cp = 0.0

                pnl = None
                if last_cp is not None and ep is not None and qty is not None:
                    pnl = round((last_cp - ep) * abs(qty) * 100, 2)

                async with pool.acquire() as conn:
                    async with conn.transaction():
                        await conn.execute(
                            """UPDATE option_positions
                               SET status='closed', closed_at=NOW(), close_reason='not_in_scan',
                                   total_realized_pnl=$2, updated_at=NOW()
                               WHERE id=$1""",
                            row["id"], pnl,
                        )
                        await conn.execute(
                            """INSERT INTO option_trade_log
                               (position_id, contract_symbol, underlying, event_type,
                                contract_price, realized_pnl, notes)
                               VALUES ($1,$2,$3,'closed',$4::NUMERIC,$5::NUMERIC,$6)""",
                            row["id"], row["contract_symbol"], row["underlying"],
                            last_cp, pnl,
                            f"Position closed — no longer in broker scan (last price: "
                            f"{'${:.2f}'.format(last_cp) if last_cp else 'unknown'})",
                        )

                log.info("options_monitor.position_closed_not_seen",
                         contract=row["contract_symbol"], account=row["account_label"],
                         last_price=last_cp, pnl=pnl)

        # ── 3. Scan OI walls for each active underlying ───────────────────────
        await self._scan_oi_walls(pool)

        # ── 4. Scan unusual options flow (volume delta + importance score) ────
        await self._scan_unusual_flow(pool)

        log.info("options_monitor.scan_complete")

    async def _scan_oi_walls(self, pool: asyncpg.Pool):
        """
        Scan options chains for collapsing OI walls (Item 1: OI Wall Detector).
        An OI wall is a strike with >= OI_WALL_MIN_OI open interest.
        When a wall drops >= OI_WALL_DROP_THRESHOLD between scans, fire an alert
        and write a signal to the DB.
        Confidence = oi_size_pct×0.40 + drop_speed×0.35 + price_proximity×0.25
        """
        if not POLYGON_API_KEY:
            return

        rows = await pool.fetch(
            "SELECT DISTINCT underlying FROM option_positions WHERE status='active'"
        )
        underlyings = [r["underlying"] for r in rows]
        if not underlyings:
            return

        import aiohttp as _ah

        for underlying in underlyings:
            try:
                url = (
                    f"https://api.polygon.io/v3/snapshot/options/{underlying.upper()}"
                    f"?limit=250&apiKey={POLYGON_API_KEY}"
                )
                async with _ah.ClientSession() as sess:
                    async with sess.get(url, timeout=_ah.ClientTimeout(total=15)) as resp:
                        if resp.status != 200:
                            continue
                        data = await resp.json()

                snaps = data.get("results") or []
                if not snaps:
                    continue

                # Aggregate OI by strike across calls and puts
                strike_oi: dict[str, int] = {}
                for snap in snaps:
                    details = snap.get("details") or {}
                    strike  = details.get("strike_price")
                    oi      = int(snap.get("open_interest") or 0)
                    if strike and oi > 0:
                        key = str(float(strike))
                        strike_oi[key] = strike_oi.get(key, 0) + oi

                if not strike_oi:
                    continue

                max_oi = max(strike_oi.values())
                current_price = await _fetch_underlying_price(underlying)

                redis_key = f"options:oi_walls:{underlying}"
                prev_raw  = await self.redis.get(redis_key)
                prev_walls: dict[str, int] = json.loads(prev_raw) if prev_raw else {}

                for strike_key, oi in strike_oi.items():
                    if oi < OI_WALL_MIN_OI:
                        continue
                    prev_oi = prev_walls.get(strike_key, 0)
                    if prev_oi <= 0:
                        continue

                    drop_pct = (prev_oi - oi) / prev_oi
                    if drop_pct < OI_WALL_DROP_THRESHOLD:
                        continue

                    strike_f = float(strike_key)
                    # Confidence: oi_size_pct×0.40 + drop_speed×0.35 + proximity×0.25
                    oi_size_pct = min(1.0, oi / max_oi)
                    speed_score = min(1.0, drop_pct)
                    proximity   = 0.5
                    if current_price and current_price > 0:
                        pct_away = abs(strike_f - current_price) / current_price
                        proximity = max(0.0, 1.0 - pct_away * 10)

                    confidence  = round(oi_size_pct * 0.40 + speed_score * 0.35 + proximity * 0.25, 3)
                    direction   = "bullish" if strike_f < (current_price or strike_f) else "bearish"

                    log.warning(
                        "options_monitor.oi_wall_collapse",
                        underlying=underlying, strike=strike_f,
                        prev_oi=prev_oi, current_oi=oi,
                        drop_pct=round(drop_pct * 100, 1),
                        confidence=confidence,
                    )

                    # Alert to Redis
                    try:
                        await self.redis.xadd(
                            STREAMS.get("alerts", "system.alerts"),
                            {
                                "source":  "options-monitor",
                                "level":   "warning",
                                "title":   f"OI Wall Collapsing: {underlying} ${strike_f:.0f}",
                                "message": (
                                    f"Strike ${strike_f:.0f} OI dropped {drop_pct*100:.0f}% "
                                    f"({prev_oi:,} → {oi:,}) | Confidence {confidence:.2f}"
                                ),
                                "ticker":  underlying,
                            },
                            maxlen=5_000,
                        )
                    except Exception:
                        pass

                    # Write to signals table (stocks/options only — no crypto)
                    try:
                        await pool.execute(
                            """INSERT INTO signals (source, ticker, direction, confidence, payload)
                               VALUES ('oi_wall', $1, $2, $3::NUMERIC, $4::JSONB)""",
                            underlying, direction, confidence,
                            json.dumps({
                                "strike":     strike_f,
                                "prev_oi":    prev_oi,
                                "current_oi": oi,
                                "drop_pct":   round(drop_pct * 100, 1),
                                "asset_class": "option",
                            }),
                        )
                    except Exception as _dbe:
                        log.debug("options_monitor.oi_wall_db_error", error=str(_dbe))

                # Persist current scan as baseline for next comparison
                new_walls = {k: v for k, v in strike_oi.items() if v >= OI_WALL_MIN_OI}
                await self.redis.setex(redis_key, OI_WALL_CACHE_TTL, json.dumps(new_walls))

            except Exception as _oe:
                log.error("options_monitor.oi_wall_scan_error", underlying=underlying, error=str(_oe))

    async def _scan_unusual_flow(self, pool: asyncpg.Pool):
        """
        Detect unusual options flow for each active underlying.

        Per scan cycle:
          1. Fetch Polygon v3 snapshot for each underlying (same key used by OI wall scan).
          2. Load per-contract volume baseline from Redis (options:vol_snap:{underlying}).
          3. Compute vol_delta = current_day_volume - baseline_volume per contract.
          4. Filter contracts below FLOW_MIN_VOL_DELTA or FLOW_MIN_NOTIONAL.
          5. Score each hit: 0.40 × norm_notional + 0.30 × norm_vol_delta
                            + 0.20 × type_weight  + 0.10 × dir_confidence
          6. Write top FLOW_TOP_N hits to Redis "options:flow:latest" (FLOW_CACHE_TTL).
          7. Persist updated volume snapshot for next cycle comparison.
        """
        if not POLYGON_API_KEY:
            return

        rows = await pool.fetch(
            "SELECT DISTINCT underlying FROM option_positions WHERE status='active'"
        )
        underlyings = [r["underlying"] for r in rows]
        if not underlyings:
            return

        import aiohttp as _ah

        flow_hits: list[dict] = []

        for underlying in underlyings:
            try:
                url = (
                    f"https://api.polygon.io/v3/snapshot/options/{underlying.upper()}"
                    f"?limit=250&apiKey={POLYGON_API_KEY}"
                )
                async with _ah.ClientSession() as sess:
                    async with sess.get(url, timeout=_ah.ClientTimeout(total=15)) as resp:
                        if resp.status != 200:
                            continue
                        data = await resp.json()

                snaps = data.get("results") or []
                if not snaps:
                    continue

                # Load previous volume baseline
                snap_key  = f"options:vol_snap:{underlying}"
                prev_raw  = await self.redis.get(snap_key)
                prev_vols: dict[str, int] = json.loads(prev_raw) if prev_raw else {}
                new_vols:  dict[str, int] = {}

                candidates: list[dict] = []

                for snap in snaps:
                    details    = snap.get("details") or {}
                    day        = snap.get("day") or {}
                    greeks     = snap.get("greeks") or {}
                    last_quote = snap.get("last_quote") or {}

                    contract = snap.get("ticker", "")
                    if not contract:
                        continue

                    vol    = int(day.get("volume") or 0)
                    price  = float(
                        day.get("vwap") or day.get("last_price") or
                        last_quote.get("midpoint") or 0
                    )
                    strike     = float(details.get("strike_price") or 0)
                    expiry     = details.get("expiration_date") or ""
                    ctype      = (details.get("contract_type") or "").lower()
                    change_pct = float(day.get("change_percent") or 0)
                    delta      = float(greeks.get("delta") or 0)

                    new_vols[contract] = vol
                    vol_delta = max(0, vol - prev_vols.get(contract, 0))

                    if vol_delta < FLOW_MIN_VOL_DELTA or price <= 0:
                        continue
                    notional = vol_delta * price * 100
                    if notional < FLOW_MIN_NOTIONAL:
                        continue

                    # Direction inference — 3-method fallback with confidence score
                    direction      = "unknown"
                    dir_confidence = 0.5
                    if delta != 0:
                        direction      = "bullish" if delta > 0 else "bearish"
                        dir_confidence = min(0.9, 0.5 + abs(delta) * 0.4)
                    elif change_pct != 0:
                        direction      = "bullish" if change_pct > 0 else "bearish"
                        dir_confidence = 0.65
                    elif ctype == "call":
                        direction      = "bullish"
                        dir_confidence = 0.5
                    elif ctype == "put":
                        direction      = "bearish"
                        dir_confidence = 0.5

                    candidates.append({
                        "underlying":     underlying,
                        "contract":       contract,
                        "strike":         strike,
                        "expiry":         expiry,
                        "type":           ctype,
                        "price":          round(price, 2),
                        "vol_delta":      vol_delta,
                        "notional":       round(notional),
                        "direction":      direction,
                        "dir_confidence": round(dir_confidence, 2),
                        "change_pct":     round(change_pct, 2),
                        "delta":          round(delta, 3),
                    })

                # Score candidates within this underlying
                if candidates:
                    max_n = max(c["notional"]  for c in candidates) or 1
                    max_v = max(c["vol_delta"]  for c in candidates) or 1
                    for c in candidates:
                        norm_n = c["notional"]  / max_n
                        norm_v = c["vol_delta"]  / max_v
                        # type_weight: both call and put score equally (flow is directionally agnostic)
                        tw = 0.85 if c["type"] in ("call", "put") else 0.5
                        c["score"] = round(
                            0.40 * norm_n + 0.30 * norm_v + 0.20 * tw + 0.10 * c["dir_confidence"],
                            3,
                        )
                    flow_hits.extend(candidates)

                # Persist updated volume baseline for next scan
                await self.redis.setex(snap_key, VOL_SNAP_TTL, json.dumps(new_vols))

            except Exception as _fe:
                log.error("options_monitor.flow_scan_error", underlying=underlying, error=str(_fe))

        # Sort globally by score, keep top N
        flow_hits.sort(key=lambda x: -x["score"])
        top_hits = flow_hits[:FLOW_TOP_N]

        payload = json.dumps({
            "hits":  top_hits,
            "ts":    datetime.now(timezone.utc).isoformat(),
            "count": len(top_hits),
        })
        await self.redis.setex("options:flow:latest", FLOW_CACHE_TTL, payload)

        if top_hits:
            log.info(
                "options_monitor.flow_scan_complete",
                hits=len(top_hits),
                top_score=top_hits[0]["score"],
                top_contract=top_hits[0]["contract"],
            )

    async def _process_position(self, pool: asyncpg.Pool, bp: dict):
        contract_symbol = bp["contract_symbol"]
        account_label   = bp["account_label"]
        account_name    = self._account_names.get(account_label, account_label)

        # Fields already resolved by _normalise_option_position
        underlying      = bp["underlying"]
        expiration_date = bp.get("expiration_date")   # may be None for non-OCC
        option_type     = bp.get("option_type", "unknown")
        strike          = bp.get("strike")
        today           = date.today()

        # ── Look up existing DB record ─────────────────────────────────────────
        existing = await pool.fetchrow(
            """SELECT * FROM option_positions
               WHERE contract_symbol=$1 AND account_label=$2 AND status='active'""",
            contract_symbol, account_label,
        )

        # Position is visible — clear any accumulated miss count so a partial
        # dropout doesn't carry over into the next absence window.
        if existing:
            await self.redis.delete(f"options:miss:{existing['id']}")

        # ── Fetch ATR + current price (one TV MCP call, refresh if >4 h old) ───
        atr = None
        current_underlying = None
        if existing and existing["atr_14"] and existing["atr_calculated_at"]:
            age = (datetime.now(timezone.utc) - existing["atr_calculated_at"]).total_seconds()
            if age < 4 * 3600:
                atr = float(existing["atr_14"])

        if atr is None:
            # Fetch both ATR and current price from TradingView historical data
            atr, current_underlying = await _fetch_atr_and_price(underlying)
        else:
            # ATR fresh — still need current price; try Yahoo first, fall back to TV
            current_underlying = await _fetch_underlying_price(underlying)
            if current_underlying is None:
                _, current_underlying = await _fetch_atr_and_price(underlying)

        # ── Fetch earnings date (once per position) ───────────────────────────
        earnings_date = None
        if existing and existing["next_earnings_date"]:
            earnings_date = existing["next_earnings_date"]
        else:
            earnings_date = await _fetch_earnings_date(underlying)

        # ── Ex-dividend early-assignment risk check (calls only) ──────────────
        # Flag when next ex-date falls before our expiration — call holders may
        # exercise early to capture the dividend, forcing assignment.
        # Adapted from option_screener find_roll_outs.py dividend risk logic.
        ex_div_date = await _fetch_ex_dividend_date(underlying)
        if _check_early_assignment_risk(option_type, expiration_date, ex_div_date):
            days_to_ex = (ex_div_date - date.today()).days
            log.warning(
                "options_monitor.early_assignment_risk",
                contract=contract_symbol,
                underlying=underlying,
                ex_div_date=ex_div_date.isoformat(),
                expiration_date=expiration_date.isoformat() if expiration_date else None,
                days_to_ex_div=days_to_ex,
            )
            if days_to_ex <= 10:
                try:
                    _r = await get_redis()
                    await _r.xadd(
                        STREAMS.get("alerts", "system.alerts"),
                        {
                            "source":   "options-monitor",
                            "level":    "warning",
                            "title":    f"Early Assignment Risk: {underlying}",
                            "message":  (
                                f"⚠️ {contract_symbol} | Ex-div {ex_div_date} is "
                                f"{days_to_ex}d away — before expiry {expiration_date}. "
                                f"Call may be exercised early to capture dividend."
                            ),
                            "ticker":   underlying,
                            "contract": contract_symbol,
                        },
                        maxlen=5_000,
                    )
                except Exception:
                    pass

        # Initialise entry_date early (needed by chain lookup below).
        # Will be overridden below if an existing DB row is found.
        entry_date = existing["entry_date"] if existing else today

        # ── Enrich contract details via Yahoo option chain ─────────────────────
        # Runs whenever: type unknown, strike missing, expiry missing, or no delta yet
        delta = theta = vega = gamma = rho = volga = vanna = charm = pop = None
        current_iv = None
        current_opt_price = bp["current_price"]
        # Use only the live price — entry_price is stale and causes wrong expiry matches
        # for decayed options (e.g. deep-OTM near expiry matched against a later expiry
        # whose price happens to be close to the original cost basis).
        price_ref = current_opt_price if current_opt_price > 0 else 0
        expiry_locked = bool(existing and existing.get("expiry_locked"))
        # Run chain enrichment when any key field is missing.
        # Check BOTH the incoming bp (broker position) AND the existing DB row:
        # Webull v1 positions never carry strike/expiry in bp, so check the DB first.
        db_strike  = existing.get("strike")           if existing else None
        db_expiry  = existing.get("expiration_date")  if existing else None
        db_delta   = existing.get("delta")            if existing else None
        effective_strike  = strike           or db_strike
        effective_expiry  = expiration_date  or db_expiry
        needs_enrichment = (
            not expiry_locked and (
                option_type == "unknown"
                or effective_strike is None
                or effective_expiry is None
                or db_delta is None
            )
        )
        if needs_enrichment:
            chain_details = await _fetch_option_chain_details(
                underlying, float(price_ref or 0), hint_option_type=option_type,
                current_underlying_price=float(current_underlying or 0),
                entry_date=entry_date,
            )
            if chain_details:
                if option_type == "unknown" and chain_details.get("option_type"):
                    option_type = chain_details["option_type"]
                    log.info("options_monitor.type_resolved", contract=contract_symbol,
                             option_type=option_type)
                if strike is None and chain_details.get("strike"):
                    strike = chain_details["strike"]
                if expiration_date is None and chain_details.get("expiration_date"):
                    expiration_date = chain_details["expiration_date"]
                if chain_details.get("delta") is not None:
                    delta = chain_details["delta"]
                theta = chain_details.get("theta")
                vega  = chain_details.get("vega")
                gamma = chain_details.get("gamma")
                rho   = chain_details.get("rho")
                volga = chain_details.get("volga")
                vanna = chain_details.get("vanna")
                charm = chain_details.get("charm")
                pop   = chain_details.get("pop")
                current_iv = chain_details.get("iv")
                log.info("options_monitor.chain_enriched", contract=contract_symbol,
                         strike=strike, option_type=option_type, delta=delta,
                         theta=theta, vega=vega, gamma=gamma)
            else:
                log.debug("options_monitor.chain_no_match", contract=contract_symbol,
                          underlying=underlying, price_ref=price_ref)

        # Fall back to DB values when chain lookup didn't run or found nothing
        if strike is None and db_strike is not None:
            strike = float(db_strike)
        if expiration_date is None and db_expiry is not None:
            expiration_date = db_expiry
        if delta is None and existing and existing.get("delta") is not None:
            try:
                delta = float(existing["delta"])
            except Exception:
                pass
        if theta is None and existing and existing.get("theta") is not None:
            try:
                theta = float(existing["theta"])
            except Exception:
                pass
        if vega is None and existing and existing.get("vega") is not None:
            try:
                vega = float(existing["vega"])
            except Exception:
                pass
        if gamma is None and existing and existing.get("gamma") is not None:
            try:
                gamma = float(existing["gamma"])
            except Exception:
                pass
        if rho is None and existing and existing.get("rho") is not None:
            try:
                rho = float(existing["rho"])
            except Exception:
                pass
        if volga is None and existing and existing.get("volga") is not None:
            try:
                volga = float(existing["volga"])
            except Exception:
                pass
        if vanna is None and existing and existing.get("vanna") is not None:
            try:
                vanna = float(existing["vanna"])
            except Exception:
                pass
        if charm is None and existing and existing.get("charm") is not None:
            try:
                charm = float(existing["charm"])
            except Exception:
                pass
        if pop is None and existing and existing.get("pop") is not None:
            try:
                pop = float(existing["pop"])
            except Exception:
                pass

        # ── Market-aware option mark (BYE valuation fallback) ─────────────────
        # When the broker returns 0 (Webull scan dropout, after-hours gap),
        # fall through: Polygon snapshot → Black-Scholes (VIX sigma) → intrinsic.
        # current_opt_price now always holds the best available mark.
        raw_broker_price = bp["current_price"]
        if raw_broker_price and raw_broker_price > 0:
            current_opt_price = raw_broker_price
            mark_source       = "broker"
        else:
            current_opt_price, mark_source = await _compute_option_mark(
                raw_broker_price,
                contract_symbol,
                underlying,
                option_type,
                strike,
                expiration_date,
                current_underlying,
            )
            if mark_source != "broker":
                log.info(
                    "options_monitor.mark_fallback_used",
                    contract=contract_symbol,
                    source=mark_source,
                    mark=current_opt_price,
                )

        # ── Determine entry price / date ──────────────────────────────────────
        entry_price_option    = bp["entry_price"]  # option premium
        underlying_entry      = None
        entry_date            = today

        if existing:
            underlying_entry = float(existing["underlying_entry"]) if existing["underlying_entry"] else None
            entry_date       = existing["entry_date"]
            entry_price_option = float(existing["entry_price"] or entry_price_option)

        # If this is a new position and we have a current price, use it as entry
        if underlying_entry is None and current_underlying:
            underlying_entry = current_underlying

        # ── Compute ATR levels ────────────────────────────────────────────────
        levels = {}
        if underlying_entry and atr:
            levels = _build_levels(underlying_entry, atr)

        # ── Upsert into DB ────────────────────────────────────────────────────
        if existing:
            pos_id = existing["id"]
            await pool.execute(
                """UPDATE option_positions SET
                    updated_at          = NOW(),
                    qty                 = $2::NUMERIC,
                    atr_14              = $3::NUMERIC,
                    atr_calculated_at   = CASE WHEN $3::NUMERIC IS NOT NULL THEN NOW()
                                              ELSE atr_calculated_at END,
                    underlying_entry    = COALESCE(underlying_entry, $12::NUMERIC),
                    level_emergency     = $4::NUMERIC,
                    level_exit_alert    = $5::NUMERIC,
                    level_roll_1        = $6::NUMERIC,
                    level_roll_2        = $7::NUMERIC,
                    level_roll_3        = $8::NUMERIC,
                    extra_roll_levels   = $9::JSONB,
                    next_earnings_date  = $10,
                    account_name        = $17,
                    last_scan_at        = NOW(),
                    raw                 = $11::JSONB,
                    delta               = $13::NUMERIC,
                    theta               = $18::NUMERIC,
                    vega                = $19::NUMERIC,
                    gamma               = $20::NUMERIC,
                    rho                 = $21::NUMERIC,
                    volga               = $22::NUMERIC,
                    vanna               = $23::NUMERIC,
                    charm               = $24::NUMERIC,
                    pop                 = $25::NUMERIC,
                    option_type         = CASE WHEN option_type='unknown' AND $14::TEXT IS NOT NULL
                                              THEN $14::TEXT ELSE option_type END,
                    strike              = COALESCE($15::NUMERIC, strike),
                    expiration_date     = COALESCE($16::DATE, expiration_date),
                    expiry_locked       = CASE WHEN $16::DATE IS NOT NULL THEN TRUE ELSE expiry_locked END
                   WHERE id=$1""",
                pos_id,
                bp["qty"],
                atr,
                levels.get("level_emergency"),
                levels.get("level_exit_alert"),
                levels.get("level_roll_1"),
                levels.get("level_roll_2"),
                levels.get("level_roll_3"),
                json.dumps(levels.get("extra_roll_levels", [])),
                earnings_date,
                json.dumps(bp["raw"]),
                underlying_entry,   # $12 — only fills in if currently NULL
                delta,              # $13
                option_type if option_type != "unknown" else None,  # $14
                strike,             # $15
                expiration_date,    # $16
                account_name,       # $17 — always refresh from accounts.toml
                theta,              # $18
                vega,               # $19
                gamma,              # $20
                rho,                # $21
                volga,              # $22
                vanna,              # $23
                charm,              # $24
                pop,                # $25
            )
        else:
            # New position
            pos_id = await pool.fetchval(
                """INSERT INTO option_positions (
                    contract_symbol, underlying, option_type, strike, expiration_date,
                    account_label, account_name, broker, mode,
                    qty, entry_price, underlying_entry, entry_date,
                    atr_14, atr_calculated_at,
                    level_emergency, level_exit_alert, level_roll_1, level_roll_2, level_roll_3,
                    extra_roll_levels, next_earnings_date, last_scan_at, raw,
                    delta, theta, vega, gamma, rho, volga, vanna, charm, pop
                ) VALUES (
                    $1,$2,$3,$4::NUMERIC,$5,
                    $6,$7,$8,$9,
                    $10::NUMERIC,$11::NUMERIC,$12::NUMERIC,$13,
                    $14::NUMERIC, CASE WHEN $14::NUMERIC IS NOT NULL THEN NOW() ELSE NULL END,
                    $15::NUMERIC,$16::NUMERIC,$17::NUMERIC,$18::NUMERIC,$19::NUMERIC,
                    $20::JSONB,$21,NOW(),$22::JSONB,
                    $23::NUMERIC,$24::NUMERIC,$25::NUMERIC,$26::NUMERIC,$27::NUMERIC,$28::NUMERIC,$29::NUMERIC,$30::NUMERIC,$31::NUMERIC
                ) ON CONFLICT (contract_symbol, account_label) WHERE status='active'
                DO UPDATE SET
                    qty=$10::NUMERIC, atr_14=$14::NUMERIC,
                    atr_calculated_at = CASE WHEN $14::NUMERIC IS NOT NULL THEN NOW()
                                            ELSE option_positions.atr_calculated_at END,
                    underlying_entry = COALESCE(option_positions.underlying_entry, $12::NUMERIC),
                    level_emergency=$15::NUMERIC, level_exit_alert=$16::NUMERIC,
                    level_roll_1=$17::NUMERIC, level_roll_2=$18::NUMERIC, level_roll_3=$19::NUMERIC,
                    extra_roll_levels=$20::JSONB, next_earnings_date=$21,
                    last_scan_at=NOW(), updated_at=NOW(), raw=$22::JSONB,
                    delta=$23::NUMERIC, theta=$24::NUMERIC, vega=$25::NUMERIC, gamma=$26::NUMERIC,
                    rho=$27::NUMERIC, volga=$28::NUMERIC, vanna=$29::NUMERIC, charm=$30::NUMERIC, pop=$31::NUMERIC,
                    option_type = CASE WHEN option_positions.option_type='unknown' AND $3 IS NOT NULL AND $3 != 'unknown'
                                      THEN $3 ELSE option_positions.option_type END,
                    strike = COALESCE($4::NUMERIC, option_positions.strike),
                    expiration_date = COALESCE($5, option_positions.expiration_date)
                RETURNING id""",
                contract_symbol, underlying, option_type, strike, expiration_date,
                account_label, account_name, bp["broker"], bp["mode"],
                bp["qty"], entry_price_option, underlying_entry, entry_date,
                atr,
                levels.get("level_emergency"), levels.get("level_exit_alert"),
                levels.get("level_roll_1"), levels.get("level_roll_2"), levels.get("level_roll_3"),
                json.dumps(levels.get("extra_roll_levels", [])),
                earnings_date,
                json.dumps(bp["raw"]),
                delta, theta, vega, gamma, rho, volga, vanna, charm, pop,
            )
            log.info("options_monitor.position_imported",
                     contract=contract_symbol, account=account_label, underlying=underlying)

        # ── Log scan event ────────────────────────────────────────────────────
        dist_emergency  = None
        dist_exit_alert = None
        dist_roll_1     = None
        if current_underlying and levels:
            dist_emergency  = round(current_underlying - levels["level_emergency"], 4)
            dist_exit_alert = round(current_underlying - levels["level_exit_alert"], 4)
            dist_roll_1     = round(current_underlying - levels["level_roll_1"], 4)

        # notes field records fallback source when broker price was unavailable
        scan_notes = None if mark_source == "broker" else f"mark_source={mark_source}"

        await pool.execute(
            """INSERT INTO option_trade_log
               (position_id, contract_symbol, underlying, event_type,
                underlying_price, contract_price, atr_value,
                distance_emergency, distance_exit_alert, distance_roll_1, notes)
               VALUES ($1,$2,$3,'scan',
                       $4::NUMERIC,$5::NUMERIC,$6::NUMERIC,
                       $7::NUMERIC,$8::NUMERIC,$9::NUMERIC,$10)""",
            pos_id, contract_symbol, underlying,
            current_underlying, current_opt_price, atr,
            dist_emergency, dist_exit_alert, dist_roll_1, scan_notes,
        )

        # ── Greeks history snapshot ────────────────────────────────────────────
        dte_val = None
        if expiration_date:
            try:
                dte_val = max((expiration_date - date.today()).days, 0)
            except Exception:
                pass
        try:
            await pool.execute(
                """INSERT INTO greeks_history
                   (position_id, contract_symbol, underlying,
                    underlying_price, contract_price,
                    delta, gamma, theta, vega, rho, iv, dte)
                   VALUES ($1,$2,$3,$4::NUMERIC,$5::NUMERIC,
                           $6::NUMERIC,$7::NUMERIC,$8::NUMERIC,$9::NUMERIC,$10::NUMERIC,
                           $11::NUMERIC,$12)""",
                pos_id, contract_symbol, underlying,
                current_underlying, current_opt_price,
                delta, gamma, theta, vega, rho, current_iv, dte_val,
            )
        except Exception as _gh_err:
            log.debug("options_monitor.greeks_history_skip", error=str(_gh_err))

        # ── Check alert thresholds ────────────────────────────────────────────
        if current_underlying and levels and underlying_entry:
            await self._check_alerts(
                pool, pos_id, contract_symbol, underlying,
                current_underlying, levels, existing,
                option_type=option_type,
                current_strike=strike,
                current_expiry=expiration_date,
                current_contract_bid=current_opt_price,  # use resolved mark, not raw broker 0
            )

        # ── Generate/refresh chart (async, don't block scan) ─────────────────
        task = asyncio.create_task(
            self._refresh_chart(pool, pos_id, contract_symbol, underlying,
                                underlying_entry, atr, expiration_date, entry_date)
        )
        task.add_done_callback(
            lambda t: log.warning("options_monitor.chart_task_error",
                                  contract=contract_symbol,
                                  error=str(t.exception())) if not t.cancelled() and t.exception() else None
        )

    async def _check_alerts(
        self,
        pool: asyncpg.Pool,
        pos_id: uuid.UUID,
        contract: str,
        underlying: str,
        current_price: float,
        levels: dict,
        existing,
        option_type: str = "call",
        current_strike: Optional[float] = None,
        current_expiry: Optional[date] = None,
        current_contract_bid: float = 0.0,
    ):
        """Fire Redis alerts when price crosses ATR thresholds (once per level)."""
        alerts_fired = {}
        if existing and existing["alerts_fired"]:
            try:
                alerts_fired = json.loads(existing["alerts_fired"])
            except Exception:
                pass

        alert_map = [
            ("emergency",  levels["level_emergency"],  "EMERGENCY EXIT",  "🚨"),
            ("exit_alert", levels["level_exit_alert"], "Exit Alert",      "⚠️"),
            ("roll_1",     levels["level_roll_1"],     "1st Roll Signal", "📈"),
            ("roll_2",     levels["level_roll_2"],     "2nd Roll Signal", "📈"),
            ("roll_3",     levels["level_roll_3"],     "3rd Roll Signal", "📈"),
        ]
        for extra in levels.get("extra_roll_levels", []):
            key   = f"roll_extra_{extra['multiplier']:.0f}"
            label = extra["label"]
            alert_map.append((key, extra["price"], f"{label} Signal", "📈"))

        for alert_key, threshold, label, emoji in alert_map:
            if alerts_fired.get(alert_key):
                continue  # already fired

            triggered = False
            if "emergency" in alert_key or "exit" in alert_key:
                triggered = current_price <= threshold
            else:
                triggered = current_price >= threshold

            if not triggered:
                continue

            log.warning("options_monitor.alert_triggered",
                        contract=contract, underlying=underlying,
                        alert=alert_key, price=current_price, threshold=threshold)

            # Publish alert to Redis
            await self.redis.xadd(
                STREAMS.get("alerts", "system.alerts"),
                {
                    "source":   "options-monitor",
                    "level":    "critical" if "emergency" in alert_key else "warning",
                    "title":    f"Options {label}: {underlying}",
                    "message":  (
                        f"{emoji} {contract} | {label} at ${threshold:.2f} | "
                        f"Current: ${current_price:.2f}"
                    ),
                    "ticker":   underlying,
                    "contract": contract,
                },
                maxlen=5_000,
            )

            # ── Roll-candidate scoring on any roll signal ──────────────────
            roll_note = f"{label} triggered at underlying=${current_price:.2f}, threshold=${threshold:.2f}"
            if "roll" in alert_key and current_contract_bid > 0:
                days_to_exp = (current_expiry - date.today()).days if current_expiry else 0
                candidates  = await _score_roll_candidates(
                    underlying, option_type, current_strike,
                    current_expiry, current_contract_bid, days_to_exp,
                )
                if candidates:
                    best = candidates[0]
                    roll_note += (
                        f" | Top roll: {best['expiry']} ${best['strike']} "
                        f"score={best['score']} credit={best['credit_pct']}%"
                        f"{' ⚠️ex-div' if best['ex_div_risk'] else ''}"
                    )
                    log.info("options_monitor.roll_candidates",
                             contract=contract, underlying=underlying,
                             candidates=candidates)
                    # Publish ranked candidates to Redis for dashboard pickup
                    try:
                        await self.redis.setex(
                            f"options:roll_candidates:{pos_id}",
                            3600 * 4,
                            json.dumps(candidates),
                        )
                    except Exception:
                        pass

            # Log the alert event
            await pool.execute(
                """INSERT INTO option_trade_log
                   (position_id, contract_symbol, underlying, event_type,
                    underlying_price, atr_value, notes)
                   VALUES ($1,$2,$3,$4,$5::NUMERIC,$6::NUMERIC,$7)""",
                pos_id, contract, underlying,
                f"alert_{alert_key}", current_price,
                None,
                roll_note,
            )

            alerts_fired[alert_key] = True

        # Persist updated alerts_fired
        if alerts_fired:
            await pool.execute(
                "UPDATE option_positions SET alerts_fired=$2 WHERE id=$1",
                pos_id, json.dumps(alerts_fired),
            )

    async def _refresh_chart(
        self,
        pool: asyncpg.Pool,
        pos_id,
        contract: str,
        underlying: str,
        underlying_entry: Optional[float],
        atr: Optional[float],
        expiration_date,
        entry_date,
    ):
        """Generate chart and store as base64 PNG in DB raw column."""
        try:
            # Fetch OHLCV for chart via gateway
            bars_data = await DataClient().bars(underlying, days=60)
            if not bars_data:
                return
            candles = bars_data if isinstance(bars_data, list) else (
                bars_data.get("candles") or bars_data.get("bars") or
                bars_data.get("results") or []
            )
            if not candles:
                return

            pos_mock = {
                "underlying":      underlying,
                "underlying_entry": underlying_entry,
                "atr_14":          atr,
                "entry_date":      entry_date,
                "expiration_date": expiration_date,
            }
            chart_b64 = await _generate_chart(pos_mock, candles)
            if chart_b64:
                # Store chart in a dedicated column via raw JSONB update
                await pool.execute(
                    """UPDATE option_positions
                       SET raw = COALESCE(raw, '{}'::jsonb) || jsonb_build_object('chart_b64', $2::text)
                       WHERE id=$1""",
                    pos_id, chart_b64,
                )
                log.info("options_monitor.chart_saved", contract=contract)
        except Exception as e:
            log.warning("options_monitor.chart_refresh_failed", contract=contract, error=str(e))

    async def shutdown(self):
        self._running = False
        if self.redis:
            await self.redis.aclose()
        if _pool:
            await _pool.close()


async def main():
    agent = OptionsMonitor()
    try:
        await agent.run()
    finally:
        await agent.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
