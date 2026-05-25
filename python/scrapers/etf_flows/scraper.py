"""
ETF Capital Flow Scraper
Fetches daily aggregates for key ETFs via the Market Data Gateway
and computes dollar-volume flow relative to 30-day average.
"""
import asyncio
import logging

from shared.data_client import DataClient

log = logging.getLogger("scraper.etf_flows")

ETF_UNIVERSE = [
    # Broad market
    ("SPY",  "SPDR S&P 500",              "equity"),
    ("QQQ",  "Invesco QQQ (Nasdaq 100)",  "equity"),
    ("IWM",  "iShares Russell 2000",      "equity"),
    ("DIA",  "SPDR Dow Jones",            "equity"),
    ("VTI",  "Vanguard Total Market",     "equity"),
    # Sector SPDRs
    ("XLK",  "Technology",                "sector"),
    ("XLF",  "Financials",               "sector"),
    ("XLE",  "Energy",                    "sector"),
    ("XLV",  "Health Care",              "sector"),
    ("XLC",  "Communication Services",   "sector"),
    ("XLI",  "Industrials",              "sector"),
    ("XLY",  "Consumer Discretionary",   "sector"),
    ("XLP",  "Consumer Staples",         "sector"),
    ("XLB",  "Materials",               "sector"),
    ("XLRE", "Real Estate",             "sector"),
    ("XLU",  "Utilities",               "sector"),
    # Fixed income
    ("TLT",  "iShares 20Y+ Treasury",    "bond"),
    ("IEF",  "iShares 7-10Y Treasury",   "bond"),
    ("HYG",  "iShares High Yield Corp",  "bond"),
    ("LQD",  "iShares IG Corp",         "bond"),
    # Commodities / alts
    ("GLD",  "SPDR Gold Shares",         "commodity"),
    ("SLV",  "iShares Silver Trust",     "commodity"),
    ("USO",  "United States Oil",        "commodity"),
    # Volatility
    ("VXX",  "iPath S&P 500 VIX",        "volatility"),
    # Bitcoin ETFs
    ("IBIT", "iShares Bitcoin Trust",    "crypto"),
    ("FBTC", "Fidelity Bitcoin",         "crypto"),
]


def _to_bars(data) -> list[dict]:
    """Normalise gateway response to [{c, v}] dicts."""
    items = data if isinstance(data, list) else (
        data.get("bars") or data.get("candles") or data.get("results") or []
    )
    out = []
    for b in items:
        close = b.get("c") or b.get("Close") or b.get("close")
        vol   = b.get("v") or b.get("Volume") or b.get("volume") or 0
        if close is not None:
            out.append({"c": float(close), "v": int(float(vol))})
    return out


async def _fetch_one(dc: DataClient, ticker: str, name: str, category: str) -> dict | None:
    try:
        data = await dc.bars(ticker, days=40)
        if not data:
            return None
        bars = _to_bars(data)
        if not bars:
            return None

        latest        = bars[-1]
        price         = latest["c"]
        volume        = latest["v"]
        dollar_volume = price * volume

        history  = bars[:-1][-30:] if len(bars) > 1 else []
        avg_dvol = avg_vol = 0
        if history:
            dvols    = [b["c"] * b["v"] for b in history]
            avg_dvol = sum(dvols) / len(dvols) if dvols else 0
            avg_vol  = int(sum(b["v"] for b in history) / len(history))

        flow_ratio = round(dollar_volume / avg_dvol, 3) if avg_dvol > 0 else 1.0
        prev_close = bars[-2]["c"] if len(bars) > 1 else price
        change_pct = round((price - prev_close) / prev_close * 100, 3) if prev_close else 0.0

        log.info("etf_flows.scraped", ticker=ticker, flow_ratio=flow_ratio)
        return {
            "ticker":         ticker,
            "name":           name,
            "category":       category,
            "price":          price,
            "volume":         volume,
            "dollar_volume":  round(dollar_volume),
            "avg_volume_30d": avg_vol,
            "flow_ratio":     flow_ratio,
            "change_pct":     change_pct,
        }
    except Exception as e:
        log.warning("etf_flows.scrape_error", ticker=ticker, error=str(e))
        return None


async def fetch_etf_flows(api_key: str = "") -> list[dict]:
    """Fetch daily aggregates for all ETFs and compute flow metrics."""
    dc  = DataClient()
    sem = asyncio.Semaphore(5)

    async def _throttled(ticker, name, category):
        async with sem:
            return await _fetch_one(dc, ticker, name, category)

    raw = await asyncio.gather(
        *[_throttled(t, n, c) for t, n, c in ETF_UNIVERSE],
        return_exceptions=True,
    )
    return [r for r in raw if isinstance(r, dict)]
