"""
ETF Capital Flow Scraper
Uses Massive.com (Polygon.io-compatible) API to fetch daily aggregates for
key ETFs and compute dollar-volume flow relative to 30-day average.
"""
import logging
from datetime import date, timedelta

import aiohttp

log = logging.getLogger("scraper.etf_flows")

MASSIVE_BASE = "https://api.massive.com"

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


async def fetch_etf_flows(api_key: str) -> list[dict]:
    """Fetch daily aggregates for all ETFs and compute flow metrics."""
    today     = date.today()
    yesterday = today - timedelta(days=1)
    # Use previous trading day if today is weekend
    if today.weekday() >= 5:
        yesterday = today - timedelta(days=today.weekday() - 4)

    from_date = (yesterday - timedelta(days=35)).isoformat()
    to_date   = yesterday.isoformat()

    results = []
    headers = {"Authorization": f"Bearer {api_key}"}

    async with aiohttp.ClientSession(headers=headers) as session:
        for ticker, name, category in ETF_UNIVERSE:
            try:
                url = f"{MASSIVE_BASE}/v2/aggs/ticker/{ticker}/range/1/day/{from_date}/{to_date}"
                async with session.get(url, params={"adjusted": "true", "sort": "asc", "limit": 40},
                                       timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status != 200:
                        log.warning("etf_flows.fetch_failed", ticker=ticker, status=resp.status)
                        continue
                    data = await resp.json()

                results_list = data.get("results", [])
                if not results_list:
                    continue

                # Most-recent bar
                latest = results_list[-1]
                price         = float(latest.get("c", 0))
                volume        = int(latest.get("v", 0))
                dollar_volume = price * volume

                # 30-day average dollar volume (excluding today)
                history   = results_list[:-1][-30:] if len(results_list) > 1 else []
                avg_dvol  = 0
                avg_vol   = 0
                if history:
                    dvols    = [float(r.get("c", 0)) * int(r.get("v", 0)) for r in history]
                    avg_dvol = sum(dvols) / len(dvols) if dvols else 0
                    avg_vol  = int(sum(int(r.get("v", 0)) for r in history) / len(history))

                flow_ratio = round(dollar_volume / avg_dvol, 3) if avg_dvol > 0 else 1.0

                # Daily change
                prev_close  = float(results_list[-2].get("c", price)) if len(results_list) > 1 else price
                change_pct  = round((price - prev_close) / prev_close * 100, 3) if prev_close else 0.0

                results.append({
                    "ticker":        ticker,
                    "name":          name,
                    "category":      category,
                    "price":         price,
                    "volume":        volume,
                    "dollar_volume": round(dollar_volume),
                    "avg_volume_30d": avg_vol,
                    "flow_ratio":    flow_ratio,
                    "change_pct":    change_pct,
                })
                log.info("etf_flows.scraped", ticker=ticker, flow_ratio=flow_ratio)

            except Exception as e:
                log.warning("etf_flows.scrape_error", ticker=ticker, error=str(e))

    return results
