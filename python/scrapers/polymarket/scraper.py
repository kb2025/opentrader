"""
Polymarket Scraper
Fetches active prediction markets from the Polymarket public API and filters
for finance-relevant questions (earnings, Fed, rates, macro, M&A, etc.).
No API key required.
"""
import time
from typing import Optional

import aiohttp
import structlog

log = structlog.get_logger("scraper.polymarket")

POLYMARKET_API = "https://gamma-api.polymarket.com/markets"
TIMEOUT_S = 10

# Finance-relevant keywords for filtering market questions
FINANCE_KEYWORDS = {
    "earnings", "fed", "rate", "inflation", "gdp", "market", "stock",
    "economic", "unemployment", "recession", "merger", "acquisition",
}


def _classify_category(question: str) -> str:
    """Classify a market question into a high-level category."""
    q = question.lower()
    if any(kw in q for kw in ("merger", "acquisition", "acquire", "buyout", "takeover")):
        return "merger"
    if any(kw in q for kw in ("earnings", "eps", "revenue", "profit", "beat", "miss")):
        return "earnings"
    return "macro"


def _extract_ticker(question: str) -> str:
    """
    Extract a stock ticker from the question text.
    Looks for patterns like '$AAPL', '(AAPL)', or standalone uppercase 2-5 letter words
    that look like tickers. Falls back to 'MACRO'.
    """
    import re

    # $TICKER pattern first
    dollar = re.search(r'\$([A-Z]{1,5})\b', question)
    if dollar:
        return dollar.group(1)

    # (TICKER) pattern
    paren = re.search(r'\(([A-Z]{1,5})\)', question)
    if paren:
        return paren.group(1)

    # Common major tickers mentioned verbatim (case-insensitive)
    major = [
        "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "META", "NVDA", "TSLA",
        "JPM", "BAC", "GS", "MS", "WFC", "C", "XOM", "CVX", "JNJ", "PFE",
        "V", "MA", "WMT", "HD", "SPY", "QQQ", "DIA", "IWM", "VIX",
        "NFLX", "AMD", "INTC", "IBM", "ORCL", "CRM", "ADBE", "PYPL",
        "UBER", "LYFT", "SNAP", "TWTR", "SHOP", "SQ", "COIN", "HOOD",
    ]
    q_upper = question.upper()
    for ticker in major:
        # Match whole word
        if re.search(r'\b' + ticker + r'\b', q_upper):
            return ticker

    return "MACRO"


async def scrape_polymarket() -> list[dict]:
    """
    Fetch active Polymarket markets and return finance-relevant ones.
    Returns [] on any network or parse failure.
    """
    params = {"active": "true", "limit": "50"}
    timeout = aiohttp.ClientTimeout(total=TIMEOUT_S)

    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(POLYMARKET_API, params=params) as resp:
                if resp.status != 200:
                    log.warning("polymarket.api_error", status=resp.status)
                    return []
                markets = await resp.json(content_type=None)
    except Exception as e:
        log.warning("polymarket.fetch_failed", error=str(e))
        return []

    if not isinstance(markets, list):
        log.warning("polymarket.unexpected_format", type=type(markets).__name__)
        return []

    now_ms = int(time.time() * 1000)
    results: list[dict] = []

    for m in markets:
        question = (m.get("question") or "").strip()
        if not question:
            continue

        # Filter: question must contain at least one finance keyword
        q_lower = question.lower()
        if not any(kw in q_lower for kw in FINANCE_KEYWORDS):
            continue

        # Extract outcome prices safely
        outcome_prices = m.get("outcomePrices") or []
        try:
            yes_price = float(outcome_prices[0]) if len(outcome_prices) > 0 else 0.5
        except (TypeError, ValueError):
            yes_price = 0.5
        try:
            no_price = float(outcome_prices[1]) if len(outcome_prices) > 1 else round(1.0 - yes_price, 4)
        except (TypeError, ValueError):
            no_price = round(1.0 - yes_price, 4)

        # Volume
        try:
            volume = float(m.get("volume") or 0)
        except (TypeError, ValueError):
            volume = 0.0

        market_id = m.get("conditionId") or m.get("id") or ""
        ticker = _extract_ticker(question)
        category = _classify_category(question)

        results.append({
            "question":  question[:500],
            "ticker":    ticker,
            "yes_price": round(yes_price, 4),
            "no_price":  round(no_price, 4),
            "volume":    round(volume, 2),
            "market_id": str(market_id),
            "category":  category,
            "ts_utc":    now_ms,
        })

    log.info("polymarket.scrape_done", total=len(markets), matched=len(results))
    return results
