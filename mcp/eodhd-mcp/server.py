"""
EODHD MCP Server — EOD Historical Data API
Provides: quotes, EOD bars, fundamentals, analyst ratings, earnings, dividends,
insider transactions, financial news, macro indicators, breadth indicators.
"""
import json
import os
import requests
from mcp.server.fastmcp import FastMCP

EODHD_API_KEY = os.getenv("EODHD_API_KEY", "")
BASE_URL = "https://eodhd.com/api"

eodhd_server = FastMCP(
    "eodhd",
    instructions="""
# EODHD MCP Server

Provides market data from EODHD (eodhd.com) All-in-One plan.

Available tools:
- get_quote: Real-time / 15-min delayed quote for a ticker
- get_eod_bars: End-of-day OHLCV bars for a ticker
- get_fundamentals: Full company fundamentals including analyst ratings and price targets
- get_analyst_consensus: Analyst consensus rating, price target, and buy/hold/sell counts
- get_earnings: Upcoming and historical earnings data
- get_dividends: Dividend history and upcoming ex-dates
- get_insider_transactions: Recent insider buys and sells (SEC Form 4)
- get_news: Financial news with sentiment for a ticker
- get_macro_indicator: Economic / macro indicator time series (GDP, CPI, unemployment, etc.)
- get_breadth_indicators: Market breadth indicators (MMFI, MMTH, HIGN, LOWN, TRIN, etc.)
""",
)


def _get(path: str, params: dict = {}) -> dict | list | None:
    """Make a GET request to the EODHD API."""
    if not EODHD_API_KEY:
        return {"error": "EODHD_API_KEY not set"}
    p = {"api_token": EODHD_API_KEY, "fmt": "json", **params}
    try:
        r = requests.get(f"{BASE_URL}/{path}", params=p, timeout=15)
        if r.status_code == 401:
            return {"error": "Invalid EODHD API key"}
        if r.status_code == 402:
            return {"error": "EODHD subscription does not cover this endpoint"}
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        return {"error": str(e)}


def _ticker(symbol: str, exchange: str = "US") -> str:
    """Format ticker as SYMBOL.EXCHANGE for EODHD."""
    sym = symbol.upper().strip()
    if "." in sym:
        return sym
    return f"{sym}.{exchange.upper()}"


@eodhd_server.tool(
    name="get_quote",
    description="Get real-time (15-min delayed) quote for a ticker from EODHD. Args: ticker (e.g. AAPL), exchange (default US)",
)
def get_quote(ticker: str, exchange: str = "US") -> str:
    t = _ticker(ticker, exchange)
    data = _get(f"real-time/{t}")
    if isinstance(data, dict) and "error" in data:
        return json.dumps(data)
    if isinstance(data, list):
        data = data[0] if data else {}
    return json.dumps({
        "ticker":       t,
        "price":        data.get("close") or data.get("previousClose"),
        "open":         data.get("open"),
        "high":         data.get("high"),
        "low":          data.get("low"),
        "volume":       data.get("volume"),
        "change":       data.get("change"),
        "change_pct":   data.get("change_p"),
        "timestamp":    data.get("timestamp"),
    })


@eodhd_server.tool(
    name="get_eod_bars",
    description="Get end-of-day OHLCV bars for a ticker. Args: ticker, exchange (default US), from_date (YYYY-MM-DD), to_date (YYYY-MM-DD), limit (default 60)",
)
def get_eod_bars(ticker: str, exchange: str = "US", from_date: str = "", to_date: str = "", limit: int = 60) -> str:
    t = _ticker(ticker, exchange)
    params: dict = {"order": "d", "limit": limit}
    if from_date:
        params["from"] = from_date
    if to_date:
        params["to"] = to_date
    data = _get(f"eod/{t}", params)
    if isinstance(data, dict) and "error" in data:
        return json.dumps(data)
    bars = [
        {"date": b.get("date"), "open": b.get("open"), "high": b.get("high"),
         "low": b.get("low"), "close": b.get("close"), "volume": b.get("volume"),
         "adj_close": b.get("adjusted_close")}
        for b in (data or [])
    ]
    return json.dumps(bars)


@eodhd_server.tool(
    name="get_fundamentals",
    description="Get full company fundamentals for a ticker including financials, valuation, and analyst data. Args: ticker, exchange (default US)",
)
def get_fundamentals(ticker: str, exchange: str = "US") -> str:
    t = _ticker(ticker, exchange)
    data = _get(f"fundamentals/{t}")
    if isinstance(data, dict) and "error" in data:
        return json.dumps(data)
    # Return a focused summary to keep payload manageable
    general = data.get("General", {})
    highlights = data.get("Highlights", {})
    valuation = data.get("Valuation", {})
    analyst = data.get("AnalystRatings", {})
    return json.dumps({
        "ticker":          t,
        "name":            general.get("Name"),
        "sector":          general.get("Sector"),
        "industry":        general.get("Industry"),
        "market_cap":      highlights.get("MarketCapitalization"),
        "pe_ratio":        highlights.get("PERatio"),
        "eps":             highlights.get("EarningsShare"),
        "revenue":         highlights.get("RevenueTTM"),
        "profit_margin":   highlights.get("ProfitMargin"),
        "dividend_yield":  highlights.get("DividendYield"),
        "52w_high":        highlights.get("52WeekHigh"),
        "52w_low":         highlights.get("52WeekLow"),
        "target_price":    highlights.get("WallStreetTargetPrice"),
        "analyst_ratings": analyst,
        "pb_ratio":        valuation.get("PriceBookMRQ"),
        "ps_ratio":        valuation.get("PriceSalesTTM"),
    })


@eodhd_server.tool(
    name="get_analyst_consensus",
    description="Get analyst consensus rating, price target, and buy/hold/sell counts. Args: ticker, exchange (default US)",
)
def get_analyst_consensus(ticker: str, exchange: str = "US") -> str:
    t = _ticker(ticker, exchange)
    data = _get(f"fundamentals/{t}", {"filter": "AnalystRatings,Highlights"})
    if isinstance(data, dict) and "error" in data:
        return json.dumps(data)
    analyst = data.get("AnalystRatings", {})
    highlights = data.get("Highlights", {})
    if not analyst:
        return json.dumps({"ticker": t, "error": "No analyst data available"})

    rating_val = float(analyst.get("Rating") or 0)
    # EODHD rating: 1=Strong Buy, 2=Buy, 3=Hold, 4=Sell, 5=Strong Sell
    consensus = (
        "strong_buy"  if rating_val <= 1.5 else
        "buy"         if rating_val <= 2.5 else
        "hold"        if rating_val <= 3.5 else
        "sell"        if rating_val <= 4.5 else
        "strong_sell"
    ) if rating_val else "none"

    target = float(highlights.get("WallStreetTargetPrice") or 0)
    current = float(highlights.get("MostRecentQuarter") or 0)

    return json.dumps({
        "ticker":                 t,
        "consensus_rating":       consensus,
        "rating_score":           rating_val,
        "consensus_price_target": target,
        "buy_ratings":            int(analyst.get("StrongBuy", 0) or 0) + int(analyst.get("Buy", 0) or 0),
        "hold_ratings":           int(analyst.get("Hold", 0) or 0),
        "sell_ratings":           int(analyst.get("Sell", 0) or 0) + int(analyst.get("StrongSell", 0) or 0),
        "total_analysts":         (int(analyst.get("StrongBuy", 0) or 0) + int(analyst.get("Buy", 0) or 0) +
                                   int(analyst.get("Hold", 0) or 0) + int(analyst.get("Sell", 0) or 0) +
                                   int(analyst.get("StrongSell", 0) or 0)),
    })


@eodhd_server.tool(
    name="get_earnings",
    description="Get upcoming and historical earnings data for a ticker. Args: ticker, exchange (default US), limit (default 8)",
)
def get_earnings(ticker: str, exchange: str = "US", limit: int = 8) -> str:
    t = _ticker(ticker, exchange)
    data = _get(f"calendar/earnings", {"symbols": t, "limit": limit})
    if isinstance(data, dict) and "error" in data:
        return json.dumps(data)
    earnings = data.get("earnings", data) if isinstance(data, dict) else data
    return json.dumps(earnings[:limit] if isinstance(earnings, list) else earnings)


@eodhd_server.tool(
    name="get_dividends",
    description="Get dividend history and upcoming ex-dates for a ticker. Args: ticker, exchange (default US), from_date (YYYY-MM-DD)",
)
def get_dividends(ticker: str, exchange: str = "US", from_date: str = "") -> str:
    t = _ticker(ticker, exchange)
    params: dict = {"order": "d", "limit": 8}
    if from_date:
        params["from"] = from_date
    data = _get(f"div/{t}", params)
    if isinstance(data, dict) and "error" in data:
        return json.dumps(data)
    return json.dumps(data or [])


@eodhd_server.tool(
    name="get_insider_transactions",
    description="Get recent insider transactions (SEC Form 4) for a ticker. Args: ticker, exchange (default US), limit (default 20)",
)
def get_insider_transactions(ticker: str, exchange: str = "US", limit: int = 20) -> str:
    t = _ticker(ticker, exchange)
    data = _get("insider-transactions", {"code": t, "limit": limit})
    if isinstance(data, dict) and "error" in data:
        return json.dumps(data)
    transactions = data.get("data", data) if isinstance(data, dict) else data
    return json.dumps(transactions[:limit] if isinstance(transactions, list) else transactions)


@eodhd_server.tool(
    name="get_news",
    description="Get recent financial news with sentiment for a ticker. Args: ticker, exchange (default US), limit (default 10)",
)
def get_news(ticker: str, exchange: str = "US", limit: int = 10) -> str:
    t = _ticker(ticker, exchange)
    data = _get("news", {"s": t, "limit": limit, "offset": 0})
    if isinstance(data, dict) and "error" in data:
        return json.dumps(data)
    articles = (data or [])[:limit]
    return json.dumps([
        {"title": a.get("title"), "date": a.get("date"), "url": a.get("link"),
         "sentiment": a.get("sentiment", {}), "symbols": a.get("symbols", [])}
        for a in articles
    ])


@eodhd_server.tool(
    name="get_macro_indicator",
    description="Get economic / macro indicator time series. Args: country (ISO2, e.g. USA), indicator (e.g. gdp_current_usd, inflation_consumer_prices_annual, unemployment_total_percent), limit (default 10)",
)
def get_macro_indicator(country: str = "USA", indicator: str = "gdp_current_usd", limit: int = 10) -> str:
    data = _get(f"macro-indicator/{country.upper()}", {"indicator": indicator})
    if isinstance(data, dict) and "error" in data:
        return json.dumps(data)
    return json.dumps((data or [])[:limit])


@eodhd_server.tool(
    name="get_breadth_indicators",
    description="Get market breadth indicators: MMFI (% above 50MA), MMTH (% above 200MA), HIGN (52w highs), LOWN (52w lows). Returns last N bars. Args: indicator (MMFI|MMTH|HIGN|LOWN|MAHQ|MALQ|TRIN), limit (default 5)",
)
def get_breadth_indicators(indicator: str = "MMFI", limit: int = 5) -> str:
    # EODHD uses INDX exchange for breadth indicators
    # Standard NYSE breadth tickers map as follows:
    _EODHD_MAP = {
        "MMFI":  "MMFI.INDX",
        "MMTH":  "MMTH.INDX",
        "HIGN":  "HIGN.INDX",
        "LOWN":  "LOWN.INDX",
        "MAHQ":  "MAHQ.INDX",
        "MALQ":  "MALQ.INDX",
        "TRIN":  "TRIN.INDX",
    }
    sym = _EODHD_MAP.get(indicator.upper(), f"{indicator.upper()}.INDX")
    data = _get(f"eod/{sym}", {"order": "d", "limit": limit})
    if isinstance(data, dict) and "error" in data:
        # Try alternate exchange suffix
        data = _get(f"eod/{indicator.upper()}.US", {"order": "d", "limit": limit})
    if isinstance(data, dict) and "error" in data:
        return json.dumps(data)
    bars = [{"date": b.get("date"), "value": b.get("close")} for b in (data or [])]
    return json.dumps({"indicator": indicator.upper(), "bars": bars})
