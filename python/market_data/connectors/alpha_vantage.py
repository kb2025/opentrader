import os
import aiohttp
from .base import HTTPConnector, ConnectorError

AV_BASE = "https://www.alphavantage.co/query"


class AlphaVantageConnector(HTTPConnector):
    name = "alpha_vantage"
    cost_tier = "free"
    env_key = "ALPHA_VANTAGE_API_KEY"
    rate_limit_per_min = 5
    CAPABILITIES = frozenset({"bars_daily", "news", "sentiment_news", "technicals", "fundamentals"})

    def __init__(self):
        super().__init__()
        self._key = os.getenv("ALPHA_VANTAGE_API_KEY", "")

    async def call(self, data_type: str, params: dict) -> dict:
        if not self._key:
            raise ConnectorError("alpha_vantage: ALPHA_VANTAGE_API_KEY not set")
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
                if data_type == "bars_daily":
                    return await self._bars_daily(session, params)
                if data_type in ("news", "sentiment_news"):
                    return await self._news_sentiment(session, params)
                if data_type == "fundamentals":
                    return await self._fundamentals(session, params)
                if data_type == "technicals":
                    return await self._technicals(session, params)
        except ConnectorError:
            raise
        except Exception as e:
            raise ConnectorError(f"alpha_vantage: {e}") from e
        raise ConnectorError(f"alpha_vantage: unsupported data_type {data_type!r}")

    async def _bars_daily(self, session, params: dict) -> dict:
        ticker = params.get("ticker", "")
        data = await self._rate_limited_get(session, AV_BASE, params={
            "function":   "TIME_SERIES_DAILY_ADJUSTED",
            "symbol":     ticker,
            "outputsize": "full" if params.get("days", 30) > 100 else "compact",
            "apikey":     self._key,
        })
        series = data.get("Time Series (Daily)", {})
        bars = [
            {
                "date":   d,
                "open":   float(v["1. open"]),
                "high":   float(v["2. high"]),
                "low":    float(v["3. low"]),
                "close":  float(v["4. close"]),
                "volume": int(v["6. volume"]),
            }
            for d, v in sorted(series.items(), reverse=True)
        ]
        days = int(params.get("days", 30))
        return {"ticker": ticker, "bars": bars[:days]}

    async def _news_sentiment(self, session, params: dict) -> dict:
        ticker = params.get("ticker", "")
        data = await self._rate_limited_get(session, AV_BASE, params={
            "function": "NEWS_SENTIMENT",
            "tickers":  ticker,
            "limit":    str(params.get("limit", 20)),
            "apikey":   self._key,
        })
        items = data.get("feed", [])
        return {
            "ticker": ticker,
            "articles": [
                {
                    "title":     a.get("title"),
                    "url":       a.get("url"),
                    "published": a.get("time_published"),
                    "sentiment": next(
                        (ts["ticker_sentiment_score"] for ts in a.get("ticker_sentiment", [])
                         if ts.get("ticker") == ticker), 0
                    ),
                    "label": next(
                        (ts["ticker_sentiment_label"] for ts in a.get("ticker_sentiment", [])
                         if ts.get("ticker") == ticker), "Neutral"
                    ),
                }
                for a in items
            ],
        }

    async def _fundamentals(self, session, params: dict) -> dict:
        ticker = params.get("ticker", "")
        data = await self._rate_limited_get(session, AV_BASE, params={
            "function": "OVERVIEW",
            "symbol":   ticker,
            "apikey":   self._key,
        })
        return {
            "ticker":        ticker,
            "name":          data.get("Name"),
            "sector":        data.get("Sector"),
            "industry":      data.get("Industry"),
            "market_cap":    data.get("MarketCapitalization"),
            "pe_ratio":      data.get("PERatio"),
            "eps":           data.get("EPS"),
            "dividend_yield": data.get("DividendYield"),
            "52wk_high":     data.get("52WeekHigh"),
            "52wk_low":      data.get("52WeekLow"),
        }

    async def _technicals(self, session, params: dict) -> dict:
        ticker = params.get("ticker", "")
        interval = params.get("interval", "daily")
        data = await self._rate_limited_get(session, AV_BASE, params={
            "function":   "SMA",
            "symbol":     ticker,
            "interval":   interval,
            "time_period": "20",
            "series_type": "close",
            "apikey":     self._key,
        })
        values = data.get("Technical Analysis: SMA", {})
        latest_date = next(iter(values), None)
        sma20 = float(values[latest_date]["SMA"]) if latest_date else None
        return {"ticker": ticker, "interval": interval, "sma20": sma20}
