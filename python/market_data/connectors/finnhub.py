import os
import time
import aiohttp
from .base import HTTPConnector, ConnectorError

FINNHUB_BASE = "https://finnhub.io/api/v1"


class FinnhubConnector(HTTPConnector):
    name = "finnhub"
    cost_tier = "free"
    env_key = "FINNHUB_API_KEY"
    rate_limit_per_min = 60
    CAPABILITIES = frozenset({"quote", "news", "earnings", "insider_transactions", "fundamentals"})

    def __init__(self):
        super().__init__()
        self._key = os.getenv("FINNHUB_API_KEY", "")

    def _params(self, extra: dict = None) -> dict:
        p = {"token": self._key}
        if extra:
            p.update(extra)
        return p

    async def call(self, data_type: str, params: dict) -> dict:
        if not self._key:
            raise ConnectorError("finnhub: FINNHUB_API_KEY not set")
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
                if data_type == "quote":
                    return await self._quote(session, params)
                if data_type == "news":
                    return await self._news(session, params)
                if data_type == "earnings":
                    return await self._earnings(session, params)
                if data_type == "insider_transactions":
                    return await self._insider(session, params)
                if data_type == "fundamentals":
                    return await self._fundamentals(session, params)
        except ConnectorError:
            raise
        except Exception as e:
            raise ConnectorError(f"finnhub: {e}") from e
        raise ConnectorError(f"finnhub: unsupported data_type {data_type!r}")

    async def _quote(self, session, params: dict) -> dict:
        ticker = params.get("ticker", "")
        data = await self._rate_limited_get(session, f"{FINNHUB_BASE}/quote",
                                            params=self._params({"symbol": ticker}))
        return {
            "ticker": ticker,
            "last":   data.get("c"),
            "open":   data.get("o"),
            "high":   data.get("h"),
            "low":    data.get("l"),
            "prev_close": data.get("pc"),
            "change_pct": round((data["c"] - data["pc"]) / data["pc"] * 100, 2) if data.get("c") and data.get("pc") else None,
        }

    async def _news(self, session, params: dict) -> dict:
        ticker = params.get("ticker", "")
        from datetime import date, timedelta
        today = date.today().isoformat()
        week_ago = (date.today() - timedelta(days=7)).isoformat()
        data = await self._rate_limited_get(session, f"{FINNHUB_BASE}/company-news",
                                            params=self._params({"symbol": ticker, "from": week_ago, "to": today}))
        articles = data if isinstance(data, list) else []
        return {
            "ticker": ticker,
            "articles": [
                {"headline": a.get("headline"), "url": a.get("url"),
                 "published": a.get("datetime"), "source": a.get("source")}
                for a in articles[:params.get("limit", 20)]
            ],
        }

    async def _earnings(self, session, params: dict) -> dict:
        ticker = params.get("ticker", "")
        data = await self._rate_limited_get(session, f"{FINNHUB_BASE}/stock/earnings",
                                            params=self._params({"symbol": ticker, "limit": 8}))
        earnings = data if isinstance(data, list) else []
        return {"ticker": ticker, "earnings": earnings}

    async def _insider(self, session, params: dict) -> dict:
        ticker = params.get("ticker", "")
        txn_data = await self._rate_limited_get(session, f"{FINNHUB_BASE}/stock/insider-transactions",
                                                params=self._params({"symbol": ticker}))
        mspr_data = await self._rate_limited_get(session, f"{FINNHUB_BASE}/stock/insider-sentiment",
                                                 params=self._params({"symbol": ticker,
                                                                       "from": "2024-01-01",
                                                                       "to": time.strftime("%Y-%m-%d")}))
        return {
            "ticker":       ticker,
            "transactions": (txn_data.get("data") or [])[:20],
            "sentiment":    mspr_data.get("data") or [],
        }

    async def _fundamentals(self, session, params: dict) -> dict:
        ticker = params.get("ticker", "")
        data = await self._rate_limited_get(session, f"{FINNHUB_BASE}/stock/profile2",
                                            params=self._params({"symbol": ticker}))
        return {
            "ticker":      ticker,
            "name":        data.get("name"),
            "sector":      data.get("finnhubIndustry"),
            "market_cap":  data.get("marketCapitalization"),
            "shares_out":  data.get("shareOutstanding"),
            "country":     data.get("country"),
            "currency":    data.get("currency"),
            "exchange":    data.get("exchange"),
        }
