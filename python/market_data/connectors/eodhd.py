import os
import aiohttp
from .base import HTTPConnector, ConnectorError


class EODHDConnector(HTTPConnector):
    name = "eodhd"
    cost_tier = "paid"
    env_key = "EODHD_API_KEY"
    CAPABILITIES = frozenset({
        "news", "fundamentals", "dividends",
        "earnings", "insider_transactions", "breadth",
    })

    async def call(self, data_type: str, params: dict) -> dict:
        key = os.getenv("EODHD_API_KEY", "")
        base = "https://eodhd.com/api"
        ticker = params.get("ticker", "")
        async with aiohttp.ClientSession() as session:
            if data_type == "news":
                limit = int(params.get("limit", 20))
                url = f"{base}/sentiments?s={ticker}&limit={limit}&api_token={key}&fmt=json"
                return await self._rate_limited_get(session, url)
            if data_type == "fundamentals":
                url = f"{base}/fundamentals/{ticker}?api_token={key}&fmt=json"
                return await self._rate_limited_get(session, url)
            if data_type == "dividends":
                url = f"{base}/div/{ticker}?api_token={key}&fmt=json"
                return await self._rate_limited_get(session, url)
            if data_type == "earnings":
                url = f"{base}/calendar/earnings?api_token={key}&symbols={ticker}&fmt=json"
                return await self._rate_limited_get(session, url)
            if data_type == "insider_transactions":
                url = f"{base}/insider-transactions?code={ticker}&api_token={key}&fmt=json"
                return await self._rate_limited_get(session, url)
            if data_type == "breadth":
                indicator = params.get("indicator", "")
                url = f"{base}/eod/{indicator}?api_token={key}&fmt=json"
                return await self._rate_limited_get(session, url)
            raise ConnectorError(f"eodhd: unsupported data_type {data_type!r}")
