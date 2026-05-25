"""
DataClient — the only import any platform service needs for market data.

Every request goes through ot-market-data. No service knows which provider answered.

Usage:
    from shared.data_client import DataClient
    dc = DataClient()
    quote = await dc.quote("AAPL")
    bars  = await dc.bars("SPY", days=90)
"""
import os
import aiohttp
import structlog

log = structlog.get_logger("shared.data_client")

GATEWAY_URL = os.getenv("MARKET_DATA_URL", "http://ot-market-data:8090")
_TIMEOUT = aiohttp.ClientTimeout(total=30)


class DataClient:
    def __init__(self, base_url: str = GATEWAY_URL):
        self._base = base_url.rstrip("/")

    async def _get(self, path: str, params: dict = None) -> dict | None:
        url = f"{self._base}{path}"
        try:
            async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
                async with session.get(url, params=params) as resp:
                    if resp.status == 200:
                        body = await resp.json()
                        return body.get("data", body)
                    log.warning("data_client.error", url=url, status=resp.status)
                    return None
        except Exception as e:
            log.warning("data_client.failed", url=url, error=str(e))
            return None

    async def quote(self, ticker: str) -> dict | None:
        return await self._get(f"/data/quote/{ticker}")

    async def bars(self, ticker: str, days: int = 30, interval: str = "daily") -> dict | None:
        return await self._get(f"/data/bars/{ticker}", {"days": days, "interval": interval})

    async def fundamentals(self, ticker: str) -> dict | None:
        return await self._get(f"/data/fundamentals/{ticker}")

    async def analyst(self, ticker: str) -> dict | None:
        return await self._get(f"/data/analyst/{ticker}")

    async def earnings(self, ticker: str) -> dict | None:
        return await self._get(f"/data/earnings/{ticker}")

    async def dividends(self, ticker: str) -> dict | None:
        return await self._get(f"/data/dividends/{ticker}")

    async def news(self, ticker: str, limit: int = 20) -> dict | None:
        return await self._get(f"/data/news/{ticker}", {"limit": limit})

    async def sentiment(self, ticker: str) -> dict | None:
        return await self._get(f"/data/sentiment/{ticker}")

    async def technicals(self, ticker: str, interval: str = "1d") -> dict | None:
        return await self._get(f"/data/technicals/{ticker}", {"interval": interval})

    async def options_chain(self, ticker: str, expiration: str = "") -> dict | None:
        params = {"expiration": expiration} if expiration else {}
        return await self._get(f"/data/options/chain/{ticker}", params or None)

    async def options_flow(self, ticker: str) -> dict | None:
        return await self._get(f"/data/options/flow/{ticker}")

    async def dark_pool(self, ticker: str) -> dict | None:
        return await self._get(f"/data/darkpool/{ticker}")

    async def insider(self, ticker: str) -> dict | None:
        return await self._get(f"/data/insider/{ticker}")

    async def breadth(self, indicator: str) -> dict | None:
        return await self._get(f"/data/breadth/{indicator}")

    async def macro(self, indicator: str, n: int = 1) -> dict | None:
        return await self._get(f"/data/macro/{indicator}", {"n": n})

    async def classification(self, ticker: str) -> dict | None:
        return await self._get(f"/data/classification/{ticker}")

    async def short_interest(self, ticker: str) -> dict | None:
        return await self._get(f"/data/short_interest/{ticker}")

    async def avg_volume(self, ticker: str) -> float | None:
        data = await self._get(f"/data/avg_volume/{ticker}")
        if not data:
            return None
        return data.get("avg_volume") or data.get("volume")

    async def health(self) -> dict | None:
        return await self._get("/health")

    async def capabilities(self) -> dict | None:
        return await self._get("/capabilities")
