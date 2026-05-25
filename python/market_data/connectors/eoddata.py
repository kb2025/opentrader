import os
import aiohttp
from .base import HTTPConnector, ConnectorError

# EODData symbol names for standard breadth indicators
_BREADTH_MAP = {
    "MMFI": "MAHN",   # % NYSE stocks above 50-DMA
    "MMTH": "MANH",   # % NYSE stocks above 200-DMA
    "HIGN": "MAHN",   # 52-week highs (closest available)
    "LOWN": "MALN",   # 52-week lows
}


class EODDataConnector(HTTPConnector):
    name = "eoddata"
    cost_tier = "paid"
    env_key = "EODDATA_API_KEY"
    CAPABILITIES = frozenset({"breadth"})

    async def call(self, data_type: str, params: dict) -> dict:
        if data_type != "breadth":
            raise ConnectorError(f"eoddata: unsupported data_type {data_type!r}")
        key = os.getenv("EODDATA_API_KEY", "")
        indicator = params.get("indicator", "")
        eod_sym = _BREADTH_MAP.get(indicator, indicator)
        async with aiohttp.ClientSession() as session:
            url = f"https://api.eoddata.com/Quote/List/INDEX/{eod_sym}?token={key}"
            return await self._rate_limited_get(session, url)
