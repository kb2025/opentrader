import os
from .base import HTTPConnector, ConnectorError
from shared.fred_client import get_fred_client


class FREDConnector(HTTPConnector):
    name = "fred"
    cost_tier = "free"
    env_key = "FRED_API_KEY"
    CAPABILITIES = frozenset({"macro"})

    async def call(self, data_type: str, params: dict) -> dict:
        client = get_fred_client()
        if not client:
            raise ConnectorError("fred: FRED_API_KEY not set")
        try:
            indicator = params.get("indicator", "")
            if not indicator:
                raise ConnectorError("fred: 'indicator' param required")

            series_id = client.SERIES.get(indicator, indicator)
            n = int(params.get("n", 1))

            if n == 1:
                value = await client.latest(series_id)
                return {"indicator": indicator, "series_id": series_id, "value": value}
            else:
                history = await client.history(series_id, n=n)
                return {"indicator": indicator, "series_id": series_id, "history": history}

        except ConnectorError:
            raise
        except Exception as e:
            raise ConnectorError(f"fred: {e}") from e
