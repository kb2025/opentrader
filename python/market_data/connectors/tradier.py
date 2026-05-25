import os
from .base import HTTPConnector, ConnectorError
from brokers.tradier import market as tradier_market


class TradierConnector(HTTPConnector):
    name = "tradier"
    cost_tier = "broker"
    env_key = "TRADIER_API_KEY"
    CAPABILITIES = frozenset({"quote", "options_chain"})

    async def call(self, data_type: str, params: dict) -> dict:
        try:
            if data_type == "quote":
                ticker = params.get("ticker", "")
                return await tradier_market.get_quote(ticker)

            if data_type == "options_chain":
                ticker = params.get("ticker", "")
                expiration = params.get("expiration", "")
                if not expiration:
                    expirations = await tradier_market.get_option_expirations(ticker)
                    if not expirations:
                        return {"options": []}
                    expiration = expirations[0]
                chain = await tradier_market.get_option_chain(ticker, expiration)
                return {"options": chain, "expiration": expiration}

        except Exception as e:
            raise ConnectorError(f"tradier: {e}") from e

        raise ConnectorError(f"tradier: unsupported data_type {data_type!r}")
