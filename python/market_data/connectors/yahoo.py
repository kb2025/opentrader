import os
from .base import MCPConnector, ConnectorError
from shared.mcp_client import call_mcp_tool


class YahooConnector(MCPConnector):
    name = "yahoo"
    cost_tier = "free"
    env_key = None
    tool_map = {
        "get_historical_stock_prices": "bars_daily",
        "get_stock_info":              "fundamentals",
        "get_analyst_consensus":       "analyst_consensus",
        "get_earnings":                "earnings",
        "get_dividends":               "dividends",
        "get_option_chain":            "options_chain",
        "get_yahoo_finance_news":      "news",
        "get_recommendations":         "classification",
    }

    def __init__(self):
        self.mcp_url = os.getenv("YAHOO_MCP_URL", "http://ot-mcp-yahoo:8000/mcp")

    async def call(self, data_type: str, params: dict) -> dict:
        import json

        if data_type == "bars_daily":
            days = int(params.get("days", 30))
            # Map days to yfinance period string
            if days > 700:
                period = "2y"
            elif days > 350:
                period = "1y"
            elif days > 170:
                period = "6mo"
            elif days > 85:
                period = "3mo"
            else:
                period = "1mo"
            args = {"ticker": params["ticker"], "period": period, "interval": "1d"}
            raw = await call_mcp_tool(self.mcp_url, "get_historical_stock_prices", args)
            if raw is None:
                raise ConnectorError("yahoo: null bars response")
            try:
                return json.loads(raw)
            except Exception:
                return {"raw": raw}

        return await super().call(data_type, params)
