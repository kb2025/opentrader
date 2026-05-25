import os
from .base import MCPConnector


class MassiveConnector(MCPConnector):
    name = "massive"
    cost_tier = "paid"
    env_key = "MASSIVE_API_KEY"
    tool_map = {
        "get_quote":          "quote",
        "get_daily_bars":     "bars_daily",
        "get_intraday_bars":  "bars_intraday",
        "get_avg_volume":     "avg_volume",
        "get_ticker_details": "fundamentals",
        "get_financials":     "fundamentals",
        "get_earnings":       "earnings",
        "get_dividends":      "dividends",
        "get_sma":            "technicals",
        "get_sector":         "classification",
        "get_short_interest": "short_interest",
    }

    def __init__(self):
        self.mcp_url = os.getenv("MASSIVE_MCP_URL", "http://ot-mcp-massive:8000/mcp")
