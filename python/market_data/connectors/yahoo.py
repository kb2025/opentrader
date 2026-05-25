import os
from .base import MCPConnector


class YahooConnector(MCPConnector):
    name = "yahoo"
    cost_tier = "free"
    env_key = None
    tool_map = {
        "get_quote":             "quote",
        "get_price_history":     "bars_daily",
        "get_financials":        "fundamentals",
        "get_analyst_consensus": "analyst_consensus",
        "get_earnings":          "earnings",
        "get_dividends":         "dividends",
        "get_option_chain":      "options_chain",
        "get_news":              "news",
        "get_sector":            "classification",
        "get_trending":          "news",
    }

    def __init__(self):
        self.mcp_url = os.getenv("YAHOO_MCP_URL", "http://ot-mcp-yahoo:8000/mcp")
