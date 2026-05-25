import os
from .base import MCPConnector


class AlpacaConnector(MCPConnector):
    name = "alpaca"
    cost_tier = "broker"
    env_key = "ALPACA_KEY_ID"
    tool_map = {
        "get_latest_quote":  "quote",
        "get_bars":          "bars_daily",
        "get_intraday_bars": "bars_intraday",
        "get_snapshot":      "quote",
    }

    def __init__(self):
        self.mcp_url = os.getenv("ALPACA_MCP_URL", "http://ot-mcp-alpaca:8000/mcp")
