import os
from .base import MCPConnector


class TradingViewConnector(MCPConnector):
    name = "tradingview"
    cost_tier = "free"
    env_key = None
    tool_map = {
        "get_indicators": "technicals",
        "get_summary":    "technicals",
    }

    def __init__(self):
        self.mcp_url = os.getenv("TRADINGVIEW_MCP_URL", "http://ot-mcp-tradingview:8000/mcp")
