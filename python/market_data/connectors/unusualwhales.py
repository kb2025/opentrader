import os
from .base import MCPConnector


class UnusualWhalesConnector(MCPConnector):
    name = "unusualwhales"
    cost_tier = "paid"
    env_key = "UNUSUAL_WHALES_API_KEY"
    tool_map = {
        "get_ticker_flow":     "options_flow",
        "get_darkpool_recent": "dark_pool",
        "get_market_tide":     "options_flow",
        "get_flow_alerts":     "options_flow",
    }

    def __init__(self):
        self.mcp_url = os.getenv("UNUSUALWHALES_MCP_URL", "http://ot-mcp-unusualwhales:8000/mcp")
