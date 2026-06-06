"""Run the Yahoo Finance MCP server in streamable-HTTP mode."""
import os
from mcp.server.transport_security import TransportSecuritySettings
from server import yfinance_server

host = os.getenv("FASTMCP_HOST", "0.0.0.0")
port = int(os.getenv("FASTMCP_PORT", "8000"))

yfinance_server.settings.host = host
yfinance_server.settings.port = port
yfinance_server.settings.transport_security = TransportSecuritySettings(
    enable_dns_rebinding_protection=False
)

yfinance_server.run(transport="streamable-http")
