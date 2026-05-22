"""Run the EODHD MCP server in streamable-HTTP mode."""
import os
from mcp.server.transport_security import TransportSecuritySettings
from server import eodhd_server

host = os.getenv("FASTMCP_HOST", "0.0.0.0")
port = int(os.getenv("FASTMCP_PORT", "8000"))

eodhd_server.settings.host = host
eodhd_server.settings.port = port
eodhd_server.settings.transport_security = TransportSecuritySettings(
    enable_dns_rebinding_protection=False
)

eodhd_server.run(transport="streamable-http")
