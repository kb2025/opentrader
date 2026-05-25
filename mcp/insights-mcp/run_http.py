"""Run the Code Insights MCP server in streamable-HTTP mode."""
import os
from mcp.server.transport_security import TransportSecuritySettings
from server import server

host = os.getenv("FASTMCP_HOST", "0.0.0.0")
port = int(os.getenv("FASTMCP_PORT", "8000"))

server.settings.host = host
server.settings.port = port
server.settings.transport_security = TransportSecuritySettings(
    enable_dns_rebinding_protection=False
)

server.run(transport="streamable-http")
