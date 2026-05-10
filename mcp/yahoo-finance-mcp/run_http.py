"""Run the Yahoo Finance MCP server in streamable-HTTP mode."""
import os
from server import yfinance_server

if __name__ == "__main__":
    host = os.getenv("FASTMCP_HOST", "0.0.0.0")
    port = int(os.getenv("FASTMCP_PORT", "8000"))
    yfinance_server.run(transport="streamable-http", host=host, port=port)
