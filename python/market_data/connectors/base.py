"""
Pluggable connector base classes for the Market Data Gateway.

To add a new connector:
  1. Create connectors/my_provider.py inheriting BaseConnector, MCPConnector, or HTTPConnector
  2. Add entries to config/data_providers.toml
  No other files need changing — the gateway auto-discovers all subclasses.
"""
import asyncio
import os
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

import structlog
from mcp.client.streamable_http import streamablehttp_client
from mcp import ClientSession

from shared.mcp_client import call_mcp_tool

log = structlog.get_logger("market_data.connector")


class ConnectorError(Exception):
    pass


@dataclass
class DataResponse:
    data: dict
    provider: str
    cached: bool = False
    timestamp: float = field(default_factory=time.time)


class BaseConnector(ABC):
    name: str = ""
    cost_tier: str = "paid"   # "free" | "broker" | "paid"
    env_key: str | None = None

    def is_configured(self) -> bool:
        if not self.env_key:
            return True
        return bool(os.getenv(self.env_key))

    @abstractmethod
    async def probe(self) -> set[str]:
        """Return set of data_type strings this connector can serve right now.
        Must not raise — return empty set on any failure."""
        ...

    @abstractmethod
    async def call(self, data_type: str, params: dict) -> dict:
        """Fetch data. Raise ConnectorError on failure."""
        ...


# ── MCP connector base ────────────────────────────────────────────────────────

class MCPConnector(BaseConnector):
    """Base for connectors that talk to an MCP server.
    Subclasses declare mcp_url and tool_map; probe/call are handled here."""

    mcp_url: str = ""
    tool_map: dict[str, str] = {}   # tool_name → data_type

    async def _list_tools(self) -> list[str]:
        try:
            async with streamablehttp_client(self.mcp_url) as (read, write, _):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    result = await session.list_tools()
                    return [t.name for t in result.tools]
        except Exception as e:
            log.warning("connector.list_tools_failed", name=self.name, error=str(e))
            return []

    async def probe(self) -> set[str]:
        tools = await self._list_tools()
        return {self.tool_map[t] for t in tools if t in self.tool_map}

    async def call(self, data_type: str, params: dict) -> dict:
        tool = next(
            (t for t, dt in self.tool_map.items() if dt == data_type), None
        )
        if not tool:
            raise ConnectorError(f"{self.name}: no tool mapped to {data_type!r}")
        raw = await call_mcp_tool(self.mcp_url, tool, params)
        if raw is None:
            raise ConnectorError(f"{self.name}: null response for {data_type!r}")
        import json
        try:
            return json.loads(raw)
        except Exception:
            return {"raw": raw}


# ── HTTP connector base ───────────────────────────────────────────────────────

class HTTPConnector(BaseConnector):
    """Base for connectors that call external HTTP APIs directly.
    Subclasses declare CAPABILITIES (static probe) and implement call()."""

    base_url: str = ""
    rate_limit_per_min: int | None = None   # None = unlimited
    CAPABILITIES: frozenset[str] = frozenset()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if isinstance(cls.CAPABILITIES, (set, list)):
            cls.CAPABILITIES = frozenset(cls.CAPABILITIES)

    def __init__(self):
        self._last_calls: list[float] = []

    async def _rate_limited_get(self, session, url: str, **kwargs) -> dict:
        if self.rate_limit_per_min:
            now = time.monotonic()
            window = 60.0
            self._last_calls = [t for t in self._last_calls if now - t < window]
            if len(self._last_calls) >= self.rate_limit_per_min:
                wait = window - (now - self._last_calls[0]) + 0.1
                if wait > 0:
                    await asyncio.sleep(wait)
            self._last_calls.append(time.monotonic())

        async with session.get(url, **kwargs) as resp:
            if resp.status == 429:
                raise ConnectorError(f"{self.name}: rate limited (429)")
            if resp.status >= 400:
                raise ConnectorError(f"{self.name}: HTTP {resp.status} for {url}")
            return await resp.json(content_type=None)

    async def probe(self) -> set[str]:
        return set(self.CAPABILITIES) if self.is_configured() else set()
