"""
Router: auto-discovers connectors, probes capabilities, routes requests.

Adding a new connector = drop a file in connectors/, add to data_providers.toml.
No changes here needed.
"""
import importlib
import pkgutil
import time
import asyncio
import structlog
import market_data.connectors as _pkg
from market_data.connectors.base import BaseConnector, MCPConnector, HTTPConnector, ConnectorError, DataResponse
from market_data.circuit_breaker import CircuitBreaker
from market_data.cache import DataCache

log = structlog.get_logger("market_data.router")


class Router:
    def __init__(self, priority: dict[str, list[str]], ttls: dict[str, int]):
        self._priority = priority     # data_type → [connector_name, ...]
        self._ttls = ttls
        self._registry: dict[str, BaseConnector] = {}
        self._capabilities: dict[str, set[str]] = {}   # connector_name → set of data_types
        self._cb = CircuitBreaker()
        self._probe_stats: dict[str, dict] = {}
        self._cache: DataCache | None = None
        self._call_counts: dict[str, int] = {}
        self._hit_counts: dict[str, int] = {}
        self._miss_counts: dict[str, int] = {}

    def set_cache(self, cache: DataCache):
        self._cache = cache

    # ── Connector discovery ───────────────────────────────────────────────────

    def discover(self):
        _bases = {BaseConnector, MCPConnector, HTTPConnector}
        for _, mod_name, _ in pkgutil.iter_modules(_pkg.__path__):
            if mod_name == "base":
                continue
            module = importlib.import_module(f"market_data.connectors.{mod_name}")
            for obj in vars(module).values():
                if (isinstance(obj, type)
                        and issubclass(obj, BaseConnector)
                        and obj not in _bases):
                    try:
                        instance = obj()
                        if instance.is_configured():
                            self._registry[instance.name] = instance
                            log.info("router.connector_registered", name=instance.name,
                                     tier=instance.cost_tier)
                        else:
                            log.info("router.connector_skipped_no_key", name=instance.name)
                    except Exception as e:
                        log.warning("router.connector_init_failed", mod=mod_name, error=str(e))

    # ── Probe engine ──────────────────────────────────────────────────────────

    async def probe_all(self):
        tasks = {name: asyncio.create_task(self._probe_one(name, conn))
                 for name, conn in self._registry.items()}
        await asyncio.gather(*tasks.values(), return_exceptions=True)
        self._rebuild_routing_table()

    async def _probe_one(self, name: str, conn: BaseConnector):
        t0 = time.monotonic()
        try:
            caps = await asyncio.wait_for(conn.probe(), timeout=30)
            self._capabilities[name] = caps
            self._probe_stats[name] = {
                "status":    "ok",
                "probed_at": time.time(),
                "duration_ms": round((time.monotonic() - t0) * 1000),
                "capabilities": sorted(caps),
            }
            log.info("router.probe_ok", name=name, caps=sorted(caps))
        except Exception as e:
            self._capabilities[name] = set()
            self._probe_stats[name] = {
                "status":    "failed",
                "probed_at": time.time(),
                "error":     str(e),
            }
            log.warning("router.probe_failed", name=name, error=str(e))

    def _rebuild_routing_table(self):
        # Priority order from toml, filtered to connectors that actually reported capability
        self._routing: dict[str, list[BaseConnector]] = {}
        for data_type, names in self._priority.items():
            chain = []
            for name in names:
                conn = self._registry.get(name)
                if conn and data_type in self._capabilities.get(name, set()):
                    chain.append(conn)
            self._routing[data_type] = chain
            if chain:
                log.debug("router.route_built", data_type=data_type,
                          chain=[c.name for c in chain])

    # ── Request dispatch ──────────────────────────────────────────────────────

    async def fetch(self, data_type: str, params: dict) -> DataResponse:
        self._call_counts[data_type] = self._call_counts.get(data_type, 0) + 1

        # Cache check
        if self._cache:
            hit = await self._cache.get(data_type, params)
            if hit:
                self._hit_counts[data_type] = self._hit_counts.get(data_type, 0) + 1
                return DataResponse(data=hit.get("data", hit), provider="cache", cached=True,
                                    timestamp=hit.get("cached_at", time.time()))

        self._miss_counts[data_type] = self._miss_counts.get(data_type, 0) + 1

        chain = self._routing.get(data_type, [])
        if not chain:
            # Try probing on-demand if routing table has no entry
            await self.probe_all()
            chain = self._routing.get(data_type, [])

        last_err = None
        for conn in chain:
            if self._cb.is_open(conn.name):
                log.debug("router.cb_skip", connector=conn.name, data_type=data_type)
                continue
            try:
                data = await asyncio.wait_for(conn.call(data_type, params), timeout=30)
                self._cb.record_success(conn.name)
                if self._cache:
                    await self._cache.set(data_type, params, data)
                return DataResponse(data=data, provider=conn.name, cached=False)
            except Exception as e:
                last_err = e
                self._cb.record_failure(conn.name)
                log.warning("router.connector_failed", connector=conn.name,
                            data_type=data_type, error=str(e))

        raise ConnectorError(
            f"No connector could serve {data_type!r}: {last_err}"
        )

    # ── Admin ─────────────────────────────────────────────────────────────────

    def capabilities(self) -> dict:
        return {
            name: {
                "cost_tier":    self._registry[name].cost_tier,
                "capabilities": sorted(self._capabilities.get(name, set())),
                "probe":        self._probe_stats.get(name, {}),
                "circuit":      self._cb.status().get(name, {"state": "ok"}),
            }
            for name in self._registry
        }

    def health(self) -> dict:
        cb = self._cb.status()
        return {
            name: {
                "configured": conn.is_configured(),
                "state":      cb.get(name, {}).get("state", "ok"),
                "capabilities": sorted(self._capabilities.get(name, set())),
            }
            for name, conn in self._registry.items()
        }

    def stats(self) -> dict:
        result = {}
        for dt in set(list(self._call_counts) + list(self._hit_counts)):
            calls = self._call_counts.get(dt, 0)
            hits  = self._hit_counts.get(dt, 0)
            misses = self._miss_counts.get(dt, 0)
            result[dt] = {
                "calls":   calls,
                "hits":    hits,
                "misses":  misses,
                "hit_rate": round(hits / calls * 100, 1) if calls else 0,
            }
        return result
