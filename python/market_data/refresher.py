"""Background warm-list refresher. Keeps frequently-requested data current."""
import asyncio
import time
import structlog
import redis.asyncio as aioredis

log = structlog.get_logger("market_data.refresher")

WARM_LIST_KEY = "md:warm"
WARM_BATCH    = 20    # tickers refreshed per cycle


class Refresher:
    def __init__(self, redis: aioredis.Redis, router, probe_interval: int = 1800):
        self._redis = redis
        self._router = router
        self._probe_interval = probe_interval
        self._last_probe = 0.0

    async def run(self):
        while True:
            try:
                await self._maybe_probe()
                await self._refresh_warm()
            except Exception as e:
                log.error("refresher.cycle_error", error=str(e))
            await asyncio.sleep(60)

    async def _maybe_probe(self):
        now = time.monotonic()
        if now - self._last_probe >= self._probe_interval:
            log.info("refresher.probe_all")
            await self._router.probe_all()
            self._last_probe = now

    async def _refresh_warm(self):
        # Get the WARM_BATCH most-recently-accessed keys
        keys = await self._redis.zrevrange(WARM_LIST_KEY, 0, WARM_BATCH - 1)
        if not keys:
            return
        refreshed = 0
        for key in keys:
            ttl = await self._redis.ttl(key)
            if ttl < 0:
                await self._redis.zrem(WARM_LIST_KEY, key)
                continue
            # Only pro-actively refresh if within the last 20% of TTL
            # We don't know the original TTL, so refresh if < 60s remaining
            if ttl < 60:
                await self._redis.delete(key)
                await self._redis.zrem(WARM_LIST_KEY, key)
                refreshed += 1
        if refreshed:
            log.debug("refresher.warm_evicted", count=refreshed)
