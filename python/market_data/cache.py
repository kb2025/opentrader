"""Redis cache for market data. Keys use md:{type}:{params} pattern."""
import hashlib
import json
import time
import structlog
import redis.asyncio as aioredis

log = structlog.get_logger("market_data.cache")

KEY_PREFIX = "md"


def _make_key(data_type: str, params: dict) -> str:
    param_str = json.dumps(params, sort_keys=True)
    digest = hashlib.md5(param_str.encode()).hexdigest()[:8]
    ticker = params.get("ticker", params.get("indicator", ""))
    if ticker:
        return f"{KEY_PREFIX}:{data_type}:{ticker}:{digest}"
    return f"{KEY_PREFIX}:{data_type}:{digest}"


class DataCache:
    def __init__(self, redis: aioredis.Redis, ttls: dict[str, int]):
        self._redis = redis
        self._ttls = ttls    # data_type → TTL seconds; 0 = no cache

    def _ttl(self, data_type: str) -> int:
        return self._ttls.get(data_type, 300)

    async def get(self, data_type: str, params: dict) -> dict | None:
        ttl = self._ttl(data_type)
        if ttl == 0:
            return None
        key = _make_key(data_type, params)
        raw = await self._redis.get(key)
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except Exception:
            return None

    async def set(self, data_type: str, params: dict, data: dict):
        ttl = self._ttl(data_type)
        if ttl == 0:
            return
        key = _make_key(data_type, params)
        payload = json.dumps({"data": data, "cached_at": time.time()})
        await self._redis.set(key, payload, ex=ttl)
        await self._redis.zadd("md:warm", {key: time.time()})

    async def invalidate(self, data_type: str, params: dict):
        key = _make_key(data_type, params)
        await self._redis.delete(key)

    async def ttl_remaining(self, data_type: str, params: dict) -> int:
        key = _make_key(data_type, params)
        return await self._redis.ttl(key)

    async def stats(self) -> dict:
        keys = await self._redis.keys(f"{KEY_PREFIX}:*")
        warm_count = await self._redis.zcard("md:warm")
        return {"cached_keys": len(keys), "warm_list_size": warm_count}
