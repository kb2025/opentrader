"""
TTL in-memory data cache for reducing redundant API calls.
Thread-safe via asyncio.Lock for async consumers; sync_get/sync_set
available for thread-executor code (ml_predictor training pipeline).
"""
import asyncio
import time
from threading import Lock
from typing import Any, Optional


class TTLCache:
    """Async-safe in-process cache with per-entry TTL."""

    def __init__(self, default_ttl: int = 300):
        self._store: dict[str, tuple[Any, float]] = {}
        self._lock = asyncio.Lock()
        self._default_ttl = default_ttl

    async def get(self, key: str) -> Optional[Any]:
        async with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            value, expiry = entry
            if time.monotonic() > expiry:
                del self._store[key]
                return None
            return value

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        ttl = ttl if ttl is not None else self._default_ttl
        async with self._lock:
            self._store[key] = (value, time.monotonic() + ttl)

    async def get_or_fetch(self, key: str, fn, ttl: Optional[int] = None, *args, **kwargs) -> Any:
        cached = await self.get(key)
        if cached is not None:
            return cached
        value = await fn(*args, **kwargs)
        if value is not None:
            await self.set(key, value, ttl)
        return value

    def evict_expired(self) -> int:
        now = time.monotonic()
        stale = [k for k, (_, exp) in self._store.items() if now > exp]
        for k in stale:
            del self._store[k]
        return len(stale)

    def __len__(self) -> int:
        return len(self._store)


class SyncTTLCache:
    """Thread-safe sync cache for use in thread-executor contexts (e.g. sklearn training)."""

    def __init__(self, default_ttl: int = 300):
        self._store: dict[str, tuple[Any, float]] = {}
        self._lock = Lock()
        self._default_ttl = default_ttl

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            value, expiry = entry
            if time.monotonic() > expiry:
                del self._store[key]
                return None
            return value

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        ttl = ttl if ttl is not None else self._default_ttl
        with self._lock:
            self._store[key] = (value, time.monotonic() + ttl)

    def get_or_fetch(self, key: str, fn, ttl: Optional[int] = None, *args, **kwargs) -> Any:
        cached = self.get(key)
        if cached is not None:
            return cached
        value = fn(*args, **kwargs)
        if value is not None:
            self.set(key, value, ttl)
        return value

    def evict_expired(self) -> int:
        now = time.monotonic()
        with self._lock:
            stale = [k for k, (_, exp) in self._store.items() if now > exp]
            for k in stale:
                del self._store[k]
        return len(stale)

    def __len__(self) -> int:
        with self._lock:
            return len(self._store)
