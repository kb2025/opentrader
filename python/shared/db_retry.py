"""
DB write retry decorator for transient PostgreSQL/asyncpg errors.
Handles deadlocks (40P01), serialization failures (40001), and
lock timeout errors with exponential back-off.
"""
import asyncio
import functools
import logging
from typing import Callable, TypeVar

import asyncpg

log = logging.getLogger(__name__)

_RETRYABLE_PGCODES = {"40P01", "40001", "55P03"}   # deadlock / serialization / lock_not_available
_RETRYABLE_MSGS    = ("database is locked", "deadlock", "could not serialize", "lock timeout")

F = TypeVar("F")


def db_retry(max_attempts: int = 3, base_delay: float = 0.1):
    """
    Decorator for async DB write functions.  Retries up to `max_attempts`
    times on transient PostgreSQL conflict errors using exponential backoff.

    Usage::

        @db_retry(max_attempts=3)
        async def my_insert(pool, ...):
            await pool.execute(...)
    """
    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return await fn(*args, **kwargs)
                except asyncpg.PostgresError as exc:
                    code = getattr(exc, "sqlstate", "") or ""
                    msg  = str(exc).lower()
                    if code in _RETRYABLE_PGCODES or any(k in msg for k in _RETRYABLE_MSGS):
                        last_exc = exc
                        if attempt < max_attempts:
                            delay = base_delay * (2 ** (attempt - 1))
                            log.warning(
                                "db_retry.transient",
                                fn=fn.__name__,
                                attempt=attempt,
                                code=code,
                                delay=delay,
                            )
                            await asyncio.sleep(delay)
                        continue
                    raise
                except Exception:
                    raise
            raise last_exc
        return wrapper
    return decorator
