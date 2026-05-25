"""
Telemetry — fire-and-forget event emission for Code Insights dashboard.
"""
import asyncio
import functools
import json
import time
import traceback
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import structlog

log = structlog.get_logger("shared.telemetry")

TELEMETRY_STREAM = "system.telemetry"
_STREAM_MAXLEN   = 10_000


@dataclass
class TelemetryEvent:
    agent:         str
    event_name:    str
    severity:      str                   # debug | info | warn | error | critical
    payload:       dict = field(default_factory=dict)
    traceback_str: Optional[str] = None
    duration_ms:   Optional[float] = None


async def emit(event: TelemetryEvent) -> None:
    """Write one telemetry event to Redis stream. Silent on failure."""
    try:
        from shared.redis_client import get_redis  # late import — avoids circular deps
        redis  = await get_redis()
        fields: dict = {
            "agent":      event.agent,
            "event_name": event.event_name,
            "severity":   event.severity,
            "payload":    json.dumps(event.payload),
        }
        if event.traceback_str:
            fields["traceback_str"] = event.traceback_str[:4000]   # cap size
        if event.duration_ms is not None:
            fields["duration_ms"] = str(round(event.duration_ms, 2))
        await redis.xadd(TELEMETRY_STREAM, fields, maxlen=_STREAM_MAXLEN)
    except Exception as exc:
        log.debug("telemetry.emit_failed", error=str(exc))


def instrument(slow_ms: float = 5000.0):
    """
    Decorator for async functions.
    Emits 'slow_call' when duration > slow_ms, 'exception' on unhandled errors.
    Always re-raises exceptions so callers are not silently swallowed.

    Usage:
        @instrument()
        async def my_fn(): ...

        @instrument(slow_ms=2000)
        async def fast_fn(): ...
    """
    def decorator(fn: Callable) -> Callable:
        mod = fn.__module__ or ""
        agent_name = mod.split(".")[0] if "." in mod else mod

        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            t0 = time.monotonic()
            try:
                result = await fn(*args, **kwargs)
                elapsed = (time.monotonic() - t0) * 1000.0
                if elapsed > slow_ms:
                    asyncio.create_task(emit(TelemetryEvent(
                        agent      = agent_name,
                        event_name = "slow_call",
                        severity   = "warn",
                        payload    = {"function": fn.__qualname__, "duration_ms": round(elapsed, 1)},
                        duration_ms = elapsed,
                    )))
                return result
            except Exception as exc:
                elapsed = (time.monotonic() - t0) * 1000.0
                asyncio.create_task(emit(TelemetryEvent(
                    agent         = agent_name,
                    event_name    = "exception",
                    severity      = "error",
                    payload       = {"function": fn.__qualname__, "error": str(exc)},
                    traceback_str = traceback.format_exc(),
                    duration_ms   = elapsed,
                )))
                raise
        return wrapper
    return decorator
