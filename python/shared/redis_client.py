"""
Shared Redis client factory.
All agents import this — single consistent connection config.
"""
import os
import logging
import redis.asyncio as aioredis

log = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# Stream names — single source of truth
STREAMS = {
    "ticks":           "market.ticks",
    "scanner":         "scanner.signals",
    "signals":         "predictor.signals",
    "orders":          "orders.events",
    "heartbeat":       "system.hb",
    "review":          "system.review",
    "commands":        "system.commands",
    # Broker gateway streams
    "broker_commands": "broker.commands",
    "broker_fills":    "broker.fills",
}

# Consumer group names
GROUPS = {
    "orchestrator":       "orchestrator-group",
    "scheduler":          "scheduler-group",
    "predictor":          "predictor-group",
    "equity":             "trader-equity-group",
    "options":            "trader-options-group",
    "scraper":            "scraper-group",
    "scraper-ovtlyr":     "scraper-ovtlyr-group",
    "scraper-wsb":        "scraper-wsb-group",
    "scraper-seekalpha":  "scraper-seekalpha-group",
    "scraper-news":             "scraper-news-group",
    "scraper-etf-flows":        "scraper-etf-flows-group",
    "scraper-macro-regime":     "scraper-macro-regime-group",
    "aggregator":         "aggregator-group",
    "review":             "review-agent-group",
    # Broker gateway
    "broker_gateway":     "broker-gateway-group",
    "broker_fills":       "broker-fills-group",
}


async def get_redis(socket_timeout: int = 30) -> aioredis.Redis:
    """Return a Redis client.  socket_timeout must exceed any xreadgroup block= value."""
    client = await aioredis.from_url(
        REDIS_URL,
        encoding="utf-8",
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=socket_timeout,
        retry_on_timeout=True,
        health_check_interval=30,
    )
    return client


async def ensure_consumer_group(
    redis: aioredis.Redis,
    stream: str,
    group: str,
) -> None:
    """Create a consumer group on a stream if it doesn't already exist."""
    try:
        await redis.xgroup_create(stream, group, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning("redis.xgroup_create_failed", stream=stream, group=group, error=str(e))


async def ensure_streams(redis: aioredis.Redis):
    """Create all known streams and consumer groups if they don't exist."""
    for stream in STREAMS.values():
        for group in GROUPS.values():
            try:
                await redis.xgroup_create(stream, group, id="0", mkstream=True)
            except Exception as e:
                if "BUSYGROUP" not in str(e):
                    log.warning("redis.stream_setup_failed", stream=stream, group=group, error=str(e))
