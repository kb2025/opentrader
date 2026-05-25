"""
Market Data Gateway — ot-market-data
Single source of truth for all market data across the OpenTrader platform.

Consumers call GET /data/{type}/{ticker} and receive cached, provider-agnostic data.
Adding a new provider: drop a file in connectors/ + add to config/data_providers.toml.
"""
import asyncio
import os
import time
import toml
import structlog
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from shared.base_agent import BaseAgent
from shared.redis_client import get_redis

from market_data.router import Router
from market_data.cache import DataCache
from market_data.refresher import Refresher

log = structlog.get_logger("market_data")

CONFIG_PATH = os.getenv("DATA_PROVIDERS_CONFIG", "/app/config/data_providers.toml")
PORT = int(os.getenv("MARKET_DATA_PORT", "8090"))


def _load_config() -> tuple[dict[str, list[str]], dict[str, int]]:
    try:
        cfg = toml.load(CONFIG_PATH)
    except Exception as e:
        log.warning("config.load_failed", path=CONFIG_PATH, error=str(e))
        cfg = {}
    priority = {dt: v.get("providers", []) for dt, v in cfg.items()}
    ttls     = {dt: v.get("ttl", 300)      for dt, v in cfg.items()}
    return priority, ttls


# ── App lifecycle ─────────────────────────────────────────────────────────────

_router: Router | None = None
_agent: BaseAgent | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _router, _agent

    redis = await get_redis()
    priority, ttls = _load_config()

    _router = Router(priority=priority, ttls=ttls)
    _router.discover()
    _router.set_cache(DataCache(redis, ttls))

    _agent = BaseAgent("market-data")
    _agent.redis = redis

    # Initial probe
    await _router.probe_all()

    refresher = Refresher(redis, _router)
    hb_task  = asyncio.create_task(_agent.heartbeat_loop())
    ref_task = asyncio.create_task(refresher.run())

    log.info("market_data.ready", port=PORT)
    yield

    hb_task.cancel()
    ref_task.cancel()
    await redis.aclose()


app = FastAPI(title="Market Data Gateway", lifespan=lifespan)


# ── Health / capabilities / stats ─────────────────────────────────────────────

@app.get("/health")
async def health():
    return _router.health()


@app.get("/capabilities")
async def capabilities():
    return _router.capabilities()


@app.get("/stats")
async def stats():
    cache_stats = await _router._cache.stats() if _router._cache else {}
    return {"data_types": _router.stats(), "cache": cache_stats}


@app.post("/probe")
async def probe():
    before = {n: sorted(v) for n, v in _router._capabilities.items()}
    await _router.probe_all()
    after  = {n: sorted(v) for n, v in _router._capabilities.items()}
    diff = {n: {"before": before.get(n, []), "after": after.get(n, [])}
            for n in set(list(before) + list(after))
            if before.get(n) != after.get(n)}
    return {"probed_at": time.time(), "changes": diff}


@app.post("/warm")
async def warm(body: dict):
    tickers = body.get("tickers", [])
    return {"queued": tickers}


# ── Data endpoints ────────────────────────────────────────────────────────────

async def _fetch(data_type: str, params: dict) -> JSONResponse:
    if _router is None:
        raise HTTPException(503, "Gateway not ready")
    try:
        resp = await _router.fetch(data_type, params)
        return JSONResponse({
            "data":      resp.data,
            "provider":  resp.provider,
            "cached":    resp.cached,
            "timestamp": resp.timestamp,
        })
    except Exception as e:
        log.warning("gateway.fetch_failed", data_type=data_type, params=params, error=str(e))
        raise HTTPException(503, str(e))


@app.get("/data/quote/{ticker}")
async def get_quote(ticker: str):
    return await _fetch("quote", {"ticker": ticker.upper()})


@app.get("/data/bars/{ticker}")
async def get_bars(ticker: str, days: int = 30, interval: str = "daily"):
    return await _fetch("bars_daily" if interval == "daily" else "bars_intraday",
                        {"ticker": ticker.upper(), "days": days, "interval": interval})


@app.get("/data/fundamentals/{ticker}")
async def get_fundamentals(ticker: str):
    return await _fetch("fundamentals", {"ticker": ticker.upper()})


@app.get("/data/analyst/{ticker}")
async def get_analyst(ticker: str):
    return await _fetch("analyst_consensus", {"ticker": ticker.upper()})


@app.get("/data/earnings/{ticker}")
async def get_earnings(ticker: str):
    return await _fetch("earnings", {"ticker": ticker.upper()})


@app.get("/data/dividends/{ticker}")
async def get_dividends(ticker: str):
    return await _fetch("dividends", {"ticker": ticker.upper()})


@app.get("/data/news/{ticker}")
async def get_news(ticker: str, limit: int = 20):
    return await _fetch("news", {"ticker": ticker.upper(), "limit": limit})


@app.get("/data/sentiment/{ticker}")
async def get_sentiment(ticker: str):
    return await _fetch("sentiment_news", {"ticker": ticker.upper()})


@app.get("/data/technicals/{ticker}")
async def get_technicals(ticker: str, interval: str = "1d"):
    return await _fetch("technicals", {"ticker": ticker.upper(), "interval": interval})


@app.get("/data/options/chain/{ticker}")
async def get_options_chain(ticker: str, expiration: str = ""):
    params = {"ticker": ticker.upper()}
    if expiration:
        params["expiration"] = expiration
    return await _fetch("options_chain", params)


@app.get("/data/options/flow/{ticker}")
async def get_options_flow(ticker: str):
    return await _fetch("options_flow", {"ticker": ticker.upper()})


@app.get("/data/darkpool/{ticker}")
async def get_darkpool(ticker: str):
    return await _fetch("dark_pool", {"ticker": ticker.upper()})


@app.get("/data/insider/{ticker}")
async def get_insider(ticker: str):
    return await _fetch("insider_transactions", {"ticker": ticker.upper()})


@app.get("/data/breadth/{indicator}")
async def get_breadth(indicator: str):
    return await _fetch("breadth", {"indicator": indicator})


@app.get("/data/macro/{indicator}")
async def get_macro(indicator: str, n: int = 1):
    return await _fetch("macro", {"indicator": indicator, "n": n})


@app.get("/data/classification/{ticker}")
async def get_classification(ticker: str):
    return await _fetch("classification", {"ticker": ticker.upper()})


@app.get("/data/short_interest/{ticker}")
async def get_short_interest(ticker: str):
    return await _fetch("short_interest", {"ticker": ticker.upper()})


@app.get("/data/avg_volume/{ticker}")
async def get_avg_volume(ticker: str):
    return await _fetch("avg_volume", {"ticker": ticker.upper()})


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("market_data.main:app", host="0.0.0.0", port=PORT, log_level="info")
