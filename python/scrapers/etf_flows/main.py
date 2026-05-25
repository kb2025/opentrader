"""ETF Capital Flow Scraper Agent"""
import asyncio
import json
import os
from urllib.parse import urlparse, unquote

import asyncpg
import structlog

from shared.base_agent import BaseAgent
from shared.redis_client import STREAMS, GROUPS, REDIS_URL, ensure_consumer_group
from .scraper import fetch_etf_flows

log = structlog.get_logger("scraper-etf-flows")

DB_URL  = os.getenv("DB_URL", "")
API_KEY = os.getenv("MASSIVE_API_KEY", "")


class ETFFlowsAgent(BaseAgent):

    def __init__(self):
        super().__init__("scraper-etf-flows")
        self._db: asyncpg.Pool | None = None

    async def run(self):
        await self.setup()
        import redis.asyncio as aioredis
        self.redis = await aioredis.from_url(
            REDIS_URL, encoding="utf-8", decode_responses=True,
            socket_connect_timeout=10, socket_timeout=30,
            retry_on_timeout=True, health_check_interval=30,
        )
        if DB_URL:
            p = urlparse(DB_URL)
            self._db = await asyncpg.create_pool(
                min_size=1, max_size=3,
                host=p.hostname, port=p.port or 5432,
                user=p.username,
                password=unquote(p.password) if p.password else None,
                database=p.path.lstrip("/"),
            )
        try:
            await self._ensure_group()
        except Exception:
            pass
        await asyncio.gather(self.heartbeat_loop(), self._command_loop())

    async def _ensure_group(self):
        await ensure_consumer_group(self.redis, STREAMS["commands"], GROUPS["scraper-etf-flows"])

    async def _command_loop(self):
        consumer = os.getenv("HOSTNAME", "scraper-etf-flows-0")
        while self._running:
            try:
                messages = await self.redis.xreadgroup(
                    groupname=GROUPS["scraper-etf-flows"],
                    consumername=consumer,
                    streams={STREAMS["commands"]: ">"},
                    count=5, block=5000,
                )
                if not messages:
                    continue
                for _stream, entries in messages:
                    for msg_id, data in entries:
                        if data.get("command") == "trigger" and data.get("job") == "scrape_etf_flows":
                            await self._scrape()
                        await self.redis.xack(STREAMS["commands"], GROUPS["scraper-etf-flows"], msg_id)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.warning("etf_flows.command_loop_error", error=str(e))
                await asyncio.sleep(10)

    async def _scrape(self):
        try:
            rows = await fetch_etf_flows()
            if self._db:
                await self._persist(rows)
            await self._cache(rows)
            log.info("etf_flows.done", count=len(rows))
        except Exception as e:
            log.error("etf_flows.scrape_error", error=str(e))

    async def _persist(self, rows: list[dict]):
        for r in rows:
            await self._db.execute(
                """INSERT INTO etf_flow_snapshots
                   (ticker, name, category, price, volume, dollar_volume, avg_volume_30d, flow_ratio, change_pct)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)""",
                r["ticker"], r["name"], r["category"], r["price"],
                r["volume"], r["dollar_volume"], r["avg_volume_30d"],
                r["flow_ratio"], r["change_pct"],
            )

    async def _cache(self, rows: list[dict]):
        key = "etf_flows:latest"
        await self.redis.set(key, json.dumps(rows), ex=7200)
        log.info("etf_flows.cached", key=key, count=len(rows))


def main():
    asyncio.run(ETFFlowsAgent().run())


if __name__ == "__main__":
    main()
