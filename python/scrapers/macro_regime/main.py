"""Macro Regime Scraper Agent"""
import asyncio
import json
import os
from urllib.parse import urlparse, unquote

import asyncpg
import structlog

from shared.base_agent import BaseAgent
from shared.redis_client import STREAMS, GROUPS, REDIS_URL, ensure_consumer_group
from .scraper import compute_macro_regime

log = structlog.get_logger("scraper-macro-regime")

DB_URL       = os.getenv("DB_URL", "")
API_KEY      = os.getenv("MASSIVE_API_KEY", "")
FRED_API_KEY = os.getenv("FRED_API_KEY", "")


class MacroRegimeAgent(BaseAgent):

    def __init__(self):
        super().__init__("scraper-macro-regime")
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
            await self._ensure_tables()
        await ensure_consumer_group(self.redis, STREAMS["commands"], GROUPS["scraper-macro-regime"])
        await asyncio.gather(self.heartbeat_loop(), self._command_loop())

    async def _ensure_tables(self):
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS fred_macro_snapshots (
                ts       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                hy_oas   NUMERIC,
                ig_oas   NUMERIC,
                fsi      NUMERIC,
                usrec    SMALLINT
            )
        """)

    async def _command_loop(self):
        consumer = os.getenv("HOSTNAME", "scraper-macro-regime-0")
        while self._running:
            try:
                messages = await self.redis.xreadgroup(
                    groupname=GROUPS["scraper-macro-regime"],
                    consumername=consumer,
                    streams={STREAMS["commands"]: ">"},
                    count=5, block=5000,
                )
                if not messages:
                    continue
                for _stream, entries in messages:
                    for msg_id, data in entries:
                        if data.get("command") == "trigger" and data.get("job") == "scrape_macro_regime":
                            await self._scrape()
                        await self.redis.xack(STREAMS["commands"], GROUPS["scraper-macro-regime"], msg_id)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.warning("macro_regime.command_loop_error", error=str(e))
                await asyncio.sleep(10)

    async def _scrape(self):
        if not API_KEY:
            log.warning("macro_regime.no_api_key")
            return
        try:
            breadth_raw = await self.redis.get("ovtlyr:market_breadth")
            breadth_pct = None
            if breadth_raw:
                b = json.loads(breadth_raw)
                breadth_pct = float(b.get("breadth_pct", 0))

            snapshot = await compute_macro_regime(API_KEY, breadth_pct, fred_api_key=FRED_API_KEY)

            if self._db:
                await self._db.execute(
                    """INSERT INTO macro_regime_snapshots
                       (regime, bull_signals, bear_signals, total_signals, regime_score,
                        spy_trend, vix_level, dxy_trend, tlt_trend, breadth_pct, raw)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)""",
                    snapshot["regime"], snapshot["bull_signals"], snapshot["bear_signals"],
                    snapshot["total_signals"], snapshot["regime_score"],
                    snapshot["spy_trend"], snapshot["vix_level"],
                    snapshot["dxy_trend"], snapshot["tlt_trend"],
                    snapshot["breadth_pct"], json.dumps(snapshot["raw"]),
                )

                fred = snapshot.get("fred", {})
                if any(v is not None for v in fred.values()):
                    await self._db.execute(
                        """INSERT INTO fred_macro_snapshots (hy_oas, ig_oas, fsi, usrec)
                           VALUES ($1, $2, $3, $4)""",
                        fred.get("hy_oas"), fred.get("ig_oas"),
                        fred.get("fsi"), fred.get("usrec"),
                    )

            await self.redis.set("macro_regime:latest", json.dumps(snapshot), ex=7200)
            if snapshot.get("fred"):
                await self.redis.set("fred:macro:latest", json.dumps(snapshot["fred"]), ex=7200)

            log.info("macro_regime.done", regime=snapshot["regime"], score=snapshot["regime_score"],
                     hy_oas=snapshot["fred"].get("hy_oas"), fsi=snapshot["fred"].get("fsi"))
        except Exception as e:
            log.error("macro_regime.scrape_error", error=str(e))


def main():
    asyncio.run(MacroRegimeAgent().run())


if __name__ == "__main__":
    main()
