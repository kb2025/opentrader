"""
Polymarket Prediction Market Scraper Agent

Fetches active finance-relevant markets from Polymarket (earnings, macro, M&A)
and publishes implied probabilities as tick signals. Results are also persisted
to TimescaleDB for historical tracking.

Trigger job : scrape_polymarket
Schedule    : configurable via scheduler (suggested: every 30 minutes during market hours)
"""
import asyncio
import os
from datetime import datetime, timezone

import asyncpg
import structlog

from scrapers.base import BaseScraper
from .scraper import scrape_polymarket

log = structlog.get_logger("scraper-polymarket")

DB_URL = os.getenv("DB_URL", "")
MAX_QUESTION_LEN = 200


class PolymarketScraperAgent(BaseScraper):
    SOURCE      = "polymarket"
    TRIGGER_JOB = "scrape_polymarket"
    GROUP_KEY   = "scraper-polymarket"

    def __init__(self):
        super().__init__("scraper-polymarket")
        self._db: asyncpg.Pool | None = None

    async def _on_start(self):
        """Connect to TimescaleDB and ensure the polymarket_signals table exists."""
        if not DB_URL:
            log.warning("scraper-polymarket.no_db_url")
            return
        try:
            self._db = await asyncpg.create_pool(
                DB_URL,
                min_size=1,
                max_size=3,
                max_inactive_connection_lifetime=300,
            )
            await self._ensure_tables()
            log.info("scraper-polymarket.db_connected")
        except Exception as e:
            log.error("scraper-polymarket.db_connect_failed", error=str(e))
            self._db = None

    async def _ensure_tables(self):
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS polymarket_signals (
                id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                market_id  TEXT NOT NULL,
                ticker     TEXT NOT NULL,
                question   TEXT,
                yes_price  DOUBLE PRECISION,
                no_price   DOUBLE PRECISION,
                volume     DOUBLE PRECISION,
                category   TEXT,
                scraped_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        await self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_pm_signals_ticker ON polymarket_signals(ticker)"
        )
        await self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_pm_signals_scraped ON polymarket_signals(scraped_at DESC)"
        )
        await self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_pm_signals_market_id ON polymarket_signals(market_id)"
        )

    async def scrape(self):
        log.info("scraper-polymarket.scrape_start")
        results = await scrape_polymarket()

        if not results:
            log.info("scraper-polymarket.no_results")
            return

        published = 0
        persisted = 0
        now = datetime.now(timezone.utc)

        for item in results:
            ticker    = item["ticker"]
            question  = item["question"][:MAX_QUESTION_LEN]
            yes_price = item["yes_price"]
            no_price  = item["no_price"]
            volume    = item["volume"]
            market_id = item["market_id"]
            category  = item["category"]
            ts_utc    = item["ts_utc"]

            # Publish to market.ticks stream
            try:
                await self.publish(ticker, {
                    "yes_price": str(yes_price),
                    "no_price":  str(no_price),
                    "volume":    str(volume),
                    "market_id": market_id,
                    "category":  category,
                    "question":  question,
                    "ts_utc":    str(ts_utc),
                })
                published += 1
            except Exception as e:
                log.warning("scraper-polymarket.publish_error", ticker=ticker, error=str(e))

            # Persist to DB
            if self._db:
                try:
                    await self._db.execute(
                        """
                        INSERT INTO polymarket_signals
                            (market_id, ticker, question, yes_price, no_price, volume, category, scraped_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        """,
                        market_id,
                        ticker,
                        question,
                        yes_price,
                        no_price,
                        volume,
                        category,
                        now,
                    )
                    persisted += 1
                except Exception as e:
                    log.warning("scraper-polymarket.persist_error", market_id=market_id, error=str(e))

        log.info("scraper-polymarket.done", published=published, persisted=persisted)

    async def shutdown(self):
        if self._db:
            await self._db.close()
        await super().shutdown()


async def main():
    agent = PolymarketScraperAgent()
    try:
        await agent.run()
    finally:
        await agent.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
