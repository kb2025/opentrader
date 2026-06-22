"""
M&A Deals Scraper Agent

Fetches recent merger and acquisition filings from SEC EDGAR full-text search
and publishes deal signals. Also persists deal records to TimescaleDB.

Trigger job : scrape_ma_deals
Schedule    : daily at 09:00 ET (or as configured in scheduler)
"""
import asyncio
import json
import os
from datetime import datetime, timezone

import asyncpg
import structlog

from scrapers.base import BaseScraper
from .scraper import scrape_ma_deals

log = structlog.get_logger("scraper-ma-deals")

DB_URL = os.getenv("DB_URL", "")


class MADealsScraperAgent(BaseScraper):
    SOURCE      = "ma_deals"
    TRIGGER_JOB = "scrape_ma_deals"
    GROUP_KEY   = "scraper-ma-deals"

    def __init__(self):
        super().__init__("scraper-ma-deals")
        self._db: asyncpg.Pool | None = None

    async def _on_start(self):
        """Connect to TimescaleDB and ensure the ma_deals table exists."""
        if not DB_URL:
            log.warning("scraper-ma-deals.no_db_url")
            return
        try:
            self._db = await asyncpg.create_pool(
                DB_URL,
                min_size=1,
                max_size=3,
                max_inactive_connection_lifetime=300,
            )
            await self._ensure_tables()
            log.info("scraper-ma-deals.db_connected")
        except Exception as e:
            log.error("scraper-ma-deals.db_connect_failed", error=str(e))
            self._db = None

    async def _ensure_tables(self):
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS ma_deals (
                id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                acquirer   TEXT,
                target     TEXT,
                form_type  TEXT,
                filing_date TEXT,
                deal_url   TEXT,
                tickers    TEXT,
                scraped_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        await self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_ma_deals_scraped ON ma_deals(scraped_at DESC)"
        )
        await self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_ma_deals_filing_date ON ma_deals(filing_date DESC)"
        )

    async def scrape(self):
        log.info("scraper-ma-deals.scrape_start")
        deals = await scrape_ma_deals()

        if not deals:
            log.info("scraper-ma-deals.no_results")
            return

        published = 0
        persisted = 0
        now = datetime.now(timezone.utc)

        for deal in deals:
            acquirer    = deal.get("acquirer", "")
            target      = deal.get("target", "")
            form_type   = deal.get("form_type", "")
            filing_date = deal.get("filing_date", "")
            deal_url    = deal.get("deal_url", "")
            tickers     = deal.get("tickers", [])
            ts_utc      = deal.get("ts_utc", 0)
            tickers_str = json.dumps(tickers)

            # Publish to market.ticks stream — use "MA_DEAL" as ticker
            try:
                await self.publish("MA_DEAL", {
                    "acquirer":    acquirer[:200],
                    "target":      target[:200],
                    "form_type":   form_type[:20],
                    "filing_date": filing_date,
                    "deal_url":    deal_url[:500],
                    "tickers":     tickers_str,
                    "ts_utc":      str(ts_utc),
                })
                published += 1
            except Exception as e:
                log.warning("scraper-ma-deals.publish_error", acquirer=acquirer, error=str(e))

            # Persist to DB
            if self._db:
                try:
                    await self._db.execute(
                        """
                        INSERT INTO ma_deals
                            (acquirer, target, form_type, filing_date, deal_url, tickers, scraped_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        """,
                        acquirer[:200],
                        target[:200],
                        form_type[:20],
                        filing_date,
                        deal_url[:500],
                        tickers_str,
                        now,
                    )
                    persisted += 1
                except Exception as e:
                    log.warning("scraper-ma-deals.persist_error", acquirer=acquirer, error=str(e))

        log.info("scraper-ma-deals.done", published=published, persisted=persisted)

    async def shutdown(self):
        if self._db:
            await self._db.close()
        await super().shutdown()


async def main():
    agent = MADealsScraperAgent()
    try:
        await agent.run()
    finally:
        await agent.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
