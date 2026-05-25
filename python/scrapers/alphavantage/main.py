"""Alpha Vantage News Sentiment Scraper Agent"""
import asyncio
import json
import os
from datetime import datetime, timezone
from urllib.parse import urlparse, unquote

import asyncpg
import structlog

from shared.base_agent import BaseAgent
from shared.redis_client import STREAMS, GROUPS, REDIS_URL, ensure_consumer_group
from shared.assignments import load_active_assignments
from shared.crypto import decrypt_secret
from .scraper import fetch_news_sentiment, fetch_ticker_news_sentiment

log = structlog.get_logger("scraper-news")

DB_URL   = os.getenv("DB_URL", "")
_ENV_KEY = os.getenv("ALPHA_VANTAGE_API_KEY", "")

# How many tickers to include in the per-ticker sentiment fetch
AV_TICKER_LIMIT = int(os.getenv("AV_TICKER_LIMIT", "40"))
# Minimum relevance score to publish to market.ticks
AV_MIN_RELEVANCE = float(os.getenv("AV_MIN_RELEVANCE", "0.3"))


class NewsSentimentAgent(BaseAgent):

    def __init__(self):
        super().__init__("scraper-news")
        self._db: asyncpg.Pool | None = None

    async def _get_api_key(self) -> str:
        if _ENV_KEY:
            return _ENV_KEY
        if not self._db:
            return ""
        try:
            row = await self._db.fetchrow(
                "SELECT encrypted_value FROM user_secrets WHERE key='ALPHA_VANTAGE_API_KEY' LIMIT 1"
            )
            if row:
                return decrypt_secret(row["encrypted_value"])
        except Exception as e:
            log.warning("news_sentiment.key_load_error", error=str(e))
        return ""

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
            await self._ensure_ticker_table()
        await ensure_consumer_group(self.redis, STREAMS["commands"], GROUPS["scraper-news"])
        await asyncio.gather(self.heartbeat_loop(), self._command_loop())

    async def _ensure_ticker_table(self):
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS av_ticker_sentiment (
                id                       BIGSERIAL PRIMARY KEY,
                ticker                   TEXT NOT NULL,
                title                    TEXT NOT NULL,
                url                      TEXT,
                time_published           TIMESTAMPTZ,
                source                   TEXT,
                overall_sentiment_label  TEXT,
                overall_sentiment_score  REAL DEFAULT 0,
                ticker_relevance_score   REAL DEFAULT 0,
                ticker_sentiment_score   REAL DEFAULT 0,
                ticker_sentiment_label   TEXT,
                summary                  TEXT,
                scraped_at               TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(ticker, url)
            )
        """)
        await self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_av_ticker_sent_ticker ON av_ticker_sentiment(ticker)"
        )
        await self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_av_ticker_sent_pub ON av_ticker_sentiment(time_published DESC)"
        )

    async def _command_loop(self):
        consumer = os.getenv("HOSTNAME", "scraper-news-0")
        while self._running:
            try:
                messages = await self.redis.xreadgroup(
                    groupname=GROUPS["scraper-news"],
                    consumername=consumer,
                    streams={STREAMS["commands"]: ">"},
                    count=5, block=5000,
                )
                if not messages:
                    continue
                for _stream, entries in messages:
                    for msg_id, data in entries:
                        if data.get("command") == "trigger" and data.get("job") == "scrape_news_sentiment":
                            await self._scrape()
                        await self.redis.xack(STREAMS["commands"], GROUPS["scraper-news"], msg_id)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.warning("news_sentiment.command_loop_error", error=str(e))
                await asyncio.sleep(10)

    def _get_target_tickers(self) -> list[str]:
        seen: set[str] = set()
        tickers: list[str] = []

        def add(t: str):
            t = t.upper().strip()
            if t and t not in seen:
                seen.add(t)
                tickers.append(t)

        for b in ("SPY", "QQQ", "IWM", "AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "GOOGL", "META"):
            add(b)
        try:
            for a in load_active_assignments("equity") + load_active_assignments("options"):
                add(a.get("ticker", ""))
        except Exception:
            pass
        return tickers[:AV_TICKER_LIMIT]

    async def _scrape(self):
        av_key = await self._get_api_key()
        if not av_key:
            log.warning("news_sentiment.no_api_key")
            return
        try:
            # --- Topic-based fetch (existing behaviour) ---
            articles = await fetch_news_sentiment(av_key)
            if self._db:
                await self._persist(articles)
            await self._cache(articles)
            log.info("news_sentiment.topic_done", count=len(articles))

            # --- Ticker-targeted fetch (new: structured per-ticker sentiment) ---
            tickers = self._get_target_tickers()
            if tickers:
                ticker_rows = await fetch_ticker_news_sentiment(av_key, tickers)
                if self._db:
                    await self._persist_ticker_sentiment(ticker_rows)
                await self._publish_ticker_sentiment(ticker_rows)
                log.info("news_sentiment.ticker_done", tickers=len(tickers), rows=len(ticker_rows))
        except Exception as e:
            log.error("news_sentiment.scrape_error", error=str(e))

    async def _persist(self, articles: list[dict]):
        for a in articles:
            try:
                await self._db.execute(
                    """INSERT INTO news_sentiment_snapshots
                       (category, ticker, title, source, url, overall_score, relevance_score, topics, raw)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                       ON CONFLICT DO NOTHING""",
                    a["category"], a.get("ticker"), a["title"], a["source"],
                    a["url"], a["overall_score"], a["relevance_score"],
                    json.dumps(a["topics"]), json.dumps(a["raw"]),
                )
            except Exception as e:
                log.warning("news_sentiment.persist_error", error=str(e))

    async def _persist_ticker_sentiment(self, rows: list[dict]):
        for r in rows:
            try:
                pub_dt = None
                raw_time = r.get("time_published", "")
                if raw_time and len(raw_time) >= 8:
                    try:
                        pub_dt = datetime.strptime(raw_time[:15], "%Y%m%dT%H%M%S").replace(
                            tzinfo=timezone.utc
                        )
                    except ValueError:
                        pass

                await self._db.execute(
                    """INSERT INTO av_ticker_sentiment
                       (ticker, title, url, time_published, source,
                        overall_sentiment_label, overall_sentiment_score,
                        ticker_relevance_score, ticker_sentiment_score,
                        ticker_sentiment_label, summary)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                       ON CONFLICT (ticker, url) DO UPDATE SET
                           ticker_relevance_score  = EXCLUDED.ticker_relevance_score,
                           ticker_sentiment_score  = EXCLUDED.ticker_sentiment_score,
                           ticker_sentiment_label  = EXCLUDED.ticker_sentiment_label,
                           scraped_at              = NOW()
                    """,
                    r["ticker"], r["title"], r.get("url"), pub_dt,
                    r.get("source", ""),
                    r.get("overall_sentiment_label", "Neutral"),
                    r.get("overall_sentiment_score", 0.0),
                    r.get("ticker_relevance_score", 0.0),
                    r.get("ticker_sentiment_score", 0.0),
                    r.get("ticker_sentiment_label", "Neutral"),
                    r.get("summary", "") or None,
                )
            except Exception as e:
                log.warning("news_sentiment.ticker_persist_error", ticker=r.get("ticker"), error=str(e))

    async def _publish_ticker_sentiment(self, rows: list[dict]):
        """Aggregate per-ticker sentiment and publish to market.ticks."""
        by_ticker: dict[str, list[float]] = {}
        for r in rows:
            if r.get("ticker_relevance_score", 0) >= AV_MIN_RELEVANCE:
                by_ticker.setdefault(r["ticker"], []).append(r["ticker_sentiment_score"])

        for ticker, scores in by_ticker.items():
            if not scores:
                continue
            avg = sum(scores) / len(scores)
            label = (
                "bullish" if avg >= 0.15
                else "bearish" if avg <= -0.15
                else "neutral"
            )
            try:
                await self.redis.xadd(
                    STREAMS["ticks"],
                    {
                        "source":          "alpha_vantage",
                        "ticker":          ticker,
                        "sentiment_score": str(round(avg, 4)),
                        "sentiment_label": label,
                        "article_count":   str(len(scores)),
                    },
                    maxlen=50_000,
                )
            except Exception as e:
                log.warning("news_sentiment.publish_error", ticker=ticker, error=str(e))

    async def _cache(self, articles: list[dict]):
        by_cat: dict[str, list] = {}
        for a in articles:
            by_cat.setdefault(a["category"], []).append(a)
        summary = {cat: items[:10] for cat, items in by_cat.items()}
        await self.redis.set("news_sentiment:latest", json.dumps(summary), ex=3600)


def main():
    asyncio.run(NewsSentimentAgent().run())


if __name__ == "__main__":
    main()
