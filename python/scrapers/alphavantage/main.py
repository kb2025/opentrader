"""Alpha Vantage News Sentiment Scraper Agent"""
import asyncio
import base64
import hashlib
import json
import os
from urllib.parse import urlparse, unquote

import asyncpg
import structlog

from shared.base_agent import BaseAgent
from shared.redis_client import STREAMS, GROUPS, REDIS_URL
from .scraper import fetch_news_sentiment

log = structlog.get_logger("scraper-news")

DB_URL     = os.getenv("DB_URL", "")
_ENV_KEY   = os.getenv("ALPHA_VANTAGE_API_KEY", "")
_SECRET_KEY = os.getenv("SECRET_KEY", "change-me-please-set-SECRET_KEY-in-env")


def _decrypt(token: str) -> str:
    from cryptography.fernet import Fernet
    raw = hashlib.sha256(_SECRET_KEY.encode()).digest()
    return Fernet(base64.urlsafe_b64encode(raw)).decrypt(token.encode()).decode()


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
                return _decrypt(row["encrypted_value"])
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
        try:
            await self.redis.xgroup_create(
                STREAMS["commands"], GROUPS["scraper-news"], id="$", mkstream=True
            )
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                log.warning("news_sentiment.group_error", error=str(e))
        await asyncio.gather(self.heartbeat_loop(), self._command_loop())

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

    async def _scrape(self):
        av_key = await self._get_api_key()
        if not av_key:
            log.warning("news_sentiment.no_api_key")
            return
        try:
            articles = await fetch_news_sentiment(av_key)
            if self._db:
                await self._persist(articles)
            await self._cache(articles)
            log.info("news_sentiment.done", count=len(articles))
        except Exception as e:
            log.error("news_sentiment.scrape_error", error=str(e))

    async def _persist(self, articles: list[dict]):
        for a in articles:
            try:
                await self._db.execute(
                    """INSERT INTO news_sentiment_snapshots
                       (category, ticker, title, source, url, overall_score, relevance_score, topics, raw)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)""",
                    a["category"], a.get("ticker"), a["title"], a["source"],
                    a["url"], a["overall_score"], a["relevance_score"],
                    json.dumps(a["topics"]), json.dumps(a["raw"]),
                )
            except Exception as e:
                log.warning("news_sentiment.persist_error", error=str(e))

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
