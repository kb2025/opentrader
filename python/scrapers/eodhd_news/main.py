"""
EODHD News Sentiment Scraper
Fetches per-ticker financial news from EODHD (which includes native sentiment scores:
polarity, pos, neg, neu). Optionally enriches high-signal articles with LLM-generated
summaries and keyword extraction. Publishes aggregate sentiment to market.ticks so the
aggregator can factor EODHD news into the predictor signal.

Trigger job: scrape_eodhd_news
Schedule: every 30 min during active session + once after close
"""
import asyncio
import json
import os
from datetime import datetime, timezone
from urllib.parse import urlparse, unquote

import aiohttp
import asyncpg
import structlog

from shared.base_agent import BaseAgent
from shared.redis_client import STREAMS, GROUPS, REDIS_URL, ensure_consumer_group
from shared.assignments import load_active_assignments
from llm.connector import LLMConnector

log = structlog.get_logger("scraper-eodhd-news")

CMD_STREAM     = STREAMS["commands"]
TICKS_STREAM   = STREAMS["ticks"]
CONSUMER_GROUP = GROUPS["scraper-eodhd-news"]
CONSUMER_NAME  = os.getenv("HOSTNAME", "eodhd-news-0")
DB_URL         = os.getenv("DB_URL", "")
EODHD_API_KEY  = os.getenv("EODHD_API_KEY", "")
EODHD_BASE     = "https://eodhd.com/api"

# LLM enrichment: only for articles where |polarity| >= this threshold
LLM_ENRICH       = os.getenv("EODHD_NEWS_LLM_ENRICH", "true").lower() == "true"
LLM_POLARITY_MIN = float(os.getenv("EODHD_NEWS_LLM_THRESHOLD", "0.2"))
# Max articles to enrich per scrape run (keeps LLM cost bounded)
LLM_ENRICH_LIMIT = int(os.getenv("EODHD_NEWS_LLM_LIMIT", "10"))

NEWS_PER_TICKER  = int(os.getenv("EODHD_NEWS_PER_TICKER", "5"))
REQUEST_DELAY_S  = float(os.getenv("EODHD_NEWS_DELAY_S", "0.5"))

_SENTIMENT_LABELS = {
    (0.1,  1.0):  ("bullish",  0.6),
    (-0.1, 0.1):  ("neutral",  0.0),
    (-1.0, -0.1): ("bearish", -0.6),
}


def _polarity_label(polarity: float) -> tuple[str, float]:
    for (lo, hi), (label, score) in _SENTIMENT_LABELS.items():
        if lo <= polarity < hi:
            return label, score
    return "neutral", 0.0


class EodhdNewsScraper(BaseAgent):

    def __init__(self):
        super().__init__("scraper-eodhd-news")
        self._db: asyncpg.Pool | None = None
        self._llm: LLMConnector | None = None

    async def run(self):
        await self.setup()
        import redis.asyncio as aioredis
        self.redis = await aioredis.from_url(
            REDIS_URL, encoding="utf-8", decode_responses=True,
            socket_connect_timeout=10, socket_timeout=30,
            retry_on_timeout=True, health_check_interval=30,
        )
        if DB_URL:
            try:
                p = urlparse(DB_URL)
                self._db = await asyncpg.create_pool(
                    host=p.hostname, port=p.port or 5432,
                    user=p.username,
                    password=unquote(p.password) if p.password else None,
                    database=p.path.lstrip("/"),
                    min_size=1, max_size=3,
                    max_inactive_connection_lifetime=300,
                )
                await self._ensure_table()
                log.info("scraper-eodhd-news.db_connected")
            except Exception as e:
                log.error("scraper-eodhd-news.db_connect_failed", error=str(e))
        if LLM_ENRICH:
            self._llm = LLMConnector("eodhd-news")
        await ensure_consumer_group(self.redis, CMD_STREAM, CONSUMER_GROUP)
        log.info("scraper-eodhd-news.starting")
        await asyncio.gather(self.heartbeat_loop(), self._command_loop())

    async def _ensure_table(self):
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS eodhd_news (
                id           BIGSERIAL PRIMARY KEY,
                ticker       TEXT        NOT NULL,
                title        TEXT        NOT NULL,
                url          TEXT,
                published_at TIMESTAMPTZ,
                source_name  TEXT,
                polarity     REAL        DEFAULT 0,
                pos_score    REAL        DEFAULT 0,
                neg_score    REAL        DEFAULT 0,
                neu_score    REAL        DEFAULT 0,
                llm_summary  TEXT,
                llm_keywords JSONB       DEFAULT '[]',
                scraped_at   TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(ticker, url)
            )
        """)
        await self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_eodhd_news_ticker ON eodhd_news(ticker)"
        )
        await self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_eodhd_news_published ON eodhd_news(published_at DESC)"
        )

    async def _command_loop(self):
        log.info("scraper-eodhd-news.command_loop_start")
        while self._running:
            try:
                if await self.is_halted():
                    await asyncio.sleep(5)
                    continue
                messages = await self.redis.xreadgroup(
                    groupname=CONSUMER_GROUP, consumername=CONSUMER_NAME,
                    streams={CMD_STREAM: ">"}, count=5, block=5000,
                )
                if not messages:
                    continue
                for _stream, entries in messages:
                    for msg_id, data in entries:
                        await self._handle_command(msg_id, data)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error("scraper-eodhd-news.command_loop_error", error=str(e))
                await asyncio.sleep(5)

    async def _handle_command(self, msg_id: str, data: dict):
        job = data.get("job", "")
        try:
            if data.get("command") == "trigger" and job == "scrape_eodhd_news":
                await self._scrape()
        except Exception as e:
            log.error("scraper-eodhd-news.handle_error", job=job, error=str(e))
        finally:
            await self.redis.xack(CMD_STREAM, CONSUMER_GROUP, msg_id)

    async def _get_tickers(self) -> list[str]:
        seen: set[str] = set()
        tickers: list[str] = []

        def add(t: str):
            t = t.upper().strip()
            if t and t not in seen:
                seen.add(t)
                tickers.append(t)

        for b in ("SPY", "QQQ", "IWM"):
            add(b)

        try:
            assignments = load_active_assignments("equity") + load_active_assignments("options")
            for a in assignments:
                add(a.get("ticker", ""))
        except Exception as e:
            log.warning("scraper-eodhd-news.assignments_error", error=str(e))

        try:
            raw = await self.redis.get("broker:position_tickers")
            if raw:
                for t in json.loads(raw):
                    add(t)
        except Exception:
            pass

        return tickers

    async def _fetch_ticker_news(self, session: aiohttp.ClientSession, ticker: str) -> list[dict]:
        sym = f"{ticker}.US"
        try:
            async with session.get(
                f"{EODHD_BASE}/news",
                params={"s": sym, "limit": NEWS_PER_TICKER, "api_token": EODHD_API_KEY, "fmt": "json"},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    log.warning("scraper-eodhd-news.fetch_failed", ticker=ticker, status=resp.status)
                    return []
                data = await resp.json()
                return data if isinstance(data, list) else []
        except Exception as e:
            log.warning("scraper-eodhd-news.fetch_error", ticker=ticker, error=str(e))
            return []

    async def _enrich_with_llm(self, articles: list[dict]) -> dict[str, dict]:
        """Returns {url: {summary, keywords}} for articles that pass the polarity threshold."""
        enriched: dict[str, dict] = {}
        if not self._llm:
            return enriched

        candidates = [
            a for a in articles
            if abs(a.get("_polarity", 0.0)) >= LLM_POLARITY_MIN
            and a.get("link")
        ][:LLM_ENRICH_LIMIT]

        for a in candidates:
            try:
                content_snippet = (a.get("content", "") or "")[:600].strip()
                prompt = (
                    f"Title: {a.get('title', '')}\n"
                    f"Content: {content_snippet}\n\n"
                    "Analyze this financial news article. Respond with JSON only:\n"
                    '{"summary": "2-3 sentence summary focused on market impact", '
                    '"keywords": ["keyword1", "keyword2", "keyword3", "keyword4", "keyword5"]}'
                )
                result = await self._llm.complete_json(prompt, max_tokens=300)
                enriched[a["link"]] = {
                    "summary":  str(result.get("summary", ""))[:500],
                    "keywords": result.get("keywords", [])[:5],
                }
                await asyncio.sleep(0.3)
            except Exception as e:
                log.warning("scraper-eodhd-news.llm_error", url=a.get("link"), error=str(e))

        return enriched

    async def _scrape(self):
        if not EODHD_API_KEY:
            log.warning("scraper-eodhd-news.no_api_key")
            return

        tickers = await self._get_tickers()
        if not tickers:
            return

        log.info("scraper-eodhd-news.scrape_start", tickers=len(tickers))
        all_articles: list[dict] = []
        ticker_scores: dict[str, list[float]] = {}

        async with aiohttp.ClientSession() as session:
            for ticker in tickers:
                raw_articles = await self._fetch_ticker_news(session, ticker)
                for a in raw_articles:
                    sentiment = a.get("sentiment") or {}
                    polarity  = float(sentiment.get("polarity", 0.0))
                    a["_ticker"]   = ticker
                    a["_polarity"] = polarity
                    a["_pos"]      = float(sentiment.get("pos", 0.0))
                    a["_neg"]      = float(sentiment.get("neg", 0.0))
                    a["_neu"]      = float(sentiment.get("neu", 0.0))
                    all_articles.append(a)
                    ticker_scores.setdefault(ticker, []).append(polarity)
                await asyncio.sleep(REQUEST_DELAY_S)

        # LLM enrichment
        enrichment = await self._enrich_with_llm(all_articles)

        # Persist
        if self._db:
            await self._persist(all_articles, enrichment)

        # Publish aggregate sentiment per ticker to market.ticks
        for ticker, scores in ticker_scores.items():
            if not scores:
                continue
            avg_polarity = sum(scores) / len(scores)
            label, score = _polarity_label(avg_polarity)
            await self.redis.xadd(
                TICKS_STREAM,
                {
                    "source":          "eodhd",
                    "ticker":          ticker,
                    "sentiment_score": str(round(score, 4)),
                    "sentiment_label": label,
                    "article_count":   str(len(scores)),
                    "avg_polarity":    str(round(avg_polarity, 4)),
                },
                maxlen=50_000,
            )

        # Cache latest per ticker in Redis
        by_ticker: dict[str, list] = {}
        for a in all_articles:
            t = a["_ticker"]
            by_ticker.setdefault(t, []).append({
                "title":       a.get("title", ""),
                "url":         a.get("link", ""),
                "published_at": a.get("date", ""),
                "source":      a.get("source", ""),
                "polarity":    round(a["_polarity"], 4),
                "pos":         round(a["_pos"], 4),
                "neg":         round(a["_neg"], 4),
                "summary":     enrichment.get(a.get("link", ""), {}).get("summary", ""),
                "keywords":    enrichment.get(a.get("link", ""), {}).get("keywords", []),
            })
        for ticker, items in by_ticker.items():
            await self.redis.set(
                f"eodhd_news:{ticker}", json.dumps(items[:10]), ex=3600
            )

        log.info("scraper-eodhd-news.done",
                 articles=len(all_articles), tickers=len(ticker_scores),
                 llm_enriched=len(enrichment))

    async def _persist(self, articles: list[dict], enrichment: dict[str, dict]):
        for a in articles:
            url = a.get("link", "")
            llm = enrichment.get(url, {})
            try:
                published_at = None
                raw_date = a.get("date", "")
                if raw_date:
                    try:
                        published_at = datetime.fromisoformat(
                            raw_date.replace("Z", "+00:00")
                        )
                    except ValueError:
                        pass

                await self._db.execute(
                    """INSERT INTO eodhd_news
                       (ticker, title, url, published_at, source_name,
                        polarity, pos_score, neg_score, neu_score,
                        llm_summary, llm_keywords)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                       ON CONFLICT (ticker, url) DO UPDATE SET
                           polarity    = EXCLUDED.polarity,
                           llm_summary = COALESCE(EXCLUDED.llm_summary, eodhd_news.llm_summary),
                           llm_keywords= COALESCE(EXCLUDED.llm_keywords, eodhd_news.llm_keywords),
                           scraped_at  = NOW()
                    """,
                    a["_ticker"],
                    (a.get("title", "") or "")[:500],
                    url or None,
                    published_at,
                    (a.get("source", "") or "")[:100],
                    round(a["_polarity"], 6),
                    round(a["_pos"], 6),
                    round(a["_neg"], 6),
                    round(a["_neu"], 6),
                    llm.get("summary") or None,
                    json.dumps(llm.get("keywords", [])),
                )
            except Exception as e:
                log.warning("scraper-eodhd-news.persist_error",
                            ticker=a.get("_ticker"), error=str(e))


def main():
    asyncio.run(EodhdNewsScraper().run())


if __name__ == "__main__":
    main()
