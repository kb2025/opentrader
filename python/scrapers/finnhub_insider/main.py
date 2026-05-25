"""
Finnhub Insider Transactions & Sentiment Scraper

Fetches insider buying/selling activity (individual transactions) and monthly
insider sentiment (MSPR — Monthly Share Purchase Ratio) from the Finnhub free API.
These are strong empirically-validated return predictors; net insider buying is
particularly useful as a feature for the ML ensemble predictor.

Trigger job : scrape_finnhub_insider
Schedule    : daily at 17:05 ET (after market close)
"""
import asyncio
import json
import os
from datetime import datetime, date, timezone
from urllib.parse import urlparse, unquote

import aiohttp
import asyncpg
import structlog

from shared.base_agent import BaseAgent
from shared.redis_client import STREAMS, GROUPS, REDIS_URL, ensure_consumer_group
from shared.assignments import load_active_assignments

log = structlog.get_logger("scraper-finnhub-insider")

CMD_STREAM     = STREAMS["commands"]
TICKS_STREAM   = STREAMS["ticks"]
CONSUMER_GROUP = GROUPS["scraper-finnhub-insider"]
CONSUMER_NAME  = os.getenv("HOSTNAME", "finnhub-insider-0")
DB_URL         = os.getenv("DB_URL", "")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")
FINNHUB_BASE    = "https://finnhub.io/api/v1"

# Rate limit: Finnhub free tier = 60 calls/min → ~1 call/sec safe
REQUEST_DELAY_S = float(os.getenv("FINNHUB_DELAY_S", "1.2"))
# Number of months of insider sentiment history to fetch
SENTIMENT_MONTHS = int(os.getenv("FINNHUB_SENTIMENT_MONTHS", "6"))
# Max tickers to process per run (prevents overrunning free tier quota)
MAX_TICKERS = int(os.getenv("FINNHUB_INSIDER_MAX_TICKERS", "30"))

# Finnhub transaction codes
TRANSACTION_LABELS = {
    "P":  "Purchase",
    "S":  "Sale",
    "A":  "Award",
    "D":  "Disposition",
    "F":  "Tax Withholding",
    "G":  "Gift",
    "M":  "Option Exercise",
    "X":  "Exercise & Sale",
    "C":  "Conversion",
    "I":  "Discretionary",
    "J":  "Other",
}


class FinnhubInsiderScraper(BaseAgent):

    def __init__(self):
        super().__init__("scraper-finnhub-insider")
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
                await self._ensure_tables()
                log.info("scraper-finnhub-insider.db_connected")
            except Exception as e:
                log.error("scraper-finnhub-insider.db_connect_failed", error=str(e))
        await ensure_consumer_group(self.redis, CMD_STREAM, CONSUMER_GROUP)
        log.info("scraper-finnhub-insider.starting")
        await asyncio.gather(self.heartbeat_loop(), self._command_loop())

    async def _ensure_tables(self):
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS insider_transactions (
                id               BIGSERIAL PRIMARY KEY,
                ticker           TEXT    NOT NULL,
                name             TEXT,
                share            BIGINT,
                change           BIGINT,
                filing_date      DATE,
                transaction_date DATE,
                transaction_code TEXT,
                transaction_price REAL,
                scraped_at       TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(ticker, name, transaction_date, transaction_code, share)
            )
        """)
        await self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_insider_tx_ticker ON insider_transactions(ticker)"
        )
        await self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_insider_tx_date ON insider_transactions(transaction_date DESC)"
        )
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS insider_sentiment (
                id         BIGSERIAL PRIMARY KEY,
                ticker     TEXT NOT NULL,
                year       INT,
                month      INT,
                change     BIGINT,
                mspr       REAL,
                scraped_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(ticker, year, month)
            )
        """)
        await self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_insider_sent_ticker ON insider_sentiment(ticker)"
        )

    async def _command_loop(self):
        log.info("scraper-finnhub-insider.command_loop_start")
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
                log.error("scraper-finnhub-insider.command_loop_error", error=str(e))
                await asyncio.sleep(5)

    async def _handle_command(self, msg_id: str, data: dict):
        job = data.get("job", "")
        try:
            if data.get("command") == "trigger" and job == "scrape_finnhub_insider":
                await self._scrape()
        except Exception as e:
            log.error("scraper-finnhub-insider.handle_error", job=job, error=str(e))
        finally:
            await self.redis.xack(CMD_STREAM, CONSUMER_GROUP, msg_id)

    def _get_tickers(self) -> list[str]:
        seen: set[str] = set()
        tickers: list[str] = []

        def add(t: str):
            t = t.upper().strip()
            if t and "." not in t and t not in seen:  # skip non-US tickers
                seen.add(t)
                tickers.append(t)

        for b in ("SPY", "QQQ", "AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "GOOGL", "META", "GOOG"):
            add(b)
        try:
            for a in load_active_assignments("equity") + load_active_assignments("options"):
                add(a.get("ticker", ""))
        except Exception as e:
            log.warning("scraper-finnhub-insider.assignments_error", error=str(e))
        try:
            raw = self.redis.get("broker:position_tickers")
            if raw:
                for t in json.loads(raw):
                    add(t)
        except Exception:
            pass

        return tickers[:MAX_TICKERS]

    async def _fetch_transactions(self, session: aiohttp.ClientSession, ticker: str) -> list[dict]:
        try:
            async with session.get(
                f"{FINNHUB_BASE}/stock/insider-transactions",
                params={"symbol": ticker, "token": FINNHUB_API_KEY},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    log.warning("finnhub.tx_fetch_failed", ticker=ticker, status=resp.status)
                    return []
                data = await resp.json()
                return data.get("data") or []
        except Exception as e:
            log.warning("finnhub.tx_fetch_error", ticker=ticker, error=str(e))
            return []

    async def _fetch_sentiment(self, session: aiohttp.ClientSession, ticker: str) -> list[dict]:
        # Fetch last N months of insider sentiment (MSPR)
        today = date.today()
        year = today.year
        month = today.month
        # Go back SENTIMENT_MONTHS
        from_month = month - SENTIMENT_MONTHS
        from_year  = year
        while from_month <= 0:
            from_month += 12
            from_year  -= 1
        from_str = f"{from_year}-{from_month:02d}-01"
        to_str   = today.isoformat()
        try:
            async with session.get(
                f"{FINNHUB_BASE}/stock/insider-sentiment",
                params={"symbol": ticker, "from": from_str, "to": to_str, "token": FINNHUB_API_KEY},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    log.warning("finnhub.senti_fetch_failed", ticker=ticker, status=resp.status)
                    return []
                data = await resp.json()
                return data.get("data") or []
        except Exception as e:
            log.warning("finnhub.senti_fetch_error", ticker=ticker, error=str(e))
            return []

    async def _scrape(self):
        if not FINNHUB_API_KEY:
            log.warning("scraper-finnhub-insider.no_api_key")
            return

        tickers = self._get_tickers()
        if not tickers:
            return

        log.info("scraper-finnhub-insider.scrape_start", tickers=len(tickers))
        total_tx = 0
        total_sent = 0
        net_by_ticker: dict[str, int] = {}

        async with aiohttp.ClientSession() as session:
            for ticker in tickers:
                # Transactions
                txns = await self._fetch_transactions(session, ticker)
                await asyncio.sleep(REQUEST_DELAY_S)

                # Sentiment
                senti = await self._fetch_sentiment(session, ticker)
                await asyncio.sleep(REQUEST_DELAY_S)

                if self._db:
                    tx_count = await self._persist_transactions(ticker, txns)
                    senti_count = await self._persist_sentiment(ticker, senti)
                    total_tx   += tx_count
                    total_sent += senti_count

                # Compute net shares bought (last 90 days) for market.ticks signal
                net = 0
                cutoff = date.today().replace(year=date.today().year - 1)
                for tx in txns:
                    code = tx.get("transactionCode", "")
                    try:
                        tx_date = datetime.strptime(
                            tx.get("transactionDate", ""), "%Y-%m-%d"
                        ).date()
                    except (ValueError, TypeError):
                        continue
                    if tx_date < cutoff:
                        continue
                    change = int(tx.get("change") or 0)
                    if code == "P":
                        net += change
                    elif code == "S":
                        net -= abs(change)
                net_by_ticker[ticker] = net

                # Cache latest transactions in Redis
                cache_payload = [
                    {
                        "name":             tx.get("name", ""),
                        "share":            tx.get("share"),
                        "change":           tx.get("change"),
                        "filing_date":      tx.get("filingDate", ""),
                        "transaction_date": tx.get("transactionDate", ""),
                        "transaction_code": tx.get("transactionCode", ""),
                        "transaction_label": TRANSACTION_LABELS.get(
                            tx.get("transactionCode", ""), "Other"
                        ),
                        "transaction_price": tx.get("transactionPrice"),
                    }
                    for tx in (txns or [])[:20]
                ]
                await self.redis.set(
                    f"insider:{ticker}", json.dumps({"transactions": cache_payload, "net_90d": net}),
                    ex=86400
                )

        # Publish net insider signal to market.ticks
        for ticker, net in net_by_ticker.items():
            if net == 0:
                continue
            label = "bullish" if net > 0 else "bearish"
            try:
                await self.redis.xadd(
                    TICKS_STREAM,
                    {
                        "source":          "finnhub_insider",
                        "ticker":          ticker,
                        "sentiment_label": label,
                        "net_insider_shares": str(net),
                    },
                    maxlen=50_000,
                )
            except Exception as e:
                log.warning("scraper-finnhub-insider.publish_error", ticker=ticker, error=str(e))

        log.info("scraper-finnhub-insider.done",
                 tickers=len(tickers), transactions=total_tx, sentiment_rows=total_sent)

    async def _persist_transactions(self, ticker: str, txns: list[dict]) -> int:
        count = 0
        for tx in txns:
            try:
                filing_date = None
                tx_date     = None
                if tx.get("filingDate"):
                    try:
                        filing_date = datetime.strptime(tx["filingDate"], "%Y-%m-%d").date()
                    except ValueError:
                        pass
                if tx.get("transactionDate"):
                    try:
                        tx_date = datetime.strptime(tx["transactionDate"], "%Y-%m-%d").date()
                    except ValueError:
                        pass

                await self._db.execute(
                    """INSERT INTO insider_transactions
                       (ticker, name, share, change, filing_date, transaction_date,
                        transaction_code, transaction_price)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                       ON CONFLICT (ticker, name, transaction_date, transaction_code, share)
                       DO NOTHING
                    """,
                    ticker,
                    (tx.get("name") or "")[:200],
                    int(tx.get("share") or 0),
                    int(tx.get("change") or 0),
                    filing_date,
                    tx_date,
                    (tx.get("transactionCode") or "")[:10],
                    float(tx.get("transactionPrice") or 0) or None,
                )
                count += 1
            except Exception as e:
                log.warning("finnhub.tx_persist_error", ticker=ticker, error=str(e))
        return count

    async def _persist_sentiment(self, ticker: str, senti: list[dict]) -> int:
        count = 0
        for s in senti:
            try:
                await self._db.execute(
                    """INSERT INTO insider_sentiment (ticker, year, month, change, mspr)
                       VALUES ($1,$2,$3,$4,$5)
                       ON CONFLICT (ticker, year, month) DO UPDATE SET
                           change = EXCLUDED.change,
                           mspr   = EXCLUDED.mspr,
                           scraped_at = NOW()
                    """,
                    ticker,
                    int(s.get("year") or 0),
                    int(s.get("month") or 0),
                    int(s.get("change") or 0),
                    float(s.get("mspr") or 0),
                )
                count += 1
            except Exception as e:
                log.warning("finnhub.senti_persist_error", ticker=ticker, error=str(e))
        return count


def main():
    asyncio.run(FinnhubInsiderScraper().run())


if __name__ == "__main__":
    main()
