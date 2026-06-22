"""
OpenTrader Paper Trader
Subscribes to predictor.signals and simulates fills without routing to a real broker.
Tracks open positions in Redis, writes fill records to TimescaleDB paper_trades table,
and publishes simulated fills to the paper.fills Redis stream.
"""
import asyncio
import json
import math
import os
import uuid
from datetime import datetime, timezone
from typing import Optional
import asyncpg
import structlog

from shared.base_agent import BaseAgent
from shared.redis_client import STREAMS, get_redis, ensure_consumer_group, REDIS_URL

log = structlog.get_logger("paper-trader")

SIG_STREAM     = STREAMS["signals"]
PAPER_STREAM   = "paper.fills"
CONSUMER_GROUP = "paper-trader"
CONSUMER_NAME  = os.getenv("HOSTNAME", "paper-trader-0")
DB_URL         = os.getenv("DB_URL", "")

PAPER_MODE_ENABLED     = os.getenv("PAPER_MODE_ENABLED", "true").lower() == "true"
PAPER_MAX_POSITION_USD = float(os.getenv("PAPER_MAX_POSITION_USD", "5000"))

PAPER_POSITIONS_KEY = "paper:positions"


class PaperTrader(BaseAgent):

    def __init__(self):
        super().__init__("paper-trader")
        self._db: Optional[asyncpg.Pool] = None

    async def run(self):
        if not PAPER_MODE_ENABLED:
            log.info("paper-trader.disabled", reason="PAPER_MODE_ENABLED=false")
            return

        await self.setup()
        import redis.asyncio as aioredis
        self.redis = await aioredis.from_url(
            REDIS_URL, encoding="utf-8", decode_responses=True,
            socket_connect_timeout=10, socket_timeout=None,
        )

        if DB_URL:
            try:
                self._db = await asyncpg.create_pool(
                    DB_URL,
                    min_size=1, max_size=3,
                    max_inactive_connection_lifetime=300,
                )
                await self._ensure_table()
                log.info("paper-trader.db_connected")
            except Exception as e:
                log.error("paper-trader.db_connect_failed", error=str(e))

        await ensure_consumer_group(self.redis, SIG_STREAM, CONSUMER_GROUP)
        log.info("paper-trader.starting", max_pos_usd=PAPER_MAX_POSITION_USD)

        await asyncio.gather(
            self.heartbeat_loop(),
            self._signal_loop(),
        )

    async def _ensure_table(self):
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS paper_trades (
                id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                ticker      TEXT NOT NULL,
                direction   TEXT NOT NULL,
                qty         REAL NOT NULL,
                fill_price  REAL NOT NULL,
                confidence  REAL NOT NULL DEFAULT 0,
                entry_ts    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                strategy    TEXT NOT NULL DEFAULT '',
                source      TEXT NOT NULL DEFAULT 'predictor',
                closed_at   TIMESTAMPTZ,
                close_price REAL,
                pnl_usd     REAL,
                notes       TEXT
            )
        """)
        await self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_paper_trades_ticker ON paper_trades(ticker)"
        )
        await self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_paper_trades_entry ON paper_trades(entry_ts DESC)"
        )

    async def _signal_loop(self):
        log.info("paper-trader.signal_loop_start")
        while self._running:
            try:
                if await self.is_halted():
                    await asyncio.sleep(5)
                    continue

                messages = await self.redis.xreadgroup(
                    groupname=CONSUMER_GROUP,
                    consumername=CONSUMER_NAME,
                    streams={SIG_STREAM: ">"},
                    count=5,
                    block=5000,
                )
                if not messages:
                    continue

                for _stream, entries in messages:
                    for msg_id, data in entries:
                        await self._handle_signal(msg_id, data)

            except asyncio.CancelledError:
                break
            except Exception as e:
                err = str(e)
                log.error("paper-trader.signal_loop_error", error=err)
                if "NOGROUP" in err:
                    await ensure_consumer_group(self.redis, SIG_STREAM, CONSUMER_GROUP)
                await asyncio.sleep(3)

    async def _handle_signal(self, msg_id: str, data: dict):
        ticker     = data.get("ticker", "")
        direction  = data.get("direction", "long")
        confidence = float(data.get("confidence", 0.0))
        source     = data.get("source", "predictor")
        meta_raw   = data.get("metadata", "{}")
        asset_cls  = data.get("asset_class", "equity")

        # Parse metadata for strategy name
        try:
            meta = json.loads(meta_raw) if isinstance(meta_raw, str) else meta_raw
            strategy_name = meta.get("strategy", "") if isinstance(meta, dict) else ""
        except Exception:
            strategy_name = ""

        try:
            if asset_cls not in ("equity", "etf"):
                return

            # Fetch current price
            price = await self._get_price(ticker)
            if price is None or price <= 0:
                log.warning("paper-trader.no_price", ticker=ticker)
                return

            # Check for an opposing open position → close it
            pos_raw = await self.redis.hget(PAPER_POSITIONS_KEY, ticker)
            if pos_raw:
                pos = json.loads(pos_raw)
                open_dir = pos.get("direction", "long")
                is_close = (open_dir == "long" and direction == "short") or \
                           (open_dir == "short" and direction == "long")
                if is_close:
                    await self._close_position(ticker, pos, price)
                    return

            # Size position
            qty = self._size_position(price, confidence, PAPER_MAX_POSITION_USD)
            if qty < 1:
                log.info("paper-trader.qty_too_small", ticker=ticker, price=price)
                return

            # Open new paper position
            trade_id = str(uuid.uuid4())
            now_utc  = datetime.now(timezone.utc)

            pos_data = {
                "trade_id":    trade_id,
                "qty":         qty,
                "entry_price": price,
                "direction":   direction,
                "confidence":  confidence,
                "strategy":    strategy_name,
                "source":      source,
                "ts":          now_utc.isoformat(),
            }
            await self.redis.hset(PAPER_POSITIONS_KEY, ticker, json.dumps(pos_data))

            # Persist to DB
            if self._db:
                try:
                    await self._db.execute(
                        """
                        INSERT INTO paper_trades
                            (id, ticker, direction, qty, fill_price, confidence,
                             entry_ts, strategy, source)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        """,
                        uuid.UUID(trade_id),
                        ticker,
                        direction,
                        float(qty),
                        price,
                        confidence,
                        now_utc,
                        strategy_name,
                        source,
                    )
                except Exception as e:
                    log.error("paper-trader.db_insert_failed", ticker=ticker, error=str(e))

            # Publish simulated fill to paper.fills
            await self.redis.xadd(
                PAPER_STREAM,
                {
                    "event_type":  "fill",
                    "account_id":  "paper",
                    "broker":      "paper",
                    "mode":        "paper",
                    "ticker":      ticker,
                    "asset_class": asset_cls,
                    "direction":   direction,
                    "qty":         str(qty),
                    "price":       str(price),
                    "pnl":         "",
                    "order_id":    trade_id,
                    "strategy":    strategy_name,
                },
                maxlen=10_000,
            )

            log.info("paper-trader.fill_simulated",
                     ticker=ticker, direction=direction, qty=qty,
                     price=price, confidence=confidence, trade_id=trade_id)

        except Exception as e:
            log.error("paper-trader.handle_signal_error", ticker=ticker, error=str(e))
        finally:
            await self.redis.xack(SIG_STREAM, CONSUMER_GROUP, msg_id)

    async def _close_position(self, ticker: str, pos: dict, close_price: float):
        """Close an open paper position and compute P&L."""
        entry_price  = float(pos.get("entry_price", close_price))
        qty          = float(pos.get("qty", 1))
        direction    = pos.get("direction", "long")
        trade_id_str = pos.get("trade_id", "")
        strategy     = pos.get("strategy", "")

        if direction == "long":
            pnl = (close_price - entry_price) * qty
        else:
            pnl = (entry_price - close_price) * qty

        now_utc = datetime.now(timezone.utc)

        # Remove from open positions hash
        await self.redis.hdel(PAPER_POSITIONS_KEY, ticker)

        # Update DB record with close info
        if self._db and trade_id_str:
            try:
                await self._db.execute(
                    """
                    UPDATE paper_trades
                       SET closed_at   = $1,
                           close_price = $2,
                           pnl_usd     = $3
                     WHERE id = $4
                    """,
                    now_utc,
                    close_price,
                    round(pnl, 4),
                    uuid.UUID(trade_id_str),
                )
            except Exception as e:
                log.error("paper-trader.db_close_failed", ticker=ticker, error=str(e))

        # Publish close fill event
        close_id    = str(uuid.uuid4())
        close_dir   = "short" if direction == "long" else "long"
        await self.redis.xadd(
            PAPER_STREAM,
            {
                "event_type":  "fill",
                "account_id":  "paper",
                "broker":      "paper",
                "mode":        "paper",
                "ticker":      ticker,
                "asset_class": "equity",
                "direction":   close_dir,
                "qty":         str(qty),
                "price":       str(close_price),
                "pnl":         str(round(pnl, 4)),
                "order_id":    close_id,
                "strategy":    strategy,
            },
            maxlen=10_000,
        )

        log.info("paper-trader.position_closed",
                 ticker=ticker, entry=entry_price, close=close_price,
                 qty=qty, pnl_usd=round(pnl, 4), direction=direction)

    async def _get_price(self, ticker: str) -> Optional[float]:
        """Try market:price:{ticker} first, then fall back to predictor:score:{ticker} metadata."""
        try:
            raw = await self.redis.get(f"market:price:{ticker}")
            if raw:
                return float(raw)
        except Exception:
            pass

        try:
            raw = await self.redis.get(f"predictor:score:{ticker}")
            if raw:
                obj = json.loads(raw)
                for field in ("price", "entry", "last"):
                    val = obj.get(field)
                    if val:
                        return float(val)
        except Exception:
            pass

        return None

    def _size_position(self, price: float, confidence: float, max_pos_usd: float) -> int:
        if price <= 0:
            return 0
        dollars = max_pos_usd * max(0.0, min(1.0, confidence))
        return max(math.floor(dollars / price), 1)

    async def shutdown(self):
        self._running = False
        if self._db:
            await self._db.close()
        if self.redis:
            await self.redis.aclose()


async def main():
    agent = PaperTrader()
    try:
        await agent.run()
    finally:
        await agent.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
