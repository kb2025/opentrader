"""
Broker Gateway Agent
The single broker communication hub for the entire platform.

Listens on: broker.commands  (Redis stream)
Publishes to: broker.fills   (Redis stream)
Reply channel: broker:reply:{request_id}  (Redis list, blpop pattern)

Command protocol — all fields are string-encoded for Redis compatibility.
See broker_gateway/router.py for the full field reference.
"""
import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone

import structlog

from shared.base_agent   import BaseAgent
from shared.redis_client import STREAMS, GROUPS, get_redis, ensure_consumer_group
from shared.telemetry    import emit as _tel_emit, TelemetryEvent
from .registry           import BrokerRegistry
from .router             import BrokerRouter

log = structlog.get_logger("broker-gateway")

import re as _re
_OCC_RE = _re.compile(r'^[A-Z]{1,6}\d{6}[CP]\d{8}$')

def _is_option_symbol(sym: str) -> bool:
    return bool(_OCC_RE.match(sym.upper()))

CONSUMER_NAME  = os.getenv("HOSTNAME", "broker-gateway-0")
REPLY_TTL      = int(os.getenv("BROKER_REPLY_TTL_SEC", "60"))


class BrokerGatewayAgent(BaseAgent):
    """
    Consumes broker.commands, routes to the correct broker connector,
    and writes results to both broker.fills and the reply key.
    """

    def __init__(self):
        super().__init__("broker-gateway")
        self.registry = BrokerRegistry()
        self.router   = BrokerRouter(self.registry)

    async def start(self):
        await self.setup()
        await self._ensure_consumer_group()

        log.info(
            "broker-gateway.started",
            accounts=list(self.registry.summary().keys()),
        )

        await asyncio.gather(
            self.heartbeat_loop(),
            self._command_loop(),
            self._order_poll_loop(),
        )

    async def _ensure_consumer_group(self):
        await ensure_consumer_group(
            self.redis, STREAMS["broker_commands"], GROUPS["broker_gateway"]
        )

    async def _command_loop(self):
        stream = STREAMS["broker_commands"]
        group  = GROUPS["broker_gateway"]
        log.info("broker-gateway.command_loop_start")

        while self._running:
            try:
                messages = await self.redis.xreadgroup(
                    groupname    = group,
                    consumername = CONSUMER_NAME,
                    streams      = {stream: ">"},
                    count        = 10,
                    block        = 5000,
                )
                if not messages:
                    continue

                for _stream, entries in messages:
                    for msg_id, data in entries:
                        await self._handle_command(msg_id, data)

            except asyncio.CancelledError:
                break
            except Exception as e:
                err = str(e)
                log.error("broker-gateway.command_loop_error", error=err)
                if "NOGROUP" in err:
                    await self._ensure_consumer_group()
                # Redis loading its dataset after restart — wait longer
                wait = 10 if "loading" in err.lower() else 3
                await asyncio.sleep(wait)
                try:
                    await self.redis.ping()
                except Exception:
                    try:
                        await self.redis.aclose()
                    except Exception:
                        pass
                    self.redis = await get_redis()

    async def _handle_command(self, msg_id: str, cmd: dict):
        t0         = time.monotonic()
        command    = cmd.get("command", "unknown")
        request_id = cmd.get("request_id", "")
        issued_by  = cmd.get("issued_by", "unknown")

        # ── Global trading mode gate ──────────────────────────────────────────
        # When system:trading_mode = "paper_only", force all place_* commands to
        # route exclusively to sandbox/paper accounts by injecting mode=sandbox.
        _PLACE_CMDS = {"place_order", "place_option_order", "place_spread_order"}
        if command in _PLACE_CMDS:
            try:
                trading_mode = await self.redis.get("system:trading_mode")
                if trading_mode and trading_mode.lower() == "paper_only":
                    cmd = dict(cmd)
                    cmd["mode"] = "sandbox"
                    log.info("broker-gateway.paper_only_gate",
                             command=command, original_account=cmd.get("account_label", ""))
            except Exception:
                pass

        log.info(
            "broker-gateway.command",
            command=command,
            request_id=request_id,
            issued_by=issued_by,
            account_label=cmd.get("account_label", ""),
        )

        try:
            results = await self.router.route(cmd)
        except Exception as e:
            log.error("broker-gateway.route_error", command=command, error=str(e))
            results = [{
                "request_id":    request_id,
                "command":       command,
                "account_label": "",
                "broker":        "",
                "mode":          "",
                "status":        "error",
                "data":          {},
                "error":         str(e),
            }]

        # Publish each result to the fills stream
        fills_stream = STREAMS["broker_fills"]
        for r in results:
            await self.redis.xadd(
                fills_stream,
                {
                    "request_id":    r["request_id"],
                    "command":       r["command"],
                    "account_label": r["account_label"],
                    "broker":        r["broker"],
                    "mode":          r["mode"],
                    "status":        r["status"],
                    "data":          json.dumps(r["data"]),
                    "error":         r["error"],
                    "issued_by":     issued_by,
                },
                maxlen=50_000,
            )

        # Also write first result to reply key so callers can blpop
        if request_id:
            reply_key = f"broker:reply:{request_id}"
            reply_payload = json.dumps(results[0] if len(results) == 1 else results)
            await self.redis.lpush(reply_key, reply_payload)
            await self.redis.expire(reply_key, REPLY_TTL)

        # Acknowledge message
        await self.redis.xack(STREAMS["broker_commands"], GROUPS["broker_gateway"], msg_id)

        rtt_ms = (time.monotonic() - t0) * 1000.0
        asyncio.create_task(_tel_emit(TelemetryEvent(
            agent      = "broker-gateway",
            event_name = "broker_latency",
            severity   = "info",
            payload    = {
                "command":       command,
                "broker":        results[0].get("broker", "") if results else "",
                "account_label": results[0].get("account_label", "") if results else "",
                "status":        results[0].get("status", "error") if results else "error",
            },
            duration_ms = rtt_ms,
        )))

        log.info(
            "broker-gateway.command_done",
            command=command,
            request_id=request_id,
            results=len(results),
            statuses=[r["status"] for r in results],
        )

    async def _order_poll_loop(self):
        """Every 60 s, fetch orders from all brokers and emit fill events.

        Two complementary detection methods:
        1. Transition detection — order was open last poll, now filled.
        2. Today-fill detection — order filled today and not yet emitted (catches
           manually-placed trades and fills that occurred before gateway start).
        3. Position-change detection — for brokers where the order-list API is
           unavailable (e.g. Webull on restricted subscriptions), compare position
           snapshots between polls and infer buys/sells from qty changes.
        """
        POLL_INTERVAL = int(os.getenv("ORDER_POLL_INTERVAL_SEC", "60"))
        # account_label → set of open order_ids (transition detection)
        known_open:   dict[str, set] = {}
        # account_label → set of order_ids already emitted (dedup for today-fill path)
        known_emitted: dict[str, set] = {}
        # account_label → {ticker: {"qty": float, "price": float}} — position snapshots
        # used as fallback when get_orders is unavailable
        known_pos_snap: dict[str, dict] = {}
        # Brokers that do not expose order history via get_orders
        _POS_TRACK_BROKERS = frozenset({"webull"})

        # Broker-specific open statuses (lowercase)
        _OPEN_STATUSES = frozenset({
            "open", "partially_filled", "partial_filled",
            "pending_new", "new", "accepted", "held",
            "working",          # Webull
            "pending_cancel",   # Tradier
        })
        _FILL_STATUSES = frozenset({
            "filled", "complete", "completed",
            "full_fill",        # Webull
        })

        await asyncio.sleep(15)  # let the command loop start first
        log.info("broker-gateway.order_poll_start", interval=POLL_INTERVAL)

        while self._running:
            try:
                today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

                for rec in self.registry.all_records():
                    try:
                        orders = await rec.connector.get_orders(status="all")
                    except Exception as e:
                        log.warning("broker-gateway.order_poll_fetch_error",
                                    account=rec.label, error=str(e))
                        continue

                    prev_open     = known_open.get(rec.label, set())
                    prev_emitted  = known_emitted.get(rec.label, set())
                    curr_open:    set[str] = set()
                    to_emit = []

                    for o in orders:
                        if not isinstance(o, dict):
                            continue
                        oid = str(o.get("id") or o.get("order_id") or o.get("clientOrderId") or "")
                        if not oid:
                            continue
                        status = str(o.get("status", "")).lower()

                        if status in _OPEN_STATUSES:
                            curr_open.add(oid)

                        elif status in _FILL_STATUSES:
                            # Method 1: was open last cycle → just filled
                            if oid in prev_open:
                                to_emit.append(o)
                            # Method 2: filled today but not yet emitted (covers manual
                            # trades placed directly on the broker, or post-restart catch-up)
                            elif oid not in prev_emitted:
                                date_fields = [
                                    str(o.get("transaction_date", "")),
                                    str(o.get("filledTime", "")),
                                    str(o.get("filled_at", "")),
                                    str(o.get("create_date", "")),
                                    str(o.get("createTime", "")),
                                ]
                                if any(today_str in d for d in date_fields if d and d != "None"):
                                    to_emit.append(o)

                    known_open[rec.label]    = curr_open
                    known_emitted[rec.label] = prev_emitted | {
                        str(o.get("id") or o.get("order_id") or o.get("clientOrderId") or "")
                        for o in to_emit
                    }

                    for o in to_emit:
                        try:
                            asset_cls = str(o.get("asset_class") or o.get("assetClass") or "equity").lower()
                            side      = str(o.get("side") or "").lower()
                            direction = "long" if side in ("buy", "long") else ("short" if side in ("sell", "short") else side)
                            await self.redis.xadd(
                                STREAMS.get("orders", "orders.events"),
                                {
                                    "event_type":    "fill",
                                    "account_id":    rec.label,
                                    "account_label": rec.label,
                                    "broker":        rec.broker,
                                    "mode":          rec.mode,
                                    "ticker":        str(o.get("symbol") or o.get("ticker") or ""),
                                    "asset_class":   asset_cls,
                                    "direction":     direction,
                                    "order_id":      str(o.get("id") or ""),
                                    "qty":           str(o.get("quantity") or o.get("qty") or ""),
                                    "price":         str(o.get("avg_fill_price") or o.get("filledPrice") or o.get("filled_price") or ""),
                                    "source":        "order_poll",
                                },
                                maxlen=10_000,
                            )
                            log.info("broker-gateway.order_fill_detected",
                                     account=rec.label, order_id=o.get("id"), symbol=o.get("symbol"))
                        except Exception as e:
                            log.warning("broker-gateway.order_poll_emit_error", error=str(e))

                    # ── Method 3: position-change detection for brokers without order history ──
                    if rec.broker in _POS_TRACK_BROKERS:
                        try:
                            raw_pos = await rec.connector.get_positions()
                            # Only equity — skip option symbols (OCC format)
                            curr_snap = {
                                p["symbol"]: {
                                    "qty":   float(p.get("qty") or 0),
                                    "price": float(p.get("avg_entry_price") or 0),
                                }
                                for p in raw_pos
                                if p.get("symbol") and not _is_option_symbol(p["symbol"])
                                   and float(p.get("qty") or 0) > 0
                            }

                            now_ts = datetime.now(timezone.utc).isoformat()
                            _ord_stream = STREAMS.get("orders", "orders.events")

                            async def _emit_position_fill(sym, qty, price, direction):
                                await self.redis.xadd(
                                    _ord_stream,
                                    {
                                        "event_type":    "fill",
                                        "account_id":    rec.label,
                                        "account_label": rec.label,
                                        "broker":        rec.broker,
                                        "mode":          rec.mode,
                                        "ticker":        sym,
                                        "asset_class":   "equity",
                                        "direction":     direction,
                                        "order_id":      f"pos-{sym}-{rec.label}-{int(datetime.now(timezone.utc).timestamp())}",
                                        "qty":           f"{qty:.6g}",
                                        "price":         str(price) if price else "",
                                        "ts_utc":        now_ts,
                                        "source":        "position_poll",
                                    },
                                    maxlen=10_000,
                                )

                            # On first snapshot for this account, emit fills for all
                            # current holdings so they show up in Completed Trades.
                            # A Redis flag prevents re-importing on container restart.
                            init_key = f"broker:pos_init:{rec.label}"
                            if rec.label not in known_pos_snap:
                                already_imported = bool(await self.redis.get(init_key))
                                if not already_imported and curr_snap:
                                    for sym, data in curr_snap.items():
                                        await _emit_position_fill(sym, data["qty"], data["price"], "long")
                                        log.info("broker-gateway.position_imported",
                                                 account=rec.label, symbol=sym,
                                                 qty=round(data["qty"], 6), price=data["price"])
                                    await self.redis.set(init_key, "1")
                                known_pos_snap[rec.label] = curr_snap
                            else:
                                prev_snap = known_pos_snap[rec.label]
                                # Buys: new ticker or qty increase
                                for sym, cur in curr_snap.items():
                                    prev_qty = prev_snap.get(sym, {}).get("qty", 0.0)
                                    delta = cur["qty"] - prev_qty
                                    if delta > 0.0001:
                                        await _emit_position_fill(sym, delta, cur["price"], "long")
                                        log.info("broker-gateway.position_buy_detected",
                                                 account=rec.label, symbol=sym,
                                                 qty_delta=round(delta, 6), price=cur["price"])
                                # Sells: qty decrease or position closed
                                for sym, prev in prev_snap.items():
                                    curr_qty = curr_snap.get(sym, {}).get("qty", 0.0)
                                    delta = prev["qty"] - curr_qty
                                    if delta > 0.0001:
                                        await _emit_position_fill(sym, delta, 0, "short")
                                        log.info("broker-gateway.position_sell_detected",
                                                 account=rec.label, symbol=sym,
                                                 qty_delta=round(delta, 6))
                                known_pos_snap[rec.label] = curr_snap
                        except Exception as e:
                            log.warning("broker-gateway.position_poll_error",
                                        account=rec.label, error=str(e))

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error("broker-gateway.order_poll_error", error=str(e))

            await asyncio.sleep(POLL_INTERVAL)

    async def shutdown(self):
        self._running = False
        if self.redis:
            await self.redis.aclose()


async def main():
    logging.basicConfig(level=logging.INFO)
    agent = BrokerGatewayAgent()
    try:
        await agent.start()
    finally:
        await agent.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
