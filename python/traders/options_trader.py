"""
OpenTrader Options Trader
Consumes predictor.signals for options-class assets, looks up active
strategy assignments, confirms direction with TradingView indicators,
sizes contracts, and routes orders to assigned accounts through the
Broker Gateway via broker.commands stream.

Trading strategy parameters (confidence threshold, position sizing,
etc.) come exclusively from the Strategy Assignment workflow — there
are no embedded strategy values in this agent.

Options execution parameters (expiry target, OTM offset) are
operational defaults overridable via env vars; they will move into
the strategy schema once the library supports options-specific fields.
"""
import asyncio
import json
import os
import uuid
from typing import Optional

import structlog

from shared.base_agent import BaseAgent
from shared.redis_client import STREAMS, GROUPS, get_redis, ensure_consumer_group
from shared.envelope import OrderEventPayload
from shared.mcp_client import get_tv_indicators, tv_confirms_direction, get_avg_volume
from shared.assignments import load_active_assignments
from shared.exclusions import is_excluded
from shared.risk_controls import get_risk_controls, check_slippage, check_liquidity
from shared.market_tone import get_market_tone, get_tone_thresholds
from shared.telemetry import emit as _tel_emit, TelemetryEvent
from scheduler.calendar import is_market_open, is_trading_day

log = structlog.get_logger("trader-options")

SIG_STREAM     = STREAMS["signals"]
ORD_STREAM     = STREAMS["orders"]
CONSUMER_GROUP = GROUPS["options"]
CONSUMER_NAME  = os.getenv("HOSTNAME", "trader-options-0")

# Operational defaults — not strategy parameters
TRADE_MODE_DEFAULT   = os.getenv("TRADE_MODE", "sandbox")
MAX_CONTRACTS        = int(os.getenv("MAX_CONTRACTS", "1"))
SANDBOX_IGNORE_HOURS = os.getenv("SANDBOX_IGNORE_HOURS", "true").lower() == "true"
GATEWAY_TIMEOUT      = int(os.getenv("BROKER_GATEWAY_TIMEOUT_SEC", "15"))

# Options execution defaults — will move to strategy schema when supported
DEFAULT_EXPIRY_DAYS = int(os.getenv("OPTIONS_EXPIRY_DAYS", "7"))   # DTE target
OTM_OFFSET_PCT      = float(os.getenv("OPTIONS_OTM_PCT", "2.0"))   # % OTM for strike

# ── Tick-size grid (CBOE/OCC standard) ────────────────────────────────────────
# Options priced < $3.00 trade in $0.01 increments; ≥ $3.00 trade in $0.05.
_TICK_LOW  = 0.01
_TICK_HIGH = 0.05
_TICK_THRESHOLD = 3.0


def _snap_to_tick(price: float) -> float:
    """Round an options limit price to the nearest valid tick increment."""
    tick = _TICK_HIGH if price >= _TICK_THRESHOLD else _TICK_LOW
    return round(round(price / tick) * tick, 2)


def _compute_limit_price(
    bid: Optional[float],
    ask: Optional[float],
    confidence: float,
    side: str,
) -> tuple[Optional[float], str]:
    """
    Four-tier limit price selection keyed to signal confidence.

    Tier mapping (from tasty-agent order pricing model):
      natural (≥ 0.85): take liquidity immediately — ask for buys, bid for sells
      mid     (≥ 0.70): split the spread — (bid + ask) / 2
      passive (≥ 0.55): provide liquidity — bid for buys, ask for sells
      skip    (< 0.55): spread too uncertain, abort

    Returns (tick-snapped price | None, tier label).
    None means the trade should be skipped (no valid price or below threshold).
    """
    if bid is None or ask is None or bid <= 0 or ask <= 0:
        return None, "no_quote"
    if confidence >= 0.85:
        raw, tier = (ask if side == "buy" else bid), "natural"
    elif confidence >= 0.70:
        raw, tier = (bid + ask) / 2.0, "mid"
    elif confidence >= 0.55:
        raw, tier = (bid if side == "buy" else ask), "passive"
    else:
        return None, "skip"
    return _snap_to_tick(raw), tier


class OptionsTrader(BaseAgent):

    def __init__(self):
        super().__init__("trader-options")
        self._positions_today: set[str] = set()

    async def _trade_mode(self) -> str:
        try:
            stored = await self.redis.get("config:trade_mode")
            return stored if stored else TRADE_MODE_DEFAULT
        except Exception:
            return TRADE_MODE_DEFAULT

    async def run(self):
        await self.setup()
        self.redis = await get_redis()
        await self._ensure_consumer_group()
        log.info("trader-options.starting", mode=TRADE_MODE_DEFAULT)

        await asyncio.gather(
            self.heartbeat_loop(),
            self._signal_loop(),
            self._midnight_reset(),
        )

    async def _ensure_consumer_group(self):
        await ensure_consumer_group(self.redis, SIG_STREAM, CONSUMER_GROUP)

    async def _signal_loop(self):
        log.info("trader-options.signal_loop_start")
        while self._running:
            try:
                if await self.is_halted():
                    await asyncio.sleep(5)
                    continue

                messages = await self.redis.xreadgroup(
                    groupname    = CONSUMER_GROUP,
                    consumername = CONSUMER_NAME,
                    streams      = {SIG_STREAM: ">"},
                    count        = 5,
                    block        = 5000,
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
                log.error("trader-options.signal_loop_error", error=err)
                if "NOGROUP" in err:
                    await self._ensure_consumer_group()
                await asyncio.sleep(3)
                try:
                    await self.redis.ping()
                except Exception:
                    try:
                        await self.redis.aclose()
                    except Exception:
                        pass
                    from shared.redis_client import get_redis
                    self.redis = await get_redis()

    async def _handle_signal(self, msg_id: str, data: dict):
        ticker     = data.get("ticker", "")
        direction  = data.get("direction", "long")
        confidence = float(data.get("confidence", 0.0))
        asset_cls  = data.get("asset_class", "equity")

        try:
            if asset_cls != "options":
                return

            if ticker in self._positions_today:
                log.debug("trader-options.already_traded", ticker=ticker)
                return

            # Resolve active assignments whose strategy covers options.
            # Strategy parameters (min confidence, etc.) come from here.
            assignments = load_active_assignments(asset_cls)
            if not assignments:
                log.debug("trader-options.no_assignments",
                          ticker=ticker, asset_class=asset_cls)
                return

            trade_mode = await self._trade_mode()
            in_sandbox = trade_mode == "sandbox"
            if not is_trading_day():
                log.debug("trader-options.not_trading_day", ticker=ticker)
                return
            if not (in_sandbox and SANDBOX_IGNORE_HOURS):
                if not is_market_open():
                    log.debug("trader-options.market_closed", ticker=ticker)
                    return

            # Sector / ticker / industry exclusion check
            if await is_excluded(self.redis, ticker):
                return

            # TradingView confirmation — veto if indicators contradict signal
            tv = await get_tv_indicators(ticker)
            if not tv_confirms_direction(tv, direction):
                log.info("trader-options.tv_veto",
                         ticker=ticker, direction=direction,
                         tv_rec=tv.get("recommendation") if tv else "unavailable")
                return
            if tv:
                log.info("trader-options.tv_confirmed",
                         ticker=ticker, direction=direction,
                         tv_rec=tv["recommendation"],
                         buy=tv["buy"], sell=tv["sell"])

            # ── Market tone — adjust min_confidence and pricing tier ──────────
            tone            = await get_market_tone(self.redis)
            tone_thresholds = get_tone_thresholds(tone)
            tone_min_conf   = tone_thresholds.get("min_confidence")
            tone_tier       = tone_thresholds.get("price_tier_override")  # may be None
            log.info("trader-options.market_tone",
                     ticker=ticker, tone=tone,
                     tone_min_conf=tone_min_conf, tone_tier=tone_tier)

            # Place an order for each assigned account
            for assignment in assignments:
                # Use the stricter of strategy min_confidence and tone min_confidence
                effective_min_conf = max(
                    assignment["min_confidence"],
                    tone_min_conf if tone_min_conf else 0.0,
                )
                if confidence < effective_min_conf:
                    log.debug("trader-options.below_threshold",
                              ticker=ticker, conf=confidence,
                              required=effective_min_conf,
                              strategy=assignment["strategy_name"],
                              tone=tone)
                    continue

                # Medium-confidence signals → spread instead of naked option
                # (confidence 0.55–0.70: narrower spread; 0.70–0.85: standard $5 spread)
                use_spread = (0.55 <= confidence <= 0.85) and data.get("signal_type") == "spread"
                if use_spread:
                    await self._place_spread_order(
                        ticker, direction, confidence, data, assignment,
                    )
                else:
                    await self._place_option_order(
                        ticker, direction, confidence, data, assignment,
                        tone_tier=tone_tier,
                    )

        except Exception as e:
            log.error("trader-options.handle_signal_error",
                      ticker=ticker, error=str(e))
        finally:
            await self.redis.xack(SIG_STREAM, CONSUMER_GROUP, msg_id)

    async def _place_option_order(
        self,
        ticker:     str,
        direction:  str,
        confidence: float,
        data:       dict,
        assignment: dict,
        tone_tier:  Optional[str] = None,
    ):
        account_label = assignment["account_label"]
        strategy_name = assignment["strategy_name"]
        trade_mode    = await self._trade_mode()

        quote = await self._get_quote(ticker, trade_mode)
        bid   = quote.get("bid")
        ask   = quote.get("ask")
        price = quote.get("last") or ask or bid
        if not price:
            log.warning("trader-options.no_price", ticker=ticker)
            return
        price = float(price)

        # Risk controls — slippage + liquidity on the underlying
        controls = await get_risk_controls(self.redis)
        ok, spread = check_slippage(bid, ask, controls["max_slippage_pct"])
        if not ok:
            log.info("trader-options.slippage_blocked",
                     ticker=ticker, spread_pct=spread,
                     max_pct=controls["max_slippage_pct"])
            return
        if controls["min_volume_k"] > 0:
            avg_vol = await get_avg_volume(ticker)
            vol_ok, vol_k = check_liquidity(avg_vol, controls["min_volume_k"])
            if not vol_ok:
                log.info("trader-options.liquidity_blocked",
                         ticker=ticker, vol_k=vol_k,
                         min_k=controls["min_volume_k"])
                return

        opt_type      = "call" if direction == "long" else "put"
        offset        = price * (OTM_OFFSET_PCT / 100)
        target_strike = round(price + offset if opt_type == "call" else price - offset, 2)

        # ── Four-tier limit price + tick validation ────────────────────────────
        # Confidence selects the default tier; market tone can override it.
        # Tone override maps: "natural"|"mid"|"passive" — a more conservative
        # tone (bearish) forces passive even on high-confidence signals.
        order_side  = "buy"   # options trader always opens (buy to open)
        limit_price, price_tier = _compute_limit_price(bid, ask, confidence, order_side)
        if tone_tier and limit_price is not None:
            tier_order = ["passive", "mid", "natural"]
            sig_idx    = tier_order.index(price_tier) if price_tier in tier_order else 2
            tone_idx   = tier_order.index(tone_tier)  if tone_tier  in tier_order else 2
            if tone_idx < sig_idx:  # tone is more conservative — apply it
                limit_price, price_tier = _compute_limit_price(
                    bid, ask,
                    {"natural": 0.85, "mid": 0.70, "passive": 0.55}[tone_tier],
                    order_side,
                )
                price_tier = f"{tone_tier}(tone)"
        if limit_price is None:
            if price_tier == "skip":
                log.info("trader-options.confidence_below_passive_threshold",
                         ticker=ticker, confidence=confidence)
            else:
                log.info("trader-options.no_bid_ask_for_limit", ticker=ticker,
                         bid=bid, ask=ask)
                # Fall back to market order when no spread is available
                limit_price = _snap_to_tick(price) if price else None
                price_tier  = "market_fallback"
        log.info("trader-options.limit_price_selected",
                 ticker=ticker, bid=bid, ask=ask,
                 limit_price=limit_price, tier=price_tier, confidence=confidence)

        contract_symbol = await self._resolve_contract(
            ticker, opt_type, target_strike, trade_mode
        )
        if not contract_symbol:
            log.warning("trader-options.no_contract",
                        ticker=ticker, opt_type=opt_type, strike=target_strike)
            return

        # ── Analyze mode — log without touching the broker ────────────────────
        if trade_mode == "analyze":
            asyncio.create_task(_tel_emit(TelemetryEvent(
                agent="trader-options", event_name="analyze_order", severity="info",
                payload={
                    "ticker":          ticker,
                    "contract":        contract_symbol,
                    "option_type":     opt_type,
                    "strike":          target_strike,
                    "contracts":       MAX_CONTRACTS,
                    "limit_price":     limit_price,
                    "price_tier":      price_tier,
                    "account_label":   account_label,
                    "strategy":        strategy_name,
                    "position_usd":    round((limit_price or 0) * MAX_CONTRACTS * 100, 2),
                },
            )))
            await self.redis.xadd(ORD_STREAM, {
                "event_type":  "fill",
                "account_id":  account_label,
                "broker":      "analyze",
                "mode":        "analyze",
                "ticker":      contract_symbol,
                "asset_class": "options",
                "direction":   direction,
                "qty":         str(MAX_CONTRACTS),
                "price":       str(limit_price or 0),
                "order_id":    f"ANALYZE-{uuid.uuid4().hex[:8]}",
                "strategy":    strategy_name,
                "reject_reason": "",
            }, maxlen=10_000)
            log.info("trader-options.analyze_mode",
                     ticker=ticker, contract=contract_symbol,
                     opt_type=opt_type, contracts=MAX_CONTRACTS,
                     limit_price=limit_price, account=account_label)
            return

        request_id = str(uuid.uuid4())

        log.info("trader-options.placing_order",
                 ticker=ticker, contract=contract_symbol,
                 opt_type=opt_type, contracts=MAX_CONTRACTS,
                 account=account_label, strategy=strategy_name,
                 limit_price=limit_price, price_tier=price_tier,
                 mode=trade_mode)

        order_type = "limit" if limit_price and price_tier != "market_fallback" else "market"

        # ── Send to broker gateway — route to the assigned account ────────────
        await self.redis.xadd(
            STREAMS["broker_commands"],
            {
                "command":       "place_option_order",
                "request_id":    request_id,
                "symbol":        contract_symbol,
                "underlying":    ticker,
                "option_type":   opt_type,
                "contracts":     str(MAX_CONTRACTS),
                "order_type":    order_type,
                "limit_price":   str(limit_price) if limit_price else "",
                "price_tier":    price_tier,
                "account_label": account_label,
                "strategy_tag":  strategy_name,
                "mode":          trade_mode,
                "issued_by":     "trader-options",
            },
            maxlen=10_000,
        )

        # ── Wait for gateway reply ─────────────────────────────────────────────
        reply_raw = await self.redis.blpop(
            f"broker:reply:{request_id}", timeout=GATEWAY_TIMEOUT
        )
        if reply_raw is None:
            log.warning("trader-options.gateway_timeout",
                        ticker=ticker, request_id=request_id)
            return

        _, reply_json = reply_raw
        try:
            results = json.loads(reply_json)
        except Exception:
            log.error("trader-options.reply_parse_error", raw=reply_json[:200])
            return

        if not isinstance(results, list):
            results = [results]

        if not results:
            log.warning("trader-options.no_accounts_matched",
                        ticker=ticker, account=account_label)
            return

        # ── Publish order events ───────────────────────────────────────────────
        for r in results:
            acct   = r.get("account_label", account_label)
            broker = r.get("broker", assignment["broker"])
            mode   = r.get("mode", trade_mode)

            if r.get("status") == "error":
                reject_reason = r.get("error") or "broker rejected"
                log.warning("trader-options.order_rejected",
                            ticker=ticker, error=reject_reason)
                await self.redis.xadd(
                    ORD_STREAM,
                    {
                        "event_type":    "reject",
                        "account_id":    acct,
                        "broker":        broker,
                        "mode":          mode,
                        "ticker":        contract_symbol,
                        "asset_class":   "options",
                        "direction":     direction,
                        "qty":           str(MAX_CONTRACTS),
                        "price":         str(price or ""),
                        "order_id":      "",
                        "strategy":      strategy_name,
                        "reject_reason": reject_reason[:80],
                    },
                    maxlen=10_000,
                )
                continue

            rdata      = r.get("data", {})
            order_id   = str(rdata.get("id", rdata.get("orderId", request_id)))
            status     = rdata.get("status", "ok")
            event_type = (
                "fill" if status in ("ok", "filled", "open", "accepted", "pending_new", "new")
                else "reject"
            )

            if event_type == "fill":
                self._positions_today.add(ticker)

            fill_price = float(rdata.get("avg_fill_price") or rdata.get("filled_price") or limit_price or price)
            payload = OrderEventPayload(
                event_type  = event_type,
                account_id  = acct,
                broker      = broker,
                mode        = mode,
                ticker      = contract_symbol,
                asset_class = "options",
                direction   = direction,
                qty         = float(MAX_CONTRACTS),
                price       = fill_price,
                order_id    = order_id,
                strategy    = strategy_name,
            )
            await self.redis.xadd(
                ORD_STREAM,
                {
                    "event_type":  payload.event_type,
                    "account_id":  payload.account_id,
                    "broker":      payload.broker,
                    "mode":        payload.mode,
                    "ticker":      payload.ticker,
                    "asset_class": payload.asset_class,
                    "direction":   payload.direction,
                    "qty":         str(payload.qty),
                    "price":       str(payload.price or ""),
                    "order_id":    payload.order_id,
                    "strategy":    payload.strategy,
                },
                maxlen=10_000,
            )
            log.info("trader-options.order_submitted",
                     ticker=ticker, contract=contract_symbol,
                     account=acct, strategy=strategy_name,
                     order_id=order_id, event_type=event_type)

    async def _place_spread_order(
        self,
        ticker:     str,
        direction:  str,
        confidence: float,
        data:       dict,
        assignment: dict,
    ):
        """
        Place a bull call spread (long) or bear put spread (short) via the gateway.
        Spread width scales with confidence:
          ≥0.85 → $10 width; 0.70–0.85 → $5; 0.55–0.70 → $2.50
        Resolves both legs from the chain, then sends place_spread_order.
        """
        account_label = assignment["account_label"]
        trade_mode    = await self._trade_mode()

        if confidence >= 0.85:
            width = 10.0
        elif confidence >= 0.70:
            width = 5.0
        else:
            width = 2.5

        opt_type   = "call" if direction == "long" else "put"
        strategy   = "bull_call_spread" if direction == "long" else "bear_put_spread"

        # Resolve long leg (ATM or slightly ITM)
        long_leg = await self._resolve_spread_leg(ticker, opt_type, 0.0, trade_mode)
        if not long_leg:
            log.warning("trader-options.spread_no_long_leg", ticker=ticker)
            return

        long_strike = float(long_leg.get("strike") or 0)
        short_strike = (
            round(long_strike + width, 2) if direction == "long"
            else round(long_strike - width, 2)
        )

        short_leg = await self._resolve_spread_leg(ticker, opt_type, short_strike, trade_mode)
        if not short_leg:
            log.warning("trader-options.spread_no_short_leg", ticker=ticker, strike=short_strike)
            return

        long_mid  = float(long_leg.get("mid") or long_leg.get("ask") or 0)
        short_mid = float(short_leg.get("mid") or short_leg.get("bid") or 0)

        long_price  = _snap_to_tick(long_mid)
        short_price = _snap_to_tick(short_mid)
        net_debit   = round(long_price - short_price, 2)
        if net_debit <= 0:
            log.warning("trader-options.spread_zero_debit", ticker=ticker, net=net_debit)
            return

        max_risk_usd  = float(assignment.get("max_pos") or 500) * confidence
        contracts     = max(1, min(int(max_risk_usd / (net_debit * 100)), MAX_CONTRACTS))

        legs = [
            {"symbol": long_leg.get("symbol",""),  "action": "buy_to_open",  "qty": contracts,
             "limit_price": long_price,  "option_type": opt_type,
             "strike": long_strike,      "expiry": long_leg.get("expiry","")},
            {"symbol": short_leg.get("symbol",""), "action": "sell_to_open", "qty": contracts,
             "limit_price": short_price, "option_type": opt_type,
             "strike": short_strike,     "expiry": short_leg.get("expiry","")},
        ]

        request_id = str(uuid.uuid4())
        log.info("trader-options.spread_placing",
                 ticker=ticker, strategy=strategy, width=width,
                 long_strike=long_strike, short_strike=short_strike,
                 net_debit=net_debit, contracts=contracts, account=account_label)

        import json as _json
        await self.redis.xadd(
            STREAMS["broker_commands"],
            {
                "command":       "place_spread_order",
                "request_id":    request_id,
                "account_label": account_label,
                "underlying":    ticker,
                "strategy_type": strategy,
                "legs":          _json.dumps(legs),
                "net_debit":     str(net_debit),
                "duration":      "day",
                "mode":          trade_mode,
                "issued_by":     "trader-options",
            },
            maxlen=10_000,
        )

        reply_raw = await self.redis.blpop(
            f"broker:reply:{request_id}", timeout=GATEWAY_TIMEOUT
        )
        if reply_raw is None:
            log.warning("trader-options.spread_gateway_timeout", ticker=ticker)
            return

        _, reply_json = reply_raw
        try:
            r = json.loads(reply_json)
            if isinstance(r, list):
                r = r[0]
        except Exception:
            return

        if r.get("status") == "error":
            log.warning("trader-options.spread_rejected", ticker=ticker, error=r.get("error"))
            return

        self._positions_today.add(ticker)
        log.info("trader-options.spread_submitted",
                 ticker=ticker, strategy=strategy,
                 spread_group_id=r.get("data", {}).get("spread_group_id", ""),
                 account=account_label)

    async def _resolve_spread_leg(
        self,
        ticker:        str,
        opt_type:      str,
        target_strike: float,
        trade_mode:    str,
    ) -> Optional[dict]:
        """Resolve a single spread leg — returns {symbol, strike, expiry, mid, bid, ask} or None."""
        try:
            request_id = str(uuid.uuid4())
            await self.redis.xadd(
                STREAMS["broker_commands"],
                {
                    "command":       "get_option_contract",
                    "request_id":    request_id,
                    "symbol":        ticker,
                    "option_type":   opt_type,
                    "target_strike": str(target_strike) if target_strike else "",
                    "expiry_days":   str(DEFAULT_EXPIRY_DAYS),
                    "mode":          trade_mode if trade_mode != "all" else "",
                    "issued_by":     "trader-options",
                },
                maxlen=10_000,
            )
            reply_raw = await self.redis.blpop(
                f"broker:reply:{request_id}", timeout=10
            )
            if reply_raw is None:
                return None
            _, reply_json = reply_raw
            r = json.loads(reply_json)
            if isinstance(r, list):
                r = r[0]
            d = r.get("data", {})
            if not d.get("symbol"):
                return None
            return {
                "symbol": d.get("symbol"),
                "strike": d.get("strike"),
                "expiry": d.get("expiry"),
                "mid":    d.get("mid"),
                "bid":    d.get("bid"),
                "ask":    d.get("ask"),
                "delta":  d.get("delta"),
            }
        except Exception as e:
            log.warning("trader-options.spread_leg_resolve_failed",
                        ticker=ticker, error=str(e))
            return None

    async def _get_quote(self, ticker: str, trade_mode: str) -> dict:
        """Fetch full quote (last, bid, ask) via broker gateway. Returns {} on failure."""
        try:
            request_id = str(uuid.uuid4())
            await self.redis.xadd(
                STREAMS["broker_commands"],
                {
                    "command":    "get_quote",
                    "request_id": request_id,
                    "symbol":     ticker,
                    "mode":       trade_mode if trade_mode != "all" else "",
                    "issued_by":  "trader-options",
                },
                maxlen=10_000,
            )
            reply_raw = await self.redis.blpop(
                f"broker:reply:{request_id}", timeout=10
            )
            if reply_raw is None:
                return {}
            _, reply_json = reply_raw
            r = json.loads(reply_json)
            if isinstance(r, list):
                r = r[0]
            data = r.get("data", {})
            def _f(v):
                try:
                    return float(v) if v else None
                except Exception:
                    return None
            return {
                "last": _f(data.get("last")),
                "bid":  _f(data.get("bid")),
                "ask":  _f(data.get("ask")),
            }
        except Exception as e:
            log.warning("trader-options.quote_failed", ticker=ticker, error=str(e))
            return {}

    async def _get_price(self, ticker: str, trade_mode: str) -> Optional[float]:
        """Fetch latest price via broker gateway. Returns None on failure."""
        q = await self._get_quote(ticker, trade_mode)
        p = q.get("last") or q.get("ask") or q.get("bid")
        return float(p) if p else None

    async def _resolve_contract(
        self,
        ticker:        str,
        opt_type:      str,
        target_strike: float,
        trade_mode:    str,
    ) -> Optional[str]:
        """Ask broker gateway for nearest options contract."""
        try:
            request_id = str(uuid.uuid4())
            await self.redis.xadd(
                STREAMS["broker_commands"],
                {
                    "command":       "get_option_contract",
                    "request_id":    request_id,
                    "symbol":        ticker,
                    "option_type":   opt_type,
                    "target_strike": str(target_strike),
                    "expiry_days":   str(DEFAULT_EXPIRY_DAYS),
                    "mode":          trade_mode if trade_mode != "all" else "",
                    "issued_by":     "trader-options",
                },
                maxlen=10_000,
            )
            reply_raw = await self.redis.blpop(
                f"broker:reply:{request_id}", timeout=10
            )
            if reply_raw is None:
                return None
            _, reply_json = reply_raw
            r = json.loads(reply_json)
            if isinstance(r, list):
                r = r[0]
            return r.get("data", {}).get("symbol")
        except Exception as e:
            log.warning("trader-options.contract_resolve_failed",
                        ticker=ticker, error=str(e))
            return None

    async def _midnight_reset(self):
        from scheduler.calendar import now_et
        while self._running:
            now  = now_et()
            secs = (24 * 3600) - (now.hour * 3600 + now.minute * 60 + now.second)
            await asyncio.sleep(secs + 1)
            self._positions_today.clear()
            log.info("trader-options.daily_reset")

    async def shutdown(self):
        self._running = False
        if self.redis:
            await self.redis.aclose()


async def main():
    agent = OptionsTrader()
    try:
        await agent.run()
    finally:
        await agent.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
