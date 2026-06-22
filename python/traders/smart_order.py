"""
OpenTrader Smart Order Engine
Provides TWAP and VWAP order slicing for large orders.
Used by equity_trader when a position size exceeds SPLIT_ORDER_THRESHOLD_USD.

Not an agent — a standalone async module imported by traders.
"""
import asyncio
import uuid
from typing import Optional

import structlog

log = structlog.get_logger("smart-order")


class SmartOrderEngine:
    """
    Slices a large order into child orders submitted to the broker.commands stream.

    Algorithms
    ----------
    twap : equal-sized slices submitted at fixed intervals.
    vwap : slice sizes ramp up linearly toward the end of the schedule
           (heavier volume later, typical intraday VWAP shape).
    """

    def __init__(self, redis, broker_commands_stream: str):
        self._redis  = redis
        self._stream = broker_commands_stream

    async def execute(
        self,
        ticker:        str,
        direction:     str,
        total_qty:     int,
        account_label: str,
        strategy:      str,
        duration:      str = "day",
        algo:          str = "twap",
        slices:        int = 5,
        interval_sec:  float = 60.0,
        request_id:    Optional[str] = None,
    ) -> list[dict]:
        """
        Slice total_qty into `slices` child orders and submit them to the
        broker.commands stream, waiting interval_sec between each submission.

        Parameters
        ----------
        ticker        : instrument symbol
        direction     : "long" or "short"
        total_qty     : total number of shares to trade
        account_label : broker account label for routing
        strategy      : strategy tag forwarded to the gateway
        duration      : order duration ("day" | "gtc" | "ioc" etc.)
        algo          : "twap" for equal slices, "vwap" for ramp-up weights
        slices        : number of child orders
        interval_sec  : seconds to wait between consecutive child orders
        request_id    : parent request id; auto-generated if not provided

        Returns
        -------
        List of child request_id dicts: [{"slice": i, "request_id": "..."}]
        """
        if total_qty < 1:
            log.warning("smart-order.invalid_qty", ticker=ticker, total_qty=total_qty)
            return []

        slices = max(1, slices)
        parent_id = request_id or str(uuid.uuid4())

        # Compute per-slice quantities
        quantities = self._compute_slices(total_qty, slices, algo)

        log.info("smart-order.start",
                 ticker=ticker, direction=direction, total_qty=total_qty,
                 algo=algo, slices=slices, interval_sec=interval_sec,
                 parent_id=parent_id)

        results: list[dict] = []

        for i, qty in enumerate(quantities):
            if qty < 1:
                continue

            child_id = str(uuid.uuid4())
            cmd = {
                "command":           "place_order",
                "request_id":        child_id,
                "account_label":     account_label,
                "ticker":            ticker,
                "direction":         direction,
                "qty":               str(qty),
                "order_type":        "market",
                "duration":          duration,
                "strategy":          strategy,
                "issued_by":         "smart-order",
                "parent_request_id": parent_id,
            }

            try:
                await self._redis.xadd(self._stream, cmd, maxlen=10_000)
                results.append({"slice": i + 1, "request_id": child_id})
                log.info("smart-order.slice_submitted",
                         ticker=ticker, slice=i + 1, total=slices,
                         qty=qty, child_id=child_id)
            except Exception as e:
                log.error("smart-order.slice_failed",
                          ticker=ticker, slice=i + 1, error=str(e))

            # Wait before the next slice (skip wait after the last one)
            if i < len(quantities) - 1:
                await asyncio.sleep(interval_sec)

        log.info("smart-order.complete",
                 ticker=ticker, parent_id=parent_id, submitted=len(results))
        return results

    @staticmethod
    def _compute_slices(total_qty: int, slices: int, algo: str) -> list[int]:
        """
        Return a list of integer quantities that sum to total_qty.

        TWAP: equal-sized buckets; any remainder is added to the last slice.
        VWAP: linearly increasing weights [1, 2, ..., n]; heavier toward the end.
        """
        if algo == "vwap":
            # Weights 1, 2, ..., slices  (heavier toward end)
            weights = list(range(1, slices + 1))
            total_weight = sum(weights)
            raw = [total_qty * w / total_weight for w in weights]
        else:
            # TWAP: uniform
            raw = [total_qty / slices] * slices

        # Floor all values and distribute the remainder to the last slice
        floors = [int(v) for v in raw]
        remainder = total_qty - sum(floors)
        if floors:
            floors[-1] += remainder

        return floors
