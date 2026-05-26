"""
BrokerConnector — abstract base class.
Every broker module must implement this interface.
The broker gateway routes all platform commands through it.
"""
from abc import ABC, abstractmethod
from typing import Optional


class BrokerConnector(ABC):
    """
    One instance per account.
    broker_name and account_label are set by the registry at construction time.
    """

    broker_name:   str  # "tradier" | "webull" | "alpaca"
    account_label: str  # human label from accounts.toml
    account_id:    str  # broker-side account ID
    mode:          str  # "live" | "sandbox" | "paper"

    # ── Orders ───────────────────────────────────────────────────────────────

    @abstractmethod
    async def place_equity_order(
        self,
        symbol:     str,
        side:       str,           # buy | sell | sell_short | buy_to_cover
        quantity:   int,
        order_type: str = "market",
        price:      Optional[float] = None,
        stop:       Optional[float] = None,
        duration:   str = "day",
        tag:        Optional[str] = None,
    ) -> dict:
        """Place an equity or ETF order. Returns broker order dict."""

    @abstractmethod
    async def place_option_order(
        self,
        symbol:        str,        # underlying
        option_symbol: str,        # full OCC / broker option symbol
        side:          str,        # buy_to_open | sell_to_open | buy_to_close | sell_to_close
        quantity:      int,
        order_type:    str = "market",
        price:         Optional[float] = None,
        duration:      str = "day",
        tag:           Optional[str] = None,
    ) -> dict:
        """Place a single-leg options order."""

    @abstractmethod
    async def cancel_order(self, order_id: str) -> dict:
        """Cancel a specific open order."""

    @abstractmethod
    async def cancel_all_orders(self) -> list[dict]:
        """Cancel all open orders. Used by circuit breaker."""

    # ── Portfolio ─────────────────────────────────────────────────────────────

    @abstractmethod
    async def get_positions(self) -> list[dict]:
        """Return all open positions."""

    @abstractmethod
    async def get_balances(self) -> dict:
        """Return account balances / buying power."""

    @abstractmethod
    async def get_orders(self, status: str = "all") -> list[dict]:
        """Return orders. status: all | open | closed | filled."""

    # ── Market data ───────────────────────────────────────────────────────────

    @abstractmethod
    async def get_quote(self, symbol: str) -> dict:
        """Return latest quote for a symbol."""

    @abstractmethod
    async def get_quotes(self, symbols: list[str]) -> list[dict]:
        """Return latest quotes for multiple symbols."""

    # ── Multi-leg options ─────────────────────────────────────────────────────

    async def place_spread_order(
        self,
        underlying:    str,
        strategy_type: str,
        legs:          list[dict],    # list of SpreadLeg dicts
        net_debit:     Optional[float] = None,
        duration:      str = "day",
        tag:           Optional[str] = None,
    ) -> dict:
        """
        Place a multi-leg spread order.
        Default: sequential single-leg fallback.
        Override in connectors that support native multi-leg (e.g. Tradier).
        Returns combined result dict with spread_group_id and per-leg order_ids.
        """
        import uuid
        spread_group_id = str(uuid.uuid4())
        order_ids = []
        for leg in legs:
            result = await self.place_option_order(
                symbol        = underlying,
                option_symbol = leg["symbol"],
                side          = leg["action"],
                quantity      = leg["qty"],
                order_type    = "limit" if leg.get("limit_price") else "market",
                price         = leg.get("limit_price"),
                duration      = duration,
                tag           = tag,
            )
            order_id = str(result.get("id") or result.get("orderId") or "")
            order_ids.append({"leg": leg["symbol"], "order_id": order_id})
        return {
            "spread_group_id": spread_group_id,
            "strategy_type":   strategy_type,
            "underlying":      underlying,
            "order_ids":       order_ids,
            "net_debit":       net_debit,
            "status":          "ok",
        }

    # ── Options market data ───────────────────────────────────────────────────

    async def get_option_chain(self, symbol: str) -> dict:
        """
        Return options chain for the underlying symbol.
        Shape: {ticker, price, expirations, calls, puts}
        Each contract: {contract, strike, expiration, bid, ask, mid, last,
                        intrinsic, extrinsic, iv, delta, gamma, theta, vega,
                        volume, oi, itm}
        Raise NotImplementedError if the broker does not support chain data.
        """
        raise NotImplementedError(f"{self.broker_name} does not support get_option_chain")

    # ── Status ───────────────────────────────────────────────────────────────

    def status(self) -> dict:
        return {
            "broker":        self.broker_name,
            "account_label": self.account_label,
            "account_id":    self.account_id,
            "mode":          self.mode,
        }
