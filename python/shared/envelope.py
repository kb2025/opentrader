"""
OpenTrader — Message Envelope
Standard schema for all Redis Stream messages across every agent.
"""
import uuid
import time
from typing import Optional
from pydantic import BaseModel, Field


class Envelope(BaseModel):
    msg_id:  str   = Field(default_factory=lambda: str(uuid.uuid4()))
    sender:  str
    stream:  str
    ts_utc:  int   = Field(default_factory=lambda: int(time.time() * 1000))
    version: int   = 1
    payload: dict  = Field(default_factory=dict)

    def to_redis(self) -> dict:
        """Serialize for XADD."""
        return {
            "msg_id":  self.msg_id,
            "sender":  self.sender,
            "stream":  self.stream,
            "ts_utc":  str(self.ts_utc),
            "version": str(self.version),
            "payload": self.model_dump_json(),
        }

    @classmethod
    def from_redis(cls, data: dict) -> "Envelope":
        """Deserialize from XREAD."""
        import json
        payload = json.loads(data.get("payload", "{}"))
        return cls(
            msg_id  = data.get("msg_id", ""),
            sender  = data.get("sender", ""),
            stream  = data.get("stream", ""),
            ts_utc  = int(data.get("ts_utc", 0)),
            version = int(data.get("version", 1)),
            payload = payload.get("payload", payload),
        )


class HeartbeatPayload(BaseModel):
    service:  str
    status:   str = "healthy"   # healthy | degraded | recovering
    pid:      Optional[int]  = None
    uptime_s: Optional[float] = None
    metadata: dict = Field(default_factory=dict)


class SignalPayload(BaseModel):
    ticker:      str
    asset_class: str            # equity | etf | options
    direction:   str            # long | short
    confidence:  float
    entry:       Optional[float] = None
    stop:        Optional[float] = None
    target:      Optional[float] = None
    ttl_ms:      int = 30000
    source:      str = "predictor"
    metadata:    dict = Field(default_factory=dict)


class OrderEventPayload(BaseModel):
    event_type:  str            # fill | reject | cancel | partial
    account_id:  str
    broker:      str
    mode:        str
    ticker:      str
    asset_class: str
    direction:   str
    qty:         float
    price:       Optional[float] = None
    pnl:         Optional[float] = None
    order_id:    str = ""
    strategy:    str = ""


class SpreadLeg(BaseModel):
    symbol:      str        # OCC contract symbol
    action:      str        # buy_to_open | sell_to_open | buy_to_close | sell_to_close
    qty:         int
    limit_price: float      # per-leg tick-snapped price
    option_type: str        # call | put
    strike:      float
    expiry:      str        # YYYY-MM-DD


class SpreadOrderPayload(BaseModel):
    strategy_type:  str                 # bull_call_spread | bear_put_spread | bull_put_spread |
                                        # bear_call_spread | pmcc | iron_condor | straddle | strangle
    legs:           list[SpreadLeg]
    net_debit:      Optional[float]     # positive = debit paid, negative = credit received
    max_loss:       float               # abs(net_debit)*100*qty for debit; wing width - credit for condor
    max_gain:       float               # abs(net_credit)*100*qty for credit spreads
    underlying:     str
    account_label:  str
    strategy_tag:   str
    mode:           str
    request_id:     str
