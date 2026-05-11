"""
Command-based interface: !command [args]

Maps friendly command names to MCP tool calls.
To add commands for a new MCP server, add entries to COMMAND_MAP below.

Format: "cmd": (tool_name, arg_builder_fn, formatter_fn)
  - tool_name:    must match the MCP tool name exactly
  - arg_builder:  lambda(args: list[str]) -> dict  (arguments for the tool)
  - formatter:    fn(raw: str, cmd: str, args: list) -> str
"""
import structlog
from .mcp_registry import MCPRegistry
from .formatter import (
    fmt_massive_quote, fmt_massive_news, fmt_short_interest, fmt_analyst_consensus,
    fmt_generic,
    fmt_alpaca_account, fmt_alpaca_positions, fmt_alpaca_orders,
    fmt_alpaca_clock, fmt_alpaca_bars, fmt_alpaca_quote, fmt_alpaca_snapshot,
)

log = structlog.get_logger("chat-agent.commands")

# ── Command registry ──────────────────────────────────────────────────────────
# Each entry: command_alias -> (tool_name, arg_builder, formatter)

COMMAND_MAP: dict[str, tuple] = {
    # ── Massive Market Data (Polygon.io) ─────────────────────────────────────
    "quote":     ("get_quote",
                  lambda a: {"ticker": a[0].upper()},
                  fmt_massive_quote),
    "q":         ("get_quote",
                  lambda a: {"ticker": a[0].upper()},
                  fmt_massive_quote),
    "news":      ("get_ticker_news",
                  lambda a: {"ticker": a[0].upper(), "limit": int(a[1]) if len(a) > 1 else 8},
                  fmt_massive_news),
    "short":     ("get_short_interest",
                  lambda a: {"ticker": a[0].upper(), "limit": 4},
                  fmt_short_interest),
    "consensus": ("get_analyst_consensus",
                  lambda a: {"ticker": a[0].upper()},
                  fmt_analyst_consensus),
    "earnings":  ("get_earnings",
                  lambda a: {"ticker": a[0].upper(), "limit": 4},
                  fmt_generic),
    "divs":      ("get_dividends",
                  lambda a: {"ticker": a[0].upper(), "limit": 6},
                  fmt_generic),
    # ── Alpaca Trading ────────────────────────────────────────────────────────
    "account":   ("get_account_info",
                  lambda a: {},
                  fmt_alpaca_account),
    "positions": ("get_all_positions",
                  lambda a: {},
                  fmt_alpaca_positions),
    "orders":    ("get_orders",
                  lambda a: {"status": a[0] if a else "open"},
                  fmt_alpaca_orders),
    "clock":     ("get_clock",
                  lambda a: {},
                  fmt_alpaca_clock),
    "movers":    ("get_market_movers",
                  lambda a: {"market_type": a[0] if a else "stocks"},
                  fmt_generic),
    "active":    ("get_most_active_stocks",
                  lambda a: {},
                  fmt_generic),
    "bars":      ("get_stock_bars",
                  lambda a: {"symbols": a[0].upper(),
                             "timeframe": a[1] if len(a) > 1 else "1Day",
                             "limit": int(a[2]) if len(a) > 2 else 10},
                  fmt_alpaca_bars),
    "lquote":    ("get_stock_latest_quote",
                  lambda a: {"symbols": a[0].upper()},
                  fmt_alpaca_quote),
    "snapshot":  ("get_stock_snapshot",
                  lambda a: {"symbols": a[0].upper()},
                  fmt_alpaca_snapshot),
    # ── Add more MCP server commands below ───────────────────────────────────
    # "mycommand": ("tool_name", lambda a: {...}, fmt_generic),
}

# Commands that don't need a ticker argument
NO_ARGS_REQUIRED = {"account", "positions", "orders", "clock", "active", "movers"}

HELP_TEXT = """\
**OpenTrader Finance Bot**

**📊 Market Data (Polygon.io)**
`!quote <ticker>` — Real-time price, bid/ask, volume
`!q <ticker>` — Alias for !quote
`!news <ticker> [n]` — Latest news headlines
`!short <ticker>` — Short interest & days-to-cover
`!consensus <ticker>` — Analyst consensus rating & price target
`!earnings <ticker>` — Upcoming & recent earnings dates
`!divs <ticker>` — Dividend history (ex-date, pay-date, amount)

**💼 Alpaca Account & Trading**
`!account` — Account balances & buying power
`!positions` — Open positions with P&L
`!orders [open|closed|all]` — Recent orders
`!clock` — Market hours & next open/close
`!bars <ticker> [timeframe] [limit]` — OHLCV bars *(1Min/5Min/1Hour/1Day)*
`!lquote <ticker>` — Live bid/ask quote from broker
`!snapshot <ticker>` — Full market snapshot
`!movers [stocks|crypto]` — Top market movers
`!active` — Most active stocks

**🤖 AI Mode**
Mention me or DM with any natural language question.
*Examples:* `@bot What's my current P&L?` · `@bot Any bullish signals on NVDA?`
"""


async def handle_command(text: str, registry: MCPRegistry) -> str:
    """Parse and execute a !command. Returns formatted response string."""
    parts = text.strip().lstrip("!/").split()
    if not parts:
        return HELP_TEXT

    cmd  = parts[0].lower()
    args = parts[1:]

    if cmd in ("help", "h", "?", "start"):
        return HELP_TEXT

    if cmd not in COMMAND_MAP:
        close = [k for k in COMMAND_MAP if k.startswith(cmd[:2])]
        hint  = f"  Did you mean: {', '.join(f'`!{c}`' for c in close[:3])}?" if close else ""
        return f"Unknown command `!{cmd}`.{hint}\nTry `!help` for all commands."

    tool_name, arg_builder, formatter = COMMAND_MAP[cmd]

    if not args and cmd not in NO_ARGS_REQUIRED:
        return f"Usage: `!{cmd} <ticker>`"

    try:
        arguments = arg_builder(args)
    except (IndexError, ValueError) as e:
        return f"Invalid arguments for `!{cmd}`: {e}"

    log.info("commands.executing", cmd=cmd, tool=tool_name, args=arguments)
    raw = await registry.call_tool(tool_name, arguments)
    return formatter(raw, cmd, args)


def is_command(text: str) -> bool:
    return bool(text.strip()) and (text.strip()[0] in ("!", "/"))
