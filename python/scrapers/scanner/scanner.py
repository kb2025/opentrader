"""
Universe Scanner
Scans a universe of tickers against configurable technical rules using OHLCV data
fetched from the internal Market Data Gateway. Runs concurrent batched fetches.

Supported fields: rsi_14, price_change_pct, volume_ratio, price, momentum_20
Supported operators: lt, gt, eq, gte, lte
"""
import asyncio
import math
import time
from typing import Any

import aiohttp
import structlog

log = structlog.get_logger("scanner.universe")

TIMEOUT_S        = 10
BATCH_SIZE       = 20   # tickers per asyncio.gather batch


# ── Technical indicator computation ──────────────────────────────────────────

def _compute_rsi(closes: list[float], period: int = 14) -> float | None:
    """Compute RSI for the given closing prices. Returns None if insufficient data."""
    if len(closes) < period + 1:
        return None
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains  = [max(d, 0.0) for d in deltas]
    losses = [abs(min(d, 0.0)) for d in deltas]

    # Wilder's smoothing — use simple average for seed, then EMA
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs  = avg_gain / avg_loss
    return round(100.0 - 100.0 / (1.0 + rs), 2)


def _compute_price_change_pct(closes: list[float]) -> float | None:
    """Percentage change between last two closing prices."""
    if len(closes) < 2:
        return None
    prev, last = closes[-2], closes[-1]
    if prev == 0:
        return None
    return round((last - prev) / prev * 100.0, 4)


def _compute_volume_ratio(volumes: list[float], window: int = 20) -> float | None:
    """Ratio of latest volume to N-day average volume."""
    if len(volumes) < window + 1:
        return None
    avg = sum(volumes[-window - 1:-1]) / window
    if avg == 0:
        return None
    return round(volumes[-1] / avg, 4)


def _compute_momentum(closes: list[float], window: int = 20) -> float | None:
    """N-day price momentum as percentage change over the window."""
    if len(closes) < window + 1:
        return None
    base = closes[-(window + 1)]
    last = closes[-1]
    if base == 0:
        return None
    return round((last - base) / base * 100.0, 4)


def _compute_fields(bars: list[dict]) -> dict[str, Any]:
    """
    Compute all supported technical fields from a list of OHLCV bars.
    Each bar: {"date", "open", "high", "low", "close", "volume"}
    """
    if not bars:
        return {}

    closes  = [float(b.get("close") or 0) for b in bars]
    volumes = [float(b.get("volume") or 0) for b in bars]
    last_close = closes[-1] if closes else 0.0

    return {
        "price":            round(last_close, 4),
        "rsi_14":           _compute_rsi(closes, 14),
        "price_change_pct": _compute_price_change_pct(closes),
        "volume_ratio":     _compute_volume_ratio(volumes, 20),
        "momentum_20":      _compute_momentum(closes, 20),
    }


# ── Rule evaluation ───────────────────────────────────────────────────────────

OPERATORS = {
    "lt":  lambda v, t: v < t,
    "gt":  lambda v, t: v > t,
    "eq":  lambda v, t: math.isclose(v, t, rel_tol=1e-6),
    "gte": lambda v, t: v >= t,
    "lte": lambda v, t: v <= t,
}


def _evaluate_rules(fields: dict[str, Any], rules: list[dict]) -> list[str]:
    """
    Evaluate a set of rules against computed field values.
    Returns list of matched rule descriptions (empty list = no match).
    A ticker PASSES only when ALL rules match.
    """
    matched: list[str] = []

    for rule in rules:
        field = rule.get("field", "")
        op    = rule.get("op", "")
        value = rule.get("value")

        if field not in fields:
            return []  # required field missing — ticker fails

        field_val = fields.get(field)
        if field_val is None:
            return []  # couldn't compute — ticker fails

        op_fn = OPERATORS.get(op)
        if op_fn is None:
            log.warning("scanner.unknown_operator", op=op)
            return []

        try:
            if not op_fn(float(field_val), float(value)):
                return []  # at least one rule failed
            matched.append(f"{field} {op} {value}")
        except (TypeError, ValueError) as e:
            log.warning("scanner.rule_eval_error", field=field, error=str(e))
            return []

    return matched  # all rules matched


# ── Per-ticker fetch ──────────────────────────────────────────────────────────

async def _fetch_ohlcv(session: aiohttp.ClientSession, market_data_url: str, ticker: str) -> list[dict]:
    """Fetch 30 days of OHLCV from the market data gateway. Returns [] on failure."""
    url = f"{market_data_url}/ohlcv/{ticker}"
    params = {"days": "30"}
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=TIMEOUT_S)) as resp:
            if resp.status != 200:
                log.debug("scanner.ohlcv_error", ticker=ticker, status=resp.status)
                return []
            data = await resp.json(content_type=None)
            # Expect {"bars": [...]} or a list
            if isinstance(data, list):
                return data
            return data.get("bars") or data.get("ohlcv") or []
    except Exception as e:
        log.debug("scanner.ohlcv_fetch_failed", ticker=ticker, error=str(e))
        return []


async def _scan_ticker(
    session: aiohttp.ClientSession,
    market_data_url: str,
    ticker: str,
    rules: list[dict],
    ts_utc: int,
) -> dict | None:
    """Fetch OHLCV for one ticker, compute fields, evaluate rules. Returns match dict or None."""
    bars = await _fetch_ohlcv(session, market_data_url, ticker)
    if not bars:
        return None

    fields  = _compute_fields(bars)
    matched = _evaluate_rules(fields, rules)

    if not matched:
        return None

    return {
        "ticker":        ticker,
        "matched_rules": matched,
        "field_values":  fields,
        "ts_utc":        ts_utc,
    }


# ── Public API ────────────────────────────────────────────────────────────────

async def run_scan(
    universe: list[str],
    rules: list[dict],
    market_data_url: str,
) -> list[dict]:
    """
    Scan a universe of tickers against the given rules.

    Args:
        universe:        List of ticker symbols.
        rules:           List of condition dicts e.g.
                         [{"field": "rsi_14", "op": "lt", "value": 30}]
        market_data_url: Base URL of the Market Data Gateway.

    Returns:
        List of match dicts for tickers that satisfy ALL rules.
    """
    if not universe or not rules:
        return []

    ts_utc   = int(time.time() * 1000)
    matches: list[dict] = []

    async with aiohttp.ClientSession() as session:
        # Process in batches to avoid overwhelming the gateway
        for batch_start in range(0, len(universe), BATCH_SIZE):
            batch = universe[batch_start: batch_start + BATCH_SIZE]
            tasks = [
                _scan_ticker(session, market_data_url, ticker, rules, ts_utc)
                for ticker in batch
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for res in results:
                if isinstance(res, dict):
                    matches.append(res)
                elif isinstance(res, Exception):
                    log.debug("scanner.batch_error", error=str(res))

    log.info(
        "scanner.scan_done",
        universe=len(universe),
        matches=len(matches),
        rules=len(rules),
    )
    return matches
