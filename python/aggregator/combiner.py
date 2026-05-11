"""
Aggregator Combiner
Merges per-ticker sentiment from multiple scrapers with Massive MCP
fundamentals (dividends, earnings) into TickerIntelligence.
"""
import asyncio
import json
import os
from datetime import date
import structlog

from .models import TickerIntelligence

log = structlog.get_logger("aggregator.combiner")

EARNINGS_BUFFER_DAYS = int(os.getenv("EARNINGS_BUFFER_DAYS", "5"))

# Source weights for combined sentiment score (wsb + seekalpha only)
SOURCE_WEIGHTS = {
    "wsb":       0.38,   # social/retail crowd
    "seekalpha": 0.62,   # professional analysis
}


def build_intelligence(
    ticker:         str,
    sentiment_data: dict,           # source → {mention_count, sentiment_score, sentiment_label, headlines}
    massive_data:   dict,           # from fetch_massive_fundamentals()
    current_price:  float = 0.0,
    uw_flow:        dict | None = None,    # from get_uw_ticker_flow()
    uw_darkpool:    dict | None = None,    # from get_uw_darkpool()
) -> TickerIntelligence:
    """Combine all sources into a single TickerIntelligence object."""
    intel = TickerIntelligence(ticker=ticker)
    sources = []

    # ── Sentiment ─────────────────────────────────────────────────────────────
    weighted_sum   = 0.0
    weight_total   = 0.0
    all_headlines  = []
    wsb_mentions   = 0
    wsb_sentiment  = 0.0

    for source, w in SOURCE_WEIGHTS.items():
        d = sentiment_data.get(source)
        if not d:
            continue
        sources.append(source)
        score = float(d.get("sentiment_score", 0.0))
        weighted_sum  += score * w
        weight_total  += w
        headlines      = d.get("headlines", [])
        if isinstance(headlines, list):
            all_headlines.extend(headlines[:2])

        if source == "wsb":
            wsb_mentions  = int(d.get("mention_count", 0))
            wsb_sentiment = score

    if weight_total > 0:
        intel.sentiment_score = round(weighted_sum / weight_total, 4)
    intel.sentiment_label = (
        "bullish" if intel.sentiment_score > 0.05 else
        "bearish" if intel.sentiment_score < -0.05 else
        "neutral"
    )
    intel.news_headlines  = list(dict.fromkeys(all_headlines))[:5]  # dedup, keep 5
    intel.news_sentiment  = round(weighted_sum / weight_total, 4) if weight_total else 0.0
    intel.social_mentions  = wsb_mentions
    intel.social_sentiment = wsb_sentiment
    intel.social_momentum  = _compute_momentum(wsb_mentions)
    intel.sources          = sources

    # ── Dividends ─────────────────────────────────────────────────────────────
    div = massive_data.get("dividend", {})
    if div:
        intel.dividend_yield   = div.get("yield", 0.0)
        intel.dividend_annual  = div.get("annual", 0.0)
        intel.dividend_ex_date = div.get("ex_date")

    # ── Earnings ──────────────────────────────────────────────────────────────
    earnings = massive_data.get("earnings", {})
    if earnings:
        intel.earnings_date      = earnings.get("date")
        intel.earnings_days_away = earnings.get("days_away")
        intel.earnings_too_close = (
            intel.earnings_days_away is not None
            and intel.earnings_days_away <= EARNINGS_BUFFER_DAYS
        )

    # ── Unusual Whales options flow ───────────────────────────────────────────
    if uw_flow:
        intel.uw_flow_signal   = uw_flow.get("flow_signal",   "neutral")
        intel.uw_net_premium   = uw_flow.get("net_premium",   0.0)
        intel.uw_call_premium  = uw_flow.get("call_premium",  0.0)
        intel.uw_put_premium   = uw_flow.get("put_premium",   0.0)
        intel.uw_bullish_count = uw_flow.get("bullish_count", 0)
        intel.uw_bearish_count = uw_flow.get("bearish_count", 0)
        intel.uw_total_alerts  = uw_flow.get("total_alerts",  0)

    # ── Unusual Whales dark pool ──────────────────────────────────────────────
    if uw_darkpool:
        intel.uw_dp_print_count    = uw_darkpool.get("print_count",    0)
        intel.uw_dp_total_shares   = uw_darkpool.get("total_shares",   0)
        intel.uw_dp_total_notional = uw_darkpool.get("total_notional", 0.0)

    # ── Confidence delta ──────────────────────────────────────────────────────
    intel.confidence_delta = _compute_delta(intel)

    # ── Actionable flag ───────────────────────────────────────────────────────
    intel.actionable = bool(sources) and not intel.earnings_too_close

    # ── Summary ───────────────────────────────────────────────────────────────
    intel.summary = _build_summary(intel)

    return intel


def _compute_momentum(wsb_mentions: int) -> str:
    if wsb_mentions >= 10:
        return "rising"
    if wsb_mentions >= 3:
        return "flat"
    return "flat"


def _compute_delta(intel: TickerIntelligence) -> float:
    """
    Compute confidence adjustment (-0.20 → +0.20) based on intel signals.
    Positive factors: strong sentiment, analyst buy, upside, social momentum
    Negative factors: bearish sentiment, sell consensus, earnings risk
    """
    delta = 0.0

    # Sentiment contribution (max ±0.08)
    delta += intel.sentiment_score * 0.08

    # Analyst consensus (max ±0.06)
    consensus_map = {
        "strong_buy":  0.06,
        "buy":         0.04,
        "hold":        0.00,
        "sell":        -0.04,
        "strong_sell": -0.06,
        "none":        0.00,
    }
    delta += consensus_map.get(intel.analyst_consensus, 0.0)

    # Analyst upside > 15% adds +0.03, < -5% subtracts 0.03
    if intel.analyst_upside_pct > 15:
        delta += 0.03
    elif intel.analyst_upside_pct < -5:
        delta -= 0.03

    # Social momentum
    if intel.social_momentum == "rising" and intel.social_sentiment > 0:
        delta += 0.03

    # Unusual Whales options flow (max ±0.06)
    if intel.uw_flow_signal == "bullish":
        delta += 0.06
    elif intel.uw_flow_signal == "bearish":
        delta -= 0.06

    # Dark pool: large notional prints are a mild bullish signal (+0.02)
    if intel.uw_dp_total_notional >= 1_000_000:
        delta += 0.02

    # Earnings proximity penalty
    if intel.earnings_days_away is not None:
        if intel.earnings_days_away <= EARNINGS_BUFFER_DAYS:
            delta -= 0.20  # hard penalty — predictor will filter this out anyway

    return round(max(-0.20, min(0.20, delta)), 4)


def _build_summary(intel: TickerIntelligence) -> str:
    parts = []

    if intel.sentiment_label != "neutral":
        parts.append(f"{intel.sentiment_label} sentiment ({intel.sentiment_score:+.2f})")

    if intel.analyst_consensus not in ("none", "hold", ""):
        upside_str = f", {intel.analyst_upside_pct:+.1f}% upside" if intel.analyst_upside_pct else ""
        parts.append(
            f"{intel.analyst_consensus.replace('_', ' ')} consensus "
            f"({intel.analyst_buy_pct:.0%} buy{upside_str})"
        )

    if intel.social_mentions >= 3:
        parts.append(f"WSB {intel.social_mentions} mentions")

    if intel.uw_flow_signal != "neutral" and intel.uw_total_alerts > 0:
        net_m = intel.uw_net_premium / 1_000_000
        parts.append(
            f"UW flow {intel.uw_flow_signal} "
            f"(${net_m:+.1f}M net, {intel.uw_total_alerts} alerts)"
        )

    if intel.uw_dp_total_notional >= 500_000:
        notional_m = intel.uw_dp_total_notional / 1_000_000
        parts.append(f"DP ${notional_m:.1f}M ({intel.uw_dp_print_count} prints)")

    if intel.earnings_too_close and intel.earnings_date:
        parts.append(f"⚠ earnings in {intel.earnings_days_away}d ({intel.earnings_date})")

    if intel.dividend_yield > 0:
        parts.append(f"div yield {intel.dividend_yield:.1f}%")

    return " | ".join(parts) if parts else "no actionable intelligence"


async def fetch_massive_fundamentals(ticker: str) -> dict:
    """
    Fetch dividend and earnings data via Massive MCP (Polygon.io).
    Returns dict with dividend and earnings sub-dicts.
    """
    from shared.mcp_client import call_mcp_tool, MASSIVE_MCP_URL

    result: dict = {"dividend": {}, "earnings": {}}

    # ── Dividends ─────────────────────────────────────────────────────────────
    try:
        raw_div = await call_mcp_tool(MASSIVE_MCP_URL, "get_dividends", {"ticker": ticker, "limit": 4})
        if raw_div:
            divs = json.loads(raw_div)
            if isinstance(divs, list) and divs:
                today = date.today()
                upcoming = [d for d in divs if d.get("ex_date") and d["ex_date"] >= today.isoformat()]
                ref = upcoming[0] if upcoming else divs[0]
                cash = float(ref.get("cash_amount") or 0)
                freq = int(ref.get("frequency") or 0)
                annual = cash * freq if freq else cash
                result["dividend"] = {
                    "yield":   0.0,  # yield requires current price — caller can compute
                    "annual":  round(annual, 4),
                    "ex_date": ref.get("ex_date"),
                }
    except Exception as e:
        log.warning("combiner.massive_div_error", ticker=ticker, error=str(e))

    # ── Earnings ──────────────────────────────────────────────────────────────
    try:
        raw_earn = await call_mcp_tool(MASSIVE_MCP_URL, "get_earnings", {"ticker": ticker, "limit": 4})
        if raw_earn:
            records = json.loads(raw_earn)
            if isinstance(records, list) and records:
                today = date.today()
                upcoming = [e for e in records if e.get("date") and e["date"] >= today.isoformat()]
                if upcoming:
                    nxt = upcoming[-1]  # earliest upcoming (list is desc, so last)
                    earnings_date = nxt["date"]
                    days_away     = (date.fromisoformat(earnings_date) - today).days
                    result["earnings"] = {"date": earnings_date, "days_away": days_away}
    except Exception as e:
        log.warning("combiner.massive_earnings_error", ticker=ticker, error=str(e))

    return result
