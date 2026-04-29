"""
Shadow Account — Counterfactual P&L Analysis

Answers: "How much did trading discipline lapses cost?"

Four discipline categories:
  noise_trade  — Low-confidence signal that lost money; should have been skipped.
  early_exit   — Exited a winner before the price peaked; left gains on the table.
  late_exit    — Held through a profitable window into a loss; ignored the stop.
  overtrading  — Multiple entries on the same ticker in the same session.
"""
import asyncio
import json
import logging
import os
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd

log = logging.getLogger("shadow_account")

_NOISE_THRESHOLD  = float(os.getenv("SHADOW_NOISE_THRESHOLD", "0.65"))
_HOLD_WINDOW_DAYS = int(os.getenv("SHADOW_HOLD_WINDOW", "20"))
_EARLY_DELTA_PCT  = 0.25   # ≥25% more on table → early_exit
_LLM_MODEL        = os.getenv("LLM_PREDICTOR_MODEL", "anthropic/claude-sonnet-4-5")


# ── Data fetching ─────────────────────────────────────────────────────────────

async def _fetch_trades(pool, date_from: date, date_to: date,
                        account_label: Optional[str]) -> list[dict]:
    """
    Fetch closed trades (status='closed') first. If none exist, fall back to
    filled trades (status='fill') with entry_price, treating the current price
    (latest OHLCV close) as the exit for unrealized P&L analysis.
    """
    q = """
        SELECT id, ts, account_id, broker, ticker, direction, qty,
               entry_price, exit_price, pnl, strategy, status
        FROM trades
        WHERE ts::date BETWEEN $1 AND $2
          AND entry_price IS NOT NULL
          AND status IN ('closed', 'fill')
    """
    args: list = [date_from, date_to]
    if account_label:
        q += " AND account_id = $3"
        args.append(account_label)
    q += " ORDER BY ts"
    rows = await pool.fetch(q, *args)
    return [dict(r) for r in rows]


async def _fetch_signals(pool, date_from: date, date_to: date) -> dict:
    """Return {(ticker, date) → {confidence, payload}} — highest-confidence signal per ticker/day."""
    rows = await pool.fetch(
        """
        SELECT ticker, ts::date AS sig_date, confidence, payload
        FROM signals
        WHERE ts::date BETWEEN $1 AND $2
          AND source = 'predictor'
        ORDER BY ts::date, ticker, confidence DESC
        """,
        date_from, date_to,
    )
    result: dict = {}
    for r in rows:
        key = (r["ticker"], r["sig_date"])
        if key not in result:
            payload = r["payload"] or {}
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except Exception:
                    payload = {}
            result[key] = {
                "confidence": float(r["confidence"] or 0),
                "payload": payload,
            }
    return result


def _fetch_ohlcv(ticker: str, start: date, end: date) -> pd.DataFrame:
    """Fetch OHLCV via Ticker.history() — always returns single-level columns."""
    import yfinance as yf
    df = yf.Ticker(ticker).history(
        start=str(start),
        end=str(end + timedelta(days=1)),
        auto_adjust=True,
    )
    # Normalise index to plain date objects
    df.index = pd.to_datetime(df.index).date
    return df


# ── Trade analysis ────────────────────────────────────────────────────────────

def _entry_date(trade: dict) -> date:
    ts = trade.get("ts")
    if isinstance(ts, datetime):
        return ts.date()
    return date.fromisoformat(str(ts)[:10])


def _analyze_trade(trade: dict, ohlcv: Optional[pd.DataFrame]) -> dict:
    entry_price = float(trade.get("entry_price") or 0)
    qty         = float(trade.get("qty") or 0)
    direction   = trade.get("direction", "long")
    signal      = trade.get("signal") or {}
    confidence  = float(signal.get("confidence") or 0)
    is_open     = trade.get("status") == "fill" and not trade.get("exit_price")

    # Compute actual P&L: use DB value if closed, else estimate from latest OHLCV close
    exit_price  = float(trade.get("exit_price") or 0)
    actual_pnl  = float(trade.get("pnl") or 0)
    if is_open and ohlcv is not None and not ohlcv.empty and entry_price > 0 and qty > 0:
        if "Close" in ohlcv.columns and len(ohlcv) > 0:
            last_close = ohlcv["Close"].iloc[-1]
            exit_price = float(last_close) if not hasattr(last_close, '__len__') else float(last_close.iloc[0])
            actual_pnl = (
                (exit_price - entry_price) * qty if direction == "long"
                else (entry_price - exit_price) * qty
            )

    entry_dt = _entry_date(trade)

    ideal_pnl        = actual_pnl
    ideal_price       = exit_price
    had_profit_window = False

    if ohlcv is not None and not ohlcv.empty and entry_price > 0 and qty > 0:
        window = ohlcv[ohlcv.index >= entry_dt].head(_HOLD_WINDOW_DAYS)

        if not window.empty and "High" in window.columns and "Low" in window.columns:
            if direction == "long":
                best_price = float(window["High"].to_numpy().max())
                had_profit_window = best_price > entry_price * 1.01
            else:
                best_price = float(window["Low"].to_numpy().min())
                had_profit_window = best_price < entry_price * 0.99

            raw_ideal = (
                (best_price - entry_price) * qty if direction == "long"
                else (entry_price - best_price) * qty
            )
            cap       = max(abs(actual_pnl) * 5 + 100, 200)
            ideal_pnl  = min(raw_ideal, cap)
            ideal_price = round(best_price, 4)

    category        = "clean"
    discipline_cost = 0.0

    if confidence > 0 and confidence < _NOISE_THRESHOLD and actual_pnl < 0:
        category        = "noise_trade"
        discipline_cost = abs(actual_pnl)

    elif actual_pnl > 0 and ideal_pnl > actual_pnl * (1 + _EARLY_DELTA_PCT):
        category        = "early_exit"
        discipline_cost = ideal_pnl - actual_pnl

    elif actual_pnl < 0 and had_profit_window:
        category        = "late_exit"
        discipline_cost = abs(actual_pnl)

    return {
        "id":                str(trade.get("id", "")),
        "ts":                str(trade.get("ts", ""))[:19],
        "ticker":            trade.get("ticker", ""),
        "direction":         direction,
        "qty":               float(qty),
        "entry_price":       float(entry_price),
        "exit_price":        float(exit_price),
        "actual_pnl":        round(actual_pnl, 2),
        "ideal_price":       ideal_price,
        "ideal_pnl":         round(ideal_pnl, 2),
        "signal_confidence": round(confidence, 4),
        "had_profit_window": had_profit_window,
        "category":          category,
        "discipline_cost":   round(discipline_cost, 2),
        "strategy":          trade.get("strategy", ""),
        "account_id":        trade.get("account_id", ""),
    }


def _detect_overtrading(scored: list[dict]) -> list[dict]:
    """Second pass: flag duplicate ticker entries on the same day as overtrading."""
    seen: dict = defaultdict(int)
    for st in scored:
        key = (st["ticker"], st["ts"][:10])
        seen[key] += 1
        if seen[key] > 1 and st["category"] == "clean" and st["actual_pnl"] < 0:
            st["category"]        = "overtrading"
            st["discipline_cost"] = abs(st["actual_pnl"])
    return scored


# ── LLM rule extraction ────────────────────────────────────────────────────────

async def _extract_rules(openrouter_key: str, scored: list[dict],
                         categories: dict) -> list[dict]:
    import aiohttp as _aiohttp

    def _line(t: dict) -> str:
        return (
            f"{t['ticker']} {t['direction']} | conf={t['signal_confidence']:.2f} | "
            f"pnl=${t['actual_pnl']:+.2f} | cat={t['category']}"
        )

    winners = [t for t in scored if t["actual_pnl"] > 0]
    losers  = [t for t in scored if t["actual_pnl"] <= 0]

    prompt = f"""You are analyzing trading discipline for an algorithmic trading system.

WINNING TRADES:
{chr(10).join(_line(t) for t in winners[:20]) or "None"}

LOSING TRADES:
{chr(10).join(_line(t) for t in losers[:20]) or "None"}

DISCIPLINE COSTS:
  noise_trade  (low-confidence losses):      ${categories.get('noise_trade',  0):+.2f}
  early_exit   (left gains on table):        ${categories.get('early_exit',   0):+.2f}
  late_exit    (held through profit to loss): ${categories.get('late_exit',   0):+.2f}
  overtrading  (repeat same ticker/day):     ${categories.get('overtrading',  0):+.2f}

Extract 3-5 specific, quantified trading rules that would have improved performance.

Return ONLY a JSON array. Each element:
{{
  "id":        "r1",
  "rule":      "concrete actionable rule text",
  "category":  "noise_trade|early_exit|late_exit|overtrading",
  "filter":    {{"type": "min_confidence|max_loss_pct|min_hold_days", "value": <number>}},
  "narrative": "one-sentence explanation",
  "est_gain":  <estimated dollar gain if rule had been applied>
}}

Return raw JSON only, no markdown fences.
"""

    try:
        async with _aiohttp.ClientSession() as session:
            async with session.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization":  f"Bearer {openrouter_key}",
                    "Content-Type":   "application/json",
                },
                json={
                    "model":       _LLM_MODEL,
                    "messages":    [
                        {"role": "system", "content": "You are a quantitative trading analyst. Return only valid JSON."},
                        {"role": "user",   "content": prompt},
                    ],
                    "max_tokens":  800,
                    "temperature": 0.2,
                },
                timeout=_aiohttp.ClientTimeout(total=30),
            ) as resp:
                body = await resp.json()
                raw  = body["choices"][0]["message"]["content"].strip()
                if raw.startswith("```"):
                    raw = "\n".join(raw.split("\n")[1:])
                    if "```" in raw:
                        raw = raw[:raw.index("```")]
                rules = json.loads(raw)
                return rules if isinstance(rules, list) else []
    except Exception as e:
        log.warning("shadow.llm_fail", error=str(e))
        return []


# ── Rule backtesting ──────────────────────────────────────────────────────────

def _backtest_rule(rule: dict, scored: list[dict]) -> dict:
    f      = rule.get("filter") or {}
    f_type = f.get("type", "")
    f_val  = float(f.get("value", 0))

    affected = 0
    gain     = 0.0

    if f_type == "min_confidence":
        for st in scored:
            c = st["signal_confidence"]
            if 0 < c < f_val:
                affected += 1
                gain += -st["actual_pnl"]   # skipping → don't lose (or don't gain)

    elif f_type == "max_loss_pct":
        for st in scored:
            if st["actual_pnl"] < 0:
                ep, q = st["entry_price"], st["qty"]
                if ep > 0 and q > 0:
                    cap_loss  = ep * q * f_val / 100
                    true_loss = abs(st["actual_pnl"])
                    if true_loss > cap_loss:
                        affected += 1
                        gain += true_loss - cap_loss

    elif f_type == "min_hold_days":
        for st in scored:
            if st.get("ideal_pnl", 0) > st["actual_pnl"] and st["actual_pnl"] > 0:
                affected += 1
                gain += st["ideal_pnl"] - st["actual_pnl"]

    return {**rule, "trades_affected": affected, "backtested_gain": round(gain, 2)}


# ── Public API ────────────────────────────────────────────────────────────────

async def run_analysis(
    pool,
    date_from:      date,
    date_to:        date,
    account_label:  Optional[str],
    openrouter_key: str,
) -> dict:
    """Run full shadow-account analysis. Returns JSON-serializable dict."""
    trades = await _fetch_trades(pool, date_from, date_to, account_label)
    if not trades:
        return {"error": "No trades found for the selected period", "trade_count": 0}

    signals_map = await _fetch_signals(pool, date_from, date_to)
    for t in trades:
        t["signal"] = signals_map.get((t["ticker"], _entry_date(t)))

    # Fetch OHLCV for all unique tickers concurrently
    tickers     = list({t["ticker"] for t in trades})
    ohlcv_start = date_from - timedelta(days=2)
    ohlcv_end   = date_to   + timedelta(days=_HOLD_WINDOW_DAYS + 5)

    ohlcv_cache: dict[str, Optional[pd.DataFrame]] = {}
    results = await asyncio.gather(
        *[
            asyncio.get_event_loop().run_in_executor(
                None, _fetch_ohlcv, ticker, ohlcv_start, ohlcv_end
            )
            for ticker in tickers
        ],
        return_exceptions=True,
    )
    for ticker, res in zip(tickers, results):
        ohlcv_cache[ticker] = None if isinstance(res, Exception) else res

    # Score each trade and detect overtrading
    scored = [_analyze_trade(t, ohlcv_cache.get(t["ticker"])) for t in trades]
    scored = _detect_overtrading(scored)

    cats: dict[str, float] = {
        "noise_trade": 0.0, "early_exit": 0.0,
        "late_exit":   0.0, "overtrading": 0.0, "clean": 0.0,
    }
    for st in scored:
        cats[st["category"]] = round(cats.get(st["category"], 0.0) + st["discipline_cost"], 2)

    total_actual     = round(sum(s["actual_pnl"]       for s in scored), 2)
    total_ideal      = round(sum(s["ideal_pnl"]        for s in scored), 2)
    total_discipline = round(sum(s["discipline_cost"]  for s in scored), 2)

    top5 = sorted(scored, key=lambda x: x["discipline_cost"], reverse=True)[:5]

    rules: list[dict] = []
    if openrouter_key and not openrouter_key.startswith("your_"):
        raw_rules = await _extract_rules(openrouter_key, scored, cats)
        rules     = [_backtest_rule(r, scored) for r in raw_rules]

    return {
        "date_from":            str(date_from),
        "date_to":              str(date_to),
        "account_label":        account_label,
        "trade_count":          len(scored),
        "actual_pnl":           total_actual,
        "ideal_pnl":            total_ideal,
        "discipline_cost":      total_discipline,
        "categories":           cats,
        "rules":                rules,
        "counterfactual_top5":  top5,
        "trades":               scored,
    }
