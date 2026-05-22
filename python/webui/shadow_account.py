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
import math
import os
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Optional

import numpy as np
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
    Combine equity trades (trades table) and closed option positions
    (option_trade_log JOIN option_positions) so all broker accounts have data.
    """
    eq_q = """
        SELECT id, ts, account_id, broker, ticker, direction,
               qty::numeric                                    AS qty,
               entry_price::numeric                           AS entry_price,
               exit_price::numeric                            AS exit_price,
               pnl::numeric                                   AS pnl,
               strategy, status,
               FALSE AS _is_option
        FROM trades
        WHERE ts::date BETWEEN $1 AND $2
          AND entry_price IS NOT NULL
          AND status IN ('closed', 'fill')
    """
    eq_args: list = [date_from, date_to]
    if account_label:
        eq_q += " AND account_id = $3"
        eq_args.append(account_label)
    eq_q += " ORDER BY ts"

    opt_q = """
        SELECT DISTINCT ON (op.account_label, op.underlying, op.strike, op.expiration_date, op.entry_price, op.entry_date)
            otl.id,
            otl.ts,
            op.account_label               AS account_id,
            op.broker,
            op.underlying                  AS ticker,
            'long'                         AS direction,
            COALESCE(otl.qty, op.qty)      AS qty,
            op.entry_price,
            otl.contract_price             AS exit_price,
            otl.realized_pnl               AS pnl,
            NULL::text                     AS strategy,
            'closed'                       AS status,
            TRUE                           AS _is_option,
            op.option_type                 AS _opt_type,
            op.strike                      AS _opt_strike,
            op.expiration_date             AS _opt_expiry
        FROM option_trade_log otl
        JOIN option_positions op ON op.id = otl.position_id
        WHERE otl.event_type = 'closed'
          AND otl.ts::date != op.entry_date
          AND otl.ts::date BETWEEN $1 AND $2
          AND op.entry_price IS NOT NULL
    """
    opt_args: list = [date_from, date_to]
    if account_label:
        opt_q += " AND op.account_label = $3"
        opt_args.append(account_label)
    opt_q += " ORDER BY op.account_label, op.underlying, op.strike, op.expiration_date, op.entry_price, op.entry_date, otl.ts ASC"

    eq_rows, opt_rows = await asyncio.gather(
        pool.fetch(eq_q, *eq_args),
        pool.fetch(opt_q, *opt_args),
    )

    combined = [dict(r) for r in eq_rows] + [dict(r) for r in opt_rows]
    combined.sort(key=lambda x: x["ts"])
    return combined


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


async def _fetch_ohlcv(ticker: str, start: date, end: date) -> pd.DataFrame:
    """Fetch OHLCV via Massive MCP — returns DataFrame with date index."""
    try:
        from shared.mcp_client import get_massive_daily_bars
        bars = await get_massive_daily_bars(ticker, str(start), str(end))
        if not bars:
            return pd.DataFrame()
        rows = {}
        for b in bars:
            try:
                d = date.fromisoformat(str(b["date"])[:10])
                rows[d] = {
                    "Open":   float(b.get("open",   0) or 0),
                    "High":   float(b.get("high",   0) or 0),
                    "Low":    float(b.get("low",    0) or 0),
                    "Close":  float(b.get("close",  0) or 0),
                    "Volume": float(b.get("volume", 0) or 0),
                }
            except Exception:
                continue
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame.from_dict(rows, orient="index")
        df.index.name = None
        return df
    except Exception as e:
        log.warning("shadow.ohlcv_fail", ticker=ticker, error=str(e))
        return pd.DataFrame()


# ── Trade analysis ────────────────────────────────────────────────────────────

def _entry_date(trade: dict) -> date:
    ts = trade.get("ts")
    if isinstance(ts, datetime):
        return ts.date()
    return date.fromisoformat(str(ts)[:10])


def _safe_float(val, default: float = 0.0) -> float:
    try:
        return float(val) if val is not None else default
    except (ValueError, TypeError):
        return default


def _analyze_trade(trade: dict, ohlcv: Optional[pd.DataFrame]) -> dict:
    is_option   = bool(trade.get("_is_option", False))
    entry_price = _safe_float(trade.get("entry_price"))
    qty         = _safe_float(trade.get("qty"))
    direction   = trade.get("direction", "long")
    signal      = trade.get("signal") or {}
    confidence  = _safe_float(signal.get("confidence"))
    is_open     = trade.get("status") == "fill" and not trade.get("exit_price")

    # Compute actual P&L: use DB value if closed, else estimate from latest OHLCV close
    exit_price  = _safe_float(trade.get("exit_price"))
    actual_pnl  = _safe_float(trade.get("pnl"))
    if is_open and not is_option and ohlcv is not None and not ohlcv.empty and entry_price > 0 and qty > 0:
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

    # For options, don't compute OHLCV-based ideal: option premium ≠ underlying price.
    # early_exit/late_exit categories are skipped; noise_trade and overtrading still apply.
    if not is_option and ohlcv is not None and not ohlcv.empty and entry_price > 0 and qty > 0:
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
        "trade_type":        "option" if is_option else "equity",
        "opt_type":          str(trade.get("_opt_type") or ""),
        "opt_strike":        _safe_float(trade.get("_opt_strike")) if trade.get("_opt_strike") else None,
        "opt_expiry":        str(trade.get("_opt_expiry") or "")[:10] if trade.get("_opt_expiry") else None,
    }


def _compute_perf_stats(scored: list[dict], date_from: date, date_to: date) -> dict:
    """
    Build a daily equity curve from closed trade P&L and compute:
    Sharpe ratio, max drawdown %, CAGR, win rate, profit factor.
    """
    if not scored:
        return {}

    # Daily P&L — group by exit date (first 10 chars of ts)
    daily: dict[str, float] = {}
    for t in scored:
        day = (t.get("ts") or "")[:10]
        if day:
            daily[day] = daily.get(day, 0.0) + float(t.get("actual_pnl") or 0)

    # Fill every calendar day in range with 0 if no trades
    all_days: list[str] = []
    d = date_from
    while d <= date_to:
        all_days.append(str(d))
        d += timedelta(days=1)

    pnl_arr   = np.array([daily.get(day, 0.0) for day in all_days], dtype=float)
    cumulative = np.cumsum(pnl_arr)

    # Sharpe on daily $ P&L (annualised, 252 trading days)
    std = float(np.std(pnl_arr)) + 1e-10
    sharpe = round(float(np.mean(pnl_arr)) / std * math.sqrt(252), 3)

    # Max drawdown on cumulative P&L curve
    peak       = np.maximum.accumulate(cumulative)
    drawdowns  = cumulative - peak
    max_dd_abs = float(np.min(drawdowns))
    max_peak   = float(np.max(peak)) if np.max(peak) > 0 else 1.0
    max_dd_pct = round(abs(max_dd_abs / max_peak * 100) if max_peak > 0 else 0.0, 2)

    # Win / loss stats
    winners = [t for t in scored if float(t.get("actual_pnl") or 0) > 0]
    losers  = [t for t in scored if float(t.get("actual_pnl") or 0) < 0]
    win_rate = round(len(winners) / len(scored) * 100, 1) if scored else 0.0
    gross_profit = sum(float(t.get("actual_pnl") or 0) for t in winners)
    gross_loss   = abs(sum(float(t.get("actual_pnl") or 0) for t in losers)) or 1e-10
    profit_factor = round(gross_profit / gross_loss, 3) if losers else 0.0

    # CAGR — use total deployed notional as base capital proxy
    deployed = sum(
        float(t.get("entry_price") or 0) * float(t.get("qty") or 0)
        for t in scored
        if t.get("entry_price") and t.get("qty")
    )
    total_return = float(cumulative[-1]) if len(cumulative) else 0.0
    n_days = max((date_to - date_from).days, 1)
    if deployed > 0:
        cagr = round(((1 + total_return / deployed) ** (365.0 / n_days) - 1) * 100, 2)
    else:
        cagr = 0.0

    # Equity curve sampled to ≤60 points for sparkline
    step = max(1, len(cumulative) // 60)
    equity_curve = [round(float(v), 2) for v in cumulative[::step]]

    return {
        "sharpe":        sharpe,
        "max_drawdown":  max_dd_pct,
        "win_rate":      win_rate,
        "profit_factor": profit_factor,
        "cagr":          cagr,
        "equity_curve":  equity_curve,
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
    f_val  = _safe_float(f.get("value", 0))

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
        *[_fetch_ohlcv(ticker, ohlcv_start, ohlcv_end) for ticker in tickers],
        return_exceptions=True,
    )
    for ticker, res in zip(tickers, results):
        ohlcv_cache[ticker] = None if isinstance(res, Exception) or (isinstance(res, pd.DataFrame) and res.empty) else res

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

    perf = _compute_perf_stats(scored, date_from, date_to)

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
        # Performance stats
        "sharpe":               perf.get("sharpe",        0.0),
        "max_drawdown":         perf.get("max_drawdown",  0.0),
        "win_rate":             perf.get("win_rate",      0.0),
        "profit_factor":        perf.get("profit_factor", 0.0),
        "cagr":                 perf.get("cagr",          0.0),
        "equity_curve":         perf.get("equity_curve",  []),
    }
