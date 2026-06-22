"""
RL Trading Agent — Tabular Q-Learning Position Sizer

A CPU-friendly tabular Q-learning agent that learns position-sizing signals
from historical OHLCV factor data. No GPU or heavy ML frameworks required —
uses only numpy (already in requirements).

State space:  discretized (momentum_bin, rsi_bin, vol_ratio_bin) — 3D grid
Action space: 5 discrete actions
  0 = hold
  1 = small_long  (e.g. 25% of max size)
  2 = full_long   (100% of max size)
  3 = small_short (25% short)
  4 = full_short  (100% short)
"""
import asyncio
import math
import os
import time
from typing import Optional

import aiohttp
import numpy as np
import structlog

log = structlog.get_logger("predictor.rl_agent")

# ── Constants ─────────────────────────────────────────────────────────────────

ACTION_LABELS = ["hold", "small_long", "full_long", "small_short", "full_short"]
N_ACTIONS     = len(ACTION_LABELS)

# Bin edges for state discretization
# momentum_20: percent change over 20 days
MOMENTUM_EDGES = [-10.0, -3.0, -1.0, 1.0, 3.0, 10.0]  # 5 bins
# rsi_14: 0–100
RSI_EDGES      = [20.0, 35.0, 50.0, 65.0, 80.0]         # 5 bins
# vol_ratio: ratio of current vol to 20d avg
VOL_RATIO_EDGES = [0.5, 0.75, 1.0, 1.5, 2.5]            # 5 bins

# In-memory agent cache keyed by ticker
_AGENT_CACHE: dict[str, "RLTradingAgent"] = {}


# ── Helper functions ──────────────────────────────────────────────────────────

def _bin_value(value: float, edges: list[float]) -> int:
    """Map a continuous value to a bin index using the given bin edges.
    Returns index in [0, len(edges)-1] — one fewer than the number of edge points
    gives us len(edges) - 1 bins plus clipping at extremes for len(edges) total bins.
    We use len(edges) bins: below first edge, between each pair, above last edge.
    """
    for i, edge in enumerate(edges):
        if value < edge:
            return i
    return len(edges)


def _reward(action: int, fwd_return: float) -> float:
    """
    Compute the reward for taking 'action' given a forward return.
    Long actions rewarded by +fwd_return, short actions by -fwd_return.
    Hold gets a small negative cost to incentivize decisive action.
    """
    if action == 0:  # hold
        return -0.01
    if action == 1:  # small_long
        return 0.25 * fwd_return - 0.005
    if action == 2:  # full_long
        return fwd_return
    if action == 3:  # small_short
        return -0.25 * fwd_return - 0.005
    if action == 4:  # full_short
        return -fwd_return
    return 0.0


def _compute_rsi(closes: list[float], period: int = 14) -> Optional[float]:
    if len(closes) < period + 1:
        return None
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains  = [max(d, 0.0) for d in deltas]
    losses = [abs(min(d, 0.0)) for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    return round(100.0 - 100.0 / (1.0 + avg_gain / avg_loss), 2)


def _compute_momentum(closes: list[float], window: int = 20) -> Optional[float]:
    if len(closes) < window + 1:
        return None
    base = closes[-(window + 1)]
    if base == 0:
        return None
    return round((closes[-1] - base) / base * 100.0, 4)


def _compute_vol_ratio(volumes: list[float], window: int = 20) -> Optional[float]:
    if len(volumes) < window + 1:
        return None
    avg = sum(volumes[-window - 1:-1]) / window
    if avg == 0:
        return None
    return round(volumes[-1] / avg, 4)


# ── Core class ────────────────────────────────────────────────────────────────

class RLTradingAgent:
    """
    Tabular Q-learning agent for position sizing.

    State:   discretized (momentum_bin, rsi_bin, vol_bin) — 3D grid
    Actions: 0=hold, 1=small_long, 2=full_long, 3=small_short, 4=full_short
    """

    def __init__(
        self,
        alpha: float = 0.1,
        gamma: float = 0.95,
        epsilon: float = 0.1,
        n_bins: int = 5,
    ):
        self.alpha   = alpha    # learning rate
        self.gamma   = gamma    # discount factor
        self.epsilon = epsilon  # exploration rate (used during training only)
        self.n_bins  = n_bins
        # Q-table shape: (n_bins, n_bins, n_bins, N_ACTIONS)
        self.q_table = np.zeros((n_bins, n_bins, n_bins, N_ACTIONS), dtype=np.float64)
        self._fitted = False

    def _discretize(self, momentum: float, rsi: float, vol_ratio: float) -> tuple[int, int, int]:
        """Map continuous factor values to (momentum_bin, rsi_bin, vol_bin) indices."""
        m_bin = min(_bin_value(momentum,  MOMENTUM_EDGES),  self.n_bins - 1)
        r_bin = min(_bin_value(rsi,       RSI_EDGES),       self.n_bins - 1)
        v_bin = min(_bin_value(vol_ratio, VOL_RATIO_EDGES), self.n_bins - 1)
        return m_bin, r_bin, v_bin

    def fit(self, episodes: list[dict]) -> dict:
        """
        Train on historical episodes.

        Each episode dict:
          {"momentum": float, "rsi": float, "vol_ratio": float, "fwd_return": float}

        Runs single-step Q-learning updates (no replay buffer).
        Returns {"episodes": int, "mean_reward": float}.
        """
        if not episodes:
            return {"episodes": 0, "mean_reward": 0.0}

        rng          = np.random.default_rng(seed=42)
        total_reward = 0.0
        n            = len(episodes)

        for ep in episodes:
            momentum   = float(ep.get("momentum",  0.0) or 0.0)
            rsi        = float(ep.get("rsi",       50.0) or 50.0)
            vol_ratio  = float(ep.get("vol_ratio", 1.0) or 1.0)
            fwd_return = float(ep.get("fwd_return", 0.0) or 0.0)

            state = self._discretize(momentum, rsi, vol_ratio)

            # ε-greedy action selection during training
            if rng.random() < self.epsilon:
                action = int(rng.integers(0, N_ACTIONS))
            else:
                action = int(np.argmax(self.q_table[state]))

            reward = _reward(action, fwd_return)
            total_reward += reward

            # Q-learning update: Q(s,a) ← Q(s,a) + α·[r + γ·max Q(s',a') - Q(s,a)]
            # Since we don't have explicit next-state transitions (single-step episodes),
            # we use the terminal state convention: next-state Q-value = 0.
            # This is equivalent to a Monte-Carlo update for immediate rewards.
            current_q = self.q_table[state][action]
            self.q_table[state][action] = current_q + self.alpha * (reward - current_q)

        self._fitted = True
        mean_reward  = total_reward / n

        log.info(
            "rl_agent.fit_done",
            episodes=n,
            mean_reward=round(mean_reward, 4),
        )
        return {"episodes": n, "mean_reward": round(mean_reward, 4)}

    def predict(self, momentum: float, rsi: float, vol_ratio: float) -> dict:
        """
        Greedy action selection (ε=0 at inference).

        Returns:
          {
            "action":       int,
            "action_label": str,
            "q_values":     list[float],
            "confidence":   float,   # softmax-like confidence of the chosen action
          }
        """
        state  = self._discretize(momentum, rsi, vol_ratio)
        q_vals = self.q_table[state].tolist()

        action = int(np.argmax(self.q_table[state]))

        # Compute a simple confidence via softmax over Q-values
        q_arr = np.array(q_vals)
        # Shift for numerical stability
        q_shifted = q_arr - q_arr.max()
        exp_q     = np.exp(np.clip(q_shifted, -20, 20))
        softmax   = exp_q / (exp_q.sum() + 1e-9)
        confidence = float(softmax[action])

        return {
            "action":       action,
            "action_label": ACTION_LABELS[action],
            "q_values":     [round(v, 6) for v in q_vals],
            "confidence":   round(confidence, 4),
        }

    def save(self, path: str) -> None:
        """Persist Q-table and metadata to a .npy file (via np.save in dict form)."""
        np.save(path, {  # type: ignore[call-overload]
            "q_table": self.q_table,
            "alpha":   self.alpha,
            "gamma":   self.gamma,
            "epsilon": self.epsilon,
            "n_bins":  self.n_bins,
            "fitted":  self._fitted,
        })
        log.info("rl_agent.saved", path=path)

    def load(self, path: str) -> None:
        """Load Q-table and metadata from a previously saved .npy file."""
        data = np.load(path, allow_pickle=True).item()
        self.q_table  = data["q_table"]
        self.alpha    = float(data.get("alpha",   self.alpha))
        self.gamma    = float(data.get("gamma",   self.gamma))
        self.epsilon  = float(data.get("epsilon", self.epsilon))
        self.n_bins   = int(data.get("n_bins",    self.n_bins))
        self._fitted  = bool(data.get("fitted",   True))
        log.info("rl_agent.loaded", path=path, fitted=self._fitted)


# ── Training helpers ──────────────────────────────────────────────────────────

async def _fetch_ohlcv(ticker: str, days: int, market_data_url: str) -> list[dict]:
    """Fetch OHLCV bars from the Market Data Gateway. Returns [] on failure."""
    url = f"{market_data_url}/ohlcv/{ticker}"
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15)
        ) as session:
            async with session.get(url, params={"days": str(days)}) as resp:
                if resp.status != 200:
                    log.warning("rl_agent.ohlcv_fetch_error", ticker=ticker, status=resp.status)
                    return []
                data = await resp.json(content_type=None)
                if isinstance(data, list):
                    return data
                return data.get("bars") or data.get("ohlcv") or []
    except Exception as e:
        log.warning("rl_agent.ohlcv_fetch_failed", ticker=ticker, error=str(e))
        return []


def _bars_to_episodes(bars: list[dict]) -> list[dict]:
    """
    Convert OHLCV bars into Q-learning episodes.
    Each episode: factors computed from a 30-day rolling window,
    fwd_return = next-day close-to-close percentage return.
    """
    if len(bars) < 32:
        return []

    closes  = [float(b.get("close") or 0) for b in bars]
    volumes = [float(b.get("volume") or 0) for b in bars]
    episodes: list[dict] = []

    # Minimum 30 bars of history needed to compute all factors
    for i in range(30, len(bars) - 1):
        c_window = closes[:i + 1]
        v_window = volumes[:i + 1]

        momentum  = _compute_momentum(c_window, 20)
        rsi       = _compute_rsi(c_window, 14)
        vol_ratio = _compute_vol_ratio(v_window, 20)

        if momentum is None or rsi is None or vol_ratio is None:
            continue

        # Forward return: next close vs current close
        fwd_close  = closes[i + 1]
        curr_close = closes[i]
        if curr_close == 0:
            continue
        fwd_return = (fwd_close - curr_close) / curr_close * 100.0

        episodes.append({
            "momentum":  momentum,
            "rsi":       rsi,
            "vol_ratio": vol_ratio,
            "fwd_return": round(fwd_return, 4),
        })

    return episodes


async def train_from_factors(
    ticker: str,
    lookback_days: int = 180,
    market_data_url: str = "http://ot-market-data:8090",
) -> "RLTradingAgent":
    """
    Fetch OHLCV data for 'ticker', compute factors, train an RLTradingAgent, and return it.
    The trained agent is also stored in _AGENT_CACHE.
    """
    log.info("rl_agent.training", ticker=ticker, lookback_days=lookback_days)
    bars = await _fetch_ohlcv(ticker, lookback_days, market_data_url)

    agent = RLTradingAgent()

    if not bars:
        log.warning("rl_agent.no_data", ticker=ticker)
        _AGENT_CACHE[ticker] = agent
        return agent

    episodes = _bars_to_episodes(bars)
    if not episodes:
        log.warning("rl_agent.insufficient_data", ticker=ticker, bars=len(bars))
        _AGENT_CACHE[ticker] = agent
        return agent

    # Run multiple passes (epochs) to improve convergence
    all_episodes = episodes * 5  # 5 epochs
    result = agent.fit(all_episodes)

    log.info(
        "rl_agent.trained",
        ticker=ticker,
        bars=len(bars),
        episodes=len(episodes),
        mean_reward=result["mean_reward"],
    )

    _AGENT_CACHE[ticker] = agent
    return agent


async def get_rl_signal(
    ticker: str,
    agent: Optional["RLTradingAgent"] = None,
    market_data_url: str = "http://ot-market-data:8090",
    lookback_days: int = 180,
) -> dict:
    """
    Get the current RL position-sizing signal for a ticker.

    If no agent is provided, checks the in-memory cache first, then trains
    a new agent if not found. Fetches the latest OHLCV bars to compute
    current factors, then calls agent.predict().

    Returns:
      {
        "ticker":       str,
        "action":       int,
        "action_label": str,
        "q_values":     list[float],
        "confidence":   float,
        "factors":      {"momentum": float, "rsi": float, "vol_ratio": float},
        "fitted":       bool,
        "ts_utc":       int,
      }
    """
    if agent is None:
        if ticker in _AGENT_CACHE and _AGENT_CACHE[ticker]._fitted:
            agent = _AGENT_CACHE[ticker]
        else:
            agent = await train_from_factors(ticker, lookback_days, market_data_url)

    # Fetch recent bars to compute current factor values (need at least 35 bars)
    bars = await _fetch_ohlcv(ticker, 60, market_data_url)

    # Defaults — if we can't compute factors, use neutral state
    momentum  = 0.0
    rsi       = 50.0
    vol_ratio = 1.0

    if bars and len(bars) >= 22:
        closes  = [float(b.get("close") or 0) for b in bars]
        volumes = [float(b.get("volume") or 0) for b in bars]
        m = _compute_momentum(closes, min(20, len(closes) - 1))
        r = _compute_rsi(closes, min(14, len(closes) - 1))
        v = _compute_vol_ratio(volumes, min(20, len(volumes) - 1))
        if m is not None:
            momentum = m
        if r is not None:
            rsi = r
        if v is not None:
            vol_ratio = v

    prediction = agent.predict(momentum, rsi, vol_ratio)

    return {
        "ticker":       ticker,
        "action":       prediction["action"],
        "action_label": prediction["action_label"],
        "q_values":     prediction["q_values"],
        "confidence":   prediction["confidence"],
        "factors":      {
            "momentum":  round(momentum, 4),
            "rsi":       round(rsi, 2),
            "vol_ratio": round(vol_ratio, 4),
        },
        "fitted":       agent._fitted,
        "ts_utc":       int(time.time() * 1000),
    }
