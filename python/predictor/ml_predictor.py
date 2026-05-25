"""
ML Predictor Ensemble
Walk-forward trained RandomForest + GradientBoosting + Ridge models that
produce a directional confidence score from OHLCV features. Blended with
the rule-based scorer output as a weighted composite before LLM refinement.

Feature set (29):
  ret_5, ret_10, ret_20          — price momentum at 3 short lookbacks
  ret_21, ret_63, ret_126,
  ret_252                        — 1-month, 1-quarter, 6-month, 1-year returns
  mom_accel                      — ret_21 - ret_63  (short vs medium momentum)
  trend_slope                    — ret_63 - ret_252 (medium vs long-term trend)
  rsi_14                         — RSI(14)
  macd_hist                      — MACD histogram / price (normalized)
  bb_pos                         — Bollinger Band position (-1=lower, +1=upper)
  sma20_pct, sma50_pct,
  sma200_pct                     — price vs moving averages
  vol_ratio                      — today's volume / 20-day avg volume
  vol_trend                      — 5-day avg volume / 20-day avg volume
  vol_momentum                   — volume × price change (force index proxy)
  mfi_14                         — Money Flow Index(14): volume-weighted pressure oscillator
  mfi_slope_2                    — 2-bar rate of change of MFI (momentum direction/confirmation)
  atr_pct                        — ATR(14) / price (volatility proxy)
  candle_body                    — (close-open)/(high-low) candle shape
  bid_ask_proxy                  — (close-low)/(high-low) buying pressure proxy
  open_gap                       — (open - prev_close)/prev_close: overnight gap (retail/emotional move)
  intraday_ret                   — (close - open)/open: intraday return (smart money proxy, SMFI-inspired)
  earnings_gap                   — signed max overnight gap > 2% in last 63 days (PEAD proxy)
  spy_rel_21                     — ticker 21-day return minus SPY 21-day return (short-term alpha)
  spy_rel_63                     — ticker 63-day return minus SPY 63-day return (medium-term alpha)

Walk-forward:
  Fetch 2 years daily OHLCV → engineer features → create binary labels
  (price up/down ≥ 1% in 5 days) → train on first 80% → validate on last 20%
  → return ensemble probability for current day

Models are cached in-memory per (ticker, date) and retrained once per day.
If sklearn is unavailable the module degrades gracefully to no-op.
"""
import asyncio
import logging
import math
from datetime import date
from typing import Optional

log = logging.getLogger("predictor.ml")

try:
    import numpy as np
    import pandas as pd
    from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
    from sklearn.linear_model import RidgeClassifier
    from sklearn.preprocessing import StandardScaler
    from sklearn.metrics import accuracy_score
    _SKLEARN_OK = True
except ImportError:
    _SKLEARN_OK = False
    log.warning("ml_predictor: scikit-learn not installed — ML ensemble disabled")


FORWARD_DAYS   = 5       # predict price movement this many days out
LABEL_THRESH   = 0.01    # 1% move required to count as signal
TRAIN_RATIO    = 0.80    # fraction of history used for training
MIN_TRAIN_ROWS = 120     # minimum rows needed to train (≈ 6 months)
HISTORY_DAYS   = 730     # 2 years of daily OHLCV

# Benchmark cache: stores SPY OHLCV DataFrame keyed by calendar date
# Populated once per day by _fetch_benchmark(); avoids repeated API calls
# across multiple concurrent ticker predictions running in thread executors.
from shared.data_cache import SyncTTLCache as _SyncTTLCache
_BENCHMARK_CACHE: _SyncTTLCache = _SyncTTLCache(default_ttl=3600)  # 1-hour TTL
BENCHMARK_TICKER = "SPY"  # broad market benchmark for relative momentum features


def _compute_rsi(close: "pd.Series", period: int = 14) -> "pd.Series":
    delta    = close.diff()
    gain     = delta.clip(lower=0).rolling(period).mean()
    loss     = (-delta.clip(upper=0)).rolling(period).mean()
    rs       = gain / loss.replace(0, float("nan"))
    return 100 - (100 / (1 + rs))


def _compute_mfi(close: "pd.Series", high: "pd.Series", low: "pd.Series",
                 volume: "pd.Series", period: int = 14) -> "pd.Series":
    typical  = (close + high + low) / 3
    raw_mf   = typical * volume
    pos_mf   = raw_mf.where(typical > typical.shift(1), 0.0)
    neg_mf   = raw_mf.where(typical < typical.shift(1), 0.0)
    pos_sum  = pos_mf.rolling(period).sum()
    neg_sum  = neg_mf.rolling(period).sum()
    total    = (pos_sum + neg_sum).replace(0, float("nan"))
    return (pos_sum / total) * 100


def _engineer_features(
    df: "pd.DataFrame",
    benchmark_df: "Optional[pd.DataFrame]" = None,
) -> "pd.DataFrame":
    """Return a DataFrame of ML features aligned to the input index."""
    close  = df["Close"].squeeze()
    high   = df["High"].squeeze()
    low    = df["Low"].squeeze()
    open_  = df["Open"].squeeze()
    volume = df["Volume"].squeeze()

    f = pd.DataFrame(index=df.index)

    # Short-term momentum
    f["ret_5"]  = close.pct_change(5)
    f["ret_10"] = close.pct_change(10)
    f["ret_20"] = close.pct_change(20)

    # Multi-timeframe momentum (1m, 1q, 6m, 1y)
    f["ret_21"]  = close.pct_change(21)
    f["ret_63"]  = close.pct_change(63)
    f["ret_126"] = close.pct_change(126)
    f["ret_252"] = close.pct_change(252)

    # Cross-timeframe momentum ratios
    f["mom_accel"]   = f["ret_21"] - f["ret_63"]   # short vs medium
    f["trend_slope"] = f["ret_63"] - f["ret_252"]   # medium vs long

    # RSI
    f["rsi_14"] = _compute_rsi(close, 14) / 100.0  # normalize to 0-1

    # MACD histogram (normalized by price)
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd  = ema12 - ema26
    sig   = macd.ewm(span=9, adjust=False).mean()
    f["macd_hist"] = (macd - sig) / close.replace(0, float("nan"))

    # Bollinger Band position
    sma20  = close.rolling(20).mean()
    std20  = close.rolling(20).std()
    f["bb_pos"] = (close - sma20) / (2 * std20.replace(0, float("nan")))

    # SMA relative position
    sma50  = close.rolling(50).mean()
    sma200 = close.rolling(200).mean()
    f["sma20_pct"]  = (close - sma20)  / sma20.replace(0, float("nan"))
    f["sma50_pct"]  = (close - sma50)  / sma50.replace(0, float("nan"))
    f["sma200_pct"] = (close - sma200) / sma200.replace(0, float("nan"))

    # Volume features
    avg_vol20    = volume.rolling(20).mean()
    avg_vol5     = volume.rolling(5).mean()
    f["vol_ratio"]    = volume / avg_vol20.replace(0, float("nan"))
    f["vol_trend"]    = avg_vol5 / avg_vol20.replace(0, float("nan"))  # 5d vs 20d avg
    # Force index proxy: volume × price change direction
    price_change      = close.pct_change(1)
    f["vol_momentum"] = (volume * price_change) / avg_vol20.replace(0, float("nan"))

    # Money Flow Index: level + 2-bar slope (EA31337 crossover confirmation pattern)
    _mfi_raw        = _compute_mfi(close, high, low, volume, 14)
    f["mfi_14"]     = _mfi_raw / 100.0
    f["mfi_slope_2"] = _mfi_raw.diff(2) / 100.0

    # ATR volatility
    hl_range    = high - low
    f["atr_pct"] = hl_range.rolling(14).mean() / close.replace(0, float("nan"))

    # Candle body direction
    hl_safe      = (high - low).replace(0, float("nan"))
    f["candle_body"] = (close - open_) / hl_safe

    # Bid/ask proxy: buying pressure = (close - low) / (high - low)
    # Approximates where price closed within the bar's range (1=full buying, 0=full selling)
    f["bid_ask_proxy"] = (close - low) / hl_safe

    # Smart Money Flow decomposition (daily approximation of SMFI):
    # Split each day's return into overnight gap (retail/emotional) and intraday move (institutional)
    prev_close        = close.shift(1)
    f["open_gap"]     = (open_ - prev_close) / prev_close.replace(0, float("nan"))
    f["intraday_ret"] = (close - open_) / open_.replace(0, float("nan"))

    # Post-Earnings Announcement Drift (PEAD) proxy:
    # The signed maximum overnight gap > 2% in the trailing 63 days approximates
    # the market's most recent earnings reaction (direction + magnitude of surprise).
    gaps = f["open_gap"].copy()
    gaps_63 = gaps.iloc[-63:] if len(gaps) >= 63 else gaps
    large_gaps = gaps_63[gaps_63.abs() > 0.02]
    if len(large_gaps) > 0:
        peak_idx = large_gaps.abs().idxmax()
        earnings_gap_val = float(large_gaps.loc[peak_idx])
    else:
        earnings_gap_val = 0.0
    f["earnings_gap"] = earnings_gap_val  # scalar broadcast — same value for all rows

    # Sector relative momentum: ticker return minus broad market (SPY) return.
    # Measures whether the stock is generating alpha vs the market.
    if benchmark_df is not None and len(benchmark_df) >= 63:
        spy_close = benchmark_df["Close"].squeeze()
        # Align index: inner join on dates present in both series
        aligned = close.to_frame("tk").join(spy_close.to_frame("spy"), how="inner")
        if len(aligned) >= 63:
            f["spy_rel_21"] = (
                aligned["tk"].pct_change(21) - aligned["spy"].pct_change(21)
            ).reindex(f.index)
            f["spy_rel_63"] = (
                aligned["tk"].pct_change(63) - aligned["spy"].pct_change(63)
            ).reindex(f.index)
        else:
            f["spy_rel_21"] = 0.0
            f["spy_rel_63"] = 0.0
    else:
        f["spy_rel_21"] = 0.0
        f["spy_rel_63"] = 0.0

    return f


def _create_labels(close: "pd.Series", direction: str) -> "pd.Series":
    """
    Binary label: 1 if the price moves in `direction` by at least LABEL_THRESH
    over the next FORWARD_DAYS trading days, 0 otherwise.
    """
    future_ret = close.shift(-FORWARD_DAYS) / close - 1
    if direction == "long":
        return (future_ret >= LABEL_THRESH).astype(int)
    else:
        return (future_ret <= -LABEL_THRESH).astype(int)


def _fetch_ohlcv(ticker: str) -> "Optional[pd.DataFrame]":
    """Fetch 2yr daily OHLCV from Polygon.io. Returns None on failure."""
    try:
        import os
        from datetime import date, timedelta
        from polygon import RESTClient

        api_key = os.getenv("MASSIVE_API_KEY", "")
        if not api_key:
            log.warning("ml_predictor: MASSIVE_API_KEY not set, skipping %s", ticker)
            return None
        client   = RESTClient(api_key)
        to_date  = date.today().isoformat()
        frm_date = (date.today() - timedelta(days=HISTORY_DAYS)).isoformat()
        bars = client.get_aggs(ticker.upper(), 1, "day", frm_date, to_date,
                               limit=750, adjusted=True)
        if not bars or len(bars) < MIN_TRAIN_ROWS + FORWARD_DAYS + 50:
            return None
        rows = [
            {"Date": date.fromtimestamp(b.timestamp / 1000),
             "Open": b.open, "High": b.high, "Low": b.low,
             "Close": b.close, "Volume": b.volume}
            for b in bars
        ]
        df = pd.DataFrame(rows).set_index("Date").sort_index()
        return df
    except Exception as e:
        log.warning("ml_predictor: polygon fetch failed for %s: %s", ticker, e)
        return None


def _fetch_benchmark() -> "Optional[pd.DataFrame]":
    """
    Fetch SPY OHLCV for relative momentum features. Results cached 1h in
    _BENCHMARK_CACHE to avoid repeated API calls across ticker predictions.
    Returns None on failure; callers fall back to 0.0 for relative features.
    """
    cache_key = f"benchmark:{BENCHMARK_TICKER}"
    cached = _BENCHMARK_CACHE.get(cache_key)
    if cached is not None:
        return cached
    result = _fetch_ohlcv(BENCHMARK_TICKER)
    if result is not None:
        _BENCHMARK_CACHE.set(cache_key, result)
    return result


def _train_and_predict(ticker: str, direction: str) -> dict:
    """
    Synchronous: fetch data, engineer features, walk-forward train ensemble,
    and return prediction for the most recent row.

    Returns dict with keys:
      ml_confidence   float 0-1   ensemble probability for the signal direction
      val_accuracy    float 0-1   out-of-sample accuracy on validation split
      model_count     int         number of models that contributed
      feature_count   int
    """
    df = _fetch_ohlcv(ticker)
    if df is None:
        return {}

    benchmark_df = _fetch_benchmark()
    feats  = _engineer_features(df, benchmark_df)
    labels = _create_labels(df["Close"].squeeze(), direction)

    # Align, drop NaN rows (need full feature window + forward label)
    combined = feats.copy()
    combined["__label__"] = labels
    combined.dropna(inplace=True)

    # Remove last FORWARD_DAYS rows (no label yet — future is unknown)
    valid_rows = combined.iloc[:-FORWARD_DAYS]
    if len(valid_rows) < MIN_TRAIN_ROWS:
        return {}

    X_all = valid_rows.drop(columns=["__label__"]).values
    y_all = valid_rows["__label__"].values

    n_train = int(len(X_all) * TRAIN_RATIO)
    if n_train < 60 or (len(X_all) - n_train) < 20:
        return {}

    X_train, X_val = X_all[:n_train], X_all[n_train:]
    y_train, y_val = y_all[:n_train], y_all[n_train:]

    scaler  = StandardScaler()
    X_train_s = scaler.fit_transform(X_train)
    X_val_s   = scaler.transform(X_val)

    models = [
        ("rf",    RandomForestClassifier(n_estimators=100, max_depth=6,
                                          min_samples_leaf=5, random_state=42,
                                          n_jobs=1)),
        ("gb",    GradientBoostingClassifier(n_estimators=80, learning_rate=0.05,
                                              max_depth=4, subsample=0.8,
                                              random_state=42)),
        ("ridge", RidgeClassifier(alpha=1.0)),
    ]

    trained, val_probas = [], []
    for name, model in models:
        try:
            model.fit(X_train_s, y_train)
            # Gather per-model validation probabilities
            if hasattr(model, "predict_proba"):
                p = model.predict_proba(X_val_s)[:, 1]
            else:
                # Ridge: sigmoid of decision function
                d = model.decision_function(X_val_s)
                p = 1.0 / (1.0 + np.exp(-d))
            val_probas.append(p)
            trained.append((name, model))
        except Exception as e:
            log.debug("ml_predictor: model %s failed for %s: %s", name, ticker, e)

    if not trained:
        return {}

    # Ensemble validation accuracy (using 0.5 threshold on avg probability)
    avg_val_p = np.mean(val_probas, axis=0)
    val_acc   = float(accuracy_score(y_val, (avg_val_p >= 0.5).astype(int)))

    # Predict on today's features (most recent row of full feats, no label needed)
    today_feats = feats.dropna().iloc[[-1]].values
    if len(today_feats) == 0:
        return {}

    today_s = scaler.transform(today_feats)
    today_probas = []
    for name, model in trained:
        try:
            if hasattr(model, "predict_proba"):
                p = float(model.predict_proba(today_s)[0, 1])
            else:
                d = float(model.decision_function(today_s)[0])
                p = 1.0 / (1.0 + math.exp(-d))
            today_probas.append(p)
        except Exception:
            pass

    if not today_probas:
        return {}

    ml_conf = float(np.mean(today_probas))

    return {
        "ml_confidence": round(ml_conf, 4),
        "val_accuracy":  round(val_acc, 4),
        "model_count":   len(trained),
        "feature_count": X_train.shape[1],
    }


class MLEnsemble:
    """
    Async wrapper around the synchronous _train_and_predict pipeline.
    Maintains a per-(ticker, direction, date) in-memory cache so models
    are only retrained once per calendar day per ticker.
    """

    def __init__(self):
        # cache: (ticker, direction, date) -> result dict
        self._cache: dict[tuple, dict] = {}

    async def predict(
        self,
        ticker: str,
        direction: str,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> dict:
        """
        Return ML ensemble result for ticker+direction.
        Training runs in a thread executor to avoid blocking the event loop.
        Returns empty dict if sklearn is unavailable or training fails.
        """
        if not _SKLEARN_OK:
            return {}

        cache_key = (ticker, direction, date.today())
        if cache_key in self._cache:
            return self._cache[cache_key]

        try:
            lp = loop or asyncio.get_event_loop()
            result = await lp.run_in_executor(
                None, _train_and_predict, ticker, direction
            )
        except Exception as e:
            log.warning("ml_predictor: predict failed for %s/%s: %s",
                        ticker, direction, e)
            result = {}

        self._cache[cache_key] = result
        if result:
            log.info(
                "ml_predictor: %s/%s → ml_conf=%.3f val_acc=%.3f models=%d",
                ticker, direction,
                result.get("ml_confidence", 0),
                result.get("val_accuracy", 0),
                result.get("model_count", 0),
            )
        return result

    def clear_old_cache(self):
        """Evict entries from prior days to prevent unbounded growth."""
        today = date.today()
        stale = [k for k in self._cache if k[2] != today]
        for k in stale:
            del self._cache[k]
