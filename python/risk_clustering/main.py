"""
Stock Risk Clustering Agent
Triggered weekly by the scheduler. Segments the active ticker universe into
4 risk tiers (very_low / low / medium / high) using K-Means on 9 features:
  volatility_13w, price_change_13w, beta, pe_ratio, pb_ratio,
  roe, roa, fcf_yield, earnings_yield

Results are stored in stock_risk_clusters (TimescaleDB) and published to Redis.
"""
import asyncio
import json
import logging
import os
from datetime import date

import asyncpg
import numpy as np

from shared.base_agent   import BaseAgent
from shared.redis_client import STREAMS, GROUPS, ensure_consumer_group
from shared.data_client  import DataClient
from shared.assignments  import load_active_assignments

log = logging.getLogger("risk-clustering")

CONSUMER_NAME = os.getenv("HOSTNAME", "risk-clustering-0")
TRIGGER_JOB   = "run_risk_clustering"
N_CLUSTERS    = int(os.getenv("RISK_CLUSTER_K", "4"))
BATCH_SIZE    = int(os.getenv("RISK_CLUSTER_BATCH", "8"))
BATCH_DELAY   = float(os.getenv("RISK_CLUSTER_DELAY", "1.2"))

_FEATURE_NAMES = [
    "volatility", "price_change", "beta",
    "pe_ratio",   "pb_ratio",
    "roe",        "roa",
    "fcf_yield",  "earnings_yield",
]
_RISK_TIERS = ["very_low", "low", "medium", "high"]


class RiskClusteringAgent(BaseAgent):

    def __init__(self):
        super().__init__("risk-clustering")
        self._db: asyncpg.Pool | None = None

    async def start(self):
        await self.setup()
        await ensure_consumer_group(self.redis, STREAMS["commands"], GROUPS["risk-clustering"])
        await self._connect_db()
        log.info("risk-clustering.started", n_clusters=N_CLUSTERS)
        await asyncio.gather(
            self.heartbeat_loop(),
            self._command_loop(),
        )

    async def _connect_db(self):
        db_url = os.getenv("DB_URL", "")
        if not db_url:
            log.warning("risk-clustering.no_db_url")
            return
        try:
            self._db = await asyncpg.create_pool(db_url, min_size=1, max_size=3,
                                                  max_inactive_connection_lifetime=300)
            log.info("risk-clustering.db_connected")
        except Exception as e:
            log.error("risk-clustering.db_failed", error=str(e))

    async def _command_loop(self):
        log.info("risk-clustering.command_loop_start")
        while self._running:
            try:
                messages = await self.redis.xreadgroup(
                    groupname=GROUPS["risk-clustering"],
                    consumername=CONSUMER_NAME,
                    streams={STREAMS["commands"]: ">"},
                    count=5,
                    block=10000,
                )
                if not messages:
                    continue
                for _stream, entries in messages:
                    for msg_id, data in entries:
                        try:
                            if data.get("command") == "trigger" and data.get("job") == TRIGGER_JOB:
                                await self.run_clustering()
                        except Exception as e:
                            log.error("risk-clustering.run_error", error=str(e))
                        finally:
                            await self.redis.xack(STREAMS["commands"],
                                                  GROUPS["risk-clustering"], msg_id)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.warning("risk-clustering.command_loop_error", error=str(e))
                await asyncio.sleep(5)

    # ── Main clustering run ───────────────────────────────────────────────────

    async def run_clustering(self):
        log.info("risk-clustering.run_start")
        dc = DataClient()

        tickers = list({a["ticker"] for a in load_active_assignments("equity")})
        log.info("risk-clustering.universe", n=len(tickers))

        features_map: dict[str, dict] = {}
        for i in range(0, len(tickers), BATCH_SIZE):
            batch = tickers[i : i + BATCH_SIZE]
            results = await asyncio.gather(
                *[self._fetch_features(dc, t) for t in batch],
                return_exceptions=True,
            )
            for ticker, result in zip(batch, results):
                if isinstance(result, dict) and result.get("_ok"):
                    features_map[ticker] = result
            await asyncio.sleep(BATCH_DELAY)

        if len(features_map) < N_CLUSTERS + 1:
            log.warning("risk-clustering.insufficient_tickers", n=len(features_map))
            return

        labels, cluster_to_tier, silhouette, X_raw, valid_tickers = self._cluster(features_map)
        if labels is None:
            return

        run_date = date.today()
        await self._store(run_date, valid_tickers, features_map, labels, cluster_to_tier,
                          silhouette, X_raw)

        tier_counts: dict[str, int] = {}
        for lbl in labels:
            tier = cluster_to_tier[int(lbl)]
            tier_counts[tier] = tier_counts.get(tier, 0) + 1

        await self.redis.set("risk_clusters:last_run", run_date.isoformat())
        await self.redis.set("risk_clusters:tier_counts", json.dumps(tier_counts))
        await self.redis.expire("risk_clusters:last_run",    60 * 60 * 24 * 8)
        await self.redis.expire("risk_clusters:tier_counts", 60 * 60 * 24 * 8)

        log.info("risk-clustering.run_complete",
                 tickers=len(valid_tickers), silhouette=silhouette, tier_counts=tier_counts)

    # ── Feature fetching ──────────────────────────────────────────────────────

    async def _fetch_features(self, dc: DataClient, ticker: str) -> dict:
        feat: dict = {"ticker": ticker, "_ok": False}
        try:
            bars_data = await dc.bars(ticker, days=91)
            closes = _extract_closes(bars_data)
            if len(closes) >= 10:
                log_ret = [np.log(closes[i] / closes[i - 1]) for i in range(1, len(closes))]
                feat["volatility"]    = float(np.std(log_ret) * np.sqrt(252))
                feat["price_change"]  = float((closes[-1] - closes[0]) / closes[0] * 100)
        except Exception as e:
            log.debug(f"risk-clustering.bars_err ticker={ticker} {e}")

        try:
            fund = await dc.fundamentals(ticker)
            if fund:
                feat["beta"]           = _flt(fund.get("beta"))
                feat["pe_ratio"]       = _flt(fund.get("pe_ratio"))
                feat["pb_ratio"]       = _flt(fund.get("pb_ratio"))
                feat["roe"]            = _flt(fund.get("roe"))
                feat["roa"]            = _flt(fund.get("roa"))
                feat["fcf_yield"]      = _flt(fund.get("free_cashflow_yield"))
                feat["earnings_yield"] = _flt(fund.get("earnings_yield"))
        except Exception as e:
            log.debug(f"risk-clustering.fund_err ticker={ticker} {e}")

        # Require at least volatility + one fundamental to count
        has_price  = feat.get("volatility") is not None
        has_fund   = any(feat.get(f) is not None for f in ("beta", "roe", "pe_ratio"))
        feat["_ok"] = has_price or has_fund
        return feat

    # ── K-Means ───────────────────────────────────────────────────────────────

    def _cluster(self, features_map: dict):
        from sklearn.cluster  import KMeans
        from sklearn.metrics  import silhouette_score

        valid = list(features_map.keys())
        X_raw = np.array(
            [[features_map[t].get(f) for f in _FEATURE_NAMES] for t in valid],
            dtype=float,
        )

        # Median imputation for NaN
        for j in range(X_raw.shape[1]):
            col    = X_raw[:, j]
            median = np.nanmedian(col)
            if np.isnan(median):
                median = 0.0
            col[np.isnan(col)] = median
            X_raw[:, j] = col

        # Z-score normalisation
        means = X_raw.mean(axis=0)
        stds  = X_raw.std(axis=0)
        stds[stds == 0] = 1.0
        X_scaled = (X_raw - means) / stds

        try:
            km     = KMeans(n_clusters=N_CLUSTERS, random_state=42, n_init=10)
            labels = km.fit_predict(X_scaled)
        except Exception as e:
            log.error("risk-clustering.kmeans_failed", error=str(e))
            return None, None, None, None, None

        try:
            sil = float(silhouette_score(X_scaled, labels))
        except Exception:
            sil = None

        # Map cluster_id → tier by ascending mean volatility (index 0)
        cluster_vols = {k: X_raw[labels == k, 0].mean() for k in range(N_CLUSTERS)}
        sorted_by_vol = sorted(cluster_vols, key=lambda k: cluster_vols[k])
        cluster_to_tier = {c: _RISK_TIERS[i] for i, c in enumerate(sorted_by_vol)}

        return labels, cluster_to_tier, sil, X_raw, valid

    # ── DB write ──────────────────────────────────────────────────────────────

    async def _store(self, run_date, tickers, features_map, labels,
                     cluster_to_tier, silhouette, X_raw):
        if not self._db:
            log.warning("risk-clustering.no_db_skip")
            return
        try:
            async with self._db.acquire() as conn:
                await conn.execute(
                    """INSERT INTO stock_cluster_runs
                           (run_date, n_tickers, n_clusters, features, silhouette)
                       VALUES ($1,$2,$3,$4,$5)
                       ON CONFLICT (run_date) DO UPDATE SET
                           n_tickers=$2, n_clusters=$3, features=$4,
                           silhouette=$5, created_at=NOW()""",
                    run_date, len(tickers), N_CLUSTERS,
                    _FEATURE_NAMES, silhouette,
                )
                for i, ticker in enumerate(tickers):
                    feat = features_map[ticker]
                    cid  = int(labels[i])
                    tier = cluster_to_tier[cid]
                    feat_clean = {k: v for k, v in feat.items()
                                  if k not in ("ticker", "_ok") and v is not None}
                    await conn.execute(
                        """INSERT INTO stock_risk_clusters
                               (run_date, ticker, cluster_id, risk_tier,
                                volatility, price_change, beta, pe_ratio, pb_ratio,
                                roe, roa, fcf_yield, earnings_yield, features)
                           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14::JSONB)
                           ON CONFLICT (run_date, ticker) DO UPDATE SET
                               cluster_id=$3, risk_tier=$4,
                               volatility=$5, price_change=$6, beta=$7,
                               pe_ratio=$8, pb_ratio=$9, roe=$10, roa=$11,
                               fcf_yield=$12, earnings_yield=$13, features=$14::JSONB,
                               created_at=NOW()""",
                        run_date, ticker, cid, tier,
                        feat.get("volatility"),    feat.get("price_change"),
                        feat.get("beta"),          feat.get("pe_ratio"),
                        feat.get("pb_ratio"),      feat.get("roe"),
                        feat.get("roa"),           feat.get("fcf_yield"),
                        feat.get("earnings_yield"), json.dumps(feat_clean),
                    )
            log.info("risk-clustering.db_stored", n=len(tickers), run_date=run_date.isoformat())
        except Exception as e:
            log.error("risk-clustering.db_error", error=str(e))


# ── Helpers ───────────────────────────────────────────────────────────────────

def _flt(v) -> float | None:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _extract_closes(bars_data) -> list[float]:
    if not bars_data:
        return []
    items = (
        bars_data.get("bars")
        or bars_data.get("ohlcv")
        or bars_data.get("candles")
        or (bars_data if isinstance(bars_data, list) else [])
    )
    closes = []
    for b in items:
        c = b.get("close") or b.get("c")
        if c is not None:
            try:
                closes.append(float(c))
            except (TypeError, ValueError):
                pass
    return closes


# ── Entrypoint ────────────────────────────────────────────────────────────────

async def main():
    logging.basicConfig(level=logging.INFO)
    agent = RiskClusteringAgent()
    await agent.start()


if __name__ == "__main__":
    asyncio.run(main())
