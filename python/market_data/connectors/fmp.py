"""
Financial Modeling Prep connector — uses the current /stable/ API (v3 is legacy).
Capabilities verified against the free-tier plan as of 2026-05.

Available:  quote, bars_daily, fundamentals, analyst_consensus, dividends
Restricted: news (requires paid plan), earnings (empty on free tier)
"""
import os
import aiohttp
from .base import HTTPConnector, ConnectorError

FMP_BASE = "https://financialmodelingprep.com/stable"


class FMPConnector(HTTPConnector):
    name = "fmp"
    cost_tier = "free"
    env_key = "FMP_API_KEY"
    rate_limit_per_min = 5   # free tier: 250 calls/day — conservative per-minute guard
    CAPABILITIES = frozenset({
        "quote", "bars_daily", "fundamentals", "analyst_consensus", "dividends",
    })

    def __init__(self):
        super().__init__()
        self._key = os.getenv("FMP_API_KEY", "")

    def _p(self, extra: dict = None) -> dict:
        p = {"apikey": self._key}
        if extra:
            p.update(extra)
        return p

    async def call(self, data_type: str, params: dict) -> dict:
        if not self._key:
            raise ConnectorError("fmp: FMP_API_KEY not set")
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
                if data_type == "quote":
                    return await self._quote(session, params)
                if data_type == "bars_daily":
                    return await self._bars_daily(session, params)
                if data_type == "fundamentals":
                    return await self._fundamentals(session, params)
                if data_type == "analyst_consensus":
                    return await self._analyst(session, params)
                if data_type == "dividends":
                    return await self._dividends(session, params)
        except ConnectorError:
            raise
        except Exception as e:
            raise ConnectorError(f"fmp: {e}") from e
        raise ConnectorError(f"fmp: unsupported data_type {data_type!r}")

    async def _quote(self, session, params: dict) -> dict:
        ticker = params.get("ticker", "")
        data = await self._rate_limited_get(
            session, f"{FMP_BASE}/quote", params=self._p({"symbol": ticker}),
        )
        q = data[0] if isinstance(data, list) and data else {}
        return {
            "ticker":     ticker,
            "last":       q.get("price"),
            "open":       q.get("open"),
            "high":       q.get("dayHigh"),
            "low":        q.get("dayLow"),
            "prev_close": q.get("previousClose"),
            "volume":     q.get("volume"),
            "change_pct": q.get("changePercentage"),
            "market_cap": q.get("marketCap"),
            "pe":         q.get("pe"),
            "eps":        q.get("eps"),
        }

    async def _bars_daily(self, session, params: dict) -> dict:
        ticker = params.get("ticker", "")
        days   = int(params.get("days", 30))
        data   = await self._rate_limited_get(
            session, f"{FMP_BASE}/historical-price-eod/full",
            params=self._p({"symbol": ticker}),
        )
        # /stable/historical-price-eod/full returns a flat array of bar objects
        bars_raw = data if isinstance(data, list) else (data.get("historical") or [])
        bars = [
            {
                "date":   b.get("date"),
                "open":   b.get("open"),
                "high":   b.get("high"),
                "low":    b.get("low"),
                "close":  b.get("close"),
                "volume": b.get("volume"),
            }
            for b in bars_raw[:days]
        ]
        return {"ticker": ticker, "bars": bars}

    async def _fundamentals(self, session, params: dict) -> dict:
        ticker = params.get("ticker", "")
        # Profile gives company info; key-metrics gives valuation ratios
        profile_data = await self._rate_limited_get(
            session, f"{FMP_BASE}/profile", params=self._p({"symbol": ticker}),
        )
        prof = profile_data[0] if isinstance(profile_data, list) and profile_data else {}
        metrics_data = await self._rate_limited_get(
            session, f"{FMP_BASE}/key-metrics", params=self._p({"symbol": ticker}),
        )
        met = metrics_data[0] if isinstance(metrics_data, list) and metrics_data else {}
        return {
            "ticker":         ticker,
            "name":           prof.get("companyName"),
            "sector":         prof.get("sector"),
            "industry":       prof.get("industry"),
            "market_cap":     prof.get("marketCap"),
            "country":        prof.get("country"),
            "currency":       prof.get("currency"),
            "exchange":       prof.get("exchange"),
            "description":    prof.get("description"),
            "beta":           prof.get("beta"),
            "pe_ratio":       met.get("peRatio"),
            "pb_ratio":       met.get("priceToBook"),
            "ev_ebitda":      met.get("evToEBITDA"),
            "roe":            met.get("returnOnEquity"),
            "roa":            met.get("returnOnAssets"),
            "roic":           met.get("returnOnInvestedCapital"),
            "free_cashflow_yield": met.get("freeCashFlowYield"),
            "earnings_yield": met.get("earningsYield"),
        }

    async def _analyst(self, session, params: dict) -> dict:
        ticker = params.get("ticker", "")
        data = await self._rate_limited_get(
            session, f"{FMP_BASE}/ratings-snapshot", params=self._p({"symbol": ticker}),
        )
        r = data[0] if isinstance(data, list) and data else {}
        return {
            "ticker":     ticker,
            "rating":     r.get("rating"),
            "score":      r.get("overallScore"),
            "dcf_score":  r.get("discountedCashFlowScore"),
            "roe_score":  r.get("returnOnEquityScore"),
            "roa_score":  r.get("returnOnAssetsScore"),
            "de_score":   r.get("debtToEquityScore"),
            "pe_score":   r.get("priceToEarningsScore"),
            "pb_score":   r.get("priceToBookScore"),
        }

    async def _dividends(self, session, params: dict) -> dict:
        ticker = params.get("ticker", "")
        data = await self._rate_limited_get(
            session, f"{FMP_BASE}/dividends", params=self._p({"symbol": ticker}),
        )
        records = data if isinstance(data, list) else []
        return {
            "ticker": ticker,
            "dividends": [
                {
                    "date":             d.get("date"),
                    "dividend":         d.get("dividend"),
                    "adj_dividend":     d.get("adjDividend"),
                    "record_date":      d.get("recordDate"),
                    "payment_date":     d.get("paymentDate"),
                    "declaration_date": d.get("declarationDate"),
                    "frequency":        d.get("frequency"),
                }
                for d in records[:params.get("limit", 20)]
            ],
        }
