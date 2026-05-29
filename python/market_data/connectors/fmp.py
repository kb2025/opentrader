import os
import aiohttp
from .base import HTTPConnector, ConnectorError

FMP_BASE = "https://financialmodelingprep.com/api/v3"


class FMPConnector(HTTPConnector):
    name = "fmp"
    cost_tier = "free"
    env_key = "FMP_API_KEY"
    rate_limit_per_min = 5   # free tier: 250 calls/day; be conservative per-minute
    CAPABILITIES = frozenset({
        "quote", "bars_daily", "fundamentals", "analyst_consensus",
        "earnings", "dividends", "news",
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
                if data_type == "earnings":
                    return await self._earnings(session, params)
                if data_type == "dividends":
                    return await self._dividends(session, params)
                if data_type == "news":
                    return await self._news(session, params)
        except ConnectorError:
            raise
        except Exception as e:
            raise ConnectorError(f"fmp: {e}") from e
        raise ConnectorError(f"fmp: unsupported data_type {data_type!r}")

    async def _quote(self, session, params: dict) -> dict:
        ticker = params.get("ticker", "")
        data = await self._rate_limited_get(session, f"{FMP_BASE}/quote/{ticker}", params=self._p())
        q = data[0] if isinstance(data, list) and data else {}
        return {
            "ticker":     ticker,
            "last":       q.get("price"),
            "open":       q.get("open"),
            "high":       q.get("dayHigh"),
            "low":        q.get("dayLow"),
            "prev_close": q.get("previousClose"),
            "volume":     q.get("volume"),
            "change_pct": q.get("changesPercentage"),
            "market_cap": q.get("marketCap"),
            "pe":         q.get("pe"),
            "eps":        q.get("eps"),
        }

    async def _bars_daily(self, session, params: dict) -> dict:
        ticker = params.get("ticker", "")
        days   = int(params.get("days", 30))
        data   = await self._rate_limited_get(
            session, f"{FMP_BASE}/historical-price-full/{ticker}", params=self._p(),
        )
        historical = (data.get("historical") or []) if isinstance(data, dict) else []
        bars = [
            {
                "date":   b["date"],
                "open":   b.get("open"),
                "high":   b.get("high"),
                "low":    b.get("low"),
                "close":  b.get("close"),
                "volume": b.get("volume"),
            }
            for b in historical[:days]
        ]
        return {"ticker": ticker, "bars": bars}

    async def _fundamentals(self, session, params: dict) -> dict:
        ticker = params.get("ticker", "")
        profile_data = await self._rate_limited_get(
            session, f"{FMP_BASE}/profile/{ticker}", params=self._p(),
        )
        prof = profile_data[0] if isinstance(profile_data, list) and profile_data else {}
        # Key metrics TTM for ratios
        metrics_data = await self._rate_limited_get(
            session, f"{FMP_BASE}/key-metrics-ttm/{ticker}", params=self._p(),
        )
        met = metrics_data[0] if isinstance(metrics_data, list) and metrics_data else {}
        return {
            "ticker":         ticker,
            "name":           prof.get("companyName"),
            "sector":         prof.get("sector"),
            "industry":       prof.get("industry"),
            "market_cap":     prof.get("mktCap"),
            "country":        prof.get("country"),
            "currency":       prof.get("currency"),
            "exchange":       prof.get("exchangeShortName"),
            "description":    prof.get("description"),
            "beta":           prof.get("beta"),
            "pe_ratio":       met.get("peRatioTTM"),
            "pb_ratio":       met.get("pbRatioTTM"),
            "dividend_yield": met.get("dividendYieldPercentageTTM"),
            "roe":            met.get("roeTTM"),
            "debt_to_equity": met.get("debtToEquityTTM"),
            "free_cashflow_yield": met.get("freeCashFlowYieldTTM"),
            "ev_ebitda":      met.get("enterpriseValueOverEBITDATTM"),
        }

    async def _analyst(self, session, params: dict) -> dict:
        ticker = params.get("ticker", "")
        data = await self._rate_limited_get(session, f"{FMP_BASE}/rating/{ticker}", params=self._p())
        r = data[0] if isinstance(data, list) and data else {}
        return {
            "ticker":         ticker,
            "rating":         r.get("ratingRecommendation"),
            "score":          r.get("ratingScore"),
            "date":           r.get("date"),
            "dcf_score":      r.get("ratingDetailsDCFScore"),
            "roe_score":      r.get("ratingDetailsROEScore"),
            "roa_score":      r.get("ratingDetailsROAScore"),
            "de_score":       r.get("ratingDetailsDEScore"),
            "pe_score":       r.get("ratingDetailsPEScore"),
            "pb_score":       r.get("ratingDetailsPBScore"),
        }

    async def _earnings(self, session, params: dict) -> dict:
        ticker = params.get("ticker", "")
        data = await self._rate_limited_get(
            session, f"{FMP_BASE}/earnings-surprises/{ticker}", params=self._p(),
        )
        records = data if isinstance(data, list) else []
        return {
            "ticker": ticker,
            "earnings": [
                {
                    "date":     r.get("date"),
                    "actual":   r.get("actualEarningResult"),
                    "estimate": r.get("estimatedEarning"),
                    "surprise": round(
                        (r["actualEarningResult"] - r["estimatedEarning"]) / abs(r["estimatedEarning"]) * 100, 2
                    ) if r.get("estimatedEarning") else None,
                }
                for r in records[:8]
            ],
        }

    async def _dividends(self, session, params: dict) -> dict:
        ticker = params.get("ticker", "")
        data = await self._rate_limited_get(
            session, f"{FMP_BASE}/historical-price-full/stock_dividend/{ticker}", params=self._p(),
        )
        historical = (data.get("historical") or []) if isinstance(data, dict) else []
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
                }
                for d in historical[:params.get("limit", 20)]
            ],
        }

    async def _news(self, session, params: dict) -> dict:
        ticker = params.get("ticker", "")
        limit  = params.get("limit", 20)
        data = await self._rate_limited_get(
            session, f"{FMP_BASE}/stock_news",
            params=self._p({"tickers": ticker, "limit": limit}),
        )
        articles = data if isinstance(data, list) else []
        return {
            "ticker": ticker,
            "articles": [
                {
                    "headline":  a.get("title"),
                    "url":       a.get("url"),
                    "published": a.get("publishedDate"),
                    "source":    a.get("site"),
                    "summary":   a.get("text"),
                }
                for a in articles
            ],
        }
