"""
FRED (Federal Reserve Economic Data) service connector.

Wraps the FRED JSON API with rate limiting, exponential-backoff retries,
and typed convenience methods. All endpoints require an API key.

Usage:
    client = FREDClient(api_key)
    value  = await client.latest("BAMLH0A0HYM2")
    rows   = await client.history("DGS10", n=252)
    info   = await client.series_info("UNRATE")

Module-level singleton (reads FRED_API_KEY env var):
    client = get_fred_client()   # returns None if key not set
"""
import asyncio
import logging
import os
from typing import Optional

import aiohttp

log = logging.getLogger("shared.fred_client")

_BASE         = "https://api.stlouisfed.org/fred"
_MAX_RETRIES  = 3
_RATE_LIMIT   = 0.5   # seconds between calls — FRED allows 120 req/min


class FREDClient:
    """
    Async FRED API client.

    Pre-defined series IDs are available as ``FREDClient.SERIES`` — a flat
    dict of short-name → FRED series ID covering credit, rates, inflation,
    employment, and yield-curve series.
    """

    # ── Pre-defined series IDs ────────────────────────────────────────────────
    SERIES: dict[str, str] = {
        # Credit spreads
        "hy_oas":          "BAMLH0A0HYM2",   # US HY Option-Adjusted Spread (bps, daily)
        "ig_oas":          "BAMLC0A0CM",      # US IG Option-Adjusted Spread (bps, daily)
        "bb_oas":          "BAMLH0A1HYBBM2",  # BB-rated HY spread (bps, daily)
        # Financial stress
        "fsi":             "STLFSI2",         # St. Louis Financial Stress Index (weekly)
        "nfci":            "NFCI",            # Chicago Fed NFCI (weekly)
        # Recession / cycle
        "usrec":           "USREC",           # NBER Recession Indicator 0/1 (monthly)
        "lei":             "USSLIND",         # Leading Economic Index (monthly)
        # Policy rates
        "fed_funds":       "FEDFUNDS",        # Effective Fed Funds Rate (monthly)
        "sofr":            "SOFR",            # Secured Overnight Financing Rate (daily)
        # Treasury yields
        "t1m":             "DGS1MO",          # 1-month CMT
        "t3m":             "DGS3MO",          # 3-month CMT
        "t6m":             "DGS6MO",          # 6-month CMT
        "t1y":             "DGS1",            # 1-year CMT
        "t2y":             "DGS2",            # 2-year CMT
        "t5y":             "DGS5",            # 5-year CMT
        "t10y":            "DGS10",           # 10-year CMT
        "t20y":            "DGS20",           # 20-year CMT
        "t30y":            "DGS30",           # 30-year CMT
        "spread_2s10s":    "T10Y2Y",          # 10Y-2Y spread (inverted = recession watch)
        "spread_3m10y":    "T10Y3M",          # 10Y-3M spread
        # TIPS / breakeven inflation
        "tips5":           "DFII5",           # 5Y real yield
        "tips10":          "DFII10",          # 10Y real yield
        "breakeven5":      "T5YIE",           # 5Y breakeven inflation
        "breakeven10":     "T10YIE",          # 10Y breakeven inflation
        # Inflation
        "cpi":             "CPIAUCSL",        # CPI All Items (monthly)
        "core_cpi":        "CPILFESL",        # Core CPI ex food/energy (monthly)
        "pce":             "PCEPI",           # PCE Price Index (monthly)
        "core_pce":        "PCEPILFE",        # Core PCE (monthly, Fed's preferred)
        "ppi":             "PPIACO",          # PPI All Commodities (monthly)
        # Employment
        "unemployment":    "UNRATE",          # Unemployment Rate (monthly)
        "payrolls":        "PAYEMS",          # Nonfarm Payrolls (monthly, thousands)
        "jobless_claims":  "ICSA",            # Initial Jobless Claims (weekly)
        "cont_claims":     "CCSA",            # Continued Jobless Claims (weekly)
        # Money supply
        "m2":              "M2SL",            # M2 Money Supply (weekly, billions)
        # Housing
        "case_shiller":    "CSUSHPISA",       # Case-Shiller National HPI (monthly)
        "housing_starts":  "HOUST",           # Housing Starts (monthly)
    }

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("FREDClient requires a non-empty api_key")
        self._api_key  = api_key
        self._lock     = asyncio.Lock()
        self._last_req = 0.0

    # ── Internal helpers ──────────────────────────────────────────────────────

    async def _rate_limit(self):
        async with self._lock:
            loop = asyncio.get_event_loop()
            now  = loop.time()
            wait = _RATE_LIMIT - (now - self._last_req)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_req = loop.time()

    async def _get(self, endpoint: str, params: Optional[dict] = None) -> dict:
        """GET /{endpoint} with auth, rate limit, and exponential-backoff retries."""
        await self._rate_limit()
        url = f"{_BASE}/{endpoint}"
        base_params = {"api_key": self._api_key, "file_type": "json"}
        if params:
            base_params.update(params)

        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        url, params=base_params,
                        timeout=aiohttp.ClientTimeout(total=15),
                    ) as resp:
                        if resp.status == 200:
                            return await resp.json(content_type=None)
                        if resp.status == 429:
                            wait = 2 ** attempt
                            log.warning("fred_client.rate_limited", attempt=attempt, wait=wait)
                            await asyncio.sleep(wait)
                        elif resp.status in (400, 404):
                            body = await resp.text()
                            raise ValueError(f"FRED API error {resp.status}: {body[:200]}")
                        else:
                            log.warning("fred_client.http_error",
                                        status=resp.status, endpoint=endpoint, attempt=attempt)
                            await asyncio.sleep(2 ** attempt)
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                log.warning("fred_client.network_error", attempt=attempt, error=str(e))
                await asyncio.sleep(2 ** attempt)

        raise RuntimeError(f"FRED request failed after {_MAX_RETRIES} attempts: {endpoint}")

    @staticmethod
    def _parse_observations(raw: list[dict]) -> list[tuple[str, float]]:
        """Convert raw observation dicts to (date_str, float) tuples, dropping missing."""
        out: list[tuple[str, float]] = []
        for obs in raw:
            val = obs.get("value", ".")
            if val in (".", ""):
                continue
            try:
                out.append((obs["date"], float(val)))
            except (ValueError, KeyError):
                pass
        return out

    # ── Public API ────────────────────────────────────────────────────────────

    async def latest(self, series_id: str, lookback_days: int = 30) -> float | None:
        """
        Return the most recent non-null value for *series_id*.
        Fetches up to *lookback_days* most recent observations (handles weekends
        and monthly series that may not have updated yet).
        """
        data = await self._get("series/observations", {
            "series_id":  series_id,
            "sort_order": "desc",
            "limit":      str(lookback_days),
        })
        pairs = self._parse_observations(data.get("observations", []))
        return pairs[0][1] if pairs else None

    async def history(
        self,
        series_id:  str,
        n:          int = 252,
        start_date: Optional[str] = None,
        end_date:   Optional[str] = None,
    ) -> list[tuple[str, float]]:
        """
        Return up to *n* most recent observations as a list of ``(date, value)``
        tuples sorted ascending (oldest first).

        Args:
            series_id:  FRED series ID.
            n:          Max number of data points to return.
            start_date: ISO date string ``YYYY-MM-DD`` (optional lower bound).
            end_date:   ISO date string ``YYYY-MM-DD`` (optional upper bound).
        """
        params: dict = {
            "series_id":  series_id,
            "sort_order": "desc",
            "limit":      str(n + 5),   # slight over-fetch to handle missing values
        }
        if start_date:
            params["observation_start"] = start_date
        if end_date:
            params["observation_end"] = end_date
        data  = await self._get("series/observations", params)
        pairs = self._parse_observations(data.get("observations", []))
        # API returned desc; reverse to asc and cap at n
        return list(reversed(pairs))[-n:]

    async def series_info(self, series_id: str) -> dict:
        """
        Return metadata for *series_id*: title, frequency, units,
        seasonal_adjustment, observation_start/end, popularity.
        """
        data = await self._get("series", {"series_id": series_id})
        raw  = data.get("seriess", [{}])[0]
        return {
            "id":                    raw.get("id"),
            "title":                 raw.get("title"),
            "frequency":             raw.get("frequency_short"),
            "units":                 raw.get("units_short"),
            "seasonal_adjustment":   raw.get("seasonal_adjustment_short"),
            "observation_start":     raw.get("observation_start"),
            "observation_end":       raw.get("observation_end"),
            "last_updated":          raw.get("last_updated"),
            "popularity":            raw.get("popularity"),
            "notes":                 raw.get("notes"),
        }

    async def search(
        self,
        query:    str,
        limit:    int = 10,
        order_by: str = "popularity",
    ) -> list[dict]:
        """
        Search for series matching *query*.
        Returns list of dicts with ``id``, ``title``, ``frequency``, ``units``, ``popularity``.
        """
        data = await self._get("series/search", {
            "search_text": query,
            "limit":       str(limit),
            "order_by":    order_by,
            "sort_order":  "desc",
        })
        return [
            {
                "id":          s.get("id"),
                "title":       s.get("title"),
                "frequency":   s.get("frequency_short"),
                "units":       s.get("units_short"),
                "popularity":  s.get("popularity"),
            }
            for s in data.get("seriess", [])
        ]

    async def release_dates(self, series_id: str, upcoming: bool = True) -> list[str]:
        """
        Return recent or upcoming release dates for the release that publishes *series_id*.
        Dates are ISO strings (``YYYY-MM-DD``).
        """
        # Step 1: resolve series → release
        rel_data   = await self._get("series/release", {"series_id": series_id})
        releases   = rel_data.get("releases", [])
        if not releases:
            return []
        release_id = releases[0]["id"]

        # Step 2: fetch release dates
        params: dict = {
            "release_id":         str(release_id),
            "include_release_dates_with_no_data": "true",
        }
        if upcoming:
            from datetime import date
            params["realtime_start"] = date.today().isoformat()
        dates_data = await self._get("release/dates", params)
        return [d["date"] for d in dates_data.get("release_dates", [])][:20]

    async def bulk_latest(self, *series_ids: str) -> dict[str, float | None]:
        """
        Fetch the latest value for multiple series in parallel.
        Returns ``{series_id: value}`` mapping.
        """
        values = await asyncio.gather(
            *[self.latest(sid) for sid in series_ids],
            return_exceptions=True,
        )
        return {
            sid: (v if isinstance(v, (float, int, type(None))) else None)
            for sid, v in zip(series_ids, values)
        }

    async def macro_snapshot(self) -> dict:
        """
        Convenience: fetch all high-signal macro indicators in one call.
        Returns a structured dict used by the regime classifier.
        """
        ids = {
            "hy_oas":   self.SERIES["hy_oas"],
            "ig_oas":   self.SERIES["ig_oas"],
            "fsi":      self.SERIES["fsi"],
            "usrec":    self.SERIES["usrec"],
            "fed_funds": self.SERIES["fed_funds"],
            "spread_2s10s": self.SERIES["spread_2s10s"],
            "breakeven10":  self.SERIES["breakeven10"],
            "unemployment": self.SERIES["unemployment"],
            "jobless_claims": self.SERIES["jobless_claims"],
        }
        values = await asyncio.gather(
            *[self.latest(v) for v in ids.values()],
            return_exceptions=True,
        )
        result: dict[str, float | None] = {}
        for key, val in zip(ids.keys(), values):
            result[key] = val if isinstance(val, (float, int, type(None))) else None
        return result


# ── Module-level singleton ────────────────────────────────────────────────────

_singleton: FREDClient | None = None


def get_fred_client() -> FREDClient | None:
    """
    Return the module-level FREDClient singleton, or None if FRED_API_KEY is not set.
    Safe to call from any async context — instance is created once and reused.
    """
    global _singleton
    api_key = os.getenv("FRED_API_KEY", "")
    if not api_key:
        return None
    if _singleton is None:
        _singleton = FREDClient(api_key)
    return _singleton
