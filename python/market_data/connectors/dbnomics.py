"""
DBnomics Connector for the Market Data Gateway.

DBnomics aggregates time-series from 80+ statistical providers (IMF, World Bank,
Eurostat, OECD, Federal Reserve, ECB, etc.) into a single API.
No API key required.

Capabilities: macro, dbnomics
"""
import aiohttp
import structlog

from .base import HTTPConnector, ConnectorError

log = structlog.get_logger("connector.dbnomics")

DBNOMICS_BASE = "https://api.db.nomics.world/v22"
TIMEOUT_S = 10


class DBnomicsConnector(HTTPConnector):
    name = "dbnomics"
    cost_tier = "free"
    env_key = None   # No API key required
    CAPABILITIES = frozenset({"macro", "dbnomics"})

    def __init__(self):
        super().__init__()

    async def probe(self) -> set[str]:
        """Verify the DBnomics API is reachable. Return capability set or empty on failure."""
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=5)
            ) as session:
                async with session.get(
                    f"{DBNOMICS_BASE}/providers",
                    params={"limit": 5},
                    headers={"Accept": "application/json"},
                ) as resp:
                    if resp.status == 200:
                        return set(self.CAPABILITIES)
                    log.warning("dbnomics.probe_failed", status=resp.status)
                    return set()
        except Exception as e:
            log.warning("dbnomics.probe_error", error=str(e))
            return set()

    async def call(self, data_type: str, params: dict) -> dict:
        """
        Supported data_type values:
          "dbnomics_search" — search for series across all providers
          "dbnomics_series" — fetch observations for a specific series
          "macro"           — alias for dbnomics_series when provider/dataset/series given
        """
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=TIMEOUT_S),
                headers={"Accept": "application/json"},
            ) as session:
                if data_type == "dbnomics_search":
                    return await self._search(session, params)
                if data_type in ("dbnomics_series", "macro"):
                    return await self._series(session, params)
        except ConnectorError:
            raise
        except Exception as e:
            raise ConnectorError(f"dbnomics: unexpected error — {e}") from e

        raise ConnectorError(f"dbnomics: unsupported data_type {data_type!r}")

    async def _search(self, session: aiohttp.ClientSession, params: dict) -> dict:
        """Search for series by keyword query."""
        query = params.get("query", "")
        if not query:
            raise ConnectorError("dbnomics: dbnomics_search requires 'query' param")
        limit = int(params.get("limit", 10))

        url = f"{DBNOMICS_BASE}/series"
        resp_data = await self._rate_limited_get(session, url, params={"q": query, "limit": limit})

        raw_series = resp_data.get("series", {})
        docs = raw_series.get("docs") or []

        series_list = [
            {
                "provider_code": doc.get("provider_code", ""),
                "dataset_code":  doc.get("dataset_code", ""),
                "series_code":   doc.get("series_code", ""),
                "name":          doc.get("series_name", "") or doc.get("name", ""),
                "description":   doc.get("dimensions_labels_flat", "") or "",
            }
            for doc in docs
        ]

        return {
            "query":  query,
            "total":  raw_series.get("num_found", len(series_list)),
            "series": series_list,
        }

    async def _series(self, session: aiohttp.ClientSession, params: dict) -> dict:
        """Fetch observations for a specific provider/dataset/series combination."""
        provider = params.get("provider", "")
        dataset  = params.get("dataset", "")
        series   = params.get("series", "")

        if not provider or not dataset or not series:
            raise ConnectorError(
                "dbnomics: dbnomics_series requires 'provider', 'dataset', and 'series' params"
            )

        url = f"{DBNOMICS_BASE}/series/{provider}/{dataset}/{series}"
        resp_data = await self._rate_limited_get(session, url, params={"observations": 1})

        # DBnomics response structure: {"series": {"docs": [{...}]}}
        series_docs = resp_data.get("series", {}).get("docs") or []
        if not series_docs:
            raise ConnectorError(f"dbnomics: no series found for {provider}/{dataset}/{series}")

        doc = series_docs[0]
        periods = doc.get("period", []) or []
        values  = doc.get("value", []) or []

        observations = []
        for period, value in zip(periods, values):
            observations.append({
                "period": period,
                "value":  None if value == "NA" else value,
            })

        return {
            "provider":     provider,
            "dataset":      dataset,
            "series":       series,
            "name":         doc.get("series_name", "") or doc.get("name", ""),
            "observations": observations,
        }
