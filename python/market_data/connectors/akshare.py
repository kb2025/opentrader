"""
AkShare Connector for the Market Data Gateway.

AkShare is a Python library that provides access to Chinese (A-share, Hong Kong)
equity data, futures, bonds, macro data, and more. It requires the 'akshare'
package to be installed; the connector gracefully degrades if it is absent.

No API key required — AkShare wraps public data sources.

Capabilities: asia_equity, ohlcv_asia, hk_equity
"""
import asyncio
import re

import structlog

from .base import HTTPConnector, ConnectorError

log = structlog.get_logger("connector.akshare")


def _is_hk_ticker(ticker: str) -> bool:
    """Return True for HK-listed tickers (ends in .HK or is a numeric code like 00700)."""
    ticker = ticker.upper()
    if ticker.endswith(".HK"):
        return True
    # Pure numeric codes 5 digits — common HK format (e.g. 00700 for Tencent)
    if re.fullmatch(r'\d{4,6}', ticker):
        return True
    return False


def _normalize_ticker(ticker: str) -> str:
    """Strip .HK suffix for use in akshare calls that expect the bare code."""
    return ticker.upper().replace(".HK", "")


def _normalize_date(d: str) -> str:
    """Convert YYYYMMDD or YYYY-MM-DD to YYYYMMDD (akshare's expected format)."""
    return d.replace("-", "")


class AkShareConnector(HTTPConnector):
    name = "akshare"
    cost_tier = "free"
    env_key = None   # No API key — uses the akshare Python library
    CAPABILITIES = frozenset({"asia_equity", "ohlcv_asia", "hk_equity"})

    def __init__(self):
        super().__init__()

    async def probe(self) -> set[str]:
        """Return capabilities if akshare is importable, else empty set."""
        try:
            import akshare  # noqa: F401
            return set(self.CAPABILITIES)
        except ImportError:
            log.warning("akshare.not_installed")
            return set()

    async def call(self, data_type: str, params: dict) -> dict:
        """
        Supported data_type values:
          "ohlcv_asia"  — daily OHLCV for HK or CN A-share (params: ticker, start_date, end_date)
          "hk_equity"   — alias for ohlcv_asia for HK stocks
          "asia_equity" — latest spot quote (params: ticker)
        """
        try:
            import akshare as ak
        except ImportError as exc:
            raise ConnectorError("akshare not installed") from exc

        loop = asyncio.get_event_loop()

        try:
            if data_type in ("ohlcv_asia", "hk_equity"):
                return await self._ohlcv(loop, ak, params)
            if data_type == "asia_equity":
                return await self._spot_quote(loop, ak, params)
        except ConnectorError:
            raise
        except Exception as e:
            raise ConnectorError(f"akshare: {e}") from e

        raise ConnectorError(f"akshare: unsupported data_type {data_type!r}")

    async def _ohlcv(self, loop: asyncio.AbstractEventLoop, ak, params: dict) -> dict:
        """Fetch daily OHLCV bars for a HK or CN A-share ticker."""
        ticker     = params.get("ticker", "")
        start_date = _normalize_date(params.get("start_date", "20240101"))
        end_date   = _normalize_date(params.get("end_date", "20991231"))

        if not ticker:
            raise ConnectorError("akshare: 'ticker' param required for ohlcv_asia")

        if _is_hk_ticker(ticker):
            symbol = _normalize_ticker(ticker)
            df = await loop.run_in_executor(
                None,
                lambda: ak.stock_hk_hist(
                    symbol=symbol,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date,
                    adjust="qfq",
                ),
            )
        else:
            df = await loop.run_in_executor(
                None,
                lambda: ak.stock_zh_a_hist(
                    symbol=ticker,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date,
                    adjust="qfq",
                ),
            )

        if df is None or df.empty:
            raise ConnectorError(f"akshare: no data returned for {ticker}")

        bars: list[dict] = []
        for _, row in df.iterrows():
            try:
                # AkShare column names vary between HK and CN endpoints but
                # common columns: 日期/date, 开盘/open, 最高/high, 最低/low, 收盘/close, 成交量/volume
                date_val   = str(row.get("日期") or row.get("date") or row.get("Date") or "")
                open_val   = float(row.get("开盘") or row.get("open") or row.get("Open") or 0)
                high_val   = float(row.get("最高") or row.get("high") or row.get("High") or 0)
                low_val    = float(row.get("最低") or row.get("low") or row.get("Low") or 0)
                close_val  = float(row.get("收盘") or row.get("close") or row.get("Close") or 0)
                volume_val = float(row.get("成交量") or row.get("volume") or row.get("Volume") or 0)
                bars.append({
                    "date":   str(date_val)[:10],
                    "open":   open_val,
                    "high":   high_val,
                    "low":    low_val,
                    "close":  close_val,
                    "volume": volume_val,
                })
            except Exception as row_err:
                log.debug("akshare.row_parse_error", ticker=ticker, error=str(row_err))
                continue

        return {"ticker": ticker, "bars": bars}

    async def _spot_quote(self, loop: asyncio.AbstractEventLoop, ak, params: dict) -> dict:
        """Fetch latest spot price for a HK or CN A-share ticker."""
        ticker = params.get("ticker", "")
        if not ticker:
            raise ConnectorError("akshare: 'ticker' param required for asia_equity")

        if _is_hk_ticker(ticker):
            symbol = _normalize_ticker(ticker)
            df = await loop.run_in_executor(None, lambda: ak.stock_hk_spot_em())
            if df is None or df.empty:
                raise ConnectorError("akshare: empty HK spot data")
            # Filter for this symbol — column is typically '代码' (code)
            code_col = next((c for c in df.columns if "代码" in c or "code" in c.lower()), None)
            if code_col:
                row = df[df[code_col].astype(str) == symbol]
                if not row.empty:
                    price_col = next((c for c in df.columns if "最新" in c or "price" in c.lower() or "close" in c.lower()), None)
                    price = float(row.iloc[0][price_col]) if price_col else None
                    return {"ticker": ticker, "price": price, "market": "HK"}
            raise ConnectorError(f"akshare: ticker {ticker} not found in HK spot data")
        else:
            df = await loop.run_in_executor(None, lambda: ak.stock_zh_a_spot_em())
            if df is None or df.empty:
                raise ConnectorError("akshare: empty CN A-share spot data")
            code_col  = next((c for c in df.columns if "代码" in c or "code" in c.lower()), None)
            price_col = next((c for c in df.columns if "最新" in c or "price" in c.lower() or "close" in c.lower()), None)
            if code_col:
                row = df[df[code_col].astype(str) == ticker]
                if not row.empty:
                    price = float(row.iloc[0][price_col]) if price_col else None
                    return {"ticker": ticker, "price": price, "market": "CN"}
            raise ConnectorError(f"akshare: ticker {ticker} not found in CN spot data")
