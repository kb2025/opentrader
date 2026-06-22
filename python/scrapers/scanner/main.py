"""
Universe Scanner Agent

Scans a configurable universe of tickers against technical rules and publishes
matches to the scanner.alerts stream. Also writes results to a Redis sorted set
for fast lookup by the WebUI.

Trigger job : scrape_scanner
Schedule    : configurable (suggested: every 15 minutes during market hours)
"""
import asyncio
import json
import os
from datetime import date

import structlog

from scrapers.base import BaseScraper
from .scanner import run_scan

log = structlog.get_logger("scraper-scanner")

MARKET_DATA_URL  = os.getenv("MARKET_DATA_URL", "http://ot-market-data:8090")
SCAN_RULES_PATH  = os.getenv("SCAN_RULES_PATH", "/app/config/scan_rules.json")
SCANNER_ALERTS   = "scanner.alerts"   # same as STREAMS["scanner_alerts"]
REDIS_KEY_TTL    = 86_400             # 24 hours in seconds

DEFAULT_RULES = [
    {"field": "rsi_14",       "op": "lt", "value": 30},
    {"field": "volume_ratio", "op": "gt", "value": 2.0},
]

DEFAULT_UNIVERSE = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META",
    "NVDA", "TSLA", "JPM",  "BAC",  "GS",
    "XOM",  "CVX",  "JNJ",  "PFE",  "V",
    "MA",   "WMT",  "HD",   "SPY",  "QQQ",
]


def _load_rules() -> list[dict]:
    """Load scan rules from JSON file; fall back to defaults if missing/invalid."""
    try:
        with open(SCAN_RULES_PATH) as f:
            rules = json.load(f)
        if isinstance(rules, list) and rules:
            log.info("scanner.rules_loaded", path=SCAN_RULES_PATH, count=len(rules))
            return rules
        log.warning("scanner.rules_empty_or_invalid", path=SCAN_RULES_PATH)
    except FileNotFoundError:
        log.info("scanner.rules_file_missing", path=SCAN_RULES_PATH, fallback="defaults")
    except Exception as e:
        log.warning("scanner.rules_load_error", path=SCAN_RULES_PATH, error=str(e))
    return DEFAULT_RULES


def _load_universe() -> list[str]:
    """Load ticker universe from SCAN_UNIVERSE env var or use defaults."""
    raw = os.getenv("SCAN_UNIVERSE", "")
    if raw.strip():
        tickers = [t.strip().upper() for t in raw.split(",") if t.strip()]
        if tickers:
            log.info("scanner.universe_from_env", count=len(tickers))
            return tickers
    return DEFAULT_UNIVERSE


class ScannerAgent(BaseScraper):
    SOURCE      = "scanner"
    TRIGGER_JOB = "scrape_scanner"
    GROUP_KEY   = "scraper-scanner"

    def __init__(self):
        super().__init__("scraper-scanner")
        self._rules:    list[dict] = DEFAULT_RULES
        self._universe: list[str]  = DEFAULT_UNIVERSE

    async def _on_start(self):
        self._rules    = _load_rules()
        self._universe = _load_universe()
        log.info(
            "scanner.ready",
            universe=len(self._universe),
            rules=len(self._rules),
            market_data_url=MARKET_DATA_URL,
        )

    async def scrape(self):
        log.info("scanner.scan_start", universe=len(self._universe), rules=len(self._rules))

        matches = await run_scan(self._universe, self._rules, MARKET_DATA_URL)

        if not matches:
            log.info("scanner.no_matches")
            return

        today_key = f"scanner:alerts:{date.today().isoformat()}"
        published = 0

        for match in matches:
            ticker       = match["ticker"]
            matched_rules = match["matched_rules"]
            field_values  = match["field_values"]
            ts_utc        = match["ts_utc"]

            payload = {
                "source":        "scanner",
                "ticker":        ticker,
                "matched_rules": json.dumps(matched_rules),
                "field_values":  json.dumps({k: str(v) for k, v in field_values.items()}),
                "ts_utc":        str(ts_utc),
            }

            # Publish to scanner.alerts stream (NOT market.ticks)
            try:
                await self.redis.xadd(
                    SCANNER_ALERTS,
                    payload,
                    maxlen=10_000,
                )
                published += 1
            except Exception as e:
                log.warning("scanner.publish_error", ticker=ticker, error=str(e))

            # Also write to Redis sorted set for fast WebUI lookup
            try:
                alert_json = json.dumps({
                    "ticker":        ticker,
                    "matched_rules": matched_rules,
                    "field_values":  {k: str(v) for k, v in field_values.items()},
                })
                await self.redis.zadd(today_key, {alert_json: float(ts_utc)})
                await self.redis.expire(today_key, REDIS_KEY_TTL)
            except Exception as e:
                log.warning("scanner.redis_zadd_error", ticker=ticker, error=str(e))

        log.info("scanner.done", matches=len(matches), published=published)


async def main():
    agent = ScannerAgent()
    try:
        await agent.run()
    finally:
        await agent.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
