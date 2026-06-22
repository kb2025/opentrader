"""
M&A Deals Scraper
Fetches recent merger and acquisition filings from SEC EDGAR full-text search.
Parses ATOM/XML feed for SC TO-T, S-4, and 8-K filings mentioning "merger agreement".
No API key required — uses SEC EDGAR public API with required User-Agent header.
"""
import re
import time
import xml.etree.ElementTree as ET
from datetime import date, timedelta

import aiohttp
import structlog

log = structlog.get_logger("scraper.ma_deals")

EDGAR_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"
TIMEOUT_S = 15
HEADERS = {
    "User-Agent": "OpenTrader research@opentrader.local",
    "Accept": "application/atom+xml, application/xml, text/xml, */*",
}

# Regex to find potential stock tickers (1-5 uppercase letters as standalone words)
# Excludes common English words and acronyms that appear in SEC filings
TICKER_RE = re.compile(r'\b([A-Z]{1,5})\b')

# Common non-ticker words that appear in SEC filing titles (expanded blocklist)
TICKER_BLOCKLIST = {
    "THE", "AND", "FOR", "INC", "CORP", "LLC", "LTD", "CO", "GROUP", "HOLDINGS",
    "SEC", "NYSE", "NASDAQ", "FORM", "FILED", "REPORT", "MERGER", "AGREEMENT",
    "WITH", "BY", "OF", "IN", "ON", "TO", "FROM", "AT", "AS", "IS", "OR",
    "AN", "A", "BE", "ARE", "HAS", "HAD", "NOT", "THIS", "THAT", "WILL",
    "USA", "US", "UK", "EU", "LP", "GP", "PLLC", "PLC", "NV", "SA", "AG",
    "SC", "TO", "SB", "FK", "DE", "WY", "NY", "CA", "TX", "MA", "VA",
    "RE", "EX", "DIV", "ETF", "IPO", "CEO", "CFO", "CTO", "EPS", "USD",
    "PLAN", "DATE", "TYPE", "DATA", "INFO", "MORE", "LESS", "VERY", "ALL",
    "NEW", "OLD", "ONE", "TWO", "EACH", "BOTH", "SOME", "MANY", "MOST",
    "COMMON", "CLASS", "SHARE", "STOCK", "NOTES", "BONDS", "FUND",
    "TRUST", "BANK", "REAL", "ESTATE", "ENERGY", "TECH", "HEALTH", "CARE",
    "MEDIA", "CAPITAL", "GLOBAL", "NATIONAL", "AMERICAN", "FINANCIAL",
}

XML_NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "edgar": "https://www.sec.gov/Archives/edgar/",
}


def _extract_tickers(text: str) -> list[str]:
    """Extract likely stock tickers from text, filtering common non-ticker words."""
    matches = TICKER_RE.findall(text)
    seen: set[str] = set()
    tickers: list[str] = []
    for m in matches:
        m = m.upper()
        if m in TICKER_BLOCKLIST or len(m) < 2:
            continue
        if m not in seen:
            seen.add(m)
            tickers.append(m)
    return tickers[:10]  # cap to avoid noise


def _parse_company_from_title(title: str) -> tuple[str, str]:
    """
    Attempt to parse acquirer/target from a filing title.
    Many SC TO-T/S-4 titles follow: 'Acquirer Merger Agreement with Target' patterns.
    Returns (acquirer, target) strings — best-effort, may be empty strings.
    """
    # Common patterns:
    # "XYZ Corp - SC TO-T filed by ABC Inc"
    # "Merger Agreement between ABC and XYZ"
    # "ABC Inc acquisition of XYZ Corp"
    title_clean = title.strip()

    # Try "A and B" / "A with B"
    m = re.search(
        r'(?:between|merger\s+of)\s+(.+?)\s+(?:and|with)\s+(.+?)(?:\s*[-,\(]|$)',
        title_clean, re.IGNORECASE
    )
    if m:
        return m.group(1).strip()[:80], m.group(2).strip()[:80]

    # Try "A acquisition of B" / "A takeover of B"
    m = re.search(
        r'(.+?)\s+(?:acquisition|takeover|purchase)\s+of\s+(.+?)(?:\s*[-,\(]|$)',
        title_clean, re.IGNORECASE
    )
    if m:
        return m.group(1).strip()[:80], m.group(2).strip()[:80]

    # Try "filed by COMPANY" — acquirer is filer
    m = re.search(r'filed\s+by\s+(.+?)(?:\s*[-,\(]|$)', title_clean, re.IGNORECASE)
    if m:
        return m.group(1).strip()[:80], ""

    return title_clean[:80], ""


async def scrape_ma_deals() -> list[dict]:
    """
    Fetch recent M&A filings from SEC EDGAR and return parsed deal records.
    Returns [] on any failure.
    """
    today = date.today()
    start = (today - timedelta(days=30)).isoformat()
    end   = today.isoformat()

    params = {
        "q":          '"merger agreement"',
        "dateRange":  "custom",
        "startdt":    start,
        "enddt":      end,
        "forms":      "SC TO-T,S-4,8-K",
    }
    timeout = aiohttp.ClientTimeout(total=TIMEOUT_S)
    now_ms = int(time.time() * 1000)

    try:
        async with aiohttp.ClientSession(timeout=timeout, headers=HEADERS) as session:
            async with session.get(EDGAR_SEARCH_URL, params=params) as resp:
                if resp.status != 200:
                    log.warning("ma_deals.api_error", status=resp.status, url=str(resp.url))
                    return []
                raw_text = await resp.text()
    except Exception as e:
        log.warning("ma_deals.fetch_failed", error=str(e))
        return []

    # Parse XML/ATOM
    try:
        root = ET.fromstring(raw_text)
    except ET.ParseError as e:
        log.warning("ma_deals.xml_parse_error", error=str(e))
        return []

    # Strip namespace prefix for easier access
    def _tag(element) -> str:
        tag = element.tag
        if "}" in tag:
            return tag.split("}", 1)[1]
        return tag

    results: list[dict] = []

    # Iterate over <entry> elements (ATOM feed) or <item> (RSS)
    for child in root:
        if _tag(child) not in ("entry", "item"):
            continue

        title        = ""
        filing_date  = ""
        form_type    = ""
        accession_no = ""
        deal_url     = ""

        for elem in child:
            tag = _tag(elem)
            if tag == "title":
                title = (elem.text or "").strip()
            elif tag == "updated" or tag == "published" or tag == "date":
                filing_date = (elem.text or "").strip()[:10]
            elif tag == "link":
                href = elem.get("href", "")
                if href:
                    deal_url = href
            elif tag == "id":
                # EDGAR entry IDs often contain the accession number
                id_text = (elem.text or "")
                m = re.search(r'(\d{18}|\d{10}-\d{2}-\d{6})', id_text)
                if m:
                    accession_no = m.group(1)
            elif tag == "category":
                # <category term="SC TO-T" .../>
                term = elem.get("term", "")
                if term:
                    form_type = term

        if not title:
            continue

        acquirer, target = _parse_company_from_title(title)
        tickers = _extract_tickers(title)

        results.append({
            "acquirer":         acquirer,
            "target":           target,
            "form_type":        form_type or "UNKNOWN",
            "filing_date":      filing_date,
            "deal_url":         deal_url,
            "tickers":          tickers,
            "accession_number": accession_no,
            "ts_utc":           now_ms,
        })

    log.info("ma_deals.scrape_done", deals=len(results), date_range=f"{start}/{end}")
    return results
