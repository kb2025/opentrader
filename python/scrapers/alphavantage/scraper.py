"""
Alpha Vantage News Sentiment Scraper
Fetches categorized financial news with sentiment scores.
"""
import asyncio
import logging

import aiohttp

log = logging.getLogger("scraper.alphavantage")

AV_BASE = "https://www.alphavantage.co/query"

# Alpha Vantage topic categories
TOPICS = [
    "earnings",
    "ipo",
    "mergers_and_acquisitions",
    "financial_markets",
    "economy_fiscal",
    "economy_monetary",
    "economy_macro",
    "energy_transportation",
    "finance",
    "life_sciences",
    "manufacturing",
    "real_estate",
    "retail_wholesale",
    "technology",
]

# Map AV topics to our simplified categories
TOPIC_CATEGORY = {
    "earnings":                 "equities",
    "ipo":                      "equities",
    "mergers_and_acquisitions": "equities",
    "financial_markets":        "macro",
    "economy_fiscal":           "macro",
    "economy_monetary":         "macro",
    "economy_macro":            "macro",
    "energy_transportation":    "energy",
    "finance":                  "macro",
    "life_sciences":            "equities",
    "manufacturing":            "equities",
    "real_estate":              "equities",
    "retail_wholesale":         "equities",
    "technology":               "technology",
}

SENTIMENT_LABEL_SCORE = {
    "Bearish":         -0.75,
    "Somewhat-Bearish": -0.35,
    "Neutral":           0.0,
    "Somewhat-Bullish":  0.35,
    "Bullish":           0.75,
}


async def fetch_news_sentiment(api_key: str, limit: int = 50) -> list[dict]:
    """Fetch recent news articles with sentiment from Alpha Vantage."""
    results = []
    seen_urls = set()

    async with aiohttp.ClientSession() as session:
        for topic in TOPICS:  # AV free = 25 req/day; delay prevents per-minute throttle
            category = TOPIC_CATEGORY.get(topic, "macro")
            try:
                params = {
                    "function": "NEWS_SENTIMENT",
                    "topics":   topic,
                    "limit":    str(limit),
                    "sort":     "LATEST",
                    "apikey":   api_key,
                }
                async with session.get(AV_BASE, params=params,
                                       timeout=aiohttp.ClientTimeout(total=20)) as resp:
                    if resp.status != 200:
                        log.warning("av_news.fetch_failed", topic=topic, status=resp.status)
                        continue
                    data = await resp.json()

                articles = data.get("feed", [])
                for article in articles[:20]:
                    url = article.get("url", "")
                    if url in seen_urls:
                        continue
                    seen_urls.add(url)

                    overall_label = article.get("overall_sentiment_label", "Neutral")
                    overall_score = SENTIMENT_LABEL_SCORE.get(overall_label, 0.0)
                    overall_score = float(article.get("overall_sentiment_score", overall_score))

                    # Extract ticker-specific sentiment
                    ticker_sentiments = article.get("ticker_sentiment", [])
                    primary_ticker = None
                    max_relevance  = 0.0
                    for ts in ticker_sentiments:
                        relevance = float(ts.get("relevance_score", 0))
                        if relevance > max_relevance:
                            max_relevance    = relevance
                            primary_ticker   = ts.get("ticker")

                    results.append({
                        "category":        category,
                        "ticker":          primary_ticker,
                        "title":           article.get("title", "")[:500],
                        "source":          article.get("source", ""),
                        "url":             url,
                        "overall_score":   round(overall_score, 4),
                        "relevance_score": round(max_relevance, 4),
                        "topics":          [t.get("topic") for t in article.get("topics", [])],
                        "raw":             {
                            "time_published": article.get("time_published"),
                            "authors":        article.get("authors", []),
                            "summary":        article.get("summary", "")[:300],
                            "ticker_sentiment": ticker_sentiments[:5],
                        },
                    })
            except Exception as e:
                log.warning("av_news.topic_error", topic=topic, error=str(e))
            await asyncio.sleep(13)  # stay under 5 req/min AV rate limit

    return results
