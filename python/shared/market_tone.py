"""
Market Tone — VIX-keyed adaptive parameter framework.

Reads the current VIX level from Polygon.io (cached 30 min in Redis) and maps
it to one of four named tones defined in /app/config/market_tones.json:

  bullish   VIX < 15   — low vol, favour long bias, wider delta range
  neutral   VIX 15-20  — standard thresholds
  bearish   VIX 20-30  — elevated vol, raise confidence bar, passive pricing
  forgiving VIX > 30   — extreme fear, vol spike = edge, natural pricing

Adapted from option_screener tone/ framework (philfoster/option_screener).

Usage:
    from shared.market_tone import get_market_tone, get_tone_thresholds
    tone = await get_market_tone(redis)           # "bullish"|"neutral"|"bearish"|"forgiving"
    thresholds = get_tone_thresholds(tone)        # dict from market_tones.json
"""
import json
import os
import structlog

log = structlog.get_logger("shared.market_tone")

_TONES_PATH   = os.getenv("MARKET_TONES_PATH", "/app/config/market_tones.json")
_VIX_CACHE_KEY = "market:vix:latest"
_TONE_CACHE_KEY = "market:tone:latest"
_CACHE_TTL      = 1800   # 30 minutes

_FALLBACK_TONE = "neutral"
_TONES: dict = {}


def _load_tones() -> dict:
    global _TONES
    if _TONES:
        return _TONES
    try:
        with open(_TONES_PATH) as f:
            _TONES = json.load(f)
    except Exception as e:
        log.warning("market_tone.config_load_failed", path=_TONES_PATH, error=str(e))
        _TONES = {}
    return _TONES


def _classify_vix(vix: float) -> str:
    """Map a VIX value to a tone name using the configured thresholds."""
    tones = _load_tones()
    for tone_name in ("bullish", "neutral", "bearish", "forgiving"):
        cfg = tones.get(tone_name, {})
        if vix <= cfg.get("vix_max", 999):
            return tone_name
    return _FALLBACK_TONE


async def _fetch_vix(redis) -> float | None:
    """
    Fetch current VIX from Polygon.io prev-close agg. Cached 30 min in Redis.
    VIX Polygon symbol: I:VIX
    """
    try:
        cached = await redis.get(_VIX_CACHE_KEY)
        if cached:
            return float(cached)
    except Exception:
        pass

    api_key = os.getenv("MASSIVE_API_KEY", "")
    if not api_key:
        return None

    try:
        from datetime import date, timedelta
        import aiohttp
        today    = date.today()
        from_str = (today - timedelta(days=7)).isoformat()
        url = (
            f"https://api.polygon.io/v2/aggs/ticker/I:VIX/range/1/day"
            f"/{from_str}/{today.isoformat()}?adjusted=true&sort=desc&limit=1&apiKey={api_key}"
        )
        async with aiohttp.ClientSession() as sess:
            async with sess.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
        bars = data.get("results") or []
        if not bars:
            return None
        vix = float(bars[0]["c"])
        try:
            await redis.setex(_VIX_CACHE_KEY, _CACHE_TTL, str(vix))
        except Exception:
            pass
        return vix
    except Exception as e:
        log.warning("market_tone.vix_fetch_failed", error=str(e))
        return None


async def get_market_tone(redis) -> str:
    """
    Return the current market tone string. Cached in Redis for 30 min.
    Falls back to "neutral" on any error.
    """
    try:
        cached = await redis.get(_TONE_CACHE_KEY)
        if cached:
            return cached if isinstance(cached, str) else cached.decode()
    except Exception:
        pass

    vix = await _fetch_vix(redis)
    if vix is None:
        return _FALLBACK_TONE

    tone = _classify_vix(vix)
    log.info("market_tone.resolved", vix=round(vix, 2), tone=tone)

    try:
        await redis.setex(_TONE_CACHE_KEY, _CACHE_TTL, tone)
    except Exception:
        pass
    return tone


def get_tone_thresholds(tone: str) -> dict:
    """Return the threshold dict for a named tone. Falls back to neutral defaults."""
    tones = _load_tones()
    return tones.get(tone, tones.get(_FALLBACK_TONE, {}))
