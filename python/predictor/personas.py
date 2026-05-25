"""
Investor Persona Voting System
Six legendary investor personas evaluate each momentum candidate independently
in parallel (one LLM call per persona, all candidates batched per call).
Results aggregate into a weighted consensus that adjusts signal confidence.

Flow:
  run_persona_consensus(candidates) → list[PersonaConsensus]
  Each PersonaConsensus has a confidence_delta and keep flag.
"""
import asyncio
import logging
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger("predictor.personas")

# Persona definitions — weight sum = 1.0
PERSONAS: dict[str, dict] = {
    "druckenmiller": {
        "name": "Stanley Druckenmiller",
        "weight": 0.25,
        "system": (
            "You are Stanley Druckenmiller, a macro-driven trader who combines top-down "
            "analysis with bottom-up stock selection. You follow strong trends when "
            "fundamentals and liquidity support them. You size aggressively on high-conviction "
            "setups and cut losses quickly. You pay close attention to market breadth, "
            "momentum regime, and whether the macro environment is supportive."
        ),
    },
    "buffett": {
        "name": "Warren Buffett",
        "weight": 0.20,
        "system": (
            "You are Warren Buffett, a value investor who seeks durable competitive advantages "
            "at reasonable prices. You are skeptical of momentum plays without fundamental "
            "justification. You look for strong free cash flow, consistent earnings, and "
            "businesses you can understand. You have a long time horizon and prefer to "
            "sit on your hands rather than make mediocre trades."
        ),
    },
    "lynch": {
        "name": "Peter Lynch",
        "weight": 0.20,
        "system": (
            "You are Peter Lynch, a GARP investor — growth at a reasonable price. "
            "You want to understand the story behind a company: why will it grow, "
            "what's the catalyst, and does the price reflect reality? You like "
            "businesses hiding in plain sight that institutions haven't found yet. "
            "You're willing to act on momentum when fundamental growth supports it."
        ),
    },
    "wood": {
        "name": "Cathie Wood",
        "weight": 0.15,
        "system": (
            "You are Cathie Wood, an innovation-focused investor targeting disruptive "
            "technology across AI, genomics, fintech, and energy storage. You take a "
            "5-year horizon and expect exponential growth. You are bullish on momentum "
            "in disruptive sectors and negative on legacy industries being disrupted. "
            "You believe in concentrated, high-conviction positions in transformative companies."
        ),
    },
    "burry": {
        "name": "Michael Burry",
        "weight": 0.10,
        "system": (
            "You are Michael Burry, a contrarian deep value investor who looks for "
            "asymmetric opportunities the market has mispriced. You are highly skeptical "
            "of crowded momentum trades and popular narratives. You look for overlooked "
            "catalysts, real value disconnects, and situations where everyone else is wrong. "
            "You require a strong thesis before taking a position."
        ),
    },
    "greenblatt": {
        "name": "Joel Greenblatt",
        "weight": 0.10,
        "system": (
            "You are Joel Greenblatt, a systematic value investor focused on earnings yield "
            "and return on invested capital (ROIC). You buy good companies cheaply and "
            "trust the quantitative process. You are data-driven: high ROIC + cheap "
            "valuation = buy signal. You are skeptical of signals without quantitative "
            "earnings or valuation support."
        ),
    },
}

assert abs(sum(p["weight"] for p in PERSONAS.values()) - 1.0) < 1e-9, "Persona weights must sum to 1.0"


@dataclass
class PersonaVote:
    persona:    str
    name:       str
    vote:       str    # buy | pass | sell
    conviction: float  # 0.0 – 1.0
    rationale:  str
    weight:     float


@dataclass
class PersonaConsensus:
    ticker:             str
    keep:               bool
    confidence_delta:   float
    weighted_bullish:   float
    weighted_bearish:   float
    votes:              list[PersonaVote] = field(default_factory=list)
    summary:            str = ""


def _build_candidate_lines(candidates) -> str:
    lines = []
    for t in candidates:
        breadth = t.metadata.get("breadth_pct", 50)
        lines.append(
            f"- {t.ticker}: {t.direction}, conf={t.confidence:.2f}, "
            f"analyst={t.metadata.get('analyst_consensus', 'none')}, "
            f"sentiment={t.metadata.get('sentiment_label', 'neutral')}, "
            f"breadth={breadth:.0f}%"
            + (f", earnings_in={t.metadata.get('earnings_days_away')}d"
               if t.metadata.get("earnings_days_away") is not None else "")
        )
    return "\n".join(lines)


async def _persona_batch_vote(
    persona_key: str,
    persona:     dict,
    candidate_lines: str,
    ticker_set:  set,
    llm,
) -> list[dict]:
    """
    One LLM call: persona evaluates all candidates and returns a vote list.
    Returns list of {ticker, vote, conviction, rationale} dicts.
    """
    prompt = (
        f"The following momentum signals have been flagged by a systematic screener:\n\n"
        f"{candidate_lines}\n\n"
        "Evaluate each signal from your investment perspective.\n"
        "Return a JSON array of objects for tickers you have meaningful insight on:\n"
        '[{"ticker": "XYZ", "vote": "buy"|"pass"|"sell", '
        '"conviction": 0.0-1.0, "rationale": "brief reason"}]\n'
        "Omit tickers you have no strong view on. Return [] if none qualify."
    )
    try:
        result = await llm.complete_json(
            prompt=prompt,
            system=persona["system"],
            max_tokens=500,
        )
        if not isinstance(result, list):
            return []
        # Validate and filter to known tickers
        out = []
        for item in result:
            if not isinstance(item, dict):
                continue
            ticker = str(item.get("ticker", "")).upper()
            vote = str(item.get("vote", "pass")).lower()
            if ticker not in ticker_set or vote not in ("buy", "pass", "sell"):
                continue
            conviction = float(item.get("conviction", 0.5))
            conviction = max(0.0, min(1.0, conviction))
            out.append({
                "ticker":    ticker,
                "vote":      vote,
                "conviction": conviction,
                "rationale":  str(item.get("rationale", ""))[:120],
            })
        return out
    except Exception as e:
        log.debug("persona.vote_failed", persona=persona_key, error=str(e))
        return []


def _aggregate_votes(
    ticker: str,
    all_votes: list[tuple[str, dict, list[dict]]],
) -> PersonaConsensus:
    """
    Aggregate persona votes for a single ticker into a consensus.
    all_votes: list of (persona_key, persona_def, vote_items_from_that_persona)
    """
    persona_votes: list[PersonaVote] = []

    for key, pdef, vote_list in all_votes:
        for v in vote_list:
            if v["ticker"] != ticker:
                continue
            persona_votes.append(PersonaVote(
                persona    = key,
                name       = pdef["name"],
                vote       = v["vote"],
                conviction = v["conviction"],
                rationale  = v["rationale"],
                weight     = pdef["weight"],
            ))
            break  # one vote per persona per ticker

    if not persona_votes:
        # No persona had an opinion — neutral, keep, no delta
        return PersonaConsensus(
            ticker=ticker, keep=True, confidence_delta=0.0,
            weighted_bullish=0.0, weighted_bearish=0.0,
            summary="No persona weighed in — neutral pass.",
        )

    total_weight = sum(p.weight for p in persona_votes)
    if total_weight == 0:
        total_weight = 1.0

    bullish = sum(p.weight * p.conviction for p in persona_votes if p.vote == "buy")
    bearish = sum(p.weight * p.conviction for p in persona_votes if p.vote == "sell")

    # Normalize to coverage of actual participating personas
    w_bullish = bullish / total_weight
    w_bearish = bearish / total_weight

    # Net confidence delta capped at ±0.15
    net = w_bullish - w_bearish
    delta = round(max(-0.15, min(0.15, net * 0.15)), 4)

    # Keep unless consensus is strongly bearish (weighted_bearish > weighted_bullish + 0.25)
    keep = not (w_bearish > w_bullish + 0.25 and w_bearish > 0.35)

    # Build summary from top-conviction votes
    top = sorted(persona_votes, key=lambda p: p.conviction, reverse=True)[:3]
    summary = "; ".join(f"{p.name}({p.vote}): {p.rationale}" for p in top)

    return PersonaConsensus(
        ticker          = ticker,
        keep            = keep,
        confidence_delta = delta,
        weighted_bullish = round(w_bullish, 3),
        weighted_bearish = round(w_bearish, 3),
        votes           = persona_votes,
        summary         = summary,
    )


async def run_persona_consensus(candidates, llm) -> list[PersonaConsensus]:
    """
    Run all personas against all candidates in parallel (6 concurrent LLM calls,
    each persona evaluating all candidates in one batch prompt).

    Returns a list of PersonaConsensus aligned 1:1 with `candidates`.
    """
    if not candidates:
        return []

    candidate_lines = _build_candidate_lines(candidates)
    ticker_set = {t.ticker for t in candidates}

    # Fire one call per persona concurrently
    tasks = [
        _persona_batch_vote(key, pdef, candidate_lines, ticker_set, llm)
        for key, pdef in PERSONAS.items()
    ]
    raw_results = await asyncio.gather(*tasks, return_exceptions=True)

    # Pair each result with its persona
    all_votes: list[tuple[str, dict, list[dict]]] = []
    for (key, pdef), result in zip(PERSONAS.items(), raw_results):
        if isinstance(result, Exception):
            log.warning("persona.batch_failed", persona=key, error=str(result))
            result = []
        all_votes.append((key, pdef, result))

    # Aggregate per ticker, preserving candidate order
    return [_aggregate_votes(t.ticker, all_votes) for t in candidates]
