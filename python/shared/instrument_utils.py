"""
Ticker consistency guard for LLM prompts.
Prevents symbol hallucination (stripped suffixes, substitutions) in agents that
call the LLM with a specific equity or options ticker in context.
"""


def build_instrument_context(ticker: str) -> str:
    """2-sentence guard to prepend to any LLM system prompt involving a single ticker."""
    return (
        f"The instrument under analysis is '{ticker.upper()}'. "
        "Use this exact symbol unchanged in all reasoning, tool calls, and output — "
        "do not abbreviate, modify, or substitute it."
    )


def guard_tickers(tickers: list[str]) -> str:
    """Guard for multi-ticker prompts (e.g. predictor batch ranking)."""
    syms = ", ".join(f"'{t.upper()}'" for t in tickers)
    return (
        f"The instruments under analysis are: {syms}. "
        "Use each symbol exactly as provided — do not modify, abbreviate, or substitute any of them."
    )
