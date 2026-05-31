"""
Regime Gate
2D decision matrix combining macro regime (risk_on | neutral | risk_off)
and technical regime (5-class overbought/oversold) to produce a position
size multiplier and hard-skip flags for equity_trader.

Matrix for LONG direction:
  ┌─────────────┬──────────────────┬──────────┬─────────┬────────────┬───────────────────┐
  │ Macro\Tech  │ STRONG_OVERSOLD  │ OVERSOLD │ NEUTRAL │ OVERBOUGHT │ STRONG_OVERBOUGHT │
  ├─────────────┼──────────────────┼──────────┼─────────┼────────────┼───────────────────┤
  │ risk_on     │      1.25        │   1.10   │  1.00   │    0.70    │      SKIP         │
  │ neutral     │      1.00        │   0.85   │  0.75   │    0.50    │      SKIP         │
  │ risk_off    │      0.65        │   0.50   │  SKIP   │    SKIP    │      SKIP         │
  └─────────────┴──────────────────┴──────────┴─────────┴────────────┴───────────────────┘

For SHORT direction the matrix is mirrored (overbought = favorable).
"""

_SKIP = 0.0  # sentinel — results in (allowed=False, ...)

# (macro_regime, technical_regime) → size_multiplier for LONG direction
_LONG_MATRIX: dict[tuple[str, str], float] = {
    ("risk_on",  "STRONG_OVERSOLD"):   1.25,
    ("risk_on",  "OVERSOLD"):          1.10,
    ("risk_on",  "NEUTRAL"):           1.00,
    ("risk_on",  "OVERBOUGHT"):        0.70,
    ("risk_on",  "STRONG_OVERBOUGHT"): _SKIP,

    ("neutral",  "STRONG_OVERSOLD"):   1.00,
    ("neutral",  "OVERSOLD"):          0.85,
    ("neutral",  "NEUTRAL"):           0.75,
    ("neutral",  "OVERBOUGHT"):        0.50,
    ("neutral",  "STRONG_OVERBOUGHT"): _SKIP,

    ("risk_off", "STRONG_OVERSOLD"):   0.65,
    ("risk_off", "OVERSOLD"):          0.50,
    ("risk_off", "NEUTRAL"):           _SKIP,
    ("risk_off", "OVERBOUGHT"):        _SKIP,
    ("risk_off", "STRONG_OVERBOUGHT"): _SKIP,
}

# For shorts, mirror: overbought is favorable, oversold is unfavorable
_SHORT_MATRIX: dict[tuple[str, str], float] = {
    ("risk_on",  "STRONG_OVERBOUGHT"): 1.25,
    ("risk_on",  "OVERBOUGHT"):        1.10,
    ("risk_on",  "NEUTRAL"):           1.00,
    ("risk_on",  "OVERSOLD"):          0.70,
    ("risk_on",  "STRONG_OVERSOLD"):   _SKIP,

    ("neutral",  "STRONG_OVERBOUGHT"): 1.00,
    ("neutral",  "OVERBOUGHT"):        0.85,
    ("neutral",  "NEUTRAL"):           0.75,
    ("neutral",  "OVERSOLD"):          0.50,
    ("neutral",  "STRONG_OVERSOLD"):   _SKIP,

    ("risk_off", "STRONG_OVERBOUGHT"): 1.25,
    ("risk_off", "OVERBOUGHT"):        1.10,
    ("risk_off", "NEUTRAL"):           1.00,
    ("risk_off", "OVERSOLD"):          0.70,
    ("risk_off", "STRONG_OVERSOLD"):   _SKIP,
}


def evaluate_regime_gate(
    macro: str,
    tech: str,
    direction: str,
) -> tuple[bool, str, float]:
    """
    Returns (allowed, reason, size_multiplier).

    allowed       — False means skip this trade
    reason        — human-readable label for logging
    size_mult     — multiply max_pos_usd by this before sizing
                    (only meaningful when allowed=True)
    """
    macro = (macro or "neutral").lower().strip()
    tech  = (tech  or "NEUTRAL").upper().strip()
    direction = (direction or "long").lower()

    matrix = _LONG_MATRIX if direction == "long" else _SHORT_MATRIX

    mult = matrix.get((macro, tech))

    if mult is None:
        # Unknown combination — default neutral
        return True, f"regime_gate: unknown ({macro}/{tech}), default allow", 1.00

    if mult == _SKIP:
        return (
            False,
            f"regime_gate: SKIP macro={macro} tech={tech} dir={direction}",
            0.0,
        )

    reason = f"regime_gate: macro={macro} tech={tech} dir={direction} mult={mult:.2f}"
    return True, reason, mult
