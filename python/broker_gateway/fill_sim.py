"""
Market Impact & Fill Probability Simulator
Estimates execution cost for equity orders using a simplified Almgren-Chriss model.

Model:
  slippage_bps = η × σ_daily × (Q / ADV)^γ × 10000

where:
  η = 0.1  (temporary impact coefficient for US equities)
  γ = 0.6  (empirical exponent — sub-linear impact scaling)
  σ_daily = atr_pct (ATR/price as daily volatility proxy)
  Q / ADV = participation rate

This is a conservative estimate appropriate for planning; actual slippage
depends on intraday liquidity and venue-specific fill behavior.
"""

_GAMMA = 0.6   # temporary impact exponent
_ETA   = 0.1   # impact coefficient


def estimate_impact(
    quantity:         int,
    avg_daily_volume: int,
    price:            float,
    atr_pct:          float = 0.02,
) -> dict:
    """
    Estimate market impact for an equity order.

    Returns:
        slippage_bps      estimated temporary price impact in basis points
        slippage_usd      slippage_bps × notional / 10000
        fill_prob         probability of full fill at intended price (0–1)
        notional          order notional value
        participation     Q/ADV participation rate
        recommended_lots  suggested number of child orders to stay under 5% ADV
    """
    if avg_daily_volume <= 0 or price <= 0 or quantity <= 0:
        return {
            "slippage_bps":    0.0,
            "slippage_usd":    0.0,
            "fill_prob":       1.0,
            "notional":        round(quantity * price, 2),
            "participation":   0.0,
            "recommended_lots": 1,
        }

    participation = quantity / avg_daily_volume
    slippage_bps = _ETA * atr_pct * (participation ** _GAMMA) * 10_000
    slippage_bps = round(min(slippage_bps, 500.0), 2)

    notional    = round(quantity * price, 2)
    slippage_usd = round(notional * slippage_bps / 10_000, 2)

    # Fill probability decays with participation — high participation → unfavorable fills
    fill_prob = max(0.05, 1.0 - min(0.95, participation * 5.0))

    # Recommend splitting if participation exceeds 5% of ADV per lot
    max_lot_size = max(1, int(avg_daily_volume * 0.05))
    recommended_lots = max(1, -(-quantity // max_lot_size))  # ceiling division

    return {
        "slippage_bps":    slippage_bps,
        "slippage_usd":    slippage_usd,
        "fill_prob":       round(fill_prob, 3),
        "notional":        notional,
        "participation":   round(participation, 6),
        "recommended_lots": recommended_lots,
    }
