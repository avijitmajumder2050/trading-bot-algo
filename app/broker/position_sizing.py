import logging
from app.broker.fund_manager import get_cached_fund
from app.broker.leverage_manager import get_leverage

logger = logging.getLogger(__name__)

# Only size against this fraction of available fund. Sizing at 100%
# left ~₹50 of headroom (NTPC 2026-09-24: 473 qty needed ₹30,811 of
# ₹30,862), so charges from an earlier trade and Dhan's own margin
# rounding tipped it into an RMS "insufficient funds" rejection
# (short by ₹9.10).
FUND_UTILIZATION = 0.98


def calculate_position_size(
    price: float,
    entry: float,
    sl: float,
    sec_id: str,
    max_loss: float = 1000
):
    sl_point = abs(entry - sl)
    if sl_point <= 0:
        logger.error("❌ Invalid SL")
        return 0, 0.0, 0.0

    # Risk based qty
    qty_by_risk = int(max_loss / sl_point)

    # Fund based qty
    leverage = get_leverage(sec_id)
    fund = get_cached_fund()

    qty_by_fund = int((fund * FUND_UTILIZATION * leverage) / price)

    qty = max(0, min(qty_by_risk, qty_by_fund))

    return qty, qty * sl_point, qty * price
