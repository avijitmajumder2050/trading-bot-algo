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

    # Say WHICH limit produced the qty — "Qty zero" alone gave no clue that
    # the fund side (fund / leverage) was the cause.
    logger.info(
        f"📐 Sizing {sec_id}: risk qty={qty_by_risk} (₹{max_loss:.0f} / SL gap {sl_point:.2f}), "
        f"fund qty={qty_by_fund} (fund ₹{fund:,.2f} × {FUND_UTILIZATION} × leverage {leverage:g} / price {price}) -> qty={qty}"
    )
    if qty <= 0:
        if fund <= 0:
            reason = "available fund is ₹0 (fund fetch failed or balance used up)"
        elif qty_by_fund <= 0:
            reason = f"fund ₹{fund:,.2f} × leverage {leverage:g} can't buy even 1 share at ₹{price}"
        else:
            reason = f"SL gap {sl_point:.2f} is larger than the ₹{max_loss:.0f} risk budget"
        logger.error(f"❌ Position size 0 for {sec_id}: {reason}")

    return qty, qty * sl_point, qty * price
