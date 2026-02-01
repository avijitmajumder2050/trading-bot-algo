import logging
from app.broker.fund_manager import get_cached_fund
from app.broker.leverage_manager import get_leverage

logger = logging.getLogger(__name__)


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

    qty_by_fund = int((fund * leverage) / price)

    qty = max(0, min(qty_by_risk, qty_by_fund))

    return qty, qty * sl_point, qty * price
