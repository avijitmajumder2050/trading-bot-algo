# app/execution/trade_executor.py

import time
import logging
from app.execution.position_manager import PositionManager
from app.broker.dhan_super_client import DhanSuperBroker
from app.broker.market_data import get_ltp

def execute_trade(stock, dhan_context):
    """
    Execute trade using Dhan Super Orders.
    SL and target are managed automatically via Super Orders.
    Partial booking and trailing logic modifies the super order legs.
    """

    broker = DhanSuperBroker(dhan_context)
    side = stock["Signal"].upper()

    # 1️⃣ Place Super Order
    order_id = broker.place_trade(stock)
    if not order_id:
        logging.error(f"❌ Failed to place Super Order for {stock['Stock Name']}")
        return

    logging.info(f"🚀 Monitoring trade for {stock['Stock Name']}")

    # 2️⃣ Init Position Manager (only for tracking 1R / 1.5R levels)
    pm = PositionManager(
        entry=stock["Entry"],
        sl=stock["SL"],
        qty=stock["Quantity"],
        side=side
    )

    # 3️⃣ Monitor LTP and manage Super Order legs
    while True:
        ltp = get_ltp(stock["Security ID"])
        if not ltp:
            time.sleep(1)
            continue

        action = pm.process_ltp(ltp)

        # 1R reached → partial book
        if action == "PARTIAL_BOOK":
            logging.info(f"🔹 1R reached for {stock['Stock Name']} | Partial booking half qty")
            broker.partial_book(order_id, stock["Quantity"] // 2)

        # 1.5R reached → trail SL
        elif action == "TRAIL_SL":
            logging.info(f"🔁 1.5R reached for {stock['Stock Name']} | Trailing SL to entry")
            broker.trail_sl(order_id, stock["Entry"])

        time.sleep(1)
