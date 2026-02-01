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
    
    order_info = broker.place_trade(stock)   # now returns dict
    if not order_info:
        logging.error(f"❌ Failed to place Super Order for {stock['Stock Name']}")
        return False   

    order_id = order_info["order_id"]        # extract order_id from dict
    entry_price = order_info["entry"]        # can use for monitoring
    sl_price = order_info["sl"]
    qty = order_info["qty"]

    logging.info(f"🚀 Super Order placed for {stock['Stock Name']} | Entry: {entry_price}, SL: {sl_price}, Qty: {qty}")
    

    logging.info(f"🚀 Monitoring trade for {stock['Stock Name']}")

    # 2️⃣ Init Position Manager (only for tracking 1R / 1.5R levels)
    pm = PositionManager(
        entry=entry_price,
        sl=sl_price,
        qty=qty,
        side=side
    )

    # 3️⃣ Monitor LTP and manage Super Order legs
    while True:
        ltp = get_ltp(stock["Security ID"])
        if not ltp:
            time.sleep(1)
            continue
        
        logging.info(
            f"📈 LTP Monitor | {stock['Stock Name']} | LTP={ltp}"
        )
        action = pm.process_ltp(ltp)

        # 1R reached → partial book
        if action == "PARTIAL_BOOK":
            logging.info(f"🔹 1R reached for {stock['Stock Name']} | Partial booking half qty")
            broker.partial_book(order_id, qty // 2)

        # 1.5R reached → trail SL
        elif action == "TRAIL_SL":
            logging.info(f"🔁 1.5R reached for {stock['Stock Name']} | Trailing SL to entry")
            broker.trail_sl(order_id, entry_price)

        
        # ⏱️ WAIT 30 SECONDS BEFORE NEXT CHECK
        time.sleep(30)
