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
     
    # ─────────────────────────────────────────────
    # WAIT UNTIL ORDER IS TRADED
    # ─────────────────────────────────────────────
    logging.info(f"⏳ Waiting for order to be TRADED...")

    max_wait_seconds = 600
    start_time = time.time()

    while True:
        order_status = broker.get_order_status(order_id)

        logging.info(
            f"📊 Order Status | {stock['Stock Name']} | {order_status}"
        )

        # ✅ If traded → start LTP monitoring
        if order_status == "TRADED":
            logging.info(
                f"✅ Order TRADED | {stock['Stock Name']} | Starting LTP monitor"
            )
            break

        # ❌ If rejected/cancelled → stop
        if order_status in ["REJECTED", "CANCELLED"]:
            logging.error(
                f"❌ Order {order_status} | {stock['Stock Name']}"
            )
            return False

        # ⏳ Timeout protection
        if time.time() - start_time > max_wait_seconds:
            logging.warning(
        f"⏰ Order not traded within timeout for {stock['Stock Name']}. Cancelling order..."
    )
            try:
                broker.exit_trade(order_id)  # Cancels ENTRY_LEG
                logging.info(f"🛑 Order cancelled due to timeout | ID: {order_id}")
            except Exception as e:
                logging.error(f"❌ Failed to cancel order: {e}")

            return False

        time.sleep(30)


    

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
        # 🔎 First check if trade already exited
        #order_status = broker.get_order_status(order_id)
        #if order_status in ["CANCELLED", "REJECTED"]:
         #   logging.warning(f"❌ Trade cancelled externally | {stock['Stock Name']}")
         #   break

        # 🔎 Check Super Order exit status
        exit_status = broker.check_super_order_exit(order_id)
        if exit_status == "PARENT_CANCELLED":
            logging.warning(f"❌ Parent order cancelled | {stock['Stock Name']}")
            break
        elif exit_status == "PARENT_REJECTED":
            logging.error(f"❌ Parent order rejected | {stock['Stock Name']}")
            break
        elif exit_status == "STOP_LOSS_HIT":
            logging.info(f"🛑 STOP LOSS HIT | {stock['Stock Name']}")
            break
        elif exit_status == "TARGET_HIT":
            logging.info(f"🎯 TARGET HIT | {stock['Stock Name']}")
            break
        elif exit_status == "EXIT_CANCELLED":
            logging.info(f"⚫ Trade exited manually | {stock['Stock Name']}")
            break

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
        
        # Full exit logic → separate condition
        elif action == "EXIT_TRADE":
            logging.info(f"🛑 EXIT_TRADE triggered for {stock['Stock Name']} | Exiting at MARKET STOP_LOSS")
            broker.exit_trade_market(order_id, side=side, ltp=ltp)
            logging.info(f"✅ Trade fully exited for {stock['Stock Name']}")
            break  # Stop monitoring


        
        # ⏱️ WAIT 30 SECONDS BEFORE NEXT CHECK
        time.sleep(30)
