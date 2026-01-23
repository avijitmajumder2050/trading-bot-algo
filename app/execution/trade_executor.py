#app/execution/trade_executor.py
import logging
from app.execution.position_manager import PositionManager
from app.broker.dhan_client import place_entry, place_sl, cancel_order
from app.broker.market_data import get_ltp
import time

def execute_trade(stock):
    sec_id = stock["Security ID"]
    qty = stock["Quantity"]
    side = stock["Signal"].upper()

    logging.info(f"Placing entry for {stock['Stock Name']} | Side: {side} | Qty: {qty}")

    # 1️⃣ Place Entry
    place_entry(sec_id, side, qty)

    # 2️⃣ Place initial SL
    sl_side = "SELL" if side == "BUY" else "BUY"
    sl_resp = place_sl(sec_id, sl_side, qty, stock["SL"])
    sl_order_id = sl_resp["data"]["order_id"]

    # 3️⃣ Initialize Position Manager
    pm = PositionManager(stock["Entry"], stock["SL"], qty)
    pm.sl_order_id = sl_order_id

    # 4️⃣ Monitor LTP
    while True:
        ltp = get_ltp(sec_id)
        if not ltp:
            time.sleep(1)
            continue

        action = pm.process_ltp(ltp)

        if action == "PARTIAL_BOOK":
            logging.info("Partial book triggered at 1R")
            cancel_order(pm.sl_order_id)
            place_entry(sec_id, sl_side, qty // 2)
            sl_resp = place_sl(sec_id, sl_side, qty // 2, stock["Entry"])
            pm.sl_order_id = sl_resp["data"]["order_id"]

        elif action == "TRAIL_SL":
            logging.info("Trail SL triggered at 1.5R")
            cancel_order(pm.sl_order_id)
            new_sl = stock["Entry"]
            sl_resp = place_sl(sec_id, sl_side, qty // 2, new_sl)
            pm.sl_order_id = sl_resp["data"]["order_id"]

        time.sleep(1)
