# app/broker/dhan_super_client.py

import logging
import json
from app.broker.super_order import SuperOrder
from app.broker.market_data import get_ltp
import time

class DhanSuperBroker:
    """
    Broker wrapper for DHAN Super Orders
    Handles entry, target, stop-loss, trailing jump, and partial exits.
    """

    def __init__(self, dhan_context):
        self.super = SuperOrder(dhan_context)

    def place_trade(self, stock, trailing_multiplier=0.5):
        """
        Place a Super Order on DHAN.

        Args:
            stock (dict): Stock info with keys:
                          'Stock Name', 'Security ID', 'Entry', 'SL', 'Quantity', 'Signal', optionally 'Target'
            trailing_multiplier (float): fraction of risk to use for trailing jump

        Returns:
            str: Super Order ID if successful, None otherwise
        """
        try:
            entry = stock["Entry"]
            sl = stock["SL"]
            qty = stock["Quantity"]
            side = stock["Signal"].upper()  # "BUY" or "SELL"
            instrument_id=str(stock["Security ID"])
            ltp = get_ltp(stock["Security ID"])
            if not ltp:
                time.sleep(1)
                ltp = get_ltp(stock["Security ID"])
                

            # Risk & trailing calculation
            risk = abs(entry - sl)
            trailing_jump = round(risk * trailing_multiplier, 2)

            # Target calculation if not provided
            target = stock.get("Target")
            if not target or target <= 0:
                target = entry + 1.5 * risk if side == "BUY" else entry - 1.5 * risk
                target = round(target, 2)

            # Place super order
            resp = self.super.place_super_order(
                security_id=str(stock["Security ID"]),
                exchange_segment="NSE",          # string
                transaction_type=side,           # "BUY" / "SELL"
                quantity=qty,
                order_type="LIMIT",             # string
                product_type="INTRADAY",         # string
                price=ltp,
                stopLossPrice=sl,
                targetPrice=target,
                trailingJump=trailing_jump,
                tag=f"{stock['Stock Name']}_AUTO"
            )

            # DHAN sometimes returns a string; convert to dict
            if isinstance(resp, str):
                resp = json.loads(resp)

            if resp.get("status") != "success":
                logging.error(f"❌ Failed to place Super Order: {resp}")
                return None

            order_id = resp["data"]["orderId"]
            logging.info(f"✅ Super Order placed | Entry: {entry}, SL: {sl}, Target: {target} | ID: {order_id}")
            return order_id

        except Exception:
            logging.exception(f"❌ Failed to place Super Order for {stock.get('Stock Name', 'UNKNOWN')}")
            return None

    def partial_book(self, order_id, new_qty):
        logging.info(f"🔹 Partial booking → Qty {new_qty}")
        return self.super.modify_super_order(
            order_id=order_id,
            order_type="MARKET",
            leg_name="ENTRY_LEG",
            quantity=new_qty
        )

    def trail_sl(self, order_id, new_sl, trailing_jump=0.0):
        logging.info(f"🔁 Trailing SL → {new_sl}, jump: {trailing_jump}")
        return self.super.modify_super_order(
            order_id=order_id,
            order_type=None,
            leg_name="STOP_LOSS_LEG",
            stopLossPrice=new_sl,
            trailingJump=trailing_jump
        )

    def exit_trade(self, order_id):
        logging.warning(f"🛑 Cancelling Super Order {order_id}")
        return self.super.cancel_super_order(order_id, "ENTRY_LEG")
