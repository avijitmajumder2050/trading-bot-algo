# app/broker/dhan_super_client.py

import logging
import json
import time
from app.config.dhan_auth import dhan  # DHAN SDK with enums
from app.broker.super_order import SuperOrder
from app.broker.market_data import get_ltp
from app.broker.fund_manager import init_fund_cache
from app.broker.leverage_manager import init_leverage_cache
from app.broker.position_sizing import calculate_position_size




class DhanSuperBroker:
    """
    Broker wrapper for DHAN Super Orders
    Handles entry, target, stop-loss, trailing jump, and partial exits.
    """

    def __init__(self, dhan_context):
        self.super = SuperOrder(dhan_context)

    def place_trade(self, stock, trailing_multiplier=0.5, max_ltp_retries=3, ltp_sleep=1):
        """
        Place a Super Order on DHAN with robust LTP fetching, trailing stop-loss,
        and calculated target if not provided.

        Args:
            stock (dict): Stock info with keys:
                          'Stock Name', 'Security ID', 'Entry', 'SL', 'Quantity', 'Signal', optionally 'Target'
            trailing_multiplier (float): fraction of risk to use for trailing jump
            max_ltp_retries (int): max attempts to fetch LTP if None
            ltp_sleep (int/float): seconds to wait between retries

        Returns:
             dict: {
            "order_id": str,
            "entry": float,
            "sl": float,
            "qty": int
        } or None if failed
    
        """
        try:
            # Extract stock info
            name = stock.get("Stock Name", "UNKNOWN")
            instrument_id = str(stock["Security ID"])
            
            strategy_entry = stock["Entry"]
            sl = stock["SL"]
            #qty = stock["Quantity"]
            side_str = stock["Signal"].upper()  # "BUY" or "SELL"
            side_enum = dhan.BUY if side_str == "BUY" else dhan.SELL
            # -------------------------------
            # Init fund & leverage cache (SAFE)
            # -------------------------------
            init_fund_cache()
            init_leverage_cache()

            # -------------------------------
            # Fetch LTP with retries
            # -------------------------------
            ltp = None
            for attempt in range(max_ltp_retries):
                ltp = get_ltp(stock["Security ID"])
                if ltp is not None:
                    break
                logging.warning(f"LTP fetch failed for {name}, retry {attempt + 1}/{max_ltp_retries}")
                time.sleep(ltp_sleep)

            if ltp is None:
                logging.error(f"❌ Unable to fetch LTP for {name}. Aborting order.")
                return None
            
            

            # -------------------------------
            # Skip order if price already crossed entry
            # -------------------------------
            if (side_str == "BUY" and ltp < strategy_entry) or (side_str == "SELL" and ltp > strategy_entry):
                logging.warning(f"⚠️ Skipping {side_str} order for {name}: LTP={ltp} crossed entry={strategy_entry}")
                return None
            # -------------------------------
            # FINAL EXECUTION VALIDATION
            # -------------------------------
           
            

            qty, risk_amt, exposure = calculate_position_size(price=ltp,entry=ltp,sl=sl,sec_id=instrument_id,max_loss=1000)

            if qty <= 0:
                logging.error(f"❌ Qty zero after validation | {name} | "f"LTP={ltp}, SL={sl}")
                return None

            logging.info(f"✅ Final Execution Check | {name} | "f"Qty={qty}, Entry={ltp}, SL={sl}, Risk=₹{risk_amt}")

            
            
            # -------------------------------
            # Risk, trailing jump, and target calculation
            # -------------------------------
            risk = abs(ltp - sl)
            
            trailing_jump = round(risk * trailing_multiplier, 2)

            target = stock.get("Target")
            if not target or target <= 0:
                # Use entry as base for target (safer than LTP)
                target = round(ltp + 1.5 * risk if side_str == "BUY" else ltp - 1.5 * risk, 2)

            # -------------------------------
            # Prepare payload for logging
            # -------------------------------
            order_payload = {
                "transactionType": side_str,
                "exchangeSegment": "NSE",
                "productType": "INTRADAY",
                "orderType": "LIMIT",
                "securityId": instrument_id,
                "quantity": qty,
                "price": ltp,
                "targetPrice": target,
                "stopLossPrice": sl,
                "trailingJump": trailing_jump,
                "correlationId": f"{name}_AUTO"
            }
            logging.info("📦 DHAN SuperOrder Payload:\n%s", json.dumps(order_payload, indent=2))

            # -------------------------------
            # Place Super Order (using DHAN enums)
            # -------------------------------
            resp = self.super.place_super_order(
                security_id=instrument_id,
                exchange_segment=dhan.NSE,
                transaction_type=side_enum,
                quantity=qty,
                order_type=dhan.LIMIT,
                product_type=dhan.INTRA,
                price=ltp,
                targetPrice=target,
                stopLossPrice=sl,
                trailingJump=trailing_jump,
                tag=f"{name}_AUTO"
            )

            # Convert response if string
            if isinstance(resp, str):
                resp = json.loads(resp)

            if resp.get("status") != "success":
                logging.error(f"❌ Failed to place Super Order for {name}: {resp}")
                return None

            order_id = resp["data"]["orderId"]
            logging.info(f"✅ Super Order placed for {name} | Entry: {ltp}, SL: {sl}, Target: {target} | ID: {order_id}")
            return {
            "order_id": order_id,
            "entry": ltp,
            "sl": sl,
            "qty": qty
        }

        except Exception:
            logging.exception(f"❌ Exception placing Super Order for {stock.get('Stock Name', 'UNKNOWN')}")
            return None

    def partial_book(self, order_id, new_qty):
        logging.info(f"🔹 Partial booking → Qty {new_qty}")
        resp = self.super.modify_super_order(
            order_id=order_id,
            order_type=dhan.MARKET,
            leg_name="ENTRY_LEG",
            quantity=new_qty
        )
        logging.info(f"Partial book response: {resp}")
        return resp

    def trail_sl(self, order_id, new_sl, trailing_jump=1.0):
        logging.info(f"🔁 Trailing SL → {new_sl}, jump: {trailing_jump}")
        resp = self.super.modify_super_order(
            order_id=order_id,
            order_type=None,
            leg_name="STOP_LOSS_LEG",
            stopLossPrice=new_sl,
            trailingJump=trailing_jump
        )
        logging.info(f"Trail SL response: {resp}")
        return resp

    def exit_trade(self, order_id):
        logging.warning(f"🛑 Cancelling Super Order {order_id}")
        resp = self.super.cancel_super_order(order_id, "ENTRY_LEG")
        logging.info(f"Exit trade response: {resp}")
        return resp
    
    def exit_trade_market(self, order_id, side, ltp, buffer=1):
        """
        Exit a trade immediately using MARKET on STOP_LOSS_LEG.
        Adds a small buffer below/above LTP to satisfy DHAN API validation rules.

        Args:
            order_id (str): Super Order ID.
            side (str): "BUY" or "SELL".
            ltp (float): Current Last Traded Price.
            buffer (float): Small adjustment to allow API to trigger STOP_LOSS_LEG.

        Returns:
            dict: API response from DHAN modify_super_order call.
        """
        side = side.upper()
        if side not in ("BUY", "SELL"):
            raise ValueError("Side must be BUY or SELL")

        # Determine stopLossPrice to trigger immediately
        if side == "BUY":
            stop_price = round(ltp - buffer, 2)  # must be below LTP for BUY
        else:  # SELL
            stop_price = round(ltp + buffer, 2)  # must be above LTP for SELL

        logging.info(f"🛑 Exiting trade | Order ID: {order_id} | Side: {side} | Trigger Price: {stop_price}")

        resp = self.super.modify_super_order(
            order_id=order_id,
            order_type=dhan.MARKET,
            leg_name="STOP_LOSS_LEG",
            stopLossPrice=stop_price,
            trailingJump=1  # can be 0 if you want instant exit
        )

        logging.info(f"Exit trade MARKET response: {resp}")
        return resp
    
    def get_order_status(self, order_id):
        """
        Fetch order status using DHAN SDK.
        Returns orderStatus string or None.
        """
        try:
            # Using global dhan (as per your architecture)
            resp = dhan.get_order_by_id(order_id)

            if isinstance(resp, str):
                resp = json.loads(resp)

            if resp.get("status") != "success":
                logging.error(f"❌ Failed to fetch order status: {resp}")
                return None

            data = resp.get("data", [])

            if not data:
                logging.warning(f"⚠️ No order data found for {order_id}")
                return None

            # ✅ Your API clearly returns a list
            order_status = data[0].get("orderStatus")
            return order_status

            

        except Exception:
            logging.exception(f"❌ Exception fetching order status for {order_id}")
            return None