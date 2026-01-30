import logging
from app.broker.super_order import SuperOrder

class DhanSuperBroker:

    def __init__(self, dhan_context):
        self.super = SuperOrder(dhan_context)

    def place_trade(self, stock,trailing_multiplier=0.5):
        """
        Place a Dhan Super Order.
        Automatically calculates target as 1:1.5 R if not provided.
        SL is managed by the Super Order.
        """
        try:
            entry = stock["Entry"]
            sl = stock["SL"]
            qty = stock["Quantity"]
            side = stock["Signal"].upper()

            # Risk calculation
            risk = abs(entry - sl)
            # Auto-calculate trailing jump
            trailing_jump = risk * trailing_multiplier

            # Target calculation: default 1.5 R if not provided
            target = stock.get("Target", None)
            if not target or target <= 0:
                if side == "BUY":
                    target = entry + 1.5 * risk
                else:
                    target = entry - 1.5 * risk

            resp = self.super.place_super_order(
                security_id=stock["Security ID"],
                exchange_segment="NSE",
                transaction_type=side,
                quantity=qty,
                order_type="MARKET",
                product_type="INTRA",
                price=entry,
                stopLossPrice=sl,
                targetPrice=target,
                trailingJump=trailing_jump,
                tag=f"{stock['Stock Name']}_AUTO"
            )

            order_id = resp["data"]["orderId"]
            logging.info(f"✅ Super Order placed | Entry: {entry}, SL: {sl}, Target: {target} | {order_id}")
            return order_id

        except Exception:
            logging.exception("❌ Super Order placement failed")
            return None

    def partial_book(self, order_id, new_qty):
        logging.info(f"🔹 Partial booking → Qty {new_qty}")
        return self.super.modify_super_order(
            order_id=order_id,
            order_type="MARKET",
            leg_name="ENTRY_LEG",
            quantity=new_qty
        )

    def trail_sl(self, order_id, new_sl):
        logging.info(f"🔁 Trailing SL → {new_sl}")
        return self.super.modify_super_order(
            order_id=order_id,
            order_type=None,
            leg_name="STOP_LOSS_LEG",
            stopLossPrice=new_sl
        )

    def exit_trade(self, order_id):
        logging.warning(f"🛑 Cancelling Super Order {order_id}")
        return self.super.cancel_super_order(order_id, "ENTRY_LEG")
