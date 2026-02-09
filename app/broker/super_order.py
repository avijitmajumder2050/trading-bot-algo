# app/broker/super_order.py

import logging

class SuperOrder:
    def __init__(self, dhan_client):
        """
        Initialize with dhanhq client instance.
        """
        self.dhan_client = dhan_client  # dhanhq object

    def place_super_order(
        self,
        security_id,
        exchange_segment,
        transaction_type,
        quantity,
        order_type,
        product_type,
        price,
        targetPrice=0.0,
        stopLossPrice=0.0,
        trailingJump=0.0,
        tag=None
    ):
        """
        Place a Super Order using dhanhq SDK method.
        """
        try:
            response = self.dhan_client.place_super_order(
                security_id=str(security_id),
                exchange_segment=exchange_segment.upper(),
                transaction_type=transaction_type.upper(),
                quantity=int(quantity),
                order_type=order_type.upper(),
                product_type=product_type.upper(),
                price=float(price),
                targetPrice=float(targetPrice),
                stopLossPrice=float(stopLossPrice),
                trailingJump=float(trailingJump),
                tag=tag
            )
            return response
        except Exception as e:
            logging.exception(f"❌ Failed to place Super Order for {security_id}: {e}")
            return None

    def modify_super_order(
        self,
        order_id,
        order_type,
        leg_name,
        quantity=0,
        price=0.0,
        targetPrice=0.0,
        stopLossPrice=0.0,
        trailingJump=0.0
    ):
        try:
            response = self.dhan_client.modify_super_order(
                order_id=order_id,
                order_type=order_type,
                leg_name=leg_name,
                quantity=quantity,
                price=price,
                targetPrice=targetPrice,
                stopLossPrice=stopLossPrice,
                trailingJump=trailingJump
            )
            return response
        except Exception as e:
            logging.exception(f"❌ Failed to modify Super Order {order_id}: {e}")
            return None

    def cancel_super_order(self, order_id, leg):
        try:
            response = self.dhan_client.cancel_super_order(order_id, leg)
            return response
        except Exception as e:
            logging.exception(f"❌ Failed to cancel Super Order {order_id} leg {leg}: {e}")
            return None
    def get_super_order_list(self):
        """
        Fetch all Super Orders using Dhan SDK.
        Returns:
        dict: API response
        """
        try:
            response = self.dhan_client.get_super_order_list()
            return response
        except Exception as e:
            logging.exception(f"❌ Failed to fetch Super Orders: {e}")
            return None
   