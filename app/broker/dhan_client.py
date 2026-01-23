#app/broker/dhan_client.py
from app.config.dhan_auth import dhan
import logging

def place_entry(security_id, side, qty):
    """Place market entry order"""
    return dhan.place_order(
        security_id=str(security_id),
        exchange_segment=dhan.NSE,
        transaction_type=side.upper(),
        quantity=qty,
        order_type=dhan.MARKET,
        product_type=dhan.INTRA,
        price=0
    )

def place_sl(security_id, side, qty, trigger_price):
    """Place SL order"""
    return dhan.place_order(
        security_id=str(security_id),
        exchange_segment=dhan.NSE,
        transaction_type=side.upper(),
        quantity=qty,
        order_type=dhan.SLM,
        price=trigger_price,
        trigger_price=trigger_price,
        product_type=dhan.INTRA
    )

def cancel_order(order_id):
    try:
        dhan.cancel_order(order_id)
    except Exception:
        logging.exception(f"❌ SL cancel failed for order {order_id}")
