#app/broker/market_data.py
from app.config.dhan_auth import dhan

def get_ltp(security_id):
    """Fetch LTP via REST (fallback if WebSocket not used)"""
    resp = dhan.ohlc_data(securities={"NSE_EQ": [str(security_id)]})
    data = resp.get("data", [])
    if not data:
        return None
    return data[0].get("last_price")
