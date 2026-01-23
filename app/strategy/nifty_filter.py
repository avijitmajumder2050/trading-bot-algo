#app/strategy/nifty_filter.py
def is_nifty_trade_allowed(signal, nifty_ltp, nifty_prev_close):
    """
    BUY: Nifty today >= prev_close - 50
    SELL: Nifty today <= prev_close + 30
    """
    if signal.upper() == "BUY":
        return nifty_ltp >= nifty_prev_close - 50
    return nifty_ltp <= nifty_prev_close + 30
