#app/strategy/nifty_filter.py
def is_nifty_trade_allowed(signal, nifty_ltp, nifty_prev_close):
    """
    BUY: Nifty today >= prev_close - 50
    SELL: Nifty today <= prev_close + 30
    """
    if signal.upper() == "BUY":
        return nifty_ltp >= nifty_prev_close - 50
    return nifty_ltp <= nifty_prev_close + 30


# Replaced is_nifty_trade_allowed() as the entry filter (2026-10-08).
# Backtest, 98 Nifty-100 stocks, 1-min data, ₹1,000 risk, live exits:
#   Apr-Jun (out of sample): Nifty filter -6.9R, breadth 50% +5.1R
#   Jul-Oct:                 Nifty filter +2.5R, breadth 50% +12.6R
# The Nifty filter (BUY unless Nifty is 50+ pts down) almost never
# blocked a BUY, so it kept taking BUY breakouts on falling-market days.
BREADTH_THRESHOLD = 0.5


def is_breadth_trade_allowed(signal, advance_fraction, threshold=BREADTH_THRESHOLD):
    """
    BUY:  at least `threshold` of the universe is above its previous close
    SELL: at most 1 - `threshold` is (i.e. at least that share is down/flat)
    """
    if signal.upper() == "BUY":
        return advance_fraction >= threshold
    return advance_fraction <= 1 - threshold
