
#app/strategy/stock_selector.py
import logging
import pandas as pd

def select_best_stock(df: pd.DataFrame):
    """
    Select stock with lowest %SL.
    Ignore if only 1 stock in CSV.
    """
    if df.empty:
        logging.info("CSV empty")
        return None

    if len(df) == 1:
        logging.info("❌ Only 1 stock in CSV, skipping trade for today")
        return None

    df["SL_PCT"] = abs(df["Entry"] - df["SL"]) / df["Entry"] * 100
    best = df.sort_values("SL_PCT").iloc[0]
    logging.info(f"Selected {best['Stock Name']} | SL% {best['SL_PCT']:.2f}")
    return best.to_dict()



# Ranking purely by lowest SL% kept picking the tightest ranges, which
# get stopped out by noise. A 0.4% floor looked best on 10 days
# (2026-09-23..10-07) but not over 6 months (Apr-Oct 2026, 98 Nifty-100
# stocks, 1-min data): signals at 0.3-0.4% did better than 0.4-0.8%.
# 0.3% with the breadth filter was the only setup positive in both
# Apr-Jun (+5.1R) and Jul-Oct (+12.6R).
MIN_SL_PCT = 0.3
MAX_SL_PCT = 2.5


def rank_stocks(df: pd.DataFrame):
    """
    Keep stocks whose SL% is within [MIN_SL_PCT, MAX_SL_PCT], rank by
    lowest SL%, return list of dicts. Ignores if CSV is empty.
    """
    if df.empty:
        logging.info("CSV empty")
        return []

    # Calculate Stop-Loss % for ranking
    df["SL_PCT"] = abs(df["Entry"] - df["SL"]) / df["Entry"] * 100

    in_band = df["SL_PCT"].between(MIN_SL_PCT, MAX_SL_PCT)
    for _, s in df[~in_band].iterrows():
        logging.info(f"🚫 {s['Stock Name']} skipped | SL% {s['SL_PCT']:.2f} outside {MIN_SL_PCT}-{MAX_SL_PCT}%")

    # Sort by lowest SL% (risk)
    ranked_df = df[in_band].sort_values("SL_PCT")

    ranked_stocks = ranked_df.to_dict("records")
    logging.info(f"🔹 Ranked stocks: {[s['Stock Name'] for s in ranked_stocks]}")
    return ranked_stocks

