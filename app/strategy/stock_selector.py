
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



def rank_stocks(df: pd.DataFrame):
    """
    Rank stocks by lowest SL% (risk), return list of dicts.
    Ignores if CSV is empty.
    """
    if df.empty:
        logging.info("CSV empty")
        return []

    if len(df) == 1:
        logging.info("❌ Only 1 stock in CSV, skipping trade for today")
        return []

    # Calculate Stop-Loss % for ranking
    df["SL_PCT"] = abs(df["Entry"] - df["SL"]) / df["Entry"] * 100

    # Sort by lowest SL% (risk)
    ranked_df = df.sort_values("SL_PCT")

    ranked_stocks = ranked_df.to_dict("records")
    logging.info(f"🔹 Ranked stocks: {[s['Stock Name'] for s in ranked_stocks]}")
    return ranked_stocks

