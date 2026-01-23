
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
