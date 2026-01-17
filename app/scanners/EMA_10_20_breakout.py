import time
import logging
import pandas as pd
from ta.trend import EMAIndicator
from datetime import datetime, timedelta, date
from pytz import timezone
from dhanhq import DhanContext, dhanhq

from app.config.aws_ssm import get_ssm_param
from app.config.aws_s3 import read_csv_from_s3
from app.config.settings import (
    IST,
    LOG_DIR,
    S3_BUCKET,
    MAP_FILE_KEY,
    EOD_DATA_PREFIX
)

# ==============================
# LOGGING
# ==============================
def setup_logging():
    import os
    os.makedirs(LOG_DIR, exist_ok=True)
    log_path = f"{LOG_DIR}/ema_10_20_breakout.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s — %(levelname)s — %(message)s",
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler()
        ],
    )
    return log_path


# ==============================
# MAIN SCANNER
# ==============================
def run_emabreakout_check():
    setup_logging()

    # ---- Load secrets from SSM ----
    client_id = get_ssm_param("/dhan/client_id")
    access_token = get_ssm_param("/dhan/access_token")

    dhan = dhanhq(DhanContext(client_id, access_token))

    today_date = datetime.now(IST).date()
    today = pd.Timestamp(today_date)

    # NSE holidays
    NSE_HOLIDAYS = {
        date(2025, 1, 26),
        date(2025, 3, 29),
        date(2025, 4, 14),
        date(2025, 5, 1),
        date(2025, 8, 15),
        date(2025, 10, 2),
        date(2025, 10, 24),
        date(2025, 12, 25),
    }

    def last_trading_day(cur):
        d = cur - timedelta(days=1)
        while d.weekday() >= 5 or d in NSE_HOLIDAYS:
            d -= timedelta(days=1)
        return d

    prev_td = last_trading_day(today_date)

    # ---- Load mapping.csv from S3 ----
    df_map = read_csv_from_s3(S3_BUCKET, MAP_FILE_KEY)
    df_map = df_map[["Stock Name", "Instrument ID", "Market Cap", "Setup_Case"]].dropna()
    df_map["Instrument ID"] = df_map["Instrument ID"].astype(int)

    instrument_ids = df_map["Instrument ID"].tolist()

    # ---- Fetch live quotes ----
    live_data = {}
    for i in range(0, len(instrument_ids), 1000):
        batch = instrument_ids[i:i + 1000]
        try:
            resp = dhan.quote_data(securities={"NSE_EQ": batch})
            live_data.update(resp["data"]["data"].get("NSE_EQ", {}))
        except Exception as e:
            logging.error(f"Quote API error: {e}")
        time.sleep(0.5)

    matched = []

    # ---- Process each stock ----
    for _, row in df_map.iterrows():
        stock = row["Stock Name"]
        instrument_id = row["Instrument ID"]
        market_cap = float(row["Market Cap"])

        eod_key = f"{EOD_DATA_PREFIX}/{instrument_id}.csv"

        try:
            df = read_csv_from_s3(S3_BUCKET, eod_key)
        except Exception:
            logging.warning(f"EOD missing for {stock}")
            continue

        if str(instrument_id) not in live_data:
            continue

        try:
            df.columns = df.columns.str.lower()
            df["date"] = pd.to_datetime(df["date"])
            df.set_index("date", inplace=True)

            live = live_data[str(instrument_id)]
            ohlc = live["ohlc"]

            df.loc[today] = {
                "open": ohlc["open"],
                "high": ohlc["high"],
                "low": ohlc["low"],
                "close": live["last_price"],
                "volume": live["volume"],
            }
            df.sort_index(inplace=True)

            if len(df) < 50:
                continue

            df["ema10"] = EMAIndicator(df["close"], 10).ema_indicator()
            df["ema20"] = EMAIndicator(df["close"], 20).ema_indicator()
            df["ema50"] = EMAIndicator(df["close"], 50).ema_indicator()

            latest = df.iloc[-1]
            prev = df.iloc[-2]

            overall = (
                latest["close"] > 100
                and market_cap > 500
                and latest["volume"] > 70000
                and latest["low"] > latest["open"] * 0.96
                and latest["close"] > prev["high"]
                and latest["ema10"] > latest["ema50"]
                and latest["ema20"] > latest["ema50"]
            )

            if overall:
                logging.info(f"🚀 EMA BREAKOUT → {stock}")
                matched.append({"Stock": stock, "Price": latest["close"]})

        except Exception as e:
            logging.error(f"{stock} failed: {e}")

    return (
        pd.DataFrame(matched).to_csv(index=False)
        if matched
        else "No EMA breakout signals today"
    )
