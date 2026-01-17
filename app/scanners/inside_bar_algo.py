import os
import time
import pandas as pd
import pytz
import boto3
import logging
from io import StringIO
from datetime import datetime, date, timedelta
from logging.handlers import RotatingFileHandler
from dhanhq import DhanContext, dhanhq

from app.config.settings import (
    S3_BUCKET, AWS_REGION, IST,
    MAP_FILE_KEY, CANDLE_FILE_KEY, FILTERED_FILE_KEY
)
from app.config.aws_ssm import get_param
from app.utils.alert_goodresult import strong_quarterly_alert

# ─────────────────────────────
# LOGGING (NO LOGIC CHANGE)
# ─────────────────────────────
LOG_FILE = "logs/insidebar_breakout.log"
os.makedirs("logs", exist_ok=True)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(filename)s:%(lineno)d | %(message)s"
)

if not logger.handlers:
    file_handler = RotatingFileHandler(
        LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=5
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

logger.propagate = False
logger.info("🚀 Inside-Bar Breakout module loaded")

# ─────────────────────────────
# AWS S3 Client
# ─────────────────────────────
s3 = boto3.client("s3", region_name=AWS_REGION)

# ─────────────────────────────
# Dhan Credentials from SSM
# ─────────────────────────────
DHAN_CLIENT_ID = get_param("/dhan/client_id")
DHAN_ACCESS_TOKEN = get_param("/dhan/access_token")
dhan = dhanhq(DhanContext(client_id=DHAN_CLIENT_ID, access_token=DHAN_ACCESS_TOKEN))

# ─────────────────────────────
# Globals
# ─────────────────────────────
last_insidebar_run_date = None
cached_fund = None

# ─────────────────────────────
# Helpers
# ─────────────────────────────
def load_csv_from_s3(key):
    try:
        logger.info(f"📥 Loading CSV from S3: {key}")
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        csv_str = obj["Body"].read().decode("utf-8")
        df = pd.read_csv(StringIO(csv_str))
        df.columns = df.columns.str.lower()
        logger.info(f"✅ Loaded {len(df)} rows from {key}")
        return df
    except Exception:
        logger.exception(f"❌ Failed to load CSV from S3 ({key})")
        return pd.DataFrame()

def upload_csv_to_s3(df, key):
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=csv_buffer.getvalue())
        logger.info(f"💾 Uploaded CSV to S3: {key}")
    except Exception:
        logger.exception(f"❌ Failed to upload CSV to S3 ({key})")

def get_available_balance():
    global cached_fund
    if cached_fund is not None:
        return cached_fund
    try:
        resp = dhan.get_fund_limits()
        cached_fund = float(resp["data"].get("availabelBalance", 0))
        logger.info(f"💰 Available fund: {cached_fund}")
        return cached_fund
    except Exception:
        logger.exception("⚠️ Error fetching available balance")
        return 0

def calculate_position_size(price, entry, sl_price, sec_id, leverage_dict=None):
    sl_point = abs(entry - sl_price)
    if sl_point == 0:
        return 0, 0, 0

    max_loss = 1000
    qty_by_risk = int(max_loss / sl_point)
    leverage = leverage_dict.get(str(sec_id), 1) if leverage_dict else 1

    fund = get_available_balance()
    qty_by_fund = int((fund * leverage) / price)
    quantity = min(qty_by_risk, qty_by_fund)

    return quantity, quantity * sl_point, quantity * price

def safe_extract_quotes(resp):
    if not isinstance(resp, dict):
        return {}
    if "data" in resp and isinstance(resp["data"], dict):
        return resp["data"].get("data", {}).get("NSE_EQ", {})
    return resp.get("NSE_EQ", {})

def batch(lst, size=400):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

# ─────────────────────────────
# Inside-Bar Breakout Tracker
# ─────────────────────────────
def track_insidebar_algo_breakouts_bot():
    global cached_fund

    df_filtered = load_csv_from_s3(FILTERED_FILE_KEY)
    if df_filtered.empty:
        logger.warning("🚫 Inside-bar list is empty")
        return []

    breakout_hits = []
    sec_ids = df_filtered["security id"].astype(int).tolist()
    cached_fund = get_available_balance()

    for pkt in batch(sec_ids):
        time.sleep(1.1)
        resp = dhan.quote_data(securities={"NSE_EQ": pkt})
        live_quotes = safe_extract_quotes(resp)

        if not live_quotes:
            logger.warning(f"⚠️ Empty quote packet")
            continue

        for sec_id in pkt:
            row = df_filtered[df_filtered["security id"] == sec_id].iloc[0]
            high_1st = float(row["1st 15m high"])
            low_1st = float(row["1st 15m low"])

            quote = live_quotes.get(str(sec_id))
            if not quote:
                continue

            day_low = float(quote["ohlc"]["low"])
            if low_1st > day_low:
                logger.info(
                    f"🛑 Skipping {row['stock name']} – day low {day_low} < 1st-15m low {low_1st}"
                )
                continue

            ltp = float(quote["last_price"])
            if ltp > high_1st:
                sl = float(row["2nd 15m low"])
                sl_point = high_1st - sl
                sl_percent = (sl_point / high_1st) * 100

                if sl_percent > 2:
                    logger.info(
                        f"❌ Skipping {row['stock name']} — SL {sl_percent:.2f}% > 2%"
                    )
                    continue

                qty, loss, exp = calculate_position_size(
                    ltp, high_1st, sl, sec_id
                )
                if qty == 0:
                    continue

                breakout_hits.append({
                    "Stock Name": row["stock name"],
                    "Security ID": sec_id,
                    "Price": ltp,
                    "Signal": "BUY",
                    "Entry": high_1st,
                    "SL": sl,
                    "SL_Point": round(sl_point, 2),
                    "Quantity": qty,
                    "Proximity Score": 0,
                    "Expected Loss": round(loss, 2),
                    "Exposure": round(exp, 2),
                    "Available Fund": cached_fund,
                    "Leverage": 1
                })

    # Strong Quarterly Filter
    try:
        time.sleep(2)
        alerts, _ = strong_quarterly_alert()
        valid_symbols = {a.split()[1] for a in alerts if "🔔" in a}
        breakout_hits = [
            hit for hit in breakout_hits
            if hit["Stock Name"] in valid_symbols
        ]
        logger.info(
            f"✅ After strong_quarterly_alert filter: {len(breakout_hits)} hits"
        )
    except Exception:
        logger.exception("⚠️ strong_quarterly_alert filter failed")

    return breakout_hits

# ─────────────────────────────
# Main Runner
# ─────────────────────────────
if __name__ == "__main__":
    logger.info("🔍 Starting Inside-Bar breakout scanner")
    hits = track_insidebar_algo_breakouts_bot()
    logger.info(f"✅ Breakout hits found: {len(hits)}")

