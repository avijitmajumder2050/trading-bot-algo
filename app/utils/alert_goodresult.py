import pandas as pd
import time
import logging
import os
import sys
from datetime import datetime
from dhanhq import DhanContext, dhanhq
from logging.handlers import RotatingFileHandler
import boto3
from io import StringIO
from app.config.settings import S3_BUCKET, AWS_REGION, IST, MAP_FILE_KEY
from app.config.aws_ssm import get_param

# === Logging Setup ===
log_file = "logs/goodresult_alerts.log"
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=5, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

# === AWS S3 Setup ===
s3 = boto3.client("s3", region_name=AWS_REGION)

# === Load credentials from SSM ===
DHAN_CLIENT_ID = get_param("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = get_param("DHAN_ACCESS_TOKEN")

# === Dhan API Setup ===
dhan_context = DhanContext(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
dhan = dhanhq(dhan_context)

# === Utilities ===
def batch_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def fetch_live_data(instrument_ids):
    live_data = {}
    logging.info(f"📡 Fetching live data for {len(instrument_ids)} instruments...")
    for batch in batch_list(instrument_ids, 1000):
        try:
            response = dhan.quote_data(securities={"NSE_EQ": batch})
            if isinstance(response, dict) and "data" in response and "data" in response["data"]:
                batch_data = response["data"]["data"].get("NSE_EQ", {})
                live_data.update({int(k): v for k, v in batch_data.items()})
                logging.info(f"✅ Received {len(batch_data)} quotes in batch")
            else:
                logging.warning("⚠️ Unexpected response structure from Dhan API")
        except Exception as e:
            logging.warning(f"⚠️ Batch fetch error: {e}")
        time.sleep(0.5)
    logging.info(f"📊 Total live quotes fetched: {len(live_data)}")
    return live_data

# === Load CSV from S3 ===
def read_csv_from_s3(key):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    except Exception as e:
        logging.error(f"❌ Failed to read CSV from S3 ({key}): {e}")
        return pd.DataFrame()

# === Load EOD data from S3 ===
def load_today_data_with_ema(instrument_id, live):
    key = f"eod_data/{instrument_id}.csv"
    df = read_csv_from_s3(key)
    if df.empty:
        logging.warning(f"⚠️ Missing EOD file for {instrument_id}")
        return None

    df = df.rename(columns=str.lower)
    df = df[["date", "open", "high", "low", "close", "volume"]].dropna()
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    df.sort_index(inplace=True)

    today = pd.Timestamp(datetime.today().date())
    ohlc = live["ohlc"]
    df.loc[today] = {
        "open": ohlc["open"],
        "high": ohlc["high"],
        "low": ohlc["low"],
        "close": live["last_price"],
        "volume": live["volume"]
    }
    df.sort_index(inplace=True)

    if len(df) < 10:
        logging.warning(f"⚠️ Not enough data for EMA calculation: {instrument_id}")
        return None

    df["ema10"] = df["close"].ewm(span=10, adjust=False).mean()
    df["ema20"] = df["close"].ewm(span=20, adjust=False).mean()
    df["ema50"] = df["close"].ewm(span=50, adjust=False).mean()

    latest = df.iloc[-1]
    prev = df.iloc[-2]
    if prev["close"] > prev["open"]:
        prev_high = prev["close"]
    elif prev["close"] < prev["open"]:
        prev_high = prev["open"]
    else:
        prev_high = prev["high"]

    return {
        "prev_high": prev_high,
        "today_low": latest["low"],
        "ema10": latest["ema10"],
        "ema20": latest["ema20"],
        "ema50": latest["ema50"]
    }

# === Main Alert Function ===
def strong_quarterly_alert():
    df_map = read_csv_from_s3(MAP_FILE_KEY)
    if df_map.empty:
        return [], []

    df_map = df_map[["Stock Name", "Instrument ID", "Market Cap", "Setup_Case"]].dropna()
    df_map["Instrument ID"] = df_map["Instrument ID"].astype(int)
    df_map = df_map[df_map["Setup_Case"].isin(["Case A", "Case B", "Case C"])]
    if df_map.empty:
        logging.info("ℹ️ No instruments with Setup_Case found.")
        return [], []

    instrument_ids = df_map["Instrument ID"].tolist()
    live_data = fetch_live_data(instrument_ids)

    breakout_rows = []
    for _, row in df_map.iterrows():
        iid = row["Instrument ID"]
        symbol = row["Stock Name"]
        live = live_data.get(iid)
        if not live:
            continue

        eod = load_today_data_with_ema(iid, live)
        if not eod:
            continue

        prev_high = eod["prev_high"]
        today_low = eod["today_low"]
        ema10, ema20, ema50 = round(eod["ema10"], 2), round(eod["ema20"], 2), round(eod["ema50"], 2)
        ltp = live.get("last_price", 0)

        if ltp > prev_high and ema20 > ema50 and (today_low < ema10 or today_low < ema20):
            change = round((ltp - prev_high) / prev_high * 100, 2)
            breakout_rows.append({
                "symbol": symbol,
                "ltp": ltp,
                "prev_high": prev_high,
                "change": change,
                "setup_case": row["Setup_Case"],
                "ema10": ema10,
                "ema20": ema20
            })

    breakout_rows.sort(key=lambda x: x["change"], reverse=True)
    top_15 = breakout_rows[:15]
    alerts = [f"🔔 {b['symbol']} LTP {b['ltp']} > Prev High {b['prev_high']} (+{b['change']}%)" for b in top_15]

    return alerts, top_15

# === Run Script ===
if __name__ == "__main__":
    logging.info("🚀 Running stock breakout alert check...")
    alerts, popups = strong_quarterly_alert()
    print(alerts)
