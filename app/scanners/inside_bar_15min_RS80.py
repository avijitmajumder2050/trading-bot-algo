# app/scanners/inside_bar_15min_RS80.py
import time
import pandas as pd
import pytz
from datetime import datetime, date
import boto3
from app.config.settings import S3_BUCKET, AWS_REGION, IST, MAP_FILE_KEY
from app.config.aws_ssm import get_param
from dhanhq import DhanContext, dhanhq

# === Load secrets from SSM ===
client_id = get_param("/dhan/client_id")
access_token = get_param("/dhan/access_token")

# === Dhan API ===
dhan = dhanhq(DhanContext(client_id=client_id, access_token=access_token))

# === S3 Client ===
s3 = boto3.client("s3", region_name=AWS_REGION)

# === S3 File Keys ===
CANDLE_FILE_KEY = "uploads/15min_data_RS80.csv"
FILTERED_KEY = "uploads/inside_bar_15min_RS80.csv"
BREAKOUT_KEY = "uploads/insidebar_breakouts_RS80.csv"

# === Scan state ===
last_insidebar_run_date = None

# ----------------------------
# S3 Helper Functions
# ----------------------------
def read_s3_csv(key):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        df = pd.read_csv(obj['Body'])
        df.columns = df.columns.str.lower()
        return df
    except Exception as e:
        print(f"❌ Failed to read S3 CSV {key}: {e}")
        return pd.DataFrame()

def write_s3_csv(df, key):
    try:
        csv_buffer = df.to_csv(index=False)
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=csv_buffer)
        print(f"💾 Saved CSV to S3: {key}")
    except Exception as e:
        print(f"❌ Failed to write CSV to S3 {key}: {e}")

# ----------------------------
# Fetch 15-min candles
# ----------------------------
def get_15min_candles(security_id, interval):
    try:
        time.sleep(0.2)
        from_date = date.today().strftime('%Y-%m-%d')
        to_date = from_date
        response = dhan.intraday_minute_data(
            security_id=security_id,
            exchange_segment='NSE_EQ',
            instrument_type='EQUITY',
            from_date=from_date,
            to_date=to_date,
            interval=interval
        )
        data = response.get("data", {})
        if not data or not data.get("timestamp"):
            return None

        df = pd.DataFrame({
            "datetime": pd.to_datetime(data["timestamp"], unit="s"),
            "open": data["open"],
            "high": data["high"],
            "low": data["low"],
            "close": data["close"],
            "volume": data["volume"]
        }).sort_values("datetime").reset_index(drop=True)

        return df
    except Exception as e:
        print(f"❌ 15min candle error for {security_id}: {e}")
        return None

# ----------------------------
# Inside Bar Pattern Detection
# ----------------------------
def save_inside_bars(candle_df, map_df):
    filtered = []
    today_str = date.today().strftime('%Y-%m-%d')

    for _, row in map_df.iterrows():
        sec_id = int(row["instrument id"])
        stock_name = row["stock name"]

        df = candle_df[candle_df["security id"] == sec_id].copy()
        df["datetime"] = pd.to_datetime(df["datetime"])
        if df["datetime"].dt.tz is None or df["datetime"].dt.tz is pytz.UTC:
            df["datetime"] = df["datetime"].dt.tz_localize("UTC").dt.tz_convert(IST)

        df["date"] = df["datetime"].dt.date.astype(str)
        df_today = df[df["date"] == today_str].sort_values("datetime").reset_index(drop=True)
        df_today = df_today[df_today["datetime"].dt.time <= datetime.strptime("09:45", "%H:%M").time()]
        if len(df_today) < 2:
            continue

        c1, c2 = df_today.iloc[0], df_today.iloc[1]
        if c2["high"] < c1["high"] and c2["low"] > c1["low"]:
            filtered.append({
                "stock name": stock_name,
                "security id": sec_id,
                "1st 15m high": c1["high"],
                "1st 15m low": c1["low"],
                "2nd 15m high": c2["high"],
                "2nd 15m low": c2["low"]
            })
            print(f"✅ Inside Bar: {stock_name}")

    if filtered:
        write_s3_csv(pd.DataFrame(filtered), FILTERED_KEY)

# ----------------------------
# Main Scanner
# ----------------------------
def run_inside_bar_algo_scan(interval):
    global last_insidebar_run_date
    if last_insidebar_run_date == date.today():
        print("⏩ Inside bar scan already run today")
        return

    df_map = read_s3_csv(MAP_FILE_KEY)
    df_map = df_map[df_map["setup_case"].isin(["Case A", "Case B"])]
    df_map["instrument id"] = df_map["instrument id"].astype(int)

    all_data = []
    for _, row in df_map.iterrows():
        sec_id = int(row["instrument id"])
        stock_name = row["stock name"]
        df = get_15min_candles(sec_id, interval)
        if df is None:
            continue
        df["stock name"] = stock_name
        df["security id"] = sec_id
        all_data.append(df)
        print(f"✅ {stock_name} — {len(df)} candles")

    if not all_data:
        print("🛑 No 15min candle data fetched.")
        return

    df_candles = pd.concat(all_data, ignore_index=True)
    df_candles["datetime"] = pd.to_datetime(df_candles["datetime"]).dt.tz_localize("UTC").dt.tz_convert(IST)
    save_inside_bars(df_candles.copy(), df_map)

    # Save 15-min candles to S3
    df_candles["datetime"] = df_candles["datetime"].dt.strftime('%Y-%m-%d %H:%M:%S')
    write_s3_csv(df_candles, CANDLE_FILE_KEY)

    last_insidebar_run_date = date.today()


# ----------------------------
# Main
# ----------------------------
if __name__ == "__main__":
    run_inside_bar_algo_scan(interval=5)
