import os
import io
import boto3
import pandas as pd
from ta.trend import EMAIndicator
from datetime import datetime
from app.config.settings import S3_BUCKET, MAP_FILE_KEY, EOD_DATA_PREFIX

# === AWS S3 Client ===
s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "ap-south-1"))

# === Output folders locally ===
OUTPUT_DIR = "outputs"
OUTPUT_ALIGNED = os.path.join(OUTPUT_DIR, "ema200_breakout_alignment.csv")
OUTPUT_WATCHLIST = os.path.join(OUTPUT_DIR, "ema200_breakout_watchlist.csv")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# === Helper Functions ===
def calculate_ema(df, period):
    return EMAIndicator(close=df["close"], window=period).ema_indicator()

def detect_recent_ema200_cross(df, lookback_days=10):
    if len(df) < 200:
        return False, None

    df["ema10"] = calculate_ema(df, 10)
    df["ema20"] = calculate_ema(df, 20)
    df["ema50"] = calculate_ema(df, 50)
    df["ema200"] = calculate_ema(df, 200)

    crossed_recently = False
    cross_date = None

    for i in range(-lookback_days, 0):
        if i - 1 >= -len(df):
            prev = df.iloc[i - 1]
            curr = df.iloc[i]
            if prev["close"] < prev["ema200"] and curr["close"] > curr["ema200"]:
                crossed_recently = True
                cross_date = curr["date"]
                break

    return crossed_recently, cross_date

def check_alignment(latest):
    """Return True if EMA10 > EMA20 > EMA50 > EMA200"""
    return latest["ema10"] > latest["ema20"] > latest["ema50"] > latest["ema200"]

# === S3 Helpers ===
def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))

def run_ema200_scanner():
    # === Load mapping file from S3 ===
    df_map = read_csv_from_s3(S3_BUCKET, MAP_FILE_KEY)[["Stock Name", "Instrument ID"]].dropna()
    df_map["Instrument ID"] = df_map["Instrument ID"].astype(int)

    aligned_results = []
    watchlist_results = []

    for _, row in df_map.iterrows():
        stock = row["Stock Name"]
        instrument_id = row["Instrument ID"]
        s3_key = f"{EOD_DATA_PREFIX}/{instrument_id}.csv"

        try:
            df = read_csv_from_s3(S3_BUCKET, s3_key)
        except Exception:
            print(f"⚠️ Missing file for {stock} in S3 → {s3_key}")
            continue

        df.columns = df.columns.str.lower()
        if "close" not in df.columns or "date" not in df.columns:
            print(f"⚠️ Skipping {stock} (missing 'date' or 'close')")
            continue

        df["date"] = pd.to_datetime(df["date"])
        df = df[["date", "close"]].dropna()

        try:
            crossed, cross_date = detect_recent_ema200_cross(df)
            latest = df.iloc[-1]

            if crossed:
                if check_alignment(latest):
                    aligned_results.append({
                        "Stock Name": stock,
                        "Instrument ID": instrument_id,
                        "Cross Date": cross_date,
                        "Date": latest["date"],
                        "Close": latest["close"]
                    })
                    print(f"✅ {stock}: crossover on {cross_date.date()} — alignment valid")
                else:
                    watchlist_results.append({
                        "Stock Name": stock,
                        "Instrument ID": instrument_id,
                        "Cross Date": cross_date,
                        "Date": latest["date"],
                        "Close": latest["close"]
                    })
                    #print(f"⚠️ {stock}: crossover on {cross_date.date()} — alignment NOT yet")
        except Exception as e:
            print(f"❌ Error in {stock}: {e}")

    # Save locally
    if aligned_results:
        pd.DataFrame(aligned_results).to_csv(OUTPUT_ALIGNED, index=False)
        print(f"\n✅ Aligned breakout saved → {OUTPUT_ALIGNED}")
    else:
        print("\n⚠️ No stocks fully aligned today")

    if watchlist_results:
        pd.DataFrame(watchlist_results).to_csv(OUTPUT_WATCHLIST, index=False)
        print(f"\n⚠️ Watchlist saved → {OUTPUT_WATCHLIST}")
    else:
        print("\nℹ️ No stocks in watchlist today")

    return aligned_results, watchlist_results

# === Run directly ===
if __name__ == "__main__":
    run_ema200_scanner()
