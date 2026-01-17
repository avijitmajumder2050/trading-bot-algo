# ==========================================================
# File: ema200_breakout_swing.py
# ==========================================================
import os
import io
import boto3
import pandas as pd
import logging
from logging.handlers import RotatingFileHandler
from ta.trend import EMAIndicator
from datetime import datetime
from app.config.settings import S3_BUCKET, MAP_FILE_KEY, EOD_DATA_PREFIX

# ─────────────────────────────
# LOGGING SETUP
# ─────────────────────────────
LOG_FILE = "logs/ema200_breakout_swing.log"
os.makedirs("logs", exist_ok=True)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(filename)s:%(lineno)d | %(message)s"
)

if not logger.handlers:
    file_handler = logging.handlers.RotatingFileHandler(
        LOG_FILE, maxBytes=5*1024*1024, backupCount=5
    )
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

logger.propagate = False
logger.info("🚀 EMA200 Breakout Swing module loaded")

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

def upload_file_to_s3(local_path: str, bucket: str, s3_key: str):
    try:
        s3.upload_file(local_path, bucket, s3_key)
        logger.info(f"☁️ Uploaded to S3 → s3://{bucket}/{s3_key}")
    except Exception:
        logger.exception(f"❌ S3 upload failed for {local_path}")

# === Main EMA200 scanner ===
def run_ema200_scanner():
    # Load mapping file from S3
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
            logger.warning(f"⚠️ Missing file for {stock} in S3 → {s3_key}")
            continue

        df.columns = df.columns.str.lower()
        if "close" not in df.columns or "date" not in df.columns:
            logger.warning(f"⚠️ Skipping {stock} (missing 'date' or 'close')")
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
                    logger.info(f"✅ {stock}: crossover on {cross_date.date()} — alignment valid")
                else:
                    watchlist_results.append({
                        "Stock Name": stock,
                        "Instrument ID": instrument_id,
                        "Cross Date": cross_date,
                        "Date": latest["date"],
                        "Close": latest["close"]
                    })
        except Exception:
            logger.exception(f"❌ Error in {stock}")

    # Save locally
    if aligned_results:
        pd.DataFrame(aligned_results).to_csv(OUTPUT_ALIGNED, index=False)
        logger.info(f"\n✅ Aligned breakout saved → {OUTPUT_ALIGNED}")
    else:
        logger.warning("\n⚠️ No stocks fully aligned today")

    if watchlist_results:
        pd.DataFrame(watchlist_results).to_csv(OUTPUT_WATCHLIST, index=False)
        logger.info(f"\n⚠️ Watchlist saved → {OUTPUT_WATCHLIST}")
    else:
        logger.info("\nℹ️ No stocks in watchlist today")

    # === Upload CSVs to fixed S3 path ===
    if os.path.exists(OUTPUT_ALIGNED):
        upload_file_to_s3(
            OUTPUT_ALIGNED,
            S3_BUCKET,
            "uploads/ema200_breakout_alignment.csv"
        )

    if os.path.exists(OUTPUT_WATCHLIST):
        upload_file_to_s3(
            OUTPUT_WATCHLIST,
            S3_BUCKET,
            "uploads/ema200_breakout_watchlist.csv"
        )

    return aligned_results, watchlist_results

# === Run directly ===
if __name__ == "__main__":
    logger.info("🔍 Running EMA200 breakout scanner...")
    run_ema200_scanner()
