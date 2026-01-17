# ==========================================================
# File: nifty_15m_opposite_breakout_scan.py
# ==========================================================

import io
import os
import time
import logging
import pandas as pd
from datetime import datetime, date
from dhanhq import DhanContext, dhanhq
from logging.handlers import RotatingFileHandler
import boto3

from app.config.settings import (
    IST,
    S3_BUCKET,
    AWS_REGION,
    MAP_FILE_KEY,
    NIFTYMAP_FILE_KEY
)
from app.config.aws_ssm import get_param

# ==========================================================
# LOGGING (NO LOGIC CHANGE)
# ==========================================================
LOG_FILE = "logs/nifty_15m_opposite_breakout.log"
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
logger.info("📈 Nifty 15m Opposite Breakout Scanner started")

# ==========================================================
# AWS CLIENTS
# ==========================================================
s3 = boto3.client("s3", region_name=AWS_REGION)

# ==========================================================
# DHAN API (SSM)
# ==========================================================
dhan = dhanhq(
    DhanContext(
        client_id=get_param("/dhan/client_id"),
        access_token=get_param("/dhan/access_token"),
    )
)

# ==========================================================
# S3 HELPERS (LOGGING ONLY)
# ==========================================================
def read_csv_from_s3(key):
    try:
        logger.info(f"📥 Reading S3 file: s3://{S3_BUCKET}/{key}")
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        df = pd.read_csv(io.BytesIO(obj["Body"].read()))
        logger.info(f"✅ Loaded {len(df)} rows from {key}")
        return df
    except Exception:
        logger.exception(f"❌ Failed to read S3 file: {key}")
        raise


def write_csv_to_s3(df, key):
    try:
        logger.info(f"📤 Writing {len(df)} rows to S3 → {key}")
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=csv_buffer.getvalue()
        )
        logger.info(f"✅ Uploaded to s3://{S3_BUCKET}/{key}")
    except Exception:
        logger.exception(f"❌ Failed to upload {key}")

# ==========================================================
# LOAD NIFTY MAPPING (S3)
# ==========================================================
def load_nifty_mapping():
    logger.info("📊 Loading NIFTY mapping")
    df = read_csv_from_s3(NIFTYMAP_FILE_KEY)
    logger.info(f"Columns found: {list(df.columns)}")

    security_ids = df["Instrument ID"].dropna().astype(int).tolist()

    id_to_name = dict(
        zip(df["Instrument ID"].astype(str), df["Stock Name"])
    )

    id_to_leverage = dict(
        zip(
            df["Instrument ID"].astype(str),
            df.get("MIS_LEVERAGE", 1)
        )
    )

    logger.info(f"✅ Loaded {len(security_ids)} NIFTY stocks")
    return security_ids, id_to_name, id_to_leverage


nifty_security_ids, nifty_id_to_stock_name, nifty_id_to_leverage = load_nifty_mapping()

# ==========================================================
# FUNDS & POSITION SIZE
# ==========================================================
def get_available_balance():
    try:
        r = dhan.get_fund_limits()
        return float(r["data"].get("availabelBalance", 0))
    except Exception:
        logger.exception("❌ Failed to fetch fund limits")
        return 0


def calculate_position_size(price, entry, sl, sec_id):
    sl_point = abs(entry - sl)
    if sl_point == 0:
        return 0, 0, 0

    max_loss = 1000
    qty_by_risk = int(max_loss / sl_point)

    leverage = nifty_id_to_leverage.get(str(sec_id), 1)
    fund = get_available_balance()
    qty_by_fund = int((fund * leverage) / price)

    qty = min(qty_by_risk, qty_by_fund)
    return qty, qty * sl_point, qty * price

# ==========================================================
# UTILITIES
# ==========================================================
def candle_color(c):
    if c["close"] > c["open"]:
        return "GREEN"
    if c["close"] < c["open"]:
        return "RED"
    return "DOJI"


def is_market_open():
    now = datetime.now(IST).time()
    return now >= datetime.strptime("09:15", "%H:%M").time()

# ==========================================================
# FETCH FIRST TWO 15M CANDLES
# ==========================================================
def get_first_two_15m_candles(security_id):
    logger.debug(f"Fetching candles for {security_id}")
    today = date.today().strftime("%Y-%m-%d")
    time.sleep(0.2)

    r = dhan.intraday_minute_data(
        security_id=str(security_id),
        exchange_segment="NSE_EQ",
        instrument_type="EQUITY",
        from_date=today,
        to_date=today,
        interval=15,
    )

    d = r.get("data", {})
    if not d or not d.get("timestamp"):
        logger.warning(f"No candle data for {security_id}")
        return None

    df = pd.DataFrame({
        "datetime": pd.to_datetime(d["timestamp"], unit="s", utc=True).tz_convert(IST),
        "open": d["open"],
        "high": d["high"],
        "low": d["low"],
        "close": d["close"],
    })

    return df.sort_values("datetime").head(2)

# ==========================================================
# BUILD OPPOSITE RANGES (S3)
# ==========================================================
def build_opposite_ranges():
    logger.info("📐 Building opposite ranges")
    today = date.today().strftime("%Y-%m-%d")
    rows = []

    for sec_id in nifty_security_ids:
        candles = get_first_two_15m_candles(sec_id)
        if candles is None or len(candles) < 2:
            continue

        c1, c2 = candles.iloc[0], candles.iloc[1]

        if candle_color(c1) != candle_color(c2):
            rows.append({
                "date": today,
                "security_id": sec_id,
                "stock_name": nifty_id_to_stock_name.get(str(sec_id)),
                "c2_high": c2["high"],
                "c2_low": c2["low"],
                "range_high": max(c1["high"], c2["high"]),
                "range_low": min(c1["low"], c2["low"]),
            })

    if rows:
        df = pd.DataFrame(rows)
        write_csv_to_s3(df, "uploads/nifty_15m_opposite_ranges.csv")
        logger.info(f"✅ Opposite ranges created: {len(df)} rows")
    else:
        logger.warning("⚠️ No opposite ranges found")

# ==========================================================
# LIVE BREAKOUT SCAN (S3)
# ==========================================================
def scan_nifty_stocks():
    logger.info("🔍 Starting breakout scan")

    ranges = read_csv_from_s3(
        "uploads/nifty_15m_opposite_ranges.csv"
    ).set_index("security_id").to_dict("index")

    logger.info(f"Loaded {len(ranges)} ranges")

    quote_data = dhan.quote_data(
        securities={"NSE_EQ": nifty_security_ids}
    )

    results = []

    for sec_id, stock_data in quote_data["data"]["data"]["NSE_EQ"].items():
        try:
            sec_id = int(sec_id)
            if sec_id not in ranges:
                continue

            price = float(stock_data["last_price"])
            r = ranges[sec_id]

            if price > r["range_high"]:
                signal, entry, sl = "BUY", r["range_high"], r["c2_low"]
            elif price < r["range_low"]:
                signal, entry, sl = "SELL", r["range_low"], r["c2_high"]
            else:
                continue

            qty, loss, exposure = calculate_position_size(price, entry, sl, sec_id)
            if qty <= 0:
                continue

            results.append({
                "Stock Name": r["stock_name"],
                "Security ID": sec_id,
                "Price": price,
                "Signal": signal,
                "Entry": entry,
                "SL": sl,
                "Quantity": qty,
                "Expected Loss": round(loss, 2),
                "Exposure": exposure,
            })
        except Exception:
            logger.exception(f"❌ Error processing security {sec_id}")

    if results:
        write_csv_to_s3(
            pd.DataFrame(results),
            "uploads/nifty_15m_breakout_signals.csv"
        )
        logger.info(f"🚀 NIFTY Breakout signals generated: {len(results)}")

    return results

# ==========================================================
# MAIN
# ==========================================================
if __name__ == "__main__":
    if not is_market_open():
        logger.warning("⛔ Market closed")
        exit()

    build_opposite_ranges()
    scan_nifty_stocks()

