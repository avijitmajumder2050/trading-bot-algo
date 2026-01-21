# ==========================================================
# File: nifty_15m_opposite_breakout_scan.py
# ==========================================================

import io
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
# LOGGING
# ==========================================================
LOG_FILE = "logs/nifty_15m_opposite_breakout.log"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(message)s"
)

file_handler = RotatingFileHandler(
    LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=5
)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

logger.handlers.clear()
logger.addHandler(file_handler)
logger.addHandler(console_handler)

logger.info("📈 Nifty 15m Opposite Breakout Scanner started")

# ==========================================================
# AWS CLIENTS
# ==========================================================
s3 = boto3.client("s3", region_name=AWS_REGION)

# ==========================================================
# DHAN API
# ==========================================================
dhan = dhanhq(
    DhanContext(
        client_id=get_param("/dhan/client_id"),
        access_token=get_param("/dhan/access_token"),
    )
)

# ==========================================================
# S3 HELPERS
# ==========================================================
def read_csv_from_s3(key):
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))

def write_csv_to_s3(df, key):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=csv_buffer.getvalue())
    logger.info(f"✅ Uploaded → s3://{S3_BUCKET}/{key}")

# ==========================================================
# LOAD NIFTY MAPPING
# ==========================================================
def load_nifty_mapping():
    df = read_csv_from_s3(MAP_FILE_KEY)

    security_ids = df["Instrument ID"].dropna().astype(int).tolist()

    id_to_name = dict(zip(df["Instrument ID"].astype(str), df["Stock Name"]))
    id_to_leverage = dict(
        zip(df["Instrument ID"].astype(str), df.get("MIS_LEVERAGE", 1))
    )

    logger.info(f"Loaded {len(security_ids)} NIFTY stocks")
    return security_ids, id_to_name, id_to_leverage

nifty_security_ids, nifty_id_to_stock_name, nifty_id_to_leverage = load_nifty_mapping()

# ==========================================================
# FUNDS
# ==========================================================
def get_available_balance():
    try:
        r = dhan.get_fund_limits()

        if r.get("status") != "success":
            logger.error(f"❌ Fund API status error: {r}")
            return 0

        data = r.get("data")
        if not isinstance(data, dict):
            logger.error(f"❌ Fund API returned invalid data: {data}")
            return 0

        balance = float(data.get("availabelBalance", 0))
        logger.info(f"💰 Available Balance: {balance}")
        return balance

    except Exception:
        logger.exception("❌ Failed to fetch fund limits")
        return 0

# ==========================================================
# POSITION SIZE
# ==========================================================
def calculate_position_size(price, entry, sl, sec_id, available_fund):
    sl_point = abs(entry - sl)
    if sl_point == 0:
        return 0, 0, 0

    max_loss = 1000
    qty_by_risk = int(max_loss / sl_point)

    leverage = nifty_id_to_leverage.get(str(sec_id), 1)
    if str(sec_id) not in nifty_id_to_leverage:
        logger.warning(f"⚠️ Leverage missing for sec_id={sec_id}. Using default=1")
    else:
        logger.info(f"📊 Leverage for sec_id={sec_id} = {leverage}")

    qty_by_fund = int((available_fund * leverage) / price)
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
# FIRST TWO 15M CANDLES
# ==========================================================
def get_first_two_15m_candles(security_id):
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
    if not isinstance(d, dict) or not d.get("timestamp"):
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
# BUILD OPPOSITE RANGES
# ==========================================================
def build_opposite_ranges():
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
        write_csv_to_s3(pd.DataFrame(rows), "uploads/nifty_15m_opposite_ranges.csv")

# ==========================================================
# DHAN QUOTE WITH RETRY
# ==========================================================
def get_nse_quotes_with_retry(security_ids):
    for attempt in (1, 2):
        try:
            logger.info(f"📡 Fetching DHAN quotes (attempt {attempt})")
            q = dhan.quote_data(securities={"NSE_EQ": security_ids})
            nse = q.get("data", {}).get("data", {}).get("NSE_EQ")

            if not isinstance(nse, dict):
                raise ValueError(q)

            logger.info(f"✅ Quotes fetched: {len(nse)}")
            return nse

        except Exception as e:
            logger.error(f"❌ Quote fetch failed: {e}")
            if attempt == 1:
                time.sleep(1)

    return None

# ==========================================================
# LIVE SCAN
# ==========================================================
def scan_nifty_stocks():
    logger.info("🔍 Starting breakout scan")

    df = read_csv_from_s3("uploads/nifty_15m_opposite_ranges.csv")
    if df.empty:
        return []

    df["security_id"] = df["security_id"].astype(int)
    ranges = df.set_index("security_id").to_dict("index")

    available_fund = get_available_balance()
    if available_fund <= 0:
        logger.warning("No available funds")
        return []

    quotes = get_nse_quotes_with_retry(nifty_security_ids)
    if not quotes:
        return []

    results = []

    for sec_id, stock_data in quotes.items():
        try:
            sec_id = int(sec_id)
            r = ranges.get(sec_id)
            if not r:
                continue

            price = float(stock_data["last_price"])

            if price > r["range_high"]:
                signal, entry, sl = "BUY", r["range_high"], r["c2_low"]
            elif price < r["range_low"]:
                signal, entry, sl = "SELL", r["range_low"], r["c2_high"]
            else:
                continue

            qty, loss, exposure = calculate_position_size(
                price, entry, sl, sec_id, available_fund
            )

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

        except Exception as e:
            logger.error(f"Skipping sec_id={sec_id}: {e}")

    if results:
        write_csv_to_s3(
            pd.DataFrame(results),
            "uploads/nifty_15m_breakout_signals.csv"
        )

    return results

# ==========================================================
# MAIN
# ==========================================================
if __name__ == "__main__":
    if not is_market_open():
        logger.warning("Market closed")
        exit()

    build_opposite_ranges()
    scan_nifty_stocks()
