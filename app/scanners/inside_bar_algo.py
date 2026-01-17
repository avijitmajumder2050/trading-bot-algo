import os
import time
import pandas as pd
import pytz
import boto3
from io import StringIO
from datetime import datetime, date, timedelta
from dhanhq import DhanContext, dhanhq
from app.config.settings import S3_BUCKET, AWS_REGION, IST, MAP_FILE_KEY, CANDLE_FILE_KEY, FILTERED_FILE_KEY
from app.config.aws_ssm import get_param
from app.utils.alert_goodresult import strong_quarterly_alert

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
cached_fund = None  # cached available fund to reduce API hits

# ─────────────────────────────
# Helpers
# ─────────────────────────────
def load_csv_from_s3(key):
    """Load CSV from S3 bucket."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        csv_str = obj["Body"].read().decode("utf-8")
        df = pd.read_csv(StringIO(csv_str))
        df.columns = df.columns.str.lower()
        return df
    except Exception as e:
        print(f"❌ Failed to load CSV from S3 ({key}): {e}")
        return pd.DataFrame()

def upload_csv_to_s3(df, key):
    """Upload CSV dataframe to S3 bucket."""
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=csv_buffer.getvalue())
        print(f"💾 Uploaded CSV to S3: {key}")
    except Exception as e:
        print(f"❌ Failed to upload CSV to S3 ({key}): {e}")

def get_available_balance():
    global cached_fund
    if cached_fund is not None:
        return cached_fund
    try:
        resp = dhan.get_fund_limits()
        cached_fund = float(resp["data"].get("availabelBalance", 0))  # API typo
        return cached_fund
    except Exception as e:
        print(f"⚠️ Error fetching available balance: {e}")
        return 0

def calculate_position_size(price, entry, sl_price, sec_id, leverage_dict=None):
    """Calculate position size based on risk and available fund."""
    sl_point = abs(entry - sl_price)
    if sl_point == 0:
        return 0, 0, 0

    max_loss = 1000
    qty_by_risk = int(max_loss / sl_point)
    leverage = 1
    if leverage_dict:
        leverage = leverage_dict.get(str(sec_id), 1)

    fund = get_available_balance()
    eff_fund = fund * leverage
    qty_by_fund = int(eff_fund / price)
    quantity = min(qty_by_risk, qty_by_fund)
    expected_loss = quantity * sl_point
    exposure = quantity * price
    return quantity, expected_loss, exposure

def safe_extract_quotes(resp):
    """Extract NSE_EQ quotes safely from Dhan response."""
    if not isinstance(resp, dict):
        return {}
    if "data" in resp and isinstance(resp["data"], dict):
        return resp["data"].get("data", {}).get("NSE_EQ", {})
    return resp.get("NSE_EQ", {})

def batch(lst, size=400):
    """Split list into batches."""
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

# ─────────────────────────────
# Inside-Bar Breakout Tracker
# ─────────────────────────────
def track_insidebar_algo_breakouts_bot():
    global cached_fund
    df_filtered = load_csv_from_s3(FILTERED_FILE_KEY)
    if df_filtered.empty:
        print("🚫 Inside-bar list is empty.")
        return []

    breakout_hits = []
    df_filtered.columns = df_filtered.columns.str.lower()
    sec_ids = df_filtered["security id"].astype(int).tolist()
    cached_fund = get_available_balance()

    for pkt in batch(sec_ids):
        time.sleep(1.1)
        resp = dhan.quote_data(securities={"NSE_EQ": pkt})
        live_quotes = safe_extract_quotes(resp)
        if not live_quotes:
            print(f"⚠️ Empty quote packet: {resp}")
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
                print(f"🛑 Skipping {row['stock name']} – day low {day_low} < 1st‑15m low {low_1st}")
                continue

            ltp = float(quote["last_price"])
            if ltp > high_1st:
                sl = float(row["2nd 15m low"])
                sl_point = high_1st - sl
                sl_percent = (sl_point / high_1st) * 100
                if sl_percent > 2:
                    print(f"❌ Skipping {row['stock name']} — SL ({sl_percent:.2f}%) > 2%")
                    continue

                qty, loss, exp = calculate_position_size(ltp, high_1st, sl, sec_id)
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
                    "Leverage": 1  # optional: could use row.get("MIS_LEVERAGE",1)
                })

    # Strong Quarterly Filter
    try:
        time.sleep(2)
        alerts, _ = strong_quarterly_alert()
        valid_symbols = {a.split()[1] for a in alerts if "🔔" in a}
        breakout_hits = [hit for hit in breakout_hits if hit["Stock Name"] in valid_symbols]
        print(f"✅ After strong_quarterly_alert filter: {len(breakout_hits)} hits")
    except Exception as e:
        print(f"⚠️ strong_quarterly_alert filter failed: {e}")

    return breakout_hits

# ─────────────────────────────
# Main Runner
# ─────────────────────────────
if __name__ == "__main__":
    print("🔍 Starting Inside-Bar breakout scanner...")
    hits = track_insidebar_algo_breakouts_bot()
    print(f"✅ Breakout hits found: {len(hits)}")
