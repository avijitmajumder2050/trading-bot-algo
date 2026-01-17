# app/config/settings.py

import os
from pytz import timezone
from datetime import time

# --- Timezone ---
IST = timezone("Asia/Kolkata")

# --- Scan Times ---
INSIDEBAR_SCAN_TIME = time(9, 31)  # 9:31 AM

# --- AWS Config ---
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
S3_BUCKET = os.getenv("S3_BUCKET", "dhan-trading-data")
MAP_FILE_KEY = "uploads/mapping.csv"
EOD_DATA_PREFIX = "eod_data"   # 👈 folder in S3

# --- Logs ---
LOG_DIR = "logs"

# --- Telegram Keywords ---
TRIGGER_KEYWORDS = ["scanner", "scan", "momentum", "interday", "intraday"]
SWING_KEYWORDS = ["swing", "position"]
CROSS_KEYWORDS = ["ema cross", "cross ema", "ema crossover", "crossover"]
