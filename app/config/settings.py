# app/config/settings.py

import os
from pytz import timezone
from datetime import time
from app.config.aws_ssm import get_param

# --- Timezone ---
IST = timezone("Asia/Kolkata")

# --- Scan Times ---
INSIDEBAR_SCAN_TIME = time(9, 31)  # 9:31 AM

# --- AWS Config ---
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
S3_BUCKET = os.getenv("S3_BUCKET", "dhan-trading-data")
MAP_FILE_KEY = "uploads/mapping.csv"
NIFTYMAP_FILE_KEY="uploads/nifty_mapping.csv"
# S3 keys
CANDLE_FILE_KEY = "uploads/inside_bar_15min_data_RS80.csv"   # 15-min candle CSV in S3
FILTERED_FILE_KEY = "uploads/inside_bar_15min_RS80.csv"  # optional filtered output
EOD_DATA_PREFIX = "eod_data"   # 👈 folder in S3

# --- Logs ---
LOG_DIR = "logs"

# =========================
# TELEGRAM (FROM SSM)
# =========================
BOT_TOKEN = get_param("/trading-bot/telegram/BOT_TOKEN", decrypt=True)
CHAT_ID = get_param("/trading-bot/telegram/CHAT_ID")

# --- Telegram Keywords ---
TRIGGER_KEYWORDS = ["scanner", "scan", "momentum", "interday", "intraday"]
SWING_KEYWORDS = ["swing", "position"]
CROSS_KEYWORDS = ["ema cross", "cross ema", "ema crossover", "crossover"]
