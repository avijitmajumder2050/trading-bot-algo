from telegram import Update
from telegram.ext import ContextTypes
from app.config.settings import *
from app.bot.telegram_sender import send_telegram_message
from app.utils.symbol_formatter import format_symbol_string
from app.scanners.EMA_10_20_breakout import run_emabreakout_check, ema_cross
from app.scanners.ema200_breakout_swing import run_ema200_scanner

import asyncio

# =============================================
# Disclaimer Footer (added)
# =============================================
FOOTER = (
    "\n\n<i>⚠️ This is for educational purposes only. "
    "No buy/sell recommendation. Trade at your own risk.</i>"
)

# Prevent overlapping scans
ema_lock = asyncio.Lock()


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.lower()

    # EMA Momentum Scan
    if any(k in text for k in TRIGGER_KEYWORDS):
        async with ema_lock:
            output = run_emabreakout_check()
            symbols = format_symbol_string(output)

            msg = f"""
<b>📊 Momentum Scan</b>

<b>FYERS Copy:</b>
<pre>{symbols}</pre>

<pre>{output}</pre>
"""
            await update.message.reply_text(msg + FOOTER, parse_mode="HTML")

    # EMA Cross Scan
    elif any(k in text for k in CROSS_KEYWORDS):
        async with ema_lock:
            output = ema_cross()
            symbols = format_symbol_string(output)

            msg = f"""
<b>📊 EMA Cross Scan</b>

<b>FYERS Copy:</b>
<pre>{symbols}</pre>

<pre>{output}</pre>
"""
            await update.message.reply_text(msg + FOOTER, parse_mode="HTML")

    # EMA200 Swing/Position Scan
    elif any(k in text for k in SWING_KEYWORDS):
        async with ema_lock:
            aligned, watchlist = run_ema200_scanner()
            aligned_names = [stock["Stock Name"] for stock in aligned]
            aligned_str = ", ".join(aligned_names) if aligned_names else "None"

            msg = f"""
<b>📊 EMA200 Swing/Position Scan</b>

Aligned Stocks: {aligned_str}
Watchlist Count: {len(watchlist)}
"""
            await update.message.reply_text(msg + FOOTER, parse_mode="HTML")


# =============================================
# scan_command placeholder for main.py
# =============================================
async def scan_command(update: Update = None, context: ContextTypes.DEFAULT_TYPE = None):
    """
    Placeholder scan command for compatibility.
    Can be used to trigger scheduled scans manually or via bot command.
    """
    async with ema_lock:
        output = run_emabreakout_check()
        symbols = format_symbol_string(output)
        msg = f"""
<b>📊 Scheduled / Manual Scan Results</b>

<pre>{symbols}</pre>
"""

        if update:
            await update.message.reply_text(msg + FOOTER, parse_mode="HTML")
        else:
            print(msg)
