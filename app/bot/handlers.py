# app/bot/handlers.py
from telegram import Update
from telegram.ext import ContextTypes
from app.config.settings import *
from app.bot.telegram_sender import send_telegram_message
from app.utils.symbol_formatter import format_symbol_string
from app.scanners.EMA_10_20_breakout import run_emabreakout_check, ema_cross
from app.scanners.ema200_breakout_swing import run_ema200_scanner

import asyncio

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
            await update.message.reply_text(msg, parse_mode="HTML")

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
            await update.message.reply_text(msg, parse_mode="HTML")

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
            await update.message.reply_text(msg, parse_mode="HTML")
