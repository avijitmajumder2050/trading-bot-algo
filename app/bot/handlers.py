from telegram import Update
from telegram.ext import ContextTypes
from app.config.settings import *
from app.bot.telegram_sender import send_telegram_message
from app.utils.symbol_formatter import format_symbol_string


import asyncio

# =============================================
# Disclaimer Footer (added)
# =============================================
FOOTER = (
    "\n\n<i>⚠️ This is for educational purposes only. "
    "No buy/sell recommendation. Trade at your own risk.</i>"
)




async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Simple Telegram handler.
    No scans, no indicators, no scanners.
    """
    text = update.message.text.lower()

    msg = f"""
<b>🤖 Trading Bot Online</b>

Received message:
<pre>{text}</pre>

Scanner features are currently disabled.
"""
    await update.message.reply_text(msg + FOOTER, parse_mode="HTML")
