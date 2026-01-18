# app/bot/telegram_sender.py
import os
import logging
import asyncio
import requests
from app.config.settings import BOT_TOKEN, CHAT_ID

# Standard footer for all messages
TELEGRAM_FOOTER = "\n\n⚠️ This is for educational purposes only. Not a buy/sell recommendation. Trade at your own risk."

async def send_telegram_message(message: str):
    # Append footer automatically
    full_message = f"{message}{TELEGRAM_FOOTER}"
    
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": full_message, "parse_mode": "HTML"}
    
    try:
        requests.post(url, data=payload, timeout=5)
        logging.info(f"📩 Sent alert: {full_message}")
    except Exception as e:
        logging.error(f"❌ Telegram send error: {e}")
