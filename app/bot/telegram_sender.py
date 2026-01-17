# app/bot/telegram_sender.py
import os
import logging
import asyncio
import requests
from app.config.settings import BOT_TOKEN, CHAT_ID

async def send_telegram_message(message: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        requests.post(url, data=payload, timeout=5)
        logging.info(f"📩 Sent alert: {message}")
    except Exception as e:
        logging.error(f"❌ Telegram send error: {e}")
