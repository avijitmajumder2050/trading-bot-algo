import asyncio
import logging
import os
from telegram.ext import ApplicationBuilder, MessageHandler, CommandHandler, filters

from app.bot.handlers import handle_message, scan_command
from app.bot.scheduler import (
    insidebar_daily_scheduler,
    insidebar_breakout_tracker,
    opposite_15m_scheduler,
    opposite_15m_breakout_tracker,
)
from app.config.aws_ssm import get_param

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/bot.log",
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)

BOT_TOKEN = get_param("/trading-bot/telegram/BOT_TOKEN")
CHAT_ID = get_param("/trading-bot/telegram/CHAT_ID")

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Handlers
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CommandHandler("scan", scan_command))

    logging.info("🤖 Telegram bot started (polling enabled)")

    # ---- Start background schedulers ----
    asyncio.get_event_loop().create_task(insidebar_daily_scheduler())
    asyncio.get_event_loop().create_task(insidebar_breakout_tracker())
    asyncio.get_event_loop().create_task(opposite_15m_scheduler())
    asyncio.get_event_loop().create_task(opposite_15m_breakout_tracker())

    # ---- START POLLING (this was missing) ----
    app.run_polling()

if __name__ == "__main__":
    main()
