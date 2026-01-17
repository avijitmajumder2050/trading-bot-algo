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

# ───────────────────────────────
# Ensure logs folder exists
# ───────────────────────────────
os.makedirs("logs", exist_ok=True)

# ───────────────────────────────
# Logging
# ───────────────────────────────
logging.basicConfig(
    filename="logs/bot.log",
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)

# ───────────────────────────────
# Load secrets from AWS SSM
# ───────────────────────────────
BOT_TOKEN = get_param("/trading-bot/telegram/BOT_TOKEN")
CHAT_ID = get_param("/trading-bot/telegram/CHAT_ID")

# ───────────────────────────────
# Main application
# ───────────────────────────────
async def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Handlers
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CommandHandler("scan", scan_command))

    # ---- Telegram lifecycle (MANUAL) ----
    await app.initialize()
    await app.start()

    logging.info("🤖 Telegram bot started")

    # ---- Background schedulers ----
    asyncio.create_task(insidebar_daily_scheduler())
    asyncio.create_task(insidebar_breakout_tracker())
    asyncio.create_task(opposite_15m_scheduler())
    asyncio.create_task(opposite_15m_breakout_tracker())

    # ---- Keep app alive forever ----
    await asyncio.Event().wait()

# ───────────────────────────────
# Entry point
# ───────────────────────────────
if __name__ == "__main__":
    asyncio.run(main())
