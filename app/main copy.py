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
    terminate_at
)
from app.config.aws_ssm import get_param

# ───────────────────────────────
# Ensure logs directory exists
# ───────────────────────────────
os.makedirs("logs", exist_ok=True)

# ───────────────────────────────
# Logging (APP LEVEL)
# ───────────────────────────────
logging.basicConfig(
    filename="logs/bot.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# ───────────────────────────────
# Silence noisy libraries
# ───────────────────────────────
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("telegram.ext").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)

# AWS SDK noise (THIS FIXES YOUR ISSUE)
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("s3transfer").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# ───────────────────────────────
# Load secrets from AWS SSM
# ───────────────────────────────
BOT_TOKEN = get_param("/trading-bot/telegram/BOT_TOKEN")
CHAT_ID = get_param("/trading-bot/telegram/CHAT_ID")

# ───────────────────────────────
# Main
# ───────────────────────────────
def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Telegram handlers
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CommandHandler("scan", scan_command))

    logging.info("🤖 Telegram bot started (polling enabled)")

    # Background schedulers
    loop = asyncio.get_event_loop()
    loop.create_task(insidebar_daily_scheduler())
    loop.create_task(insidebar_breakout_tracker())
    loop.create_task(opposite_15m_scheduler())
    loop.create_task(opposite_15m_breakout_tracker())
    loop.create_task(terminate_at(target_hour=10, target_minute=30))  # <-- fix here
    # Start polling
    app.run_polling()

# ───────────────────────────────
# Entry point
# ───────────────────────────────
if __name__ == "__main__":
    main()

