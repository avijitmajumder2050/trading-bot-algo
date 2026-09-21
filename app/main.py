import logging
import os

from telegram.ext import (
    ApplicationBuilder,
    MessageHandler,
    filters,
)

from app.bot.handlers import handle_message
from app.bot.scheduler import (
    terminate_at,
    run_nifty_breakout_trade,
    poll_quantile_order_intents,
    terminate_after_quantile_window,
)
from app.config.aws_ssm import get_param


# ───────────────────────────────
# Logging (FORCED – DO NOT USE basicConfig)
# ───────────────────────────────
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "bot.log")

os.makedirs(LOG_DIR, exist_ok=True)

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# 🔥 CRITICAL: remove PTB / preloaded handlers
if root_logger.handlers:
    root_logger.handlers.clear()

file_handler = logging.FileHandler(LOG_FILE, mode="a")
file_handler.setFormatter(
    logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
)

console_handler = logging.StreamHandler()
console_handler.setFormatter(
    logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )
)

root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

logger = logging.getLogger(__name__)

logger.info("✅ Logging system initialized")


# ───────────────────────────────
# Silence noisy libraries
# ───────────────────────────────
for lib in [
    "telegram",
    "telegram.ext",
    "httpx",
    "asyncio",
    "boto3",
    "botocore",
    "s3transfer",
    "urllib3",
]:
    logging.getLogger(lib).setLevel(logging.WARNING)


# ───────────────────────────────
# Load secrets
# ───────────────────────────────
BOT_TOKEN = get_param("/trading-bot/telegram/BOT_TOKEN")


# ───────────────────────────────
# Background jobs (PTB SAFE)
# ───────────────────────────────
async def post_init(app):
    logger.info("🚀 Starting background jobs")

    app.create_task(run_nifty_breakout_trade())
    app.create_task(terminate_at(target_hour=15, target_minute=10))
    app.create_task(poll_quantile_order_intents())
    app.create_task(terminate_after_quantile_window())



# ───────────────────────────────
# Main
# ───────────────────────────────
def main():
    logger.info("🤖 Building Telegram application")

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message)
    )

    logger.info("🤖 Telegram bot started")
    app.run_polling()


# ───────────────────────────────
# Entry
# ───────────────────────────────
if __name__ == "__main__":
    main()
