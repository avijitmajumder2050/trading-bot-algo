import logging
import os
from telegram.ext import (
    ApplicationBuilder,
    MessageHandler,
    CommandHandler,
    filters,
)

from app.bot.handlers import handle_message, scan_command
from app.bot.scheduler import (
    insidebar_daily_scheduler,
    insidebar_breakout_tracker,
    opposite_15m_scheduler,
    opposite_15m_breakout_tracker,
    terminate_at,
    run_nifty_breakout_trade,
)
from app.config.aws_ssm import get_param

# ───────────────────────────────
# Ensure logs directory exists
# ───────────────────────────────
os.makedirs("logs", exist_ok=True)

# ───────────────────────────────
# Logging
# ───────────────────────────────
logging.basicConfig(
    filename="logs/bot.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# Silence noisy libraries
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

logger = logging.getLogger(__name__)

# ───────────────────────────────
# Load secrets
# ───────────────────────────────
BOT_TOKEN = get_param("/trading-bot/telegram/BOT_TOKEN")

# ───────────────────────────────
# Register background jobs (CRITICAL)
# ───────────────────────────────
async def post_init(app):
    logger.info("🚀 Starting background schedulers")

    app.create_task(insidebar_daily_scheduler())
    app.create_task(insidebar_breakout_tracker())
    app.create_task(opposite_15m_scheduler())
    app.create_task(opposite_15m_breakout_tracker())
    app.create_task(terminate_at(target_hour=10, target_minute=30))
    app.create_task(run_nifty_breakout_trade())

# ───────────────────────────────
# Main
# ───────────────────────────────
def main():
    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(post_init)   # ✅ THIS IS THE FIX
        .build()
    )

    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message)
    )
    app.add_handler(CommandHandler("scan", scan_command))

    logger.info("🤖 Telegram bot started")
    app.run_polling()

# ───────────────────────────────
# Entry
# ───────────────────────────────
if __name__ == "__main__":
    main()
