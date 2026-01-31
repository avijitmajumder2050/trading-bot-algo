# app/bot/scheduler.py
import asyncio
import logging
import boto3
from datetime import datetime, time
from app.config.settings import IST, INSIDEBAR_SCAN_TIME
from app.config.dhan_auth import dhan
from app.bot.telegram_sender import send_telegram_message

from app.utils.get_instance_id import get_instance_id  # your existing function

import threading
from app.config.aws_s3 import read_csv_from_s3
from app.strategy.stock_selector import select_best_stock,rank_stocks
from app.strategy.nifty_filter import is_nifty_trade_allowed
from app.execution.trade_executor import execute_trade
from app.broker.market_data import get_nifty_ltp_and_prev_close

# --------------------------
# InsideBar 5-min scan state
# --------------------------
insidebar_done = None
insidebar_enabled = False
insidebar_alerted = set()
insidebar_alert_lock = threading.Lock()
insidebar_lock = asyncio.Lock()

# --------------------------
# 15-min Opposite Candle state
# --------------------------
opposite_done = None
opposite_enabled = False
opposite_alerted = set()
opposite_alert_lock = threading.Lock()
opposite_lock = asyncio.Lock()



# --------------------------
# EC2 Termination Scheduler
# --------------------------
def terminate_instance(instance_id, region="ap-south-1"):
    try:
        ec2 = boto3.client("ec2", region_name=region)
        ec2.terminate_instances(InstanceIds=[instance_id])
        logging.info(f"✅ Termination command sent for instance: {instance_id}")
    except Exception as e:
        logging.error(f"❌ Termination failed: {e}")

async def terminate_at(target_hour=10, target_minute=40):
    instance_id = get_instance_id()
    if not instance_id or instance_id == "UNKNOWN":
        logging.error("❌ Cannot terminate — instance ID not found")
        return

    while True:
        now = datetime.now()
        if now.hour == target_hour and now.minute == target_minute:
            logging.info(f"🕓 Time reached {target_hour}:{target_minute}, terminating instance...")
            terminate_instance(instance_id)
            break
        await asyncio.sleep(20)

BUCKET = "dhan-trading-data"
CSV_KEY = "uploads/nifty_15m_breakout_signals.csv"

async def run_nifty_breakout_trade():
    """
    Reads S3 CSV, ranks stocks by risk, applies Nifty filter,
    and tries executing trade sequentially until one succeeds.
    """
    try:
        logging.info("📥 Reading breakout signals from S3")
        df = read_csv_from_s3(BUCKET, CSV_KEY)

        ranked_stocks = rank_stocks(df)
        if not ranked_stocks:
            logging.info("❌ No valid stocks for breakout today")
            await send_telegram_message("❌ No valid stocks for breakout today")
            return

        # 2️⃣ Nifty quotes
        nifty_ltp, nifty_prev_close = get_nifty_ltp_and_prev_close()
        if not nifty_ltp or not nifty_prev_close:
            logging.error("❌ Failed to fetch Nifty quotes, skipping trade.")
            await send_telegram_message("❌ Failed to fetch Nifty quotes, skipping trade.")
            return
        net_change = nifty_ltp - nifty_prev_close
        logging.info(
    f"📊 Nifty LTP: {nifty_ltp}, Prev Close: {nifty_prev_close}, Net Change: {net_change:+.2f}"
)
        

        # 3️⃣ Try each stock in ranked order
        loop = asyncio.get_running_loop()
        for attempt, stock in enumerate(ranked_stocks, start=1):
            allowed = is_nifty_trade_allowed(stock["Signal"], nifty_ltp, nifty_prev_close)
            logging.info(
                f"🔹 Attempt {attempt}: Checking {stock['Stock Name']} | Signal: {stock['Signal']} "
                f"| Nifty filter passed: {allowed} | Nifty LTP: {nifty_ltp}, Prev Close: {nifty_prev_close}, Net Change: {net_change:+.2f}"
            )
            if not is_nifty_trade_allowed(stock["Signal"], nifty_ltp, nifty_prev_close):
                logging.info(f"❌ Nifty filter failed for {stock['Stock Name']}, skipping")
                await send_telegram_message(
                    f"❌ Trade skipped for {stock['Stock Name']} | Nifty filter not passed\n"
                    f"Nifty LTP: {nifty_ltp}, Prev Close: {nifty_prev_close}"
                )
                continue

            logging.info(f"🚀 Attempt {attempt}: Executing trade for {stock['Stock Name']} | {stock['Signal']}")
            await send_telegram_message(
                f"🚀 Attempt {attempt}: Executing trade for {stock['Stock Name']} | {stock['Signal']}\n"
                f"Entry: {stock['Entry']}\nSL: {stock['SL']}\nQty: {stock['Quantity']}"
            )

            success = await loop.run_in_executor(None, execute_trade, stock, dhan)
            if success:
                logging.info(f"✅ Trade executed successfully for {stock['Stock Name']} on attempt {attempt}")
                await send_telegram_message(
                    f"✅ Trade executed successfully for {stock['Stock Name']} on attempt {attempt}"
                )
                break
            else:
                logging.error(f"❌ Trade failed for {stock['Stock Name']} on attempt {attempt}")
                await send_telegram_message(
                    f"❌ Trade FAILED for {stock['Stock Name']} on attempt {attempt}, trying next best stock..."
                )

        else:
            logging.error("❌ All trade attempts failed")
            await send_telegram_message("❌ All trade attempts failed today")

    except Exception as e:
        logging.error(f"❌ Error in run_nifty_breakout_trade: {e}")
        await send_telegram_message(f"❌ Trade execution error: {e}")
