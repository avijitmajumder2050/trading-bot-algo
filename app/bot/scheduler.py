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
from app.config.aws_s3 import read_csv_from_s3,S3_BUCKET
from app.strategy.stock_selector import select_best_stock,rank_stocks
from app.strategy.nifty_filter import is_nifty_trade_allowed
from app.execution.trade_executor import execute_trade
from app.broker.market_data import get_nifty_ltp_and_prev_close
from app.integrations import quantile_order_intents
import random

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





JOURNAL_KEY = "uploads/fyers_trade_journal.csv"


def has_active_trade():
    try:
        df = read_csv_from_s3(
            S3_BUCKET,
            JOURNAL_KEY
        )

        if df is None or df.empty:
            return False

        df.columns = df.columns.str.strip().str.lower()

        # Nothing in this codebase ever writes CLOSED/SL_HIT back to this
        # journal for a trade that's actually finished (it's only ever
        # read here) - confirmed live: rows going back to early August
        # are still sitting at status ACTIVE, weeks after any real
        # intraday MIS position would have closed. Treating every one of
        # those as "still open" made this check permanently true, which
        # made terminate_after_delay() send a Telegram alert every 60
        # seconds forever on every run. Only a row dated *today* can
        # plausibly still be a real open position.
        today_str = datetime.now(IST).strftime("%Y-%m-%d")
        active = df[
            df["status"]
            .astype(str)
            .str.upper()
            .isin(["OPEN", "ACTIVE"])
            & (df["trade_date"].astype(str).str.strip() == today_str)
        ]

        if not active.empty:
            symbol = active.iloc[-1]["symbol"]
            status = active.iloc[-1]["status"]

            logging.info(
                f"📈 Active trade found: {symbol} ({status})"
            )

            return True

        # Also hold off termination while a Quantile-triggered auto-order
        # is still claimed/open — a second, independent check so
        # self-termination logic never assumes the trade journal CSV is
        # the only place an open position could be recorded.
        if quantile_order_intents.has_open_intents():
            logging.info("📈 Open Quantile order intent found")
            return True

        return False

    except Exception as e:
        logging.error(
            f"Failed to check trade journal: {e}"
        )
        return True   # safest option


# --------------------------
# Terminate as soon as it's safe, instead of sitting idle (billable)
# until the terminate_at(15:10) hard backstop. Gated to never check
# before Quantile's breakout window closes (10:30 IST, matching
# app.py's AUTO_BREAKOUT_WINDOW_END in the Quantile repo) — the nifty
# strategy's own "no trade" conclusion lands right at market open
# (~9:31), well before that, and terminating on that alone could kill
# the instance before Quantile's breakout race has had its full window
# to produce a winner. has_active_trade() is the single source of
# truth for "is it safe yet" — it already covers both today's nifty
# journal rows and any open Quantile order intent.
# --------------------------
QUANTILE_WINDOW_END_HOUR = 10
QUANTILE_WINDOW_END_MINUTE = 30


async def terminate_after_quantile_window():
    now = datetime.now(IST)
    target = now.replace(hour=QUANTILE_WINDOW_END_HOUR, minute=QUANTILE_WINDOW_END_MINUTE, second=0, microsecond=0)
    if now < target:
        await asyncio.sleep((target - now).total_seconds())

    while True:
        if has_active_trade():
            await asyncio.sleep(60)
            continue

        instance_id = get_instance_id()
        if not instance_id or instance_id == "UNKNOWN":
            logging.error("❌ Cannot terminate — instance ID not found")
            return

        logging.info("✅ No trade active past Quantile window — terminating EC2")
        await send_telegram_message("✅ No trade today — terminating EC2 to save cost")
        terminate_instance(instance_id)
        return


# --------------------------
# Quantile order-intent polling — the dedicated-IP order execution
# side of app.py's _breakout_watch_once() -> quantile-order-intents
# hand-off (see connectors/order_intent_connector.py in the Quantile
# repo). Idempotency gate 2 (claim_pending_intent's conditional
# update) is what makes it safe for this loop to overlap with itself
# or run more than once.
# --------------------------
QUANTILE_POLL_INTERVAL_SECONDS = 15


async def poll_quantile_order_intents():
    while True:
        try:
            for intent in quantile_order_intents.list_pending_intents():
                entry_id = intent["entry_id"]
                claimed = quantile_order_intents.claim_pending_intent(entry_id)
                if claimed is None:
                    continue  # another poll cycle already claimed it

                stock = {
                    "Stock Name": claimed["symbol"],
                    # int, not the raw string from DynamoDB — dhan.quote_data()
                    # (used by get_ltp) silently fails with an empty error body
                    # on a string security id; place_trade() casts back to str
                    # itself for the order-placement calls, which do expect a
                    # string, so this only needs to be an int for the LTP path.
                    "Security ID": int(claimed["security_id"]),
                    "Entry": float(claimed["entry_price"]),
                    "SL": float(claimed["sl_price"]),
                    "Signal": claimed.get("side", "BUY"),
                }

                logging.info(f"📥 Claimed Quantile order intent | {stock['Stock Name']}")
                await send_telegram_message(
                    f"📥 Placing auto-order for {stock['Stock Name']} (from Quantile breakout win)"
                )

                loop = asyncio.get_running_loop()
                result = await loop.run_in_executor(None, execute_trade, stock, dhan)

                if not result:
                    quantile_order_intents.mark_intent_result(entry_id, "failed")
                    await send_telegram_message(f"❌ Auto-order failed for {stock['Stock Name']}")
                    continue

                is_paper = bool(result.get("paper"))
                quantile_order_intents.mark_intent_result(
                    entry_id,
                    "paper_filled" if is_paper else "closed",
                    order_id=result.get("order_id"),
                    outcome=result.get("outcome"),
                    filled_qty=result.get("qty"),
                    target_price=result.get("target"),
                    trailing_jump=result.get("trailing_jump"),
                )
                await send_telegram_message(
                    f"{'📝 PAPER' if is_paper else '✅'} {stock['Stock Name']} | outcome={result.get('outcome')}"
                )

        except Exception as e:
            logging.error(f"❌ Error in poll_quantile_order_intents: {e}")

        await asyncio.sleep(QUANTILE_POLL_INTERVAL_SECONDS)

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



async def terminate_after_delay(max_delay_minutes=5):

    delay_minutes = random.randint(
        1,
        max_delay_minutes
    )

    logging.info(
        f"🕒 EC2 termination scheduled in {delay_minutes} minute(s)"
    )

    await send_telegram_message(
        f"🕒 EC2 termination scheduled in {delay_minutes} minute(s)"
    )

    await asyncio.sleep(
        delay_minutes * 60
    )

    while True:

        if has_active_trade():

            logging.info(
                "⏳ Active trade exists. Waiting for trade closure before terminating EC2."
            )

            await send_telegram_message(
                "⏳ Active trade exists. EC2 termination postponed."
            )

            await asyncio.sleep(60)

            continue

        logging.info(
            "✅ No active trades found. Proceeding with EC2 termination."
        )

        await send_telegram_message(
            "✅ Trade closed. Terminating EC2."
        )

        instance_id = get_instance_id()

        if not instance_id or instance_id == "UNKNOWN":
            logging.error(
                "❌ Cannot terminate — instance ID not found"
            )
            return

        terminate_instance(instance_id)

        break


CSV_KEY = "uploads/nifty_15m_breakout_signals.csv"
# --------------------------
# Daily trade state
# --------------------------
trade_executed_today = False  # ✅ Added to prevent multiple trades per day

# Confirmed live (2026-09-23): this used to run once, immediately at
# boot (~9:27 IST), try every ranked stock in the CSV exactly once,
# and simply give up for the day the moment it ran out - all 14
# signals that morning were SELLs the Nifty filter rejected (Nifty was
# up), and there was no second chance even though the CSV could have
# picked up better signals later. Now gated to not even look before
# 9:45 IST, requires at least NIFTY_BREAKOUT_MIN_SIGNALS rows in the
# CSV before attempting anything, and - if nothing works out on a
# given pass - keeps periodically re-reading the CSV and retrying
# instead of quitting after one pass. Bounded naturally by the
# existing termination backstops (terminate_at, and once a trade
# succeeds, terminate_after_delay), not an explicit end time here.
NIFTY_BREAKOUT_START_HOUR = 9
NIFTY_BREAKOUT_START_MINUTE = 45
NIFTY_BREAKOUT_MIN_SIGNALS = 2
NIFTY_BREAKOUT_POLL_INTERVAL_SECONDS = 60


async def run_nifty_breakout_trade():
    global trade_executed_today

    # Skip if a trade has already succeeded today
    if trade_executed_today:
        logging.info("⚠️ Trade already executed today, skipping further attempts")
        return

    now = datetime.now(IST)
    target = now.replace(hour=NIFTY_BREAKOUT_START_HOUR, minute=NIFTY_BREAKOUT_START_MINUTE, second=0, microsecond=0)
    if now < target:
        wait_seconds = (target - now).total_seconds()
        logging.info(f"⏳ Waiting until {NIFTY_BREAKOUT_START_HOUR}:{NIFTY_BREAKOUT_START_MINUTE:02d} IST before checking breakout signals ({wait_seconds:.0f}s)")
        await asyncio.sleep(wait_seconds)

    while not trade_executed_today:
        try:
            logging.info("📥 Reading breakout signals from S3")
            df = read_csv_from_s3(S3_BUCKET, CSV_KEY)

            ranked_stocks = rank_stocks(df)
            if len(ranked_stocks) < NIFTY_BREAKOUT_MIN_SIGNALS:
                logging.info(f"⏳ Only {len(ranked_stocks)} signal(s) in CSV (need ≥ {NIFTY_BREAKOUT_MIN_SIGNALS}) - rechecking in {NIFTY_BREAKOUT_POLL_INTERVAL_SECONDS}s")
                await asyncio.sleep(NIFTY_BREAKOUT_POLL_INTERVAL_SECONDS)
                continue

            # 2️⃣ Nifty quotes
            nifty_ltp, nifty_prev_close = get_nifty_ltp_and_prev_close()
            if not nifty_ltp or not nifty_prev_close:
                logging.error("❌ Failed to fetch Nifty quotes, will retry")
                await asyncio.sleep(NIFTY_BREAKOUT_POLL_INTERVAL_SECONDS)
                continue

            net_change = nifty_ltp - nifty_prev_close
            logging.info(f"📊 Nifty LTP: {nifty_ltp}, Prev Close: {nifty_prev_close}, Net Change: {net_change:+.2f}")

            # 3️⃣ Try each stock in ranked order
            loop = asyncio.get_running_loop()
            for attempt, stock in enumerate(ranked_stocks, start=1):
                allowed = is_nifty_trade_allowed(stock["Signal"], nifty_ltp, nifty_prev_close)
                logging.info(
                    f"🔹 Attempt {attempt}: Checking {stock['Stock Name']} | Signal: {stock['Signal']} "
                    f"| Nifty filter passed: {allowed} | Nifty LTP: {nifty_ltp}, Prev Close: {nifty_prev_close}, Net Change: {net_change:+.2f}"
                )

                if not allowed:
                    logging.info(f"❌ Nifty filter failed for {stock['Stock Name']}, skipping")
                    await send_telegram_message(
                        f"❌ Trade skipped for {stock['Stock Name']} | Nifty filter not passed\n"
                        f"Nifty LTP: {nifty_ltp}, Prev Close: {nifty_prev_close}, Net Change: {net_change:+.2f}"
                    )
                    continue

                logging.info(f"🚀 Attempt {attempt}: Executing trade for {stock['Stock Name']} | {stock['Signal']}")
                await send_telegram_message(
                    f"🚀 Attempt {attempt}: Executing trade for {stock['Stock Name']} | {stock['Signal']}\n"
                    f"Entry: {stock['Entry']}\nSL: {stock['SL']}\nQty: {stock['Quantity']}\n"
                    f"Nifty LTP: {nifty_ltp}, Prev Close: {nifty_prev_close}, Net Change: {net_change:+.2f}"
                )

                success = await loop.run_in_executor(None, execute_trade, stock, dhan)
                if success:
                    logging.info(f"✅ Trade executed successfully for {stock['Stock Name']} on attempt {attempt}")
                    await send_telegram_message(
                        f"✅ Trade executed successfully for {stock['Stock Name']} on attempt {attempt}"
                    )
                    trade_executed_today = True  # ✅ Mark as executed
                    # 🔥 Schedule random termination in background (1–5 min)
                    asyncio.create_task(terminate_after_delay(5))
                    break
                else:
                    logging.error(f"❌ Trade failed for {stock['Stock Name']} on attempt {attempt}")
                    await send_telegram_message(
                        f"❌ Trade FAILED for {stock['Stock Name']} on attempt {attempt}, trying next best stock..."
                    )

            else:
                logging.info(f"❌ All {len(ranked_stocks)} signal(s) failed this pass - rechecking in {NIFTY_BREAKOUT_POLL_INTERVAL_SECONDS}s in case the CSV updates")
                await send_telegram_message(f"❌ All {len(ranked_stocks)} signal(s) failed this pass — still watching")
                await asyncio.sleep(NIFTY_BREAKOUT_POLL_INTERVAL_SECONDS)

        except Exception as e:
            logging.error(f"❌ Error in run_nifty_breakout_trade: {e}")
            await send_telegram_message(f"❌ Trade execution error: {e}")
            await asyncio.sleep(NIFTY_BREAKOUT_POLL_INTERVAL_SECONDS)
