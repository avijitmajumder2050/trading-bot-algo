# app/execution/trade_executor.py

import time
import logging
from app.execution.position_manager import PositionManager
from app.broker.dhan_super_client import DhanSuperBroker
from app.broker.market_data import get_ltp
from app.config.settings import IST
from datetime import datetime

def execute_trade(stock, dhan_context):
    """
    Execute trade using Dhan Super Orders.
    SL and target are managed automatically via Super Orders.
    Partial booking and trailing logic modifies the super order legs.

    Returns order_info (dict, with an added "outcome" key) on success,
    None on failure — truthy/falsy compatible with older callers that
    only checked "if success:". order_info is what a caller (e.g. the
    quantile-order-intents poller) needs to record the trade's final
    state back to Quantile.
    """

    broker = DhanSuperBroker(dhan_context)
    side = stock["Signal"].upper()

    # 1️⃣ Place Super Order

    order_info = broker.place_trade(stock)   # now returns dict
    if not order_info:
        logging.error(f"❌ Failed to place Super Order for {stock['Stock Name']}")
        return None

    order_id = order_info["order_id"]        # extract order_id from dict
    entry_price = order_info["entry"]        # can use for monitoring
    sl_price = order_info["sl"]
    qty = order_info["qty"]

    logging.info(f"🚀 Super Order placed for {stock['Stock Name']} | Entry: {entry_price}, SL: {sl_price}, Qty: {qty}")

    # PAPER_MODE: the "order" was never actually sent to Dhan, so
    # there's no real fill/exit to wait for or monitor — just report
    # what would have happened and stop here.
    if order_info.get("paper"):
        logging.info(f"📝 PAPER_MODE trade recorded for {stock['Stock Name']} | {order_info}")
        return {**order_info, "outcome": "PAPER_FILLED"}

    # ─────────────────────────────────────────────
    # WAIT UNTIL ORDER IS TRADED
    # ─────────────────────────────────────────────
    logging.info(f"⏳ Waiting for order to be TRADED...")

    max_wait_seconds = 600
    start_time = time.time()

    while True:
        order_status = broker.get_order_status(order_id)

        logging.info(
            f"📊 Order Status | {stock['Stock Name']} | {order_status}"
        )

        # ✅ If traded → start LTP monitoring
        if order_status == "TRADED":
            logging.info(
                f"✅ Order TRADED | {stock['Stock Name']} | Starting LTP monitor"
            )
            break

        # ❌ If rejected/cancelled → stop
        if order_status in ["REJECTED", "CANCELLED"]:
            logging.error(
                f"❌ Order {order_status} | {stock['Stock Name']}"
            )
            return None

        # ⏳ Timeout protection
        if time.time() - start_time > max_wait_seconds:
            logging.warning(
        f"⏰ Order not traded within timeout for {stock['Stock Name']}. Cancelling order..."
    )
            try:
                broker.exit_trade(order_id)  # Cancels ENTRY_LEG
                logging.info(f"🛑 Order cancelled due to timeout | ID: {order_id}")
            except Exception as e:
                logging.error(f"❌ Failed to cancel order: {e}")

            return None

        time.sleep(30)


    

    logging.info(f"🚀 Monitoring trade for {stock['Stock Name']}")

    # 2️⃣ Init Position Manager (only for tracking 1R / 1.5R levels)
    pm = PositionManager(
        entry=entry_price,
        sl=sl_price,
        qty=qty,
        side=side
    )

    # 3️⃣ Monitor LTP and manage Super Order legs
    name = stock["Stock Name"]
    entry_time = datetime.now(IST)

    def finish(outcome):
        """Build the result once the position is closed: exit price and
        P&L from Dhan's position book, reason from where it exited."""
        exit_price, pnl = broker.exit_fill(stock["Security ID"], side)
        if exit_price is None:
            exit_price = get_ltp(stock["Security ID"])
        reason = classify_exit(outcome, side, entry_price, sl_price, order_info.get("target"),
                               order_info.get("trailing_jump"), exit_price)
        sg = 1 if side == "BUY" else -1
        risk = abs(entry_price - sl_price)
        if pnl is None and exit_price:
            pnl = sg * (exit_price - entry_price) * qty
        rr = sg * (exit_price - entry_price) / risk if exit_price and risk else None
        logging.info(f"🏁 {name} closed | {reason} | exit={exit_price} pnl={pnl} rr={rr}")
        return {**order_info, "outcome": outcome, "exit_reason": reason, "exit_price": exit_price,
                "pnl": pnl, "rr": rr, "entry_time": entry_time.strftime("%H:%M"),
                "exit_time": datetime.now(IST).strftime("%H:%M")}

    while True:
        # 🔎 Check Super Order exit status
        exit_status = broker.check_super_order_exit(order_id)
        logging.info(f"🎯 exit_status={exit_status} | {name}")
        if exit_status == "PARENT_CANCELLED":
            logging.warning(f"❌ Parent order cancelled | {name}")
            return None
        elif exit_status == "PARENT_REJECTED":
            logging.error(f"❌ Parent order rejected | {name}")
            return None
        elif exit_status in ("STOP_LOSS_HIT", "TARGET_HIT", "EXIT_CANCELLED"):
            return finish(exit_status)

        # ⏰ Time exit - square off ourselves before Dhan's auto square-off
        # (which charges extra) and before terminate_at(15:10) kills the box.
        now = datetime.now(IST)
        if (now.hour, now.minute) >= (TIME_EXIT_HOUR, TIME_EXIT_MINUTE):
            logging.warning(f"⏰ {TIME_EXIT_HOUR}:{TIME_EXIT_MINUTE:02d} reached - squaring off {name}")
            flat = broker.square_off_now(order_id, stock["Security ID"], side)
            return finish("TIME_EXIT" if flat else "TIME_EXIT_FAILED")

        ltp = get_ltp(stock["Security ID"])
        if not ltp:
            time.sleep(1)
            continue

        logging.info(
            f"📈 LTP Monitor | {name} | LTP={ltp}"
        )
        action = pm.process_ltp(ltp)

        # 1R reached → log only. The Super Order's own trailingJump (0.5R,
        # set at placement) has already moved the SL to breakeven by the
        # time price reaches 1R. This used to also send trail_sl(entry),
        # which did nothing at exactly 1R - but if price had run past
        # ~1.5R between these 30s polls, Dhan had already trailed the SL
        # above entry and that modify pulled it back down to entry.
        if action == "TRAIL_SL":
            logging.info(
                f"🔁 1R reached for {name} | LTP={ltp} | Dhan trailing has SL at/above breakeven ({entry_price})"
            )

        # 5R backstop if the Super Order's own TARGET_LEG hasn't closed it.
        # Used to modify the SL leg to LTP ∓ 1, which only fills if price
        # then moves another rupee against us - square off instead.
        elif action == "EXIT_TRADE":
            logging.info(f"🛑 EXIT_TRADE triggered for {name} | squaring off at MARKET")
            broker.square_off_now(order_id, stock["Security ID"], side)
            return finish("TARGET_HIT")

        # ⏱️ WAIT 30 SECONDS BEFORE NEXT CHECK
        time.sleep(30)


# Square off before Dhan's own intraday auto square-off (extra ~Rs 50 +
# GST per position) and before terminate_at(15:10) terminates EC2.
TIME_EXIT_HOUR = 15
TIME_EXIT_MINUTE = 5


def classify_exit(outcome, side, entry, sl, target, jump, exit_price):
    """Human-readable exit reason. Dhan's leg statuses alone can't be
    trusted for this - JSWENERGY (2026-10-07) hit its breakeven trailing
    SL but came back as both legs CANCELLED, i.e. "manual exit". So
    anything not exited by the bot itself is judged by where it filled:
    at the target, at the original SL, or at one of the 0.5R trail steps."""
    if outcome == "TIME_EXIT":
        return f"⏰ Time exit ({TIME_EXIT_HOUR}:{TIME_EXIT_MINUTE:02d})"
    if outcome == "TIME_EXIT_FAILED":
        return "⚠️ Time exit FAILED - check position in Dhan"
    if not exit_price:
        return {"STOP_LOSS_HIT": "🛑 SL hit", "TARGET_HIT": "🎯 Target hit"}.get(outcome, "✋ Manual exit")
    sg = 1 if side == "BUY" else -1
    risk = abs(entry - sl)
    tol = 0.15 * risk
    if target and sg * (exit_price - target) >= -tol:
        return "🎯 Target hit (5R)"
    if abs(exit_price - sl) <= tol:
        return "🛑 SL hit"
    if jump and sg * (exit_price - sl) > 0:
        steps = sg * (exit_price - sl) / jump
        if abs(steps - round(steps)) * jump <= tol:
            return "🔁 Trailing SL hit"
    if outcome == "STOP_LOSS_HIT":
        return "🔁 Trailing SL hit"
    if outcome == "TARGET_HIT":
        return "🎯 Target hit"
    return "✋ Manual exit"
