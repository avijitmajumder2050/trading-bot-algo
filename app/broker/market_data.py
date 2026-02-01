#app/broker/market_data.py
from app.config.dhan_auth import dhan
import time
import logging
logger = logging.getLogger(__name__)
import json

logger = logging.getLogger(__name__)

# ==========================================================
# DHAN QUOTE WITH RETRY (GENERIC SEGMENT)
# ==========================================================
def get_quotes_with_retry(security_ids, segment, retry_delay=1):
    """
    Fetch DHAN quotes with retry support.

    Args:
        security_ids : list[int | str]
        segment      : "NSE_EQ", "IDX_I", etc.

    Returns:
        dict -> {security_id: quote_data} or None
    """
    if not isinstance(security_ids, list):
        security_ids = [security_ids]

    for attempt in (1, 2):
        try:
            logger.info(
                f"📡 Fetching DHAN quotes for {segment} {security_ids} (attempt {attempt})"
            )

            quote_data = dhan.quote_data(
                securities={segment: security_ids}
            )

            # Defensive: sometimes DHAN returns string
            if isinstance(quote_data, str):
                quote_data = json.loads(quote_data)

            segment_quotes = (
                quote_data.get("data", {})
                .get("data", {})
                .get(segment)
            )

            if not isinstance(segment_quotes, dict):
                raise ValueError(f"Invalid quote payload: {quote_data}")

            logger.info(
                f"✅ DHAN quotes fetched ({len(segment_quotes)} instruments)"
            )
            return segment_quotes

        except Exception as e:
            logger.error(
                f"❌ Quote fetch failed (attempt {attempt}) for {segment}: {e}",
                exc_info=True
            )
            if attempt == 1:
                logger.info(f"⏳ Retrying in {retry_delay} second...")
                time.sleep(retry_delay)

    logger.error("🛑 Quote fetch failed after retry")
    return None
def get_ltp_and_change(security_ids, segment):
    """
    Returns:
        {security_id: (ltp, net_change)}
    """
    quotes = get_quotes_with_retry(security_ids, segment)

    if not quotes:
        return {sec_id: (None, None) for sec_id in security_ids}

    result = {}
    for sec_id in security_ids:
        quote = quotes.get(str(sec_id))
        if quote:
            result[sec_id] = (
                quote.get("last_price"),
                quote.get("net_change")
            )
        else:
            result[sec_id] = (None, None)

    return result




def get_nifty_ltp_and_prev_close():
    """
    Fetch Nifty LTP and derive previous close using net_change.
    Segment: IDX_I
    Security ID: 13 (NIFTY 50)
    """
    NIFTY_ID = 13

    quotes = get_quotes_with_retry([NIFTY_ID], segment="IDX_I")

    if not quotes:
        return None, None

    quote = quotes.get(str(NIFTY_ID))
    if not quote:
        return None, None

    ltp = quote.get("last_price")
    net_change = quote.get("net_change")

    if ltp is None or net_change is None:
        return None, None

    prev_close = ltp - net_change
    return ltp, prev_close





def get_ltp(security_id, segment="NSE_EQ", retry_delay=1, max_attempts=7):
    """
    Fetch LTP for a single security with retry and detailed logging.

    Args:
        security_id (str/int): Instrument/security ID
        segment (str): Market segment, e.g., "NSE_EQ"
        retry_delay (int): Seconds to wait between retries
        max_attempts (int): Maximum number of retry attempts

    Returns:
        float or None: Last traded price or None if all attempts fail
    """
    for attempt in range(1, max_attempts + 1):
        try:
            resp = dhan.quote_data(securities={segment: [security_id]})

            data = resp.get("data", {})
            if not isinstance(data, dict):
                raise ValueError(f"Unexpected 'data' type: {type(data)}")

            inner_data = data.get("data", {})
            if not isinstance(inner_data, dict):
                raise ValueError(f"Unexpected 'data.data' type: {type(inner_data)}")

            segment_data = inner_data.get(segment, {})
            if not isinstance(segment_data, dict):
                raise ValueError(f"Unexpected segment data type: {type(segment_data)} | value: {segment_data}")

            quote = segment_data.get(str(security_id))
            if not quote or not isinstance(quote, dict):
                raise ValueError(f"Empty or invalid quote: {quote}")

            ltp = quote.get("last_price")
            if ltp is None:
                raise ValueError("LTP missing in quote")

            # Log success (different message if not first attempt)
            if attempt > 1:
                logger.info(f"✅ get_ltp succeeded for {security_id} on attempt {attempt}")
            logger.info(f"📡 get_ltp OK | {security_id} | LTP={ltp} | attempt={attempt}")
            return float(ltp)

        except Exception as e:
            logger.error(f"❌ get_ltp failed (attempt {attempt}) for {security_id}: {e}")
            if attempt < max_attempts:
                time.sleep(retry_delay)
            else:
                logger.error(f"❌ All {max_attempts} attempts failed for {security_id}")

    return None

