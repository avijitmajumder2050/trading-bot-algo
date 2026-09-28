import logging
import pandas as pd
import io
import boto3

from app.config.settings import S3_BUCKET, MAP_FILE_KEY, NIFTYMAP_FILE_KEY, AWS_REGION
from app.config.aws_s3 import s3

logger = logging.getLogger(__name__)

_LEVERAGE_MAP = {}


def _leverage_map_from_csv(key):
    """mapping.csv's (and raw_mapping_all_symbols.csv's) Instrument ID
    column reads as float64 (e.g. "25468.0"), but callers look up by
    the plain integer id Dhan itself uses ("25468") - str()'ing the
    raw column produced keys that could never match a real lookup, so
    EVERY instrument silently fell back to the default leverage of 1.
    Confirmed live: PAISALO (25468) is genuinely in mapping.csv with
    MIS_LEVERAGE=5, but get_leverage("25468") still missed it before
    this fix."""
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))

    if "Instrument ID" not in df.columns:
        raise ValueError(f"Instrument ID missing in {key}")

    ids = pd.to_numeric(df["Instrument ID"], errors="coerce")
    valid = ids.notna()
    leverage_col = df["MIS_LEVERAGE"] if "MIS_LEVERAGE" in df.columns else pd.Series(1, index=df.index)

    return dict(zip(ids[valid].astype(int).astype(str), leverage_col[valid]))


def _load_leverage_from_s3():
    # MAP_FILE_KEY (uploads/mapping.csv) is the primary source - it's
    # curated (RS Rating, Setup_Case etc.) rather than the full NSE
    # universe, so a real breakout winner can land outside its ~339
    # stocks (confirmed live: POWERGRID/14977 missing from mapping.csv
    # entirely, but present in nifty_mapping.csv). NIFTYMAP_FILE_KEY is
    # a fallback used only to fill in ids mapping.csv doesn't have -
    # mapping.csv's own value always wins for anything both cover.
    global _LEVERAGE_MAP

    _LEVERAGE_MAP = _leverage_map_from_csv(MAP_FILE_KEY)

    try:
        fallback = _leverage_map_from_csv(NIFTYMAP_FILE_KEY)
        added = 0
        for sec_id, lev in fallback.items():
            if sec_id not in _LEVERAGE_MAP:
                _LEVERAGE_MAP[sec_id] = lev
                added += 1
        logger.info(f"📊 Fallback map added {added} instruments not in mapping.csv")
    except Exception as exc:
        logger.warning(f"⚠️ Couldn't load fallback leverage map ({NIFTYMAP_FILE_KEY}): {exc}")

    logger.info(f"📊 Loaded leverage for {len(_LEVERAGE_MAP)} instruments")


def init_leverage_cache(force=False):
    if force or not _LEVERAGE_MAP:
        _load_leverage_from_s3()
    return _LEVERAGE_MAP


def get_leverage(sec_id: str) -> float:
    if not _LEVERAGE_MAP:
        init_leverage_cache()

    lev = _LEVERAGE_MAP.get(str(sec_id), 1)

    if str(sec_id) not in _LEVERAGE_MAP:
        logger.warning(f"⚠️ Missing leverage for {sec_id}, default=1")

    try:
        lev = float(lev)
    except (TypeError, ValueError):
        lev = float("nan")
    # MIS_LEVERAGE 0 (or blank) = no intraday leverage offered for this
    # stock — ~20% of mapping.csv, mostly recent IPOs. Used as-is it made
    # the fund-based qty exactly 0 (CLEANMAX, 2026-09-28: "Qty zero after
    # validation" with the ₹1,000 risk budget allowing 27 shares). Size at
    # 1x — the plain cash available — instead. If the broker doesn't allow
    # the stock intraday at all, Dhan rejects the order, and that
    # rejection (with its reason) shows on Quantile's Orders page.
    if lev != lev or lev <= 0:  # NaN or <= 0
        logger.warning(f"⚠️ No MIS leverage for {sec_id} (value={_LEVERAGE_MAP.get(str(sec_id))}) — sizing at 1x (cash only)")
        return 1.0
    return lev
