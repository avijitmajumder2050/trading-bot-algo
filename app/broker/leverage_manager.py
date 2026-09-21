import logging
import pandas as pd
import io
import boto3

from app.config.settings import S3_BUCKET, MAP_FILE_KEY, ALL_SYMBOLS_MAP_FILE_KEY, AWS_REGION
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
    # entirely). ALL_SYMBOLS_MAP_FILE_KEY is a broader fallback used
    # only to fill in ids mapping.csv doesn't have - mapping.csv's own
    # value always wins for anything both files cover.
    global _LEVERAGE_MAP

    _LEVERAGE_MAP = _leverage_map_from_csv(MAP_FILE_KEY)

    try:
        fallback = _leverage_map_from_csv(ALL_SYMBOLS_MAP_FILE_KEY)
        added = 0
        for sec_id, lev in fallback.items():
            if sec_id not in _LEVERAGE_MAP:
                _LEVERAGE_MAP[sec_id] = lev
                added += 1
        logger.info(f"📊 Fallback map added {added} instruments not in mapping.csv")
    except Exception as exc:
        logger.warning(f"⚠️ Couldn't load fallback leverage map ({ALL_SYMBOLS_MAP_FILE_KEY}): {exc}")

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

    return float(lev)
