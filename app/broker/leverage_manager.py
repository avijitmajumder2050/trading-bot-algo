import logging
import pandas as pd
import io
import boto3

from app.config.settings import S3_BUCKET, MAP_FILE_KEY,AWS_REGION
from app.config.aws_s3 import s3

logger = logging.getLogger(__name__)

_LEVERAGE_MAP = {}



def _load_leverage_from_s3():
    # MAP_FILE_KEY (uploads/mapping.csv), not NIFTYMAP_FILE_KEY — the
    # breakout-race winner handed off from Quantile can be any NSE
    # stock the scanner's top-10 picks, not just a Nifty constituent,
    # so a Nifty-only leverage table would silently default a real
    # non-Nifty winner's leverage to 1 even when mapping.csv has its
    # actual MIS_LEVERAGE on file.
    global _LEVERAGE_MAP

    obj = s3.get_object(Bucket=S3_BUCKET, Key=MAP_FILE_KEY)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))

    if "Instrument ID" not in df.columns:
        raise ValueError("Instrument ID missing in leverage CSV")

    if "MIS_LEVERAGE" not in df.columns:
        logger.warning("⚠️ MIS_LEVERAGE missing, defaulting to 1")

    # mapping.csv's Instrument ID column is stored/read as float64
    # (e.g. "25468.0"), but callers look up by the plain integer id
    # Dhan itself uses ("25468") - str()'ing the raw column produced
    # keys that could never match a real lookup, so EVERY instrument
    # silently fell back to the default leverage of 1. Confirmed live:
    # PAISALO (25468) is genuinely in the file with MIS_LEVERAGE=5,
    # but get_leverage("25468") still missed it before this fix.
    ids = pd.to_numeric(df["Instrument ID"], errors="coerce")
    valid = ids.notna()
    leverage_col = df["MIS_LEVERAGE"] if "MIS_LEVERAGE" in df.columns else pd.Series(1, index=df.index)

    _LEVERAGE_MAP = dict(
        zip(
            ids[valid].astype(int).astype(str),
            leverage_col[valid]
        )
    )

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
