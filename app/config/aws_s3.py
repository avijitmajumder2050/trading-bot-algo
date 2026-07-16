# app/config/aws_s3.py
import boto3
import pandas as pd
import io
import os
import logging
from botocore.exceptions import ClientError

AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")

DEFAULT_BUCKET = os.getenv("S3_BUCKET")

BUCKET_CANDIDATES = [
    DEFAULT_BUCKET,
    "new-dhan-trading-data",
    "dhan-trading-data",
]

BUCKET_CANDIDATES = [b for b in BUCKET_CANDIDATES if b]

s3 = boto3.client("s3", region_name=AWS_REGION)


def get_working_bucket():
    for bucket in dict.fromkeys(BUCKET_CANDIDATES):
        try:
            s3.head_bucket(Bucket=bucket)
            logging.info(f"Using S3 bucket: {bucket}")
            return bucket
        except ClientError:
            continue

    raise RuntimeError(
        f"No accessible bucket found: {BUCKET_CANDIDATES}"
    )


S3_BUCKET = get_working_bucket()


def read_csv_from_s3(bucket=None, key="") -> pd.DataFrame:

    bucket = bucket or S3_BUCKET

    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(io.BytesIO(obj["Body"].read()))

    except s3.exceptions.NoSuchKey:
        logging.error(f"S3 key not found: s3://{bucket}/{key}")
        return pd.DataFrame()

    except Exception as e:
        logging.error(e)
        return pd.DataFrame()


def list_s3_files(bucket=None, prefix=""):

    bucket = bucket or S3_BUCKET

    try:
        paginator = s3.get_paginator("list_objects_v2")

        keys = []

        for page in paginator.paginate(
            Bucket=bucket,
            Prefix=prefix
        ):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])

        return keys

    except Exception as e:
        logging.error(e)
        return []