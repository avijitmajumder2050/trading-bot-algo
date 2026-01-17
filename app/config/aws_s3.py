# app/config/aws_s3.py
import boto3
import pandas as pd
import io
import os
import logging

AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
S3_BUCKET = os.getenv("S3_BUCKET", "dhan-trading-data")

s3 = boto3.client("s3", region_name=AWS_REGION)

def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """
    Reads a CSV file from S3 and returns a pandas DataFrame.
    
    Args:
        bucket (str): S3 bucket name
        key (str): S3 object key (path to file)

    Returns:
        pd.DataFrame: CSV content as DataFrame
    """
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(io.BytesIO(obj["Body"].read()))
    except s3.exceptions.NoSuchKey:
        logging.error(f"❌ S3 key not found: s3://{bucket}/{key}")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"❌ Error reading CSV from S3: {e}")
        return pd.DataFrame()

def list_s3_files(bucket: str, prefix: str):
    """
    List all files under a specific S3 prefix.

    Args:
        bucket (str): S3 bucket name
        prefix (str): folder/prefix path

    Returns:
        list: List of object keys
    """
    try:
        paginator = s3.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            contents = page.get("Contents", [])
            for obj in contents:
                keys.append(obj["Key"])
        return keys
    except Exception as e:
        logging.error(f"❌ Error listing S3 files: {e}")
        return []
