import boto3

_ssm = boto3.client("ssm", region_name="ap-south-1")

def get_param(name: str, decrypt: bool = True) -> str:
    response = _ssm.get_parameter(
        Name=name,
        WithDecryption=decrypt
    )
    return response["Parameter"]["Value"]
