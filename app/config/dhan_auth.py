# app/config/dhan_auth.py
from dhanhq import DhanContext, dhanhq
from app.config.aws_ssm import get_param

_client_id = None
_access_token = None

def get_dhan_client():
    global _client_id, _access_token
    if not _client_id or not _access_token:
        _client_id = get_param("/dhan/client_id")
        _access_token = get_param("/dhan/access_token")
    return dhanhq(DhanContext(_client_id, _access_token))

dhan = get_dhan_client()
