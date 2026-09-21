"""Client for Quantile's quantile-order-intents DynamoDB table — the
hand-off point from Quantile's breakout-race winner
(app.py's _breakout_watch_once, in the Chartink_Momentum_AI_AWS repo)
to this project's dedicated-IP Dhan order execution.

This repo has no dependency on Quantile's own connectors package, so
this is a small standalone client against the same table rather than
a shared import — see that repo's connectors/order_intent_connector.py
for the write side (create_intent) and the idempotency-gate-1 design
this is the other half of.

status progresses: pending -> claimed -> paper_filled | live_filled -> closed
claim_pending_intent() is idempotency gate 2: a conditional update that
only succeeds while status is still "pending", so two overlapping poll
cycles can never both act on the same intent.
"""

import datetime
import logging
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

AWS_REGION = "ap-south-1"
TABLE_NAME = "quantile-order-intents"

logger = logging.getLogger(__name__)

_table = None


def _get_table():
    global _table
    if _table is None:
        _table = boto3.resource("dynamodb", region_name=AWS_REGION).Table(TABLE_NAME)
    return _table


def list_pending_intents():
    items = _get_table().scan(FilterExpression=Attr("status").eq("pending")).get("Items", [])
    items.sort(key=lambda i: i.get("created_at") or "")
    return items


def claim_pending_intent(entry_id):
    """Idempotency gate 2. Returns the claimed item, or None if it was
    already claimed by another poll cycle in the meantime."""
    now_iso = datetime.datetime.utcnow().isoformat()
    try:
        result = _get_table().update_item(
            Key={"entry_id": entry_id},
            UpdateExpression="SET #s = :claimed, updated_at = :now",
            ConditionExpression=Attr("status").eq("pending"),
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":claimed": "claimed", ":now": now_iso},
            ReturnValues="ALL_NEW",
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return None
        raise
    return result.get("Attributes")


def mark_intent_result(entry_id, status, **extra_fields):
    """status: paper_filled | live_filled | closed | failed. extra_fields
    e.g. order_id, outcome, filled_qty, target_price, trailing_jump.

    Confirmed live: dhan_super_client.place_trade() returns plain Python
    floats (qty/target/trailing_jump come straight out of round()) -
    boto3's DynamoDB resource rejects those outright ("Float types are
    not supported"), which silently failed every real paper/live fill
    write-back until caught by an actual e2e run. str() first avoids
    binary-float rounding artifacts (Decimal(0.1) != Decimal("0.1"))."""
    now_iso = datetime.datetime.utcnow().isoformat()
    update_parts = ["#s = :status", "updated_at = :now"]
    names = {"#s": "status"}
    values = {":status": status, ":now": now_iso}
    for i, (key, val) in enumerate(extra_fields.items()):
        if isinstance(val, float):
            val = Decimal(str(val))
        placeholder = f":v{i}"
        name_placeholder = f"#f{i}"
        update_parts.append(f"{name_placeholder} = {placeholder}")
        names[name_placeholder] = key
        values[placeholder] = val
    _get_table().update_item(
        Key={"entry_id": entry_id},
        UpdateExpression="SET " + ", ".join(update_parts),
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )


def has_open_intents():
    """claimed/paper_filled/live_filled — anything not yet closed/failed.
    Used to hold off self-termination while a trade this instance placed
    is still open."""
    items = _get_table().scan(
        FilterExpression=Attr("status").is_in(["claimed", "paper_filled", "live_filled"])
    ).get("Items", [])
    return len(items) > 0
