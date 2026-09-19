"""Track fanout independently of the Kafka publication status of an outbox."""

import os
from datetime import datetime, timedelta, timezone

import boto3


db = boto3.client("dynamodb")


def _table():
    return os.environ["OUTBOX_TABLE_NAME"]


def _key(event_id):
    return {"PK": {"S": event_id}}


def _ttl(days):
    return str(int((datetime.now(timezone.utc) + timedelta(days=days)).timestamp()))


def status(event_id):
    result = db.get_item(
        TableName=_table(), Key=_key(event_id), ConsistentRead=True,
        ProjectionExpression="fanoutStatus",
    )
    if "Item" not in result:
        raise ValueError(f"Missing outbox {event_id}")
    return result["Item"].get("fanoutStatus", {}).get("S")


def mark_read(event_id):
    try:
        db.update_item(
            TableName=_table(), Key=_key(event_id),
            UpdateExpression="SET fanoutStatus = :read, expiresAt = :ttl",
            ConditionExpression="attribute_exists(PK) AND #status = :published AND "
                                "(attribute_not_exists(fanoutStatus) OR fanoutStatus = :read)",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":published": {"S": "PUBLISHED"}, ":read": {"S": "FANOUT_ON_READ"},
                ":ttl": {"N": _ttl(int(os.environ.get("OUTBOX_SUCCESS_TTL_DAYS", "7")))},
            },
        )
    except db.exceptions.ConditionalCheckFailedException:
        if status(event_id) != "FANOUT_ON_READ":
            raise


def prepare(event_id, expected, plan_hash):
    db.update_item(
        TableName=_table(), Key=_key(event_id),
        UpdateExpression="SET fanoutPlanHash = if_not_exists(fanoutPlanHash, :hash), "
                         "fanoutExpected = if_not_exists(fanoutExpected, :expected), "
                         "fanoutCompleted = if_not_exists(fanoutCompleted, :zero), "
                         "fanoutStatus = if_not_exists(fanoutStatus, :running)",
        ConditionExpression="attribute_exists(PK) AND #status = :published AND "
                            "(attribute_not_exists(fanoutExpected) OR fanoutExpected = :expected) AND "
                            "(attribute_not_exists(fanoutPlanHash) OR fanoutPlanHash = :hash) AND "
                            "(attribute_not_exists(fanoutStatus) OR fanoutStatus = :running)",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
            ":published": {"S": "PUBLISHED"}, ":expected": {"N": str(expected)},
            ":hash": {"S": plan_hash},
            ":zero": {"N": "0"}, ":running": {"S": "IN_PROGRESS"},
        },
    )


def scheduled(event_id):
    db.update_item(
        TableName=_table(), Key=_key(event_id),
        UpdateExpression="SET fanoutScheduled = :yes",
        ConditionExpression="attribute_exists(PK) AND fanoutStatus = :running",
        ExpressionAttributeValues={":yes": {"BOOL": True}, ":running": {"S": "IN_PROGRESS"}},
    )
    complete_if_ready(event_id)


def complete_if_ready(event_id):
    try:
        db.update_item(
            TableName=_table(), Key=_key(event_id),
            UpdateExpression="SET fanoutStatus = :done, expiresAt = :ttl",
            ConditionExpression="fanoutStatus = :running AND fanoutScheduled = :yes AND "
                                "fanoutCompleted = fanoutExpected",
            ExpressionAttributeValues={
                ":done": {"S": "COMPLETED"}, ":running": {"S": "IN_PROGRESS"},
                ":yes": {"BOOL": True},
                ":ttl": {"N": _ttl(int(os.environ.get("OUTBOX_SUCCESS_TTL_DAYS", "7")))},
            },
        )
    except db.exceptions.ConditionalCheckFailedException:
        pass  # Other batches are still pending, or another worker completed it.


def record_completed(event_id, task_id):
    receipt_key = f"FANOUT_TASK#{event_id}#{task_id}"
    try:
        db.transact_write_items(TransactItems=[
            {"Put": {
                "TableName": _table(),
                "Item": {
                    "PK": {"S": receipt_key},
                    "expiresAt": {"N": _ttl(int(os.environ.get("FANOUT_RECEIPT_TTL_DAYS", "30")))},
                },
                "ConditionExpression": "attribute_not_exists(PK)",
            }},
            {"Update": {
                "TableName": _table(), "Key": _key(event_id),
                "UpdateExpression": "ADD fanoutCompleted :one",
                "ConditionExpression": "fanoutStatus = :running AND fanoutCompleted < fanoutExpected",
                "ExpressionAttributeValues": {
                    ":one": {"N": "1"}, ":running": {"S": "IN_PROGRESS"},
                },
            }},
        ])
    except db.exceptions.TransactionCanceledException:
        receipt = db.get_item(TableName=_table(), Key=_key(receipt_key), ConsistentRead=True)
        if "Item" not in receipt:
            raise
    complete_if_ready(event_id)


def failed(event_id):
    try:
        db.update_item(
            TableName=_table(), Key=_key(event_id),
            UpdateExpression="SET fanoutStatus = :failed, fanoutFailedAt = :at, expiresAt = :ttl",
            ConditionExpression="fanoutStatus = :running",
            ExpressionAttributeValues={
                ":failed": {"S": "FAILED"}, ":running": {"S": "IN_PROGRESS"},
                ":at": {"S": datetime.now(timezone.utc).isoformat()},
                ":ttl": {"N": _ttl(7)},
            },
        )
    except db.exceptions.ConditionalCheckFailedException:
        if status(event_id) not in ("FAILED", "COMPLETED"):
            raise
