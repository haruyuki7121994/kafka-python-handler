"""Turn Kafka POST_CREATED events into SQS fanout tasks.

Kafka can redeliver a batch after some SQS messages were already accepted.
Workers must therefore make each postId/followerId write idempotent.
"""

import base64
import json
import logging
import os

import boto3

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

_dynamodb = boto3.client("dynamodb")
_sqs = boto3.client("sqs")


def _author_pk(author_id):
    return author_id if author_id.startswith("USER#") else f"USER#{author_id}"


def _followers_count(author_id):
    response = _dynamodb.get_item(
        TableName=os.environ["USERS_TABLE_NAME"],
        Key={"PK": {"S": _author_pk(author_id)}},
        ProjectionExpression="followersCount",
        ConsistentRead=True,
    )
    item = response.get("Item")
    if item is None or "followersCount" not in item:
        raise ValueError(f"User {author_id} is missing followersCount")
    count = int(item["followersCount"]["N"])
    if count < 0:
        raise ValueError(f"User {author_id} has negative followersCount")
    return count


def _follower_ids(author_id):
    query = {
        "TableName": os.environ["FOLLOWERS_TABLE_NAME"],
        "KeyConditionExpression": "PK = :author",
        "ExpressionAttributeValues": {":author": {"S": _author_pk(author_id)}},
        "ProjectionExpression": "SK",
        "ConsistentRead": True,
        "Limit": int(os.environ.get("FANOUT_BATCH_SIZE", "500")),
    }
    while True:
        response = _dynamodb.query(**query)
        for item in response.get("Items", []):
            sort_key = item["SK"]["S"]
            if not sort_key.startswith("FOLLOWER#"):
                raise ValueError(f"Unexpected follower sort key: {sort_key}")
            yield sort_key[len("FOLLOWER#"):]
        cursor = response.get("LastEvaluatedKey")
        if not cursor:
            break
        query["ExclusiveStartKey"] = cursor


def _send_tasks(tasks):
    if not tasks:
        return
    response = _sqs.send_message_batch(
        QueueUrl=os.environ["FANOUT_QUEUE_URL"],
        Entries=[
            {"Id": str(index), "MessageBody": json.dumps(task, separators=(",", ":"))}
            for index, task in enumerate(tasks)
        ],
    )
    if response.get("Failed") or len(response.get("Successful", [])) != len(tasks):
        raise RuntimeError(f"SQS did not accept every fanout task: {response.get('Failed', [])}")


def process_event(event):
    if not isinstance(event, dict) or not event.get("eventId"):
        raise ValueError("Expected a JSON event with eventId")
    if event.get("eventType") != "POST_CREATED":
        log.info("Ignoring eventId=%s eventType=%s", event["eventId"], event.get("eventType"))
        return

    post = event["payload"]
    author_id = post["authorId"]
    post_id = post["postId"]
    created_at = post["createdAt"]
    if not all(isinstance(value, str) and value for value in (author_id, post_id, created_at)):
        raise ValueError("POST_CREATED needs authorId, postId and createdAt")

    count = _followers_count(author_id)
    threshold = int(os.environ.get("CELEBRITY_THRESHOLD", "100000"))
    if count > threshold:
        # The feed read path will fetch this author's posts from the posts store.
        log.info("Fanout-on-read eventId=%s authorId=%s followersCount=%s", event["eventId"], author_id, count)
        return

    batch_size = int(os.environ.get("FANOUT_BATCH_SIZE", "500"))
    if not 1 <= batch_size <= 500:
        raise ValueError("FANOUT_BATCH_SIZE must be between 1 and 500")

    follower_batch = []
    sqs_batch = []
    task_count = 0
    for follower_id in _follower_ids(author_id):
        follower_batch.append(follower_id)
        if len(follower_batch) == batch_size:
            sqs_batch.append({
                "eventId": event["eventId"], "postId": post_id,
                "authorId": author_id, "createdAt": created_at,
                "followerIds": follower_batch,
            })
            follower_batch = []
        if len(sqs_batch) == 10:
            _send_tasks(sqs_batch)
            task_count += len(sqs_batch)
            sqs_batch = []

    if follower_batch:
        sqs_batch.append({
            "eventId": event["eventId"], "postId": post_id,
            "authorId": author_id, "createdAt": created_at,
            "followerIds": follower_batch,
        })
    _send_tasks(sqs_batch)
    task_count += len(sqs_batch)
    log.info("Enqueued fanout eventId=%s authorId=%s tasks=%s", event["eventId"], author_id, task_count)


def lambda_handler(event, context):
    processed = 0
    for records in event["records"].values():
        for record in records:
            try:
                if record.get("value") is None:
                    raise ValueError("Unexpected tombstone in outbox event topic")
                payload = base64.b64decode(record["value"], validate=True)
                process_event(json.loads(payload.decode("utf-8")))
                processed += 1
            except Exception:
                log.exception(
                    "Failed Kafka record: topic=%s partition=%s offset=%s",
                    record.get("topic"), record.get("partition"), record.get("offset"),
                )
                raise  # Do not acknowledge a batch before all its SQS tasks exist.
    return {"processed": processed}
