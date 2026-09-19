"""Write SQS fanout batches to Redis and account for completed outbox fanout."""

import json
import logging
import os
from datetime import datetime, timezone

import boto3
import redis

try:
    from . import fanout_state
except ImportError:
    import fanout_state


log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
secrets = boto3.client("secretsmanager")
_redis = None


def _client():
    global _redis
    if _redis is None:
        secret = json.loads(secrets.get_secret_value(
            SecretId=os.environ["REDIS_SECRET_ARN"],
        )["SecretString"])
        client = redis.Redis(
            host=secret["host"], port=int(secret["port"]),
            username=secret.get("username"), password=secret["password"],
            ssl=secret.get("ssl", False),
            socket_connect_timeout=5, socket_timeout=10,
            retry_on_timeout=False,
        )
        client.ping()
        _redis = client
    return _redis


def _score(value):
    created = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    return int(created.timestamp() * 1000)


def process_task(task):
    for field in ("eventId", "taskId", "postId", "createdAt", "followerIds"):
        if field not in task:
            raise ValueError(f"Fanout task missing {field}")
    follower_ids = task["followerIds"]
    if not isinstance(follower_ids, list) or not 1 <= len(follower_ids) <= 500:
        raise ValueError("followerIds must contain 1 to 500 users")
    if any(not isinstance(user_id, str) or not user_id for user_id in follower_ids):
        raise ValueError("Invalid followerId")
    current_status = fanout_state.status(task["eventId"])
    if current_status == "COMPLETED":
        return
    if current_status != "IN_PROGRESS":
        raise RuntimeError(f"Fanout event {task['eventId']} has status {current_status}")

    pipeline = _client().pipeline(transaction=False)
    score = _score(task["createdAt"])
    max_entries = int(os.environ.get("FEED_MAX_ENTRIES", "1000"))
    ttl_seconds = int(os.environ.get("FEED_TTL_SECONDS", str(30 * 86400)))
    if max_entries < 1 or ttl_seconds < 1:
        raise ValueError("Feed limits must be positive")
    for follower_id in follower_ids:
        key = f"userFeeds:{follower_id}:latest"
        pipeline.zadd(key, {task["postId"]: score})
        pipeline.zremrangebyrank(key, 0, -(max_entries + 1))
        pipeline.expire(key, ttl_seconds)
    pipeline.execute()  # Replays are safe: ZADD uses the same member and score.
    fanout_state.record_completed(task["eventId"], task["taskId"])


def lambda_handler(event, context):
    failures = []
    for record in event.get("Records", []):
        task = None
        try:
            task = json.loads(record["body"])
            process_task(task)
        except Exception:
            log.exception("Fanout task failed messageId=%s", record.get("messageId"))
            try:
                # SQS redrive is configured for three receives. Leave the message
                # failed so SQS retains it in the DLQ for later repair.
                if task and int(record.get("attributes", {}).get("ApproximateReceiveCount", "1")) >= 3:
                    fanout_state.failed(task["eventId"])
            except Exception:
                log.exception("Could not mark fanout FAILED messageId=%s", record.get("messageId"))
            failures.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": failures}
