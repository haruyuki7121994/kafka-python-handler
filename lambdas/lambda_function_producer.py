"""Publish inserted DynamoDB outbox items to Kafka, then update their status."""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from datetime import timedelta
from time import sleep

import boto3
from boto3.dynamodb.types import TypeDeserializer
from kafka import KafkaProducer

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

_deserializer = TypeDeserializer()
_dynamodb = boto3.client("dynamodb")
_secrets = boto3.client("secretsmanager")
_producer = None


def _secret_json(arn):
    return json.loads(_secrets.get_secret_value(SecretId=arn)["SecretString"])


def _get_producer():
    global _producer
    if _producer is None:
        credentials = _secret_json(os.environ["KAFKA_CREDENTIALS_SECRET_ARN"])
        ca = _secret_json(os.environ["KAFKA_CA_SECRET_ARN"])["certificate"]
        ca_path = Path("/tmp/news-feed-kafka-ca.pem")
        ca_path.write_text(ca, encoding="utf-8")
        ca_path.chmod(0o600)
        _producer = KafkaProducer(
            bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"].split(","),
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_plain_username=credentials["username"],
            sasl_plain_password=credentials["password"],
            ssl_cafile=str(ca_path),
            acks="all",
            retries=0,  # The handler counts exactly three publish attempts.
            max_block_ms=5000,
            request_timeout_ms=30000,
        )
    return _producer


def _item(image):
    return {name: _deserializer.deserialize(value) for name, value in image.items()}


def _outbox_status(event_id):
    response = _dynamodb.get_item(
        TableName=os.environ["OUTBOX_TABLE_NAME"],
        Key={"PK": {"S": event_id}},
        ConsistentRead=True,
        ProjectionExpression="#status",
        ExpressionAttributeNames={"#status": "status"},
    )
    return _item(response["Item"]).get("status") if "Item" in response else None


def _update_status(event_id, status, extra_values, update_expression):
    try:
        _dynamodb.update_item(
            TableName=os.environ["OUTBOX_TABLE_NAME"],
            Key={"PK": {"S": event_id}},
            UpdateExpression=update_expression,
            ConditionExpression="attribute_exists(PK) AND #status = :pending",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":pending": {"S": "PENDING"},
                ":status": {"S": status},
                **extra_values,
            },
        )
    except _dynamodb.exceptions.ConditionalCheckFailedException:
        if _outbox_status(event_id) != status:
            raise


def _mark_published(event_id):
    _update_status(
        event_id,
        "PUBLISHED",
        {":publishedAt": {"S": datetime.now(timezone.utc).replace(tzinfo=None).isoformat()}},
        "SET #status = :status, publishedAt = :publishedAt",
    )


def _mark_failed(event_id):
    expires_at = int((datetime.now(timezone.utc) + timedelta(days=7)).timestamp())
    _update_status(
        event_id,
        "FAILED",
        {":retryCount": {"N": "3"}, ":expiresAt": {"N": str(expires_at)}},
        "SET #status = :status, retryCount = :retryCount, expiresAt = :expiresAt",
    )


def _publish(record):
    if record.get("eventName") != "INSERT":
        return
    outbox = _item(record["dynamodb"]["NewImage"])
    event_id = outbox["PK"]
    if outbox.get("status") != "PENDING":
        return
    current_status = _outbox_status(event_id)
    if current_status in ("PUBLISHED", "FAILED"):
        return
    if current_status != "PENDING":
        raise ValueError(f"Outbox {event_id} is missing or has unexpected status {current_status}")

    message = {
        "eventId": event_id,
        "eventType": outbox["eventType"],
        "aggregateId": outbox["aggregateId"],
        "payload": json.loads(outbox["payload"]),
        "createdAt": outbox["createdAt"],
    }
    value = json.dumps(message, ensure_ascii=False).encode("utf-8")
    for attempt in range(1, 4):
        try:
            _get_producer().send(
                os.environ["KAFKA_TOPIC"],
                key=event_id.encode("utf-8"),
                value=value,
            ).get(timeout=20)
        except Exception:
            log.exception("Kafka publish failed eventId=%s attempt=%s/3", event_id, attempt)
            if attempt < 3:
                sleep(attempt)
            continue
        _mark_published(event_id)
        log.info("Published outbox eventId=%s attempt=%s", event_id, attempt)
        return

    _mark_failed(event_id)
    log.error("Outbox eventId=%s marked FAILED after three Kafka attempts", event_id)


def lambda_handler(event, context):
    log.info("Full request: %s", json.dumps(event, ensure_ascii=False))
    for record in event.get("Records", []):
        try:
            _publish(record)
        except Exception:
            sequence_number = record.get("dynamodb", {}).get("SequenceNumber")
            log.exception("Failed to publish outbox stream record sequence=%s", sequence_number)
            if not sequence_number:
                raise
            # Retry only when the DynamoDB status update or record processing fails.
            return {"batchItemFailures": [{"itemIdentifier": sequence_number}]}
    return {"batchItemFailures": []}
