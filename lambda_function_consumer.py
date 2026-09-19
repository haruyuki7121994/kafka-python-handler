"""Test-only Lambda handler: log and skip failed records; acknowledge batch."""
import base64
import json
import logging

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


def process_event(event):
    """Demo only: replace with idempotent business logic before real use."""
    if not isinstance(event, dict) or not event.get("eventId"):
        raise ValueError("Expected a JSON object with eventId")
    log.info("Received eventId=%s eventType=%s",
             event["eventId"], event.get("eventType"))


def lambda_handler(event, context):
    log.info("Full request: %s", json.dumps(event, ensure_ascii=False))
    processed = 0
    failed = 0
    for records in event["records"].values():
        for record in records:
            try:
                if record.get("value") is None:
                    raise ValueError("Unexpected tombstone in outbox event topic")
                payload = base64.b64decode(record["value"], validate=True)
                process_event(json.loads(payload.decode("utf-8")))
                processed += 1
            except Exception:
                failed += 1
                log.exception(
                    "Skipping failed record: topic=%s partition=%s offset=%s",
                    record.get("topic"), record.get("partition"),
                    record.get("offset"),
                )
    # Normal return acknowledges the batch, including skipped records.
    # Counts are informational; AWS manages offset commits, not this handler.
    return {"processed": processed, "failed": failed}
