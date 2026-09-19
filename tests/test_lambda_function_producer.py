import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("AWS_DEFAULT_REGION", "ap-southeast-1")
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")
os.environ.setdefault("OUTBOX_TABLE_NAME", "outboxes")
os.environ.setdefault("KAFKA_TOPIC", "newsfeed.events")

from lambdas import lambda_function_producer as producer


def stream_record():
    return {
        "eventName": "INSERT",
        "dynamodb": {
            "SequenceNumber": "123",
            "NewImage": {
                "PK": {"S": "EVENT#123"},
                "status": {"S": "PENDING"},
                "eventType": {"S": "POST_CREATED"},
                "aggregateId": {"S": "POST#456"},
                "payload": {"S": '{"postId":"POST#456"}'},
                "createdAt": {"S": "2026-09-19T12:00:00"},
            },
        },
    }


class FakeDynamo:
    exceptions = SimpleNamespace(ConditionalCheckFailedException=Exception)

    def __init__(self):
        self.status = "PENDING"
        self.updates = []

    def get_item(self, **kwargs):
        return {"Item": {"status": {"S": self.status}}}

    def update_item(self, **kwargs):
        self.updates.append(kwargs)
        self.status = kwargs["ExpressionAttributeValues"][":status"]["S"]


class FakeKafka:
    def __init__(self, failures):
        self.failures = failures
        self.sends = []

    def send(self, *args, **kwargs):
        self.sends.append((args, kwargs))
        kafka = self

        class Future:
            def get(self, timeout):
                if len(kafka.sends) <= kafka.failures:
                    raise TimeoutError("Kafka unavailable")

        return Future()


class ProducerTests(unittest.TestCase):
    def test_second_attempt_publishes_and_marks_outbox(self):
        dynamo, kafka = FakeDynamo(), FakeKafka(failures=1)
        with patch.object(producer, "_dynamodb", dynamo), patch.object(producer, "_get_producer", return_value=kafka), patch.object(producer, "sleep"):
            result = producer.lambda_handler({"Records": [stream_record()]}, None)
        self.assertEqual(result, {"batchItemFailures": []})
        self.assertEqual(len(kafka.sends), 2)
        self.assertEqual(dynamo.status, "PUBLISHED")
        self.assertNotIn(":expiresAt", dynamo.updates[0]["ExpressionAttributeValues"])

    def test_three_failures_mark_outbox_failed_with_seven_day_ttl(self):
        dynamo, kafka = FakeDynamo(), FakeKafka(failures=3)
        with patch.object(producer, "_dynamodb", dynamo), patch.object(producer, "_get_producer", return_value=kafka), patch.object(producer, "sleep"):
            result = producer.lambda_handler({"Records": [stream_record()]}, None)
        self.assertEqual(result, {"batchItemFailures": []})
        self.assertEqual(len(kafka.sends), 3)
        self.assertEqual(dynamo.status, "FAILED")
        values = dynamo.updates[0]["ExpressionAttributeValues"]
        self.assertEqual(values[":retryCount"], {"N": "3"})
        from datetime import datetime, timezone
        seconds_remaining = int(values[":expiresAt"]["N"]) - int(datetime.now(timezone.utc).timestamp())
        self.assertTrue(7 * 86400 - 10 <= seconds_remaining <= 7 * 86400)

    def test_replayed_insert_skips_published_outbox(self):
        dynamo, kafka = FakeDynamo(), FakeKafka(failures=0)
        dynamo.status = "PUBLISHED"
        with patch.object(producer, "_dynamodb", dynamo), patch.object(producer, "_get_producer", return_value=kafka):
            result = producer.lambda_handler({"Records": [stream_record()]}, None)
        self.assertEqual(result, {"batchItemFailures": []})
        self.assertEqual(kafka.sends, [])

    def test_status_write_failure_requests_stream_retry(self):
        dynamo, kafka = FakeDynamo(), FakeKafka(failures=3)
        dynamo.update_item = lambda **kwargs: (_ for _ in ()).throw(RuntimeError("DynamoDB unavailable"))
        with patch.object(producer, "_dynamodb", dynamo), patch.object(producer, "_get_producer", return_value=kafka), patch.object(producer, "sleep"):
            result = producer.lambda_handler({"Records": [stream_record()]}, None)
        self.assertEqual(result, {"batchItemFailures": [{"itemIdentifier": "123"}]})
        self.assertEqual(dynamo.status, "PENDING")


if __name__ == "__main__":
    unittest.main()
