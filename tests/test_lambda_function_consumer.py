import base64
import json
import os
import unittest
from unittest.mock import patch

os.environ.setdefault("AWS_DEFAULT_REGION", "ap-southeast-1")
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")
os.environ.setdefault("USERS_TABLE_NAME", "users")
os.environ.setdefault("FOLLOWERS_TABLE_NAME", "followers")
os.environ.setdefault("FANOUT_QUEUE_URL", "https://sqs.example/fanout")

from lambdas import lambda_function_consumer as consumer


def kafka_event():
    message = {
        "eventId": "EVENT#1",
        "eventType": "POST_CREATED",
        "payload": {
            "authorId": "USER#author", "postId": "POST#1",
            "createdAt": "2026-09-19T10:00:00",
        },
    }
    return {"records": {"newsfeed.events-0": [{
        "value": base64.b64encode(json.dumps(message).encode()).decode(),
        "topic": "newsfeed.events", "partition": 0, "offset": 1,
    }]}}


class FakeDynamo:
    def __init__(self, count, follower_total):
        self.count = count
        self.follower_total = follower_total
        self.query_calls = 0

    def get_item(self, **kwargs):
        assert kwargs["Key"] == {"PK": {"S": "USER#author"}}
        return {"Item": {"followersCount": {"N": str(self.count)}}}

    def query(self, **kwargs):
        self.query_calls += 1
        assert kwargs["ExpressionAttributeValues"][":author"] == {"S": "USER#author"}
        start = int(kwargs.get("ExclusiveStartKey", {}).get("index", {}).get("N", "0"))
        stop = min(start + 200, self.follower_total)
        result = {"Items": [{"SK": {"S": f"FOLLOWER#USER#{i}"}} for i in range(start, stop)]}
        if stop < self.follower_total:
            result["LastEvaluatedKey"] = {"index": {"N": str(stop)}}
        return result


class FakeSQS:
    def __init__(self, fail=False):
        self.entries = []
        self.fail = fail

    def send_message_batch(self, **kwargs):
        self.entries.extend(kwargs["Entries"])
        if self.fail:
            return {"Successful": [], "Failed": [{"Id": "0", "Code": "InternalError"}]}
        return {"Successful": [{"Id": entry["Id"]} for entry in kwargs["Entries"]]}


class ConsumerTests(unittest.TestCase):
    def test_normal_author_is_split_into_500_follower_tasks(self):
        dynamo, sqs = FakeDynamo(1001, 1001), FakeSQS()
        with patch.object(consumer, "_dynamodb", dynamo), patch.object(consumer, "_sqs", sqs):
            result = consumer.lambda_handler(kafka_event(), None)
        self.assertEqual(result, {"processed": 1})
        tasks = [json.loads(entry["MessageBody"]) for entry in sqs.entries]
        self.assertEqual([len(task["followerIds"]) for task in tasks], [500, 500, 1])
        self.assertEqual(tasks[0]["followerIds"][0], "USER#0")
        self.assertEqual(tasks[-1]["followerIds"][-1], "USER#1000")
        self.assertGreater(dynamo.query_calls, 1)

    def test_celebrity_does_not_enqueue_fanout(self):
        dynamo, sqs = FakeDynamo(100001, 0), FakeSQS()
        with patch.object(consumer, "_dynamodb", dynamo), patch.object(consumer, "_sqs", sqs):
            self.assertEqual(consumer.lambda_handler(kafka_event(), None), {"processed": 1})
        self.assertEqual(dynamo.query_calls, 0)
        self.assertEqual(sqs.entries, [])

    def test_sqs_partial_failure_does_not_acknowledge_kafka_batch(self):
        dynamo, sqs = FakeDynamo(1, 1), FakeSQS(fail=True)
        with patch.object(consumer, "_dynamodb", dynamo), patch.object(consumer, "_sqs", sqs):
            with self.assertRaises(RuntimeError):
                consumer.lambda_handler(kafka_event(), None)


if __name__ == "__main__":
    unittest.main()
