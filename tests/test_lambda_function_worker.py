import json
import os
import sys
import types
import unittest
from unittest.mock import patch

os.environ.setdefault("AWS_DEFAULT_REGION", "ap-southeast-1")
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")
os.environ.setdefault("OUTBOX_TABLE_NAME", "outboxes")
os.environ.setdefault("REDIS_SECRET_ARN", "test-secret")
sys.modules.setdefault("redis", types.ModuleType("redis"))

from lambdas import lambda_function_worker as worker


class FakePipeline:
    def __init__(self, fail=False):
        self.commands = []
        self.fail = fail

    def zadd(self, *args):
        self.commands.append(("zadd", args))

    def zremrangebyrank(self, *args):
        self.commands.append(("trim", args))

    def expire(self, *args):
        self.commands.append(("expire", args))

    def execute(self):
        if self.fail:
            raise RuntimeError("Redis unavailable")


class FakeRedis:
    def __init__(self, pipeline):
        self._pipeline = pipeline

    def pipeline(self, **kwargs):
        return self._pipeline


TASK = {
    "eventId": "EVENT#1", "taskId": "0", "postId": "POST#1",
    "createdAt": "2026-09-19T10:00:00", "followerIds": ["USER#1", "USER#2"],
}


class WorkerTests(unittest.TestCase):
    def test_writes_each_feed_and_records_completion(self):
        pipeline = FakePipeline()
        with patch.object(worker, "_client", return_value=FakeRedis(pipeline)), \
                patch.object(worker.fanout_state, "status", return_value="IN_PROGRESS"), \
                patch.object(worker.fanout_state, "record_completed") as completed:
            worker.process_task(TASK)
        keys = [args[0] for name, args in pipeline.commands if name == "zadd"]
        self.assertEqual(keys, ["userFeeds:USER#1:latest", "userFeeds:USER#2:latest"])
        completed.assert_called_once_with("EVENT#1", "0")

    def test_third_failed_receive_marks_outbox_and_returns_partial_failure(self):
        record = {"messageId": "m1", "body": json.dumps(TASK),
                  "attributes": {"ApproximateReceiveCount": "3"}}
        with patch.object(worker, "_client", return_value=FakeRedis(FakePipeline(fail=True))), \
                patch.object(worker.fanout_state, "status", return_value="IN_PROGRESS"), \
                patch.object(worker.fanout_state, "record_completed") as completed, \
                patch.object(worker.fanout_state, "failed") as failed:
            result = worker.lambda_handler({"Records": [record]}, None)
        self.assertEqual(result, {"batchItemFailures": [{"itemIdentifier": "m1"}]})
        completed.assert_not_called()
        failed.assert_called_once_with("EVENT#1")


if __name__ == "__main__":
    unittest.main()
