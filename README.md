# Kafka to SQS news feed fanout

`POST_CREATED` flows through DynamoDB outbox → Kafka → consumer → SQS → Redis worker.
The Kafka producer owns the original outbox `status`: `PENDING`, `PUBLISHED`, or
`FAILED`. A successful Kafka publish sets `status=PUBLISHED`. Fanout has separate
fields on the same outbox item: `fanoutStatus`, `fanoutExpected`,
`fanoutCompleted`, and `fanoutScheduled`. The worker sets
`fanoutStatus=COMPLETED` and `expiresAt` seven days after **all** SQS tasks
complete. A task that fails on its third SQS receive sets
`fanoutStatus=FAILED` and `expiresAt` seven days later; SQS then retains the
message in the DLQ for repair. The original `status` stays `PUBLISHED` in
both cases because publication to Kafka succeeded.

The worker writes `ZADD userFeeds:{followerId}:latest postId createdAtMillis`.
Replaying a task writes the same member and score. A DynamoDB receipt per
`eventId/taskId` prevents duplicate task deliveries from increasing
`fanoutCompleted` twice. The consumer stores a hash of the task plan; a
changed follower snapshot on Kafka replay fails visibly instead of silently
counting different tasks as the same work. That case needs operator repair.
The consumer does not fan out celebrity posts; the feed read API must merge
their posts separately. Celebrity events receive `fanoutStatus=FANOUT_ON_READ`
and a seven day outbox TTL; this marks routing only, not feed delivery.

## Redis secret

Create a Secrets Manager JSON secret in `ap-southeast-1`:

```json
{"host":"your-redis-host","port":15872,"username":"default","password":"...","ssl":false}
```

Set `ssl` to `true` when your Redis endpoint supports TLS. Keep this secret
out of the repository. Set `REDIS_SECRET_ARN` along with the existing deploy
variables before running `./deploy.sh`. Ensure the Lambda can reach the Redis
endpoint; a private Redis endpoint needs suitable VPC, security group, and
outbound AWS service connectivity configuration, which this template does not
create.

The existing Lambda role needs `secretsmanager:GetSecretValue` on the Redis
secret; `sqs:ReceiveMessage`, `sqs:DeleteMessage`, and
`sqs:GetQueueAttributes` on the fanout queue; and `dynamodb:GetItem`,
`dynamodb:UpdateItem`, `dynamodb:PutItem`, and `dynamodb:TransactWriteItems`
on the outboxes table. The deploying identity needs `iam:PassRole` for the
existing role. The DynamoDB outboxes table must already have TTL enabled on
the Number attribute `expiresAt`.

SQS retries a failed message up to three receives, then moves it to the DLQ.
The queue and DLQ retention are one day, preserving the repository's existing
setting. Add a DLQ age/depth alarm and repair process; investigate failures
within that day. The outbox failure record expires after seven days as
requested. DynamoDB TTL deletion is asynchronous; an expired item may remain
visible for some time.
