#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
aws_region="${AWS_REGION:-ap-southeast-1}"

: "${STACK_NAME:?Set STACK_NAME to the CloudFormation stack name}"
: "${CODE_BUCKET:?Set CODE_BUCKET to the S3 deployment bucket}"
: "${LAMBDA_ROLE_ARN:?Set LAMBDA_ROLE_ARN to the existing Lambda role ARN}"
: "${OUTBOX_STREAM_ARN:?Set OUTBOX_STREAM_ARN to the DynamoDB outboxes stream ARN}"
: "${KAFKA_CREDENTIALS_SECRET_ARN:?Set KAFKA_CREDENTIALS_SECRET_ARN}"
: "${KAFKA_CA_SECRET_ARN:?Set KAFKA_CA_SECRET_ARN}"
: "${REDIS_SECRET_ARN:?Set REDIS_SECRET_ARN}"

for command_name in aws python3 zip; do
    command -v "$command_name" >/dev/null || {
        echo "Missing required command: $command_name" >&2
        exit 1
    }
done

if [[ "$aws_region" != "ap-southeast-1" ]]; then
    echo "This CloudFormation template requires AWS_REGION=ap-southeast-1" >&2
    exit 1
fi

version="$(python3 -c 'import time; print(time.time_ns() // 1_000_000)')"
s3_key="news-feed/kafka/deploy/${version}.zip"
temporary_dir="$(mktemp -d)"
trap 'rm -rf "$temporary_dir"' EXIT
package_dir="$temporary_dir/package"
zip_path="$temporary_dir/${version}.zip"
mkdir -p "$package_dir"

# Build dependencies for the template's Python 3.12 x86_64 Lambda runtime.
python3 -m pip install \
    --disable-pip-version-check --no-compile \
    --platform manylinux2014_x86_64 --implementation cp --python-version 3.12 \
    --only-binary=:all: --target "$package_dir" \
    -r "$repo_dir/requirements.txt"

cp "$repo_dir"/lambdas/*.py "$package_dir/"
(
    cd "$package_dir"
    zip -q -r "$zip_path" .
)

echo "Uploading s3://${CODE_BUCKET}/${s3_key}"
aws s3 cp "$zip_path" "s3://${CODE_BUCKET}/${s3_key}" \
    --region "$aws_region" --only-show-errors

echo "Deploying CloudFormation stack ${STACK_NAME} with version ${version}"
aws cloudformation deploy \
    --template-file "$repo_dir/infra/template.yml" \
    --stack-name "$STACK_NAME" \
    --region "$aws_region" \
    --parameter-overrides \
        "ExistingLambdaRoleArn=${LAMBDA_ROLE_ARN}" \
        "CodeBucket=${CODE_BUCKET}" \
        "DeploymentVersion=${version}" \
        "OutboxStreamArn=${OUTBOX_STREAM_ARN}" \
        "KafkaCredentialsSecretArn=${KAFKA_CREDENTIALS_SECRET_ARN}" \
        "KafkaCaSecretArn=${KAFKA_CA_SECRET_ARN}" \
        "RedisSecretArn=${REDIS_SECRET_ARN}" \
    --no-fail-on-empty-changeset

echo "Deployed version ${version}: s3://${CODE_BUCKET}/${s3_key}"
