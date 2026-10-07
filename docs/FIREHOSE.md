# Firehose delivery

EventBus supports CreateDeliveryStream, DescribeDeliveryStream,
DeleteDeliveryStream, PutRecord and PutRecordBatch through AWS JSON 1.1.
Streams use DirectPut and one local S3 destination. The application provisions
its bucket and selects the S3 endpoint with `--s3-endpoint`; the CLI signs S3
requests with local credentials `test` / `testtest123`. Role ARNs are validated
metadata; EventBus does not assume roles or enforce IAM.

## SNS → Firehose → S3 example

Set `EVENTBUS_ENDPOINT_URL` to your owned EventBus listener and `S3_ENDPOINT_URL`
to your owned RustFS instance. Configure EventBus with `--region us-east-1`,
`--account-id 000000000000`, `--s3-endpoint "$S3_ENDPOINT_URL"` and a task-owned
`--cognito-db` path. The example never selects an AWS endpoint. Use the
repository's pinned Python SDK environment from
[SDK verification](../tests/sdk/README.md).

```python
import gzip
import json
import os
import time
import uuid

import boto3

local = dict(region_name="us-east-1", aws_access_key_id="test",
             aws_secret_access_key="testtest123")
aws_endpoint = os.environ["EVENTBUS_ENDPOINT_URL"]
sns = boto3.client("sns", endpoint_url=aws_endpoint, **local)
firehose = boto3.client("firehose", endpoint_url=aws_endpoint, **local)
s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT_URL"], **local)
suffix = uuid.uuid4().hex
bucket, stream_name = "eventbus-" + suffix, "events-" + suffix
role = "arn:aws:iam::000000000000:role/local-delivery"
s3.create_bucket(Bucket=bucket)
stream = firehose.create_delivery_stream(
    DeliveryStreamName=stream_name, DeliveryStreamType="DirectPut",
    ExtendedS3DestinationConfiguration={
        "BucketARN": "arn:aws:s3:::" + bucket, "RoleARN": role,
        "CompressionFormat": "GZIP",
        "BufferingHints": {"SizeInMBs": 1, "IntervalInSeconds": 60},
        "Prefix": "tenant=!{partitionKeyFromQuery:tenant}/day=!{partitionKeyFromQuery:day}/!{timestamp:yyyy/MM/dd/HH/}",
        "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/!{timestamp:yyyy/MM/dd/HH/}",
        "DynamicPartitioningConfiguration": {"Enabled": True},
        "ProcessingConfiguration": {"Enabled": True, "Processors": [
            {"Type": "MetadataExtraction", "Parameters": [
                {"ParameterName": "JsonParsingEngine", "ParameterValue": "JQ-1.6"},
                {"ParameterName": "MetadataExtractionQuery",
                 "ParameterValue": '{tenant:.customer_id,day:(.timestamp|strftime("%Y-%m-%d"))}'},
            ]},
            {"Type": "AppendDelimiterToRecord"},
        ]},
    })
topic = sns.create_topic(Name="notifications-" + suffix)["TopicArn"]
subscription = sns.subscribe(
    TopicArn=topic, Protocol="firehose", Endpoint=stream["DeliveryStreamARN"],
    Attributes={"SubscriptionRoleArn": role, "RawMessageDelivery": "true",
                "FilterPolicyScope": "MessageBody",
                "FilterPolicy": json.dumps({"kind": ["accepted"]})})
sns.publish(TopicArn=topic, Message=json.dumps({
    "kind": "accepted", "customer_id": "north", "timestamp": 1577934245}))

# Wait for this example's 60-second buffering interval.
deadline = time.monotonic() + 65
while time.monotonic() < deadline:
    objects = s3.list_objects_v2(Bucket=bucket).get("Contents", [])
    if objects:
        break
    time.sleep(0.2)
else:
    raise TimeoutError("Firehose did not deliver the record")
for item in objects:
    data = s3.get_object(Bucket=bucket, Key=item["Key"])["Body"].read()
    print(item["Key"], gzip.decompress(data).decode() if item["Key"].endswith(".gz") else data.decode())
```

The successful record keeps its original JSON bytes plus the requested newline.
Its object is GZIP-compressed under `tenant=north/day=2020-01-02/`. Timestamp
prefixes use the oldest record's arrival time and `CustomTimeZone` (default UTC).
Without a timestamp expression, the default `yyyy/MM/dd/HH/` suffix is appended.
Firehose subscriptions require a standard SNS topic, a valid SubscriptionRoleArn
and a delivery-stream ARN in the topic's configured region/account.

Filters run before admission. Raw delivery contains only the message, with no
attributes. Set RawMessageDelivery=false for the SNS notification envelope;
a query that handles both shapes is
`(.Message? // . | if type=="string" then fromjson else . end) | {tenant:.customer_id}`.
Clean up only these owned resources: unsubscribe/delete the topic, successfully
delete the delivery stream, then remove the objects and bucket.

## Processing and supported settings

MetadataExtraction accepts the native JsonParsingEngine=JQ-1.6 field but executes
the pinned Go jq interpreter, not a literal jq 1.6 binary. Tests cover nested
expressions, strings, map/add, fromjson and strftime. Integer precision, regexes,
object ordering and time behavior differ from jq; see
[gojq's documented differences](https://github.com/itchyny/gojq#difference-to-jq).
Queries compile at stream creation. Unsupported functions/modules fail then;
record-dependent failures, including unsupported regexes, go to the error prefix.
Exactly one object with non-null scalar partition values is required. Queries
are limited to 5120 bytes, results to 64 KiB, execution to two seconds per record
and ten seconds per admission batch/publication. These limits are not a hard
intermediate-memory sandbox. Host modules, input streams and environment are
not supplied.

Supported processing is MetadataExtraction plus AppendDelimiterToRecord with
the newline delimiter. UNCOMPRESSED and GZIP output, CustomTimeZone,
FileExtension, BufferingHints and dynamic RetryOptions are retained and returned
by DescribeDeliveryStream. Native defaults are 5 MiB / 300 seconds; supplied
buffering hints must include both members (size 1..128 MiB, interval 0..900s).
The supported timestamp format tokens are yyyy, MM, dd, HH, mm, ss, SSS and DDD,
with single-quoted literals. Prefixes also support partitionKeyFromQuery,
firehose:random-string and error-prefix-only firehose:error-output-type;
evaluated prefixes are limited to 512 bytes. Dynamic partitioning requires
MetadataExtraction, a query-key prefix and an error prefix.

Unsupported settings are refused rather than ignored: other stream sources or
destinations, Lambda/deaggregation processors, backup, format conversion,
encryption, enabled CloudWatch logging, other compression formats and creation
Tags. List/update/tag/encryption-management operations are not implemented.

## Acceptance, retries and shutdown

PutRecordBatch validates every modeled record before mutation: 1..500 records,
at most 1000 KiB per decoded record and 4 MiB total. Malformed types/base64/limits
reject the whole request. Admission failures return ordered per-entry AWS
ErrorCode/ErrorMessage responses while later records can still succeed.
Transport accepts base64 overhead; it is not restricted to the old 1 MiB body.

Each partition has its own size/age buffer. A stream retains at most 64 MiB of
original record bytes and 100000 records, including pending deliveries. This
state is in memory; restart loses configuration and undelivered records. There
is no AWS 24-hour durable retention or active-partition quota simulation.

A materialized object keeps the same key and bytes on retries, including an
ambiguous S3 response. Dynamic RetryOptions defaults to 300 seconds (0..7200
supported); expiry moves original records into a retained error-prefix object.
Parsing, extraction and prefix failures also create uncompressed JSONL
diagnostics with base64 rawData and error metadata. Successful data uses the
configured compression; processing never reserializes its original payload.

SNS Publish success accepts the publication, not final S3 delivery. Initial
SNS capture records scheduling; admission failure uses SNS DeliveryFailure/DLQ
policy, while accepted-record S3 retries remain Firehose-owned.
App shutdown quiesces background callers and drains HTTP before closing
Firehose. Firehose stops admission, cancels/joins its worker and attempts final
delivery. Failed final flush is reported and its data stays reachable in the
manager until process exit; repeated shutdown shares that result. Failed
DeleteDeliveryStream retains the stopping stream for an explicit retry.

The opt-in [native integration test](../internal/firehose/pipeline_integration_test.go)
uses real Go SNS/Firehose SDK requests and a unique bucket on an explicitly
owned loopback RustFS endpoint. It covers filtering, raw/envelope delivery,
partitions, GZIP, error records, destination failure/recovery and final drain.

```sh
S3_ENDPOINT_URL="$S3_ENDPOINT_URL" \
  ssd-dev operation --purpose test -- \
  go test -race -tags integration ./internal/firehose ./internal/messaging
```
