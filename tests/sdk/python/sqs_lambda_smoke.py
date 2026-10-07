"""Frozen boto3 proof of owned native SQS-to-Lambda completion and settlement."""
import json
import os
import time
import uuid
from pathlib import Path


def append_row(stage, event):
    with Path(os.environ["SQS_LAMBDA_OUTPUT"]).open("a") as output:
        output.write(json.dumps({"stage": stage, "owner": os.environ["SQS_LAMBDA_OWNER"],
                                 "variant": os.environ["SQS_LAMBDA_VARIANT"], "event": event}) + "\n")


def handler(event, context):
    assert len(event["Records"]) == 1
    record = event["Records"][0]
    body = json.loads(record["body"])
    append_row("started", event)
    mode = body.get("mode", "ok")
    if mode == "fail" or (mode == "retry" and int(record["attributes"]["ApproximateReceiveCount"]) == 1):
        raise RuntimeError("owned handler failure")
    if mode == "timeout":
        while True:
            time.sleep(0.01)
    if mode == "block":
        gate = Path(os.environ["SQS_LAMBDA_GATE"])
        while not gate.exists():
            time.sleep(0.01)
    append_row("completed", event)
    return {"ok": True}


def run():
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError

    suffix = uuid.uuid4().hex[:12]
    resources = []
    config = Config(retries={"max_attempts": 0}, connect_timeout=3, read_timeout=10)
    clients = {}
    for owner in ("A", "B"):
        endpoint = os.environ["SQS_LAMBDA_ENDPOINT_" + owner]
        common = dict(endpoint_url=endpoint, region_name="us-east-1", aws_access_key_id="sdk-fake-key",
                      aws_secret_access_key="sdk-fake-secret", config=config)
        clients[owner] = (boto3.client("sqs", **common), boto3.client("lambda", **common))

    def rows(owner="A", ident=None, stage=None):
        path = Path(os.environ["SQS_LAMBDA_OUTPUT_" + owner])
        if not path.exists():
            return []
        # Captures are complete newline-delimited records, including while a
        # handler appends the next record. Do not parse an unfinished last line.
        lines = path.read_text().splitlines(keepends=True)
        result = [json.loads(line) for line in lines if line.endswith("\n")]
        if ident:
            result = [row for row in result if json.loads(row["event"]["Records"][0]["body"])["id"] == ident]
        if stage:
            result = [row for row in result if row["stage"] == stage]
        return result

    def wait(predicate, timeout=12):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = predicate()
            if result:
                return result
            time.sleep(0.02)
        raise AssertionError("owned condition did not complete")

    def error(code, operation, **kwargs):
        try:
            operation(**kwargs)
        except ClientError as failure:
            assert failure.response["Error"]["Code"] == code, failure.response
        else:
            raise AssertionError("unsupported/unknown operation unexpectedly succeeded")

    def queue(label, owner="A", fifo=False, redrive=None):
        sqs, _ = clients[owner]
        name = f"sdk-mapping-{label}-{suffix}" + (".fifo" if fifo else "")
        attributes = {"VisibilityTimeout": "1"}
        if fifo:
            attributes.update(FifoQueue="true", ContentBasedDeduplication="true")
        if redrive:
            attributes["RedrivePolicy"] = json.dumps({"deadLetterTargetArn": redrive[0], "maxReceiveCount": str(redrive[1])})
        url = sqs.create_queue(QueueName=name, Attributes=attributes)["QueueUrl"]
        arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        resources.append((owner, "queue", url))
        return url, arn

    def mapping(arn, owner="A", function="sqs-handler:live", enabled=True):
        _, functions = clients[owner]
        result = functions.create_event_source_mapping(EventSourceArn=arn, FunctionName=function, BatchSize=1, Enabled=enabled)
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 202, result
        assert result["BatchSize"] == 1 and result["EventSourceArn"] == arn
        assert result["FunctionArn"].endswith(":function:" + function), result
        assert result["State"] == ("Enabled" if enabled else "Disabled"), result
        assert result["EventSourceMappingArn"].endswith(":event-source-mapping:" + result["UUID"])
        assert result["LastModified"].timestamp() > 0
        resources.append((owner, "mapping", result["UUID"]))
        got = functions.get_event_source_mapping(UUID=result["UUID"])
        for key in ("UUID", "EventSourceArn", "FunctionArn", "BatchSize", "State"):
            assert got[key] == result[key], got
        return result["UUID"]

    def delete_mapping(ident, owner="A"):
        _, functions = clients[owner]
        deleted = functions.delete_event_source_mapping(UUID=ident)
        assert deleted["ResponseMetadata"]["HTTPStatusCode"] == 202 and deleted["State"] == "Deleting"
        resources.remove((owner, "mapping", ident))
        error("ResourceNotFoundException", functions.get_event_source_mapping, UUID=ident)

    def send(url, ident, mode="ok", owner="A", fifo=False, attributes=None):
        sqs, _ = clients[owner]
        kwargs = dict(QueueUrl=url, MessageBody=json.dumps({"id": ident, "mode": mode}))
        if fifo:
            kwargs.update(MessageGroupId="ordered", MessageDeduplicationId=ident)
        if attributes:
            kwargs["MessageAttributes"] = attributes
        return sqs.send_message(**kwargs)

    def counts(url, owner="A"):
        return clients[owner][0].get_queue_attributes(QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"])["Attributes"]

    sqs, functions = clients["A"]
    try:
        fifo_url, fifo_arn = queue("fifo", fifo=True)
        private_url, private_arn = queue("private", owner="B", fifo=True)
        # Same resource names/functions are registered independently. A source
        # known only by another broker must not be resolved through AWS or HTTP.
        error("ResourceNotFoundException", functions.create_event_source_mapping, EventSourceArn=private_arn, FunctionName="sqs-handler", BatchSize=1)
        error("ResourceNotFoundException", functions.create_event_source_mapping, EventSourceArn=fifo_arn, FunctionName="sqs-handler:absent", BatchSize=1)
        error("InvalidParameterValueException", functions.create_event_source_mapping, EventSourceArn=fifo_arn.replace("us-east-1", "eu-west-1"), FunctionName="sqs-handler", BatchSize=1)
        error("InvalidParameterValueException", functions.create_event_source_mapping, EventSourceArn=fifo_arn, FunctionName="arn:aws:lambda:us-east-1:123456789012:function:sqs-handler", BatchSize=1)
        for extra in ({"BatchSize": 11}, {"BatchSize": 1, "MaximumBatchingWindowInSeconds": 1},
                      {"BatchSize": 1, "FunctionResponseTypes": ["ReportBatchItemFailures"]},
                      {"BatchSize": 1, "FilterCriteria": {"Filters": [{"Pattern": "{}"}]}},
                      {"BatchSize": 1, "ScalingConfig": {"MaximumConcurrency": 1001}}):
            error("InvalidParameterValueException", functions.create_event_source_mapping, EventSourceArn=fifo_arn, FunctionName="sqs-handler", **extra)

        disabled = mapping(fifo_arn, enabled=False)
        send(fifo_url, "fifo-first", mode="block", fifo=True,
             attributes={"string": {"DataType": "String.custom", "StringValue": "exact string"},
                         "binary": {"DataType": "Binary", "BinaryValue": b"\x00\xff"}})
        second = send(fifo_url, "fifo-second", fifo=True)
        time.sleep(0.15)
        assert not rows(ident="fifo-first"), "disabled mapping consumed a record"
        assert counts(fifo_url)["ApproximateNumberOfMessages"] == "2"
        delete_mapping(disabled)
        enabled = mapping(fifo_arn)
        error("ResourceConflictException", functions.create_event_source_mapping, EventSourceArn=fifo_arn, FunctionName="sqs-handler:live", BatchSize=1)
        start = wait(lambda: rows(ident="fifo-first", stage="started"))[0]
        first_record = start["event"]["Records"][0]
        assert first_record["eventSource"] == "aws:sqs" and first_record["eventSourceARN"] == fifo_arn
        assert first_record["awsRegion"] == "us-east-1" and first_record["receiptHandle"]
        assert first_record["attributes"]["ApproximateReceiveCount"] == "1"
        for key in ("SenderId", "SentTimestamp", "ApproximateFirstReceiveTimestamp", "MessageGroupId", "MessageDeduplicationId", "SequenceNumber"):
            assert first_record["attributes"][key]
        assert first_record["messageAttributes"]["string"]["stringValue"] == "exact string"
        assert first_record["messageAttributes"]["binary"]["binaryValue"] == "AP8="
        assert first_record["messageAttributes"]["string"]["stringListValues"] == []
        assert start["variant"] == "alias" and start["owner"] == "A"
        assert not rows(ident="fifo-first", stage="completed") and not rows(ident="fifo-second")
        assert counts(fifo_url)["ApproximateNumberOfMessagesNotVisible"] == "1", "handler start acknowledged the lease"
        Path(os.environ["SQS_LAMBDA_GATE_A"]).write_text("released")
        wait(lambda: rows(ident="fifo-second", stage="completed"))
        assert [json.loads(row["event"]["Records"][0]["body"])["id"] for row in rows(stage="completed")] == ["fifo-first", "fifo-second"]
        assert rows(ident="fifo-second")[0]["event"]["Records"][0]["messageId"] == second["MessageId"]
        wait(lambda: counts(fifo_url)["ApproximateNumberOfMessagesNotVisible"] == "0")
        assert sqs.receive_message(QueueUrl=fifo_url).get("Messages", []) == []
        delete_mapping(enabled)

        other_mapping = mapping(private_arn, owner="B", function="sqs-handler")
        send(private_url, "isolated-B", owner="B", fifo=True)
        wait(lambda: rows(owner="B", ident="isolated-B", stage="completed"))
        assert not rows(ident="isolated-B")
        assert rows(owner="B", ident="isolated-B")[0]["owner"] == "B"
        delete_mapping(other_mapping, owner="B")

        retry_url, retry_arn = queue("retry")
        retry_mapping = mapping(retry_arn)
        send(retry_url, "retry", mode="retry")
        wait(lambda: rows(ident="retry", stage="completed"))
        retry_records = [row["event"]["Records"][0] for row in rows(ident="retry", stage="started")]
        assert len(retry_records) == 2 and [r["attributes"]["ApproximateReceiveCount"] for r in retry_records] == ["1", "2"]
        assert retry_records[0]["messageId"] == retry_records[1]["messageId"]
        assert retry_records[0]["receiptHandle"] != retry_records[1]["receiptHandle"]
        wait(lambda: counts(retry_url)["ApproximateNumberOfMessagesNotVisible"] == "0")
        assert sqs.receive_message(QueueUrl=retry_url).get("Messages", []) == []
        delete_mapping(retry_mapping)

        dlq_url, dlq_arn = queue("dlq", fifo=True)
        fail_url, fail_arn = queue("failure", fifo=True, redrive=(dlq_arn, 5))
        fail_mapping = mapping(fail_arn)
        failed_send = send(fail_url, "failure", mode="fail", fifo=True)
        dead = wait(lambda: sqs.receive_message(QueueUrl=dlq_url, MessageSystemAttributeNames=["All"]).get("Messages"))
        assert len(dead) == 1 and dead[0]["MessageId"] == failed_send["MessageId"]
        failed_records = [row["event"]["Records"][0] for row in rows(ident="failure", stage="started")]
        assert [r["attributes"]["ApproximateReceiveCount"] for r in failed_records] == [str(n) for n in range(1, 6)], failed_records
        assert not rows(ident="failure", stage="completed")
        sqs.delete_message(QueueUrl=dlq_url, ReceiptHandle=dead[0]["ReceiptHandle"])
        delete_mapping(fail_mapping)

        timeout_dlq_url, timeout_dlq_arn = queue("timeout-dlq")
        timeout_url, timeout_arn = queue("timeout", redrive=(timeout_dlq_arn, 2))
        timeout_mapping = mapping(timeout_arn, function="sqs-short")
        timeout_sent = send(timeout_url, "timeout", mode="timeout")
        expired = wait(lambda: sqs.receive_message(QueueUrl=timeout_dlq_url, MessageSystemAttributeNames=["All"]).get("Messages"))
        assert expired[0]["MessageId"] == timeout_sent["MessageId"]
        assert len(rows(ident="timeout", stage="started")) == 2
        assert not rows(ident="timeout", stage="completed")
        sqs.delete_message(QueueUrl=timeout_dlq_url, ReceiptHandle=expired[0]["ReceiptHandle"])
        delete_mapping(timeout_mapping)

        gate = Path(os.environ["SQS_LAMBDA_GATE_A"])
        gate.unlink()
        deleted_url, deleted_arn = queue("deleted")
        deleted_mapping = mapping(deleted_arn)
        send(deleted_url, "delete-pending", mode="block")
        wait(lambda: rows(ident="delete-pending", stage="started"))
        delete_mapping(deleted_mapping)
        send(deleted_url, "after-delete")
        gate.write_text("would-have-completed")
        time.sleep(1.1)
        assert not rows(ident="delete-pending", stage="completed") and not rows(ident="after-delete")
        available = sqs.receive_message(QueueUrl=deleted_url, MaxNumberOfMessages=10, MessageSystemAttributeNames=["All"])["Messages"]
        assert {json.loads(message["Body"])["id"] for message in available} == {"delete-pending", "after-delete"}
        assert next(m for m in available if json.loads(m["Body"])["id"] == "delete-pending")["Attributes"]["ApproximateReceiveCount"] == "2"
        for message in available:
            sqs.delete_message(QueueUrl=deleted_url, ReceiptHandle=message["ReceiptHandle"])

        # Leave one actual blocked handler to Go's owner teardown. The mapping
        # remains owned until Close cancels and joins its process group.
        gate.unlink()
        teardown_url, teardown_arn = queue("teardown")
        mapping(teardown_arn)
        send(teardown_url, "teardown-pending", mode="block")
        wait(lambda: rows(ident="teardown-pending", stage="started"))
        Path(os.environ["SQS_LAMBDA_PENDING_ARN"]).write_text(teardown_arn)
        resources = [resource for resource in resources if resource[2] not in (teardown_url,)]
        resources = [resource for resource in resources if resource[1] != "mapping"]
        print("PASS")
    finally:
        for owner, kind, ident in reversed(resources):
            try:
                if kind == "mapping":
                    clients[owner][1].delete_event_source_mapping(UUID=ident)
                else:
                    clients[owner][0].delete_queue(QueueUrl=ident)
            except ClientError:
                pass


if __name__ == "__main__":
    run()
