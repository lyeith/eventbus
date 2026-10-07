"""Frozen boto3 proof of native SQS batches and owned bounded concurrency.

The handler performs no queue settlement. Gates and flock-protected evidence are
fixture controls; native polling, FIFO exclusion, visibility, redrive and child
ownership are the same service/core implementations used by the application.
"""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import time
import urllib.request
import uuid


def append_row(stage, event, context):
    row = {"stage": stage, "owner": os.environ["SQS_BATCH_OWNER"],
           "variant": os.environ["SQS_BATCH_VARIANT"], "pid": os.getpid(),
           "request_id": context.aws_request_id, "function_arn": context.invoked_function_arn,
           "event": event}
    with (Path(os.environ["SQS_BATCH_ROOT"]) / "effects.jsonl").open("a", encoding="utf-8") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        stream.write(json.dumps(row) + "\n")
        stream.flush()
        fcntl.flock(stream, fcntl.LOCK_UN)


def handler(event, context):
    records = event["Records"]
    assert 1 <= len(records) <= 10, event
    bodies = [json.loads(record["body"]) for record in records]
    append_row("started", event, context)
    if any(body["mode"] == "timeout" for body in bodies):
        while True:
            time.sleep(0.01)
    if any(body["mode"] == "fail" for body in bodies) or any(
            body["mode"] == "retry" and int(record["attributes"]["ApproximateReceiveCount"]) == 1
            for body, record in zip(bodies, records)):
        append_row("failed", event, context)
        raise RuntimeError("owned full-batch failure")
    if any(body["mode"] == "gate" for body in bodies):
        gate = Path(os.environ["SQS_BATCH_ROOT"]) / ("release-" + context.aws_request_id)
        while not gate.exists():
            time.sleep(0.01)
    append_row("completed", event, context)
    return {"ok": True}


def run():
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError

    suffix = uuid.uuid4().hex[:12]
    resources = []
    roots = {owner: Path(os.environ["SQS_BATCH_ROOT_" + owner]) for owner in ("A", "B")}
    endpoints = {owner: os.environ["SQS_BATCH_ENDPOINT_" + owner] for owner in ("A", "B")}
    config = Config(retries={"max_attempts": 0}, connect_timeout=3, read_timeout=15)
    clients = {}
    for owner in ("A", "B"):
        common = dict(endpoint_url=endpoints[owner], region_name="us-east-1", aws_access_key_id="sdk-fake-key",
                      aws_secret_access_key="sdk-fake-secret", config=config)
        clients[owner] = (boto3.client("sqs", **common), boto3.client("lambda", **common))

    def rows(scenario=None, stage=None, owner="A"):
        path = roots[owner] / "effects.jsonl"
        if not path.exists():
            return []
        with path.open(encoding="utf-8") as stream:
            fcntl.flock(stream, fcntl.LOCK_SH)
            result = [json.loads(line) for line in stream]
            fcntl.flock(stream, fcntl.LOCK_UN)
        if scenario:
            result = [row for row in result if json.loads(row["event"]["Records"][0]["body"])["scenario"] == scenario]
        if stage:
            result = [row for row in result if row["stage"] == stage]
        return result

    def wait(probe, label, timeout=15):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = probe()
            if result:
                return result
            time.sleep(0.015)
        raise AssertionError("did not observe " + label)

    def error(code, operation, **kwargs):
        try:
            operation(**kwargs)
        except ClientError as failure:
            assert failure.response["Error"]["Code"] == code, failure.response
        else:
            raise AssertionError("expected " + code)

    def queue(label, owner="A", fifo=False, visibility=6, redrive=None):
        attributes = {"VisibilityTimeout": str(visibility)}
        if fifo:
            attributes.update(FifoQueue="true", ContentBasedDeduplication="true")
        if redrive:
            attributes["RedrivePolicy"] = json.dumps({"deadLetterTargetArn": redrive, "maxReceiveCount": "2"})
        url = clients[owner][0].create_queue(QueueName="sdk-batch-" + label + "-" + suffix + (".fifo" if fifo else ""),
                                           Attributes=attributes)["QueueUrl"]
        arn = clients[owner][0].get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        resources.append((owner, "queue", url))
        return url, arn

    def mapping(arn, owner="A", batch=5, concurrency=2, enabled=True, function="batch-handler:live"):
        kwargs = dict(EventSourceArn=arn, FunctionName=function, Enabled=enabled, MaximumBatchingWindowInSeconds=0)
        if batch is not None:
            kwargs["BatchSize"] = batch
        if concurrency is not None:
            kwargs["ScalingConfig"] = {"MaximumConcurrency": concurrency}
        result = clients[owner][1].create_event_source_mapping(**kwargs)
        expected_batch = 10 if batch is None else batch
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 202
        assert result["EventSourceArn"] == arn and result["FunctionArn"].endswith(":function:" + function)
        assert result["BatchSize"] == expected_batch and result["MaximumBatchingWindowInSeconds"] == 0
        assert result["State"] == ("Enabled" if enabled else "Disabled")
        if concurrency is not None:
            assert result["ScalingConfig"] == {"MaximumConcurrency": concurrency}, result
            result["ScalingConfig"]["MaximumConcurrency"] = 99
        got = clients[owner][1].get_event_source_mapping(UUID=result["UUID"])
        assert got["BatchSize"] == expected_batch and got["State"] == result["State"]
        if concurrency is not None:
            assert got["ScalingConfig"] == {"MaximumConcurrency": concurrency}, got
        else:
            assert not got.get("ScalingConfig", {}).get("MaximumConcurrency"), got
        resources.append((owner, "mapping", result["UUID"]))
        return result["UUID"]

    def delete_mapping(ident, owner="A"):
        result = clients[owner][1].delete_event_source_mapping(UUID=ident)
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 202 and result["State"] == "Deleting"
        resources.remove((owner, "mapping", ident))
        error("ResourceNotFoundException", clients[owner][1].get_event_source_mapping, UUID=ident)

    def send_backlog(url, scenario, count, owner="A", mode="ok", group=None, start=0, overrides=None, padding=0):
        results = []
        for index in range(start, start + count):
            body = {"scenario": scenario, "index": index, "mode": (overrides or {}).get(index, mode)}
            if padding:
                body["padding"] = "x" * padding
            kwargs = dict(QueueUrl=url, MessageBody=json.dumps(body), MessageAttributes={
                "string": {"DataType": "String.custom", "StringValue": "batch value"},
                "binary": {"DataType": "Binary", "BinaryValue": b"\x00\x01\xff"}})
            if group:
                kwargs.update(MessageGroupId=group, MessageDeduplicationId=scenario + "-" + group + "-" + str(index))
            results.append(clients[owner][0].send_message(**kwargs))
        return results

    def counts(url, owner="A"):
        attrs = clients[owner][0].get_queue_attributes(QueueUrl=url, AttributeNames=[
            "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"])["Attributes"]
        return int(attrs["ApproximateNumberOfMessages"]), int(attrs["ApproximateNumberOfMessagesNotVisible"])

    def release(row):
        (roots[row["owner"]] / ("release-" + row["request_id"])).write_text("released", encoding="utf-8")

    def alive(pid):
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False

    def native_records(row, arn):
        assert row["owner"] in ("A", "B") and row["variant"] in ("alias", "short")
        assert row["function_arn"].endswith(":live" if row["variant"] == "alias" else ":short")
        records = row["event"]["Records"]
        for record in records:
            assert record["eventSource"] == "aws:sqs" and record["eventSourceARN"] == arn
            assert record["awsRegion"] == "us-east-1" and record["messageId"] and record["receiptHandle"]
            assert record["md5OfBody"] == hashlib.md5(record["body"].encode()).hexdigest()
            assert record["attributes"]["SenderId"] and record["attributes"]["SentTimestamp"]
            assert record["attributes"]["ApproximateFirstReceiveTimestamp"]
            assert record["messageAttributes"]["string"]["stringValue"] == "batch value"
            assert record["messageAttributes"]["binary"]["binaryValue"] == "AAH/"
            assert record["messageAttributes"]["string"]["stringListValues"] == []
            assert record["messageAttributes"]["binary"]["binaryListValues"] == []
        return records

    def wait_empty(url, owner="A"):
        wait(lambda: counts(url, owner) == (0, 0), "all successful receipts acknowledged")

    sqs, functions = clients["A"]
    try:
        # All supported batch sizes/readback and both native scaling boundaries.
        settings_url, settings_arn = queue("settings")
        for batch in range(1, 11):
            ident = mapping(settings_arn, batch=batch, concurrency=2, enabled=False)
            delete_mapping(ident)
        ident = mapping(settings_arn, batch=10, concurrency=1000, enabled=False)
        delete_mapping(ident)
        for extra in ({"BatchSize": 11}, {"BatchSize": 1, "MaximumBatchingWindowInSeconds": 1},
                      {"BatchSize": 1, "FunctionResponseTypes": ["ReportBatchItemFailures"]}):
            error("InvalidParameterValueException", functions.create_event_source_mapping,
                  EventSourceArn=settings_arn, FunctionName="batch-handler:live", **extra)
        error("ResourceNotFoundException", functions.create_event_source_mapping,
              EventSourceArn=settings_arn, FunctionName="batch-handler:unknown", BatchSize=5)
        error("ResourceNotFoundException", functions.get_event_source_mapping, UUID=str(uuid.uuid4()))
        print("native batch sizes 1..10 and scaling 2/1000 create/readback/delete verified")

        # Omitted native BatchSize defaults to ten. Omitted ScalingConfig is the
        # documented local serial policy, proved while the first batch is held.
        url, arn = queue("default")
        sent = send_backlog(url, "default", 11, mode="gate")
        ident = mapping(arn, batch=None, concurrency=None)
        started = wait(lambda: rows("default", "started"), "default ten-record batch")
        assert len(started) == 1 and len(native_records(started[0], arn)) == 10, started
        assert counts(url) == (1, 10) and not rows("default", "completed")
        release(started[0])
        second = wait(lambda: rows("default", "started") if len(rows("default", "started")) == 2 else None,
                      "serial second default batch")[1]
        assert len(native_records(second, arn)) == 1
        release(second)
        wait_empty(url)
        assert [len(row["event"]["Records"]) for row in rows("default", "completed")] == [10, 1]
        delete_mapping(ident)
        print("default BatchSize=10; omitted scaling kept one real invocation at a time")

        # Fifteen already queued messages make two full batches achievable.
        # Frozen gates plus current native leases prove the bound without relying
        # on a sleep to assert the absence of a third concurrent invocation.
        url, arn = queue("concurrency")
        sent = send_backlog(url, "concurrency", 15, mode="gate")
        ident = mapping(arn)
        started = wait(lambda: rows("concurrency", "started") if len(rows("concurrency", "started")) >= 2 else None,
                       "two real concurrently held batches")
        assert len(started) == 2 and all(len(native_records(row, arn)) == 5 for row in started), started
        assert all(alive(row["pid"]) for row in started)
        assert counts(url) == (5, 10) and not rows("concurrency", "completed")
        release(started[0])
        third = wait(lambda: rows("concurrency", "started") if len(rows("concurrency", "started")) == 3 else None,
                     "queued third batch after one completion")[2]
        assert len(native_records(third, arn)) == 5 and alive(started[1]["pid"])
        assert counts(url) == (0, 10), "current receipts were acknowledged before handler completion"
        release(started[1])
        release(third)
        wait_empty(url)
        active, maximum = set(), 0
        for row in rows("concurrency"):
            if row["stage"] == "started":
                active.add(row["request_id"])
                maximum = max(maximum, len(active))
            elif row["stage"] == "completed":
                active.remove(row["request_id"])
        assert maximum == 2 and not active
        assert {record["messageId"] for row in rows("concurrency", "completed") for record in row["event"]["Records"]} == {item["MessageId"] for item in sent}
        delete_mapping(ident)
        print("BatchSize=5/MaximumConcurrency=2: batches [5,5,5], max active 2, third queued, no early ack")

        # Projection must enforce Lambda's full-event limit before acquiring
        # receipts. Metadata and body escaping count toward the native payload.
        url, arn = queue("payload")
        sent = send_backlog(url, "payload", 10, mode="gate", padding=800_000)
        ident = mapping(arn, batch=10, concurrency=None)
        first = wait(lambda: rows("payload", "started"), "byte-bounded projected batch")[0]
        records = native_records(first, arn)
        assert len(records) == 7, len(records)
        assert len(json.dumps(first["event"], separators=(",", ":"), ensure_ascii=False).encode()) <= 6 * 1024 * 1024
        assert counts(url) == (3, 7), "payload truncation leased records omitted from the event"
        release(first)
        second = wait(lambda: rows("payload", "started") if len(rows("payload", "started")) == 2 else None,
                      "remaining three records after projected batch completion")[1]
        assert len(native_records(second, arn)) == 3
        release(second)
        wait_empty(url)
        completed = rows("payload", "completed")
        assert {record["messageId"] for row in completed for record in row["event"]["Records"]} == {item["MessageId"] for item in sent}
        assert {record["attributes"]["ApproximateReceiveCount"] for row in completed for record in row["event"]["Records"]} == {"1"}
        delete_mapping(ident)
        print("6MiB native projection: 800KB backlog batched [7,3]; excluded records stayed visible/unleased")

        # One failing record fails the whole batch: otherwise-successful records
        # retain the same IDs and receive new leases only after visibility expiry.
        url, arn = queue("retry")
        sent = send_backlog(url, "retry", 5, overrides={0: "retry"})
        ident = mapping(arn)
        failed = wait(lambda: rows("retry", "failed"), "full-batch function failure")[0]
        first = native_records(failed, arn)
        assert len(first) == 5 and counts(url) == (0, 5) and not rows("retry", "completed")
        completed = wait(lambda: rows("retry", "completed"), "full batch visibility retry")[0]
        second = native_records(completed, arn)
        assert len(second) == 5 and {r["messageId"] for r in first} == {r["messageId"] for r in second}
        assert {r["attributes"]["ApproximateReceiveCount"] for r in first} == {"1"}
        assert {r["attributes"]["ApproximateReceiveCount"] for r in second} == {"2"}
        assert {r["receiptHandle"] for r in first}.isdisjoint({r["receiptHandle"] for r in second})
        wait_empty(url)
        delete_mapping(ident)
        print("one record failed the full five-record batch; visibility retry preserved IDs and renewed receipts")

        dlq_url, dlq_arn = queue("dlq", visibility=1)
        url, arn = queue("timeout", visibility=1, redrive=dlq_arn)
        sent = send_backlog(url, "timeout", 5, mode="timeout")
        ident = mapping(arn, function="batch-handler:short")
        wait(lambda: counts(dlq_url) == (5, 0), "five timed-out records redriven after two receives")
        attempts = rows("timeout", "started")
        assert len(attempts) == 2 and not rows("timeout", "completed"), attempts
        assert [len(native_records(row, arn)) for row in attempts] == [5, 5]
        assert [{r["attributes"]["ApproximateReceiveCount"] for r in row["event"]["Records"]} for row in attempts] == [{"1"}, {"2"}]
        dead = sqs.receive_message(QueueUrl=dlq_url, MaxNumberOfMessages=10, MessageSystemAttributeNames=["All"])["Messages"]
        assert {message["MessageId"] for message in dead} == {item["MessageId"] for item in sent}
        wait(lambda: all(not alive(row["pid"]) for row in attempts), "timed-out handler children joined")
        delete_mapping(ident)
        print("two full-batch timeout attempts; all five original messages redriven to DLQ; children joined")

        # FIFO backlog fits each initial group exactly into one batch. Distinct
        # groups execute concurrently; a sixth record in either group stays
        # queued until that group's currently held batch has completed.
        url, arn = queue("fifo", fifo=True)
        send_backlog(url, "fifo", 6, mode="gate", group="A")
        send_backlog(url, "fifo", 6, mode="gate", group="B")
        ident = mapping(arn)
        starts = wait(lambda: rows("fifo", "started") if len(rows("fifo", "started")) >= 2 else None,
                      "concurrent distinct FIFO groups")
        assert len(starts) == 2 and counts(url) == (2, 10), starts
        by_group = {}
        for row in starts:
            records = native_records(row, arn)
            groups = {record["attributes"]["MessageGroupId"] for record in records}
            assert len(records) == 5 and len(groups) == 1, records
            by_group[next(iter(groups))] = row
            assert [json.loads(record["body"])["index"] for record in records] == list(range(5))
            assert [int(record["attributes"]["SequenceNumber"]) for record in records] == sorted(int(record["attributes"]["SequenceNumber"]) for record in records)
        assert set(by_group) == {"A", "B"}
        release(by_group["A"])
        third = wait(lambda: rows("fifo", "started") if len(rows("fifo", "started")) == 3 else None,
                     "next record in released FIFO group")[2]
        records = native_records(third, arn)
        assert len(records) == 1 and records[0]["attributes"]["MessageGroupId"] == "A" and json.loads(records[0]["body"])["index"] == 5
        assert alive(by_group["B"]["pid"]), "the other group should still be held"
        release(third)
        release(by_group["B"])
        fourth = wait(lambda: rows("fifo", "started") if len(rows("fifo", "started")) == 4 else None,
                      "next record in second FIFO group")[3]
        assert len(fourth["event"]["Records"]) == 1 and fourth["event"]["Records"][0]["attributes"]["MessageGroupId"] == "B"
        release(fourth)
        wait_empty(url)
        for group in ("A", "B"):
            indexes = [json.loads(record["body"])["index"] for row in rows("fifo", "started")
                       for record in row["event"]["Records"] if record["attributes"]["MessageGroupId"] == group]
            assert indexes == list(range(6)), indexes
        delete_mapping(ident)
        print("FIFO five-record groups A/B ran concurrently; same-group sixth records waited in order")

        # Independent listener/registry owners, disabled mappings and unknown IDs.
        private_url, private_arn = queue("private", owner="B")
        error("ResourceNotFoundException", functions.create_event_source_mapping,
              EventSourceArn=private_arn, FunctionName="batch-handler:live", BatchSize=5)
        ident = mapping(private_arn, owner="B", enabled=False)
        send_backlog(private_url, "disabled", 5, owner="B")
        assert counts(private_url, "B") == (5, 0) and not rows("disabled", owner="B")
        error("ResourceNotFoundException", functions.get_event_source_mapping, UUID=ident)
        delete_mapping(ident, "B")
        ident = mapping(private_arn, owner="B")
        wait(lambda: rows("disabled", "completed", "B"), "isolated owner's batch")
        assert not rows("disabled") and rows("disabled", "completed", "B")[0]["owner"] == "B"
        wait_empty(private_url, "B")
        delete_mapping(ident, "B")
        print("disabled mapping preserved backlog; owner isolation and unknown/deleted mapping errors verified")

        # Delete and Close each cancel/join two real pending handler children.
        # Gate release after joining cannot complete either child or settle its
        # current receipt. Native visibility then makes all ten messages retryable.
        for owner, label in (("A", "delete-pending"), ("B", "close-pending")):
            url, arn = queue(label, owner=owner)
            sent = send_backlog(url, label, 10, owner=owner, mode="gate")
            ident = mapping(arn, owner=owner)
            starts = wait(lambda: rows(label, "started", owner) if len(rows(label, "started", owner)) >= 2 else None,
                          label + " two real children")
            assert len(starts) == 2 and all(len(row["event"]["Records"]) == 5 and alive(row["pid"]) for row in starts)
            assert counts(url, owner) == (0, 10)
            if owner == "A":
                delete_mapping(ident, owner)
            else:
                request = urllib.request.Request(endpoints[owner] + "/__sdk/close-mappings", data=b"", method="POST")
                with urllib.request.urlopen(request, timeout=8) as response:
                    assert response.status == 200 and json.load(response)["closed"] is True
                resources.remove((owner, "mapping", ident))
                error("ServiceException", clients[owner][1].create_event_source_mapping,
                      EventSourceArn=arn, FunctionName="batch-handler:live", BatchSize=5)
            wait(lambda: all(not alive(row["pid"]) for row in starts), label + " children joined")
            for row in starts:
                release(row)
            assert not rows(label, "completed", owner)
            assert counts(url, owner) == (0, 10), label + " acknowledged pending work"
            wait(lambda: counts(url, owner) == (10, 0), label + " visibility retained all current receipts")
            redelivered = clients[owner][0].receive_message(QueueUrl=url, MaxNumberOfMessages=10, MessageSystemAttributeNames=["All"])["Messages"]
            assert len(redelivered) == 10 and {message["MessageId"] for message in redelivered} == {item["MessageId"] for item in sent}
            assert {message["Attributes"]["ApproximateReceiveCount"] for message in redelivered} == {"2"}
            print(label + ": both actual children joined; ten pending receipts unacknowledged and redelivered")
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
