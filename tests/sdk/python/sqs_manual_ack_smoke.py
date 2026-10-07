"""Real boto3 handler-issued SQS deletion and joined mapping acknowledgment.

Business effects are committed to SQLite before optional native DeleteMessage.
Gates and observations only arrange acceptance; all leases, native responses,
mapping completion, retry and execution remain with the service owners.
"""
from contextlib import closing
import fcntl
import json
import os
from pathlib import Path
import sqlite3
import time
import urllib.request
import uuid


def sqs_client():
    import boto3
    from botocore.config import Config
    return boto3.client("sqs", endpoint_url=os.environ["SQS_MANUAL_ACK_ENDPOINT"],
                        region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test",
                        config=Config(retries={"max_attempts": 0}, connect_timeout=2, read_timeout=10))


def append_row(stage, event, context, deleted=None):
    row = {"stage": stage, "pid": os.getpid(), "request_id": context.aws_request_id,
           "function_arn": context.invoked_function_arn, "event": event, "deleted": deleted or []}
    with (Path(os.environ["SQS_MANUAL_ACK_ROOT"]) / "observations.jsonl").open("a", encoding="utf-8") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        stream.write(json.dumps(row) + "\n")
        stream.flush()
        fcntl.flock(stream, fcntl.LOCK_UN)


def wait_for_gate(context):
    gate = Path(os.environ["SQS_MANUAL_ACK_ROOT"]) / ("release-" + context.aws_request_id)
    while not gate.exists():
        time.sleep(0.01)


def handler(event, context):
    records = event["Records"]
    assert 1 <= len(records) <= 5
    assert context.invoked_function_arn.endswith(":" + os.environ["SQS_MANUAL_ACK_VARIANT"])
    assert all(record["eventSource"] == "aws:sqs" for record in records)
    bodies = [json.loads(record["body"]) for record in records]
    append_row("started", event, context)
    # A committed business table is separate from fixture observations. UNIQUE
    # message_id keeps retries idempotent without changing native delivery.
    with closing(sqlite3.connect(os.environ["SQS_MANUAL_ACK_DATABASE"], timeout=5)) as business:
        business.execute("PRAGMA synchronous=FULL")
        for record, body in zip(records, bodies):
            business.execute("INSERT INTO business_effects(message_id,scenario,group_id,position,body) VALUES(?,?,?,?,?) "
                             "ON CONFLICT(message_id) DO NOTHING",
                             (record["messageId"], body["scenario"], record["attributes"].get("MessageGroupId", ""),
                              body["index"], record["body"]))
        business.commit()
    first_attempt = all(int(record["attributes"]["ApproximateReceiveCount"]) == 1 for record in records)
    if any(body["mode"] == "expire" for body in bodies):
        append_row("awaiting-receipt", event, context)
        wait_for_gate(context)
    sqs, deleted = sqs_client(), []
    for record, body in zip(records, bodies):
        if body["manual"]:
            url = sqs.get_queue_url(QueueName=record["eventSourceARN"].split(":")[-1])["QueueUrl"]
            result = sqs.delete_message(QueueUrl=url, ReceiptHandle=record["receiptHandle"])
            assert result["ResponseMetadata"]["HTTPStatusCode"] == 200
            deleted.append(record["messageId"])
    append_row("prepared", event, context, deleted)
    if first_attempt and any(body["mode"] == "fail" for body in bodies):
        append_row("failed", event, context, deleted)
        raise RuntimeError("business succeeded for manually settled records; batch invocation failed")
    if first_attempt and any(body["mode"] == "timeout" for body in bodies):
        while True:
            time.sleep(0.01)
    if any(body["mode"] == "gate" for body in bodies):
        wait_for_gate(context)
    append_row("completed", event, context, deleted)
    return {"ok": True}


def run():
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError

    root = Path(os.environ["SQS_MANUAL_ACK_ROOT"])
    endpoint = os.environ["SQS_MANUAL_ACK_ENDPOINT"]
    config = Config(retries={"max_attempts": 0}, connect_timeout=2, read_timeout=10)
    functions = boto3.client("lambda", endpoint_url=endpoint, region_name="us-east-1",
                             aws_access_key_id="test", aws_secret_access_key="test", config=config)
    sqs = sqs_client()
    suffix = uuid.uuid4().hex[:12]
    resources = []
    with closing(sqlite3.connect(os.environ["SQS_MANUAL_ACK_DATABASE"])) as business:
        business.execute("PRAGMA journal_mode=WAL")
        business.execute("PRAGMA synchronous=FULL")
        business.execute("CREATE TABLE business_effects(sequence INTEGER PRIMARY KEY AUTOINCREMENT, "
                         "message_id TEXT UNIQUE NOT NULL,scenario TEXT NOT NULL,group_id TEXT NOT NULL, "
                         "position INTEGER NOT NULL,body TEXT NOT NULL)")
        business.commit()

    def rows(scenario, stage=None):
        path = root / "observations.jsonl"
        if not path.exists():
            return []
        with path.open(encoding="utf-8") as stream:
            fcntl.flock(stream, fcntl.LOCK_SH)
            values = [json.loads(line) for line in stream]
            fcntl.flock(stream, fcntl.LOCK_UN)
        return [row for row in values if json.loads(row["event"]["Records"][0]["body"])["scenario"] == scenario
                and (stage is None or row["stage"] == stage)]

    def wait(probe, label, timeout=12):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = probe()
            if value:
                return value
            time.sleep(0.01)
        raise AssertionError("did not observe " + label)

    def alive(pid):
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False

    def release(row):
        (root / ("release-" + row["request_id"])).write_text("released", encoding="utf-8")

    def effects(scenario):
        with closing(sqlite3.connect(os.environ["SQS_MANUAL_ACK_DATABASE"])) as business:
            return business.execute("SELECT message_id,group_id,position FROM business_effects "
                                    "WHERE scenario=? ORDER BY sequence", (scenario,)).fetchall()

    def error(code, operation, **kwargs):
        try:
            operation(**kwargs)
        except ClientError as failure:
            assert failure.response["Error"]["Code"] == code, failure.response
        else:
            raise AssertionError("expected " + code)

    def queue(label, fifo=False, visibility=4):
        name = "sdk-manual-" + label + "-" + suffix + (".fifo" if fifo else "")
        attributes = {"VisibilityTimeout": str(visibility)}
        if fifo:
            attributes.update(FifoQueue="true", ContentBasedDeduplication="true")
        url = sqs.create_queue(QueueName=name, Attributes=attributes)["QueueUrl"]
        arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        resources.append(("queue", url))
        return url, arn, name

    def send(url, scenario, count, manual=(), mode="gate", group=None, start=0):
        result = []
        for index in range(start, start + count):
            kwargs = {"QueueUrl": url, "MessageBody": json.dumps({"scenario": scenario, "index": index,
                                                                     "manual": index in manual, "mode": mode})}
            if group:
                kwargs.update(MessageGroupId=group, MessageDeduplicationId=scenario + "-" + group + "-" + str(index))
            result.append(sqs.send_message(**kwargs)["MessageId"])
        return result

    def mapping(arn, batch=5, function="manual-handler:live", concurrency=True):
        arguments = {"EventSourceArn": arn, "FunctionName": function, "BatchSize": batch,
                     "MaximumBatchingWindowInSeconds": 0, "Enabled": True}
        if concurrency:
            arguments["ScalingConfig"] = {"MaximumConcurrency": 2}
        result = functions.create_event_source_mapping(**arguments)
        assert result["EventSourceArn"] == arn and result["FunctionArn"].endswith(":" + function)
        resources.append(("mapping", result["UUID"]))
        return result["UUID"]

    def result(ident):
        return functions.get_event_source_mapping(UUID=ident)["LastProcessingResult"]

    def delete_mapping(ident):
        deleted = functions.delete_event_source_mapping(UUID=ident)
        assert deleted["ResponseMetadata"]["HTTPStatusCode"] == 202
        resources.remove(("mapping", ident))

    def counts(url):
        attributes = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=[
            "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"])["Attributes"]
        return int(attributes["ApproximateNumberOfMessages"]), int(attributes["ApproximateNumberOfMessagesNotVisible"])

    def joined(scenario):
        wait(lambda: all(not alive(row["pid"]) for row in rows(scenario, "started")), "actual child joined")

    def assert_business(scenario, ids):
        persisted = effects(scenario)
        assert len(persisted) == len(ids) and {item[0] for item in persisted} == set(ids), persisted

    try:
        for scenario, manual in (("all-manual", range(5)), ("mixed", (0, 2, 4))):
            url, arn, _ = queue(scenario)
            ids = send(url, scenario, 5, manual=manual)
            ident = mapping(arn)
            prepared = wait(lambda: rows(scenario, "prepared"), scenario + " real native batch5")[0]
            assert len(prepared["event"]["Records"]) == 5 and alive(prepared["pid"])
            assert all(record["eventSourceARN"] == arn for record in prepared["event"]["Records"])
            assert set(prepared["deleted"]) == {ids[index] for index in manual}
            assert counts(url) == (0, 5 - len(manual)), counts(url)
            assert result(ident) == "No records processed", "mapping must join before reporting completion"
            assert_business(scenario, ids)
            release(prepared)
            wait(lambda: result(ident) == "OK", "joined manual deletion is successful mapping ACK")
            joined(scenario)
            assert counts(url) == (0, 0) and len(rows(scenario, "completed")) == 1
            assert_business(scenario, ids)
            delete_mapping(ident)
        print("all-manual and mixed native batch5 settled successfully after actual child join; durable effects preserved")

        url, arn, _ = queue("fifo", fifo=True)
        fifo_ids = send(url, "fifo", 6, manual=(0, 2, 4), group="first")
        other_ids = send(url, "fifo-other", 1, group="other")
        ident = mapping(arn)
        first = wait(lambda: rows("fifo", "prepared"), "first FIFO five")[0]
        other = wait(lambda: rows("fifo-other", "prepared"), "independent FIFO group")[0]
        assert len(first["event"]["Records"]) == 5 and alive(first["pid"]) and alive(other["pid"])
        release(other)
        wait(lambda: rows("fifo-other", "completed") and not alive(other["pid"]) and result(ident) == "OK", "other group progress")
        assert counts(url) == (1, 2), counts(url)
        assert not sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=1, WaitTimeSeconds=0).get("Messages"), "FIFO next record remains blocked by original mixed lease"
        release(first)
        next_row = wait(lambda: rows("fifo", "prepared") if len(rows("fifo", "prepared")) == 2 else None, "FIFO next after mixed settlement")[1]
        assert len(next_row["event"]["Records"]) == 1 and json.loads(next_row["event"]["Records"][0]["body"])["index"] == 5
        wait(lambda: not alive(first["pid"]) and result(ident) == "OK", "FIFO mixed successful acknowledgement")
        release(next_row)
        wait(lambda: counts(url) == (0, 0) and result(ident) == "OK", "FIFO complete")
        joined("fifo")
        assert_business("fifo", fifo_ids)
        assert_business("fifo-other", other_ids)
        assert [row[2] for row in effects("fifo")] == list(range(6)), effects("fifo")
        delete_mapping(ident)
        print("FIFO mixed settlement kept group order and allowed independent group completion")

        url, arn, _ = queue("unknown")
        ids = send(url, "unknown", 1)
        ident = mapping(arn, batch=1, concurrency=False)
        prepared = wait(lambda: rows("unknown", "prepared"), "current receipt held")[0]
        error("ReceiptHandleIsInvalid", sqs.delete_message, QueueUrl=url, ReceiptHandle="not-issued")
        assert counts(url) == (0, 1)
        release(prepared)
        wait(lambda: result(ident) == "OK" and counts(url) == (0, 0), "unknown handle preserved current work")
        joined("unknown")
        assert_business("unknown", ids)
        delete_mapping(ident)

        url, arn, _ = queue("expired")
        ids = send(url, "expired", 1, manual=(0,), mode="expire")
        ident = mapping(arn, batch=1, concurrency=False)
        held = wait(lambda: rows("expired", "awaiting-receipt"), "held original receipt")[0]
        original = held["event"]["Records"][0]
        sqs.change_message_visibility(QueueUrl=url, ReceiptHandle=original["receiptHandle"], VisibilityTimeout=1)
        second = wait(lambda: sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=1, WaitTimeSeconds=2,
                                                  MessageSystemAttributeNames=["All"]).get("Messages"), "native visibility redelivery")[0]
        assert second["MessageId"] == original["messageId"] and second["ReceiptHandle"] != original["receiptHandle"]
        assert second["Attributes"]["ApproximateReceiveCount"] == "2"
        release(held)
        wait(lambda: result(ident) == "Source acknowledgment failed", "expired original lease must not attest a later owner's work")
        joined("expired")
        assert rows("expired", "prepared")[0]["deleted"] == ids, "native old-handle success is a no-op, not ownership proof"
        assert counts(url) == (0, 1)
        delete_mapping(ident)
        assert sqs.delete_message(QueueUrl=url, ReceiptHandle=original["receiptHandle"])["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert counts(url) == (0, 1)
        sqs.delete_message(QueueUrl=url, ReceiptHandle=second["ReceiptHandle"])
        assert counts(url) == (0, 0)
        assert_business("expired", ids)
        print("native unknown and expired receipt results cannot settle the current redelivered lease")

        url, arn, name = queue("replacement", visibility=75)
        ids = send(url, "replacement-old", 1)
        ident = mapping(arn, batch=1, function="manual-handler:long", concurrency=False)
        held = wait(lambda: rows("replacement-old", "prepared"), "original queue invocation")[0]
        original = held["event"]["Records"][0]
        sqs.delete_queue(QueueUrl=url)
        resources.remove(("queue", url))
        error("QueueDeletedRecently", sqs.create_queue, QueueName=name, Attributes={"VisibilityTimeout": "75"})

        def recreate():
            try:
                return sqs.create_queue(QueueName=name, Attributes={"VisibilityTimeout": "75"})["QueueUrl"]
            except ClientError as failure:
                assert failure.response["Error"]["Code"] == "QueueDeletedRecently", failure.response
                time.sleep(1)
                return None

        replacement = wait(recreate, "native same-name recreation after 60-second cooldown", timeout=70)
        assert alive(held["pid"]), "original invocation must remain live across native recreation"
        resources.append(("queue", replacement))
        new_ids = send(replacement, "replacement-new", 1, mode="ok")
        current = sqs.receive_message(QueueUrl=replacement, MaxNumberOfMessages=1, MessageSystemAttributeNames=["All"])["Messages"][0]
        release(held)
        wait(lambda: functions.get_event_source_mapping(UUID=ident)["State"] == "Disabled", "deleted original queue disables bound mapping")
        joined("replacement-old")
        assert result(ident) != "OK" and counts(replacement) == (0, 1)
        error("ReceiptHandleIsInvalid", sqs.delete_message, QueueUrl=replacement, ReceiptHandle=original["receiptHandle"])
        assert counts(replacement) == (0, 1) and not rows("replacement-new")
        assert_business("replacement-old", ids)
        delete_mapping(ident)
        sqs.delete_message(QueueUrl=replacement, ReceiptHandle=current["ReceiptHandle"])
        fresh_ids = send(replacement, "replacement-fresh", 1, mode="ok")
        fresh = mapping(arn, batch=1, concurrency=False)
        wait(lambda: result(fresh) == "OK" and counts(replacement) == (0, 0), "explicit replacement mapping")
        joined("replacement-fresh")
        assert_business("replacement-fresh", fresh_ids)
        assert current["MessageId"] == new_ids[0]
        delete_mapping(fresh)
        print("deleted/recreated queue never rebinds an original mapping or acknowledges the replacement lease")

        for scenario, function, visibility in (("failed", "manual-handler:live", 4), ("timed-out", "manual-handler:short", 3)):
            url, arn, _ = queue(scenario, visibility=visibility)
            ids = send(url, scenario, 5, manual=(0, 1, 2), mode="fail" if scenario == "failed" else "timeout")
            ident = mapping(arn, function=function, concurrency=False)
            first = wait(lambda: rows(scenario, "prepared"), "partial manual settlement before " + scenario)[0]
            wait(lambda: result(ident) == "Function invocation failed" and not alive(first["pid"]), "failed handler joined before retry")
            assert len(first["deleted"]) == 3 and counts(url) == (0, 2), counts(url)
            assert_business(scenario, ids)
            retried = wait(lambda: rows(scenario, "started") if len(rows(scenario, "started")) == 2 else None, "only unmanual messages redeliver")[1]
            records = retried["event"]["Records"]
            assert {record["messageId"] for record in records} == set(ids) - set(first["deleted"])
            assert all(record["attributes"]["ApproximateReceiveCount"] == "2" for record in records)
            wait(lambda: result(ident) == "OK" and counts(url) == (0, 0), "redelivered current lease completes")
            joined(scenario)
            assert_business(scenario, ids)
            delete_mapping(ident)
        print("failed/timed-out partial manual batches preserve unsettled records for native redelivery and idempotent durable work")

        for scenario in ("delete-joined", "close-joined"):
            url, arn, _ = queue(scenario)
            ids = send(url, scenario, 5, manual=(0, 2, 4))
            ident = mapping(arn, concurrency=False)
            held = wait(lambda: rows(scenario, "prepared"), "pending teardown child")[0]
            assert alive(held["pid"]) and counts(url) == (0, 2)
            if scenario == "delete-joined":
                delete_mapping(ident)
            else:
                request = urllib.request.Request(endpoint + "/__sdk/close-mappings", data=b"", method="POST")
                with urllib.request.urlopen(request, timeout=6) as response:
                    assert json.load(response) == {"closed": True}
                resources.remove(("mapping", ident))
            assert not alive(held["pid"]), "native owner must join the admitted actual child"
            assert not rows(scenario, "completed") and counts(url) == (0, 2)
            assert_business(scenario, ids)
            release(held)  # A joined process cannot resume or settle after teardown.
            remaining = wait(lambda: sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=5, WaitTimeSeconds=2,
                                                         MessageSystemAttributeNames=["All"]).get("Messages"), "teardown retained receipts redeliver")
            assert {message["MessageId"] for message in remaining} == set(ids) - set(held["deleted"])
            assert all(message["Attributes"]["ApproximateReceiveCount"] == "2" for message in remaining)
            for message in remaining:
                sqs.delete_message(QueueUrl=url, ReceiptHandle=message["ReceiptHandle"])
            assert counts(url) == (0, 0) and not rows(scenario, "completed")
        print("native mapping delete and terminal close joined actual children and left unmanual receipts unacknowledged")
        print("PASS")
    finally:
        for kind, ident in reversed(resources):
            if kind == "mapping":
                functions.delete_event_source_mapping(UUID=ident)
            else:
                sqs.delete_queue(QueueUrl=ident)


if __name__ == "__main__":
    run()
