"""Application producers and consumers using unchanged native boto3 contracts.

HTTP invokes producer(), which sends/publishes actual messages. Consumers own
SQLite business effects and ordinary logging. Fixture gates only arrange live
work; receipt policy, execution, retry, capture and joins stay in EventBus.
"""
from contextlib import closing
import fcntl
import json
import logging
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
import urllib.request


def root():
    return Path(os.environ["NATIVE_EVIDENCE_ROOT"])


def database():
    connection = sqlite3.connect(os.environ["NATIVE_EVIDENCE_DATABASE"], timeout=5)
    connection.execute("PRAGMA synchronous=FULL")
    return connection


def initialize_business():
    with closing(database()) as connection:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("CREATE TABLE IF NOT EXISTS produced(message_id TEXT PRIMARY KEY,lane TEXT,suite TEXT,producer_request_id TEXT,position INTEGER)")
        connection.execute("CREATE TABLE IF NOT EXISTS business(message_id TEXT PRIMARY KEY,lane TEXT,suite TEXT,status TEXT,request_id TEXT,receive_count INTEGER,child_done INTEGER DEFAULT 0)")
        connection.execute("CREATE TABLE IF NOT EXISTS attempts(message_id TEXT PRIMARY KEY,count INTEGER)")
        connection.commit()


def client(service):
    import boto3
    from botocore.config import Config
    return boto3.client(service, endpoint_url=os.environ["NATIVE_EVIDENCE_CALLBACK"],
                        region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test",
                        config=Config(retries={"max_attempts": 0}, connect_timeout=2, read_timeout=8))


def record(stage, context, *, lane, suite, ids, count=1, child_pid=0):
    value = {"stage": stage, "lane": lane, "suite": suite, "message_ids": ids,
             "request_id": context.aws_request_id, "function_arn": context.invoked_function_arn,
             "pid": os.getpid(), "child_pid": child_pid, "count": count}
    with (root() / "observations.jsonl").open("a", encoding="utf-8") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        stream.write(json.dumps(value) + "\n")
        stream.flush()
        fcntl.flock(stream, fcntl.LOCK_UN)


def producer(event, context):
    initialize_business()
    lane, suite = event["lane"], event["suite"]
    ids = []
    for position in range(event.get("count", 1)):
        payload = {"lane": lane, "suite": suite, "mode": event.get("mode", "normal"),
                   "position": position, "manual": event.get("manual", True), "payload_marker": "native-input-must-not-enter-delivery-evidence"}
        if lane == "sqs":
            result = client("sqs").send_message(QueueUrl=event["queue_url"], MessageBody=json.dumps(payload))
        else:
            assert lane == "sns"
            result = client("sns").publish(TopicArn=event["topic_arn"], Message=json.dumps(payload))
        message_id = result["MessageId"]
        ids.append(message_id)
        with closing(database()) as connection:
            connection.execute("INSERT INTO produced VALUES(?,?,?,?,?)", (message_id, lane, suite, context.aws_request_id, position))
            connection.commit()
    return {"message_ids": ids, "producer_request_id": context.aws_request_id}


def business(message_ids, payload, context, status, receive_count):
    with closing(database()) as connection:
        for message_id in message_ids:
            connection.execute("INSERT INTO business(message_id,lane,suite,status,request_id,receive_count) VALUES(?,?,?,?,?,?) "
                               "ON CONFLICT(message_id) DO UPDATE SET status=excluded.status,request_id=excluded.request_id,receive_count=excluded.receive_count",
                               (message_id, payload["lane"], payload["suite"], status, context.aws_request_id, receive_count))
        connection.commit()


def child_work(message_ids, gate):
    if gate:
        while not Path(gate).exists():
            time.sleep(0.01)
    with closing(database()) as connection:
        for message_id in message_ids:
            connection.execute("UPDATE business SET child_done=1 WHERE message_id=?", (message_id,))
        connection.commit()
    print("native-child-work-completed", flush=True)


def start_child(message_ids, gate):
    return subprocess.Popen([sys.executable, "-E", "-s", __file__, "--child-work", json.dumps(message_ids), gate],
                            stdout=sys.stdout, stderr=sys.stderr)


def release_path(request_id):
    return str(root() / ("release-" + request_id))


def queue_handler(event, context):
    records = event["Records"]
    assert 1 <= len(records) <= 5 and all(item["eventSource"] == "aws:sqs" for item in records)
    payloads = [json.loads(item["body"]) for item in records]
    payload = payloads[0]
    assert all(item["suite"] == payload["suite"] for item in payloads)
    ids = [item["messageId"] for item in records]
    count = max(int(item["attributes"]["ApproximateReceiveCount"]) for item in records)
    record("started", context, lane="sqs", suite=payload["suite"], ids=ids, count=count)
    print("native-queue-stdout:" + context.aws_request_id, flush=True)
    print("native-queue-stderr:" + context.aws_request_id, file=sys.stderr, flush=True)
    mode = payload["mode"]
    business(ids, payload, context, "SUCCESS", count)
    sqs = client("sqs")
    if mode == "stale":
        assert len(records) == 1
        if count == 1:
            queue_url = sqs.get_queue_url(QueueName=records[0]["eventSourceARN"].split(":")[-1])["QueueUrl"]
            sqs.change_message_visibility(QueueUrl=queue_url, ReceiptHandle=records[0]["receiptHandle"], VisibilityTimeout=0)
        child = start_child(ids, release_path(context.aws_request_id))
        record("stale-held", context, lane="sqs", suite=payload["suite"], ids=ids, count=count, child_pid=child.pid)
        assert child.wait() == 0
    elif count == 1 and mode == "failure":
        # Failed trusted callbacks still finish their own application sidework.
        # This ordinary unhandled failure does not orphan a logging-pipe owner.
        business(ids, payload, context, "FAILURE", count)
        try:
            raise RuntimeError(os.environ["NATIVE_EVIDENCE_PRIVATE_ERROR"] + ":sqs-unhandled")
        except RuntimeError:
            logging.exception("native queue business exception")
            raise
    elif count == 1 and mode == "timeout":
        child = start_child(ids, release_path(context.aws_request_id))
        record("failure-held", context, lane="sqs", suite=payload["suite"], ids=ids, count=count, child_pid=child.pid)
        assert child.wait() == 0  # Actual deadline kills and joins both processes.
    for item, body in zip(records, payloads):
        if body["manual"]:
            queue_url = sqs.get_queue_url(QueueName=item["eventSourceARN"].split(":")[-1])["QueueUrl"]
            deleted = sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=item["receiptHandle"])
            assert deleted["ResponseMetadata"]["HTTPStatusCode"] == 200
    if mode == "blocked":
        child = start_child(ids, release_path(context.aws_request_id))
        record("manual-held", context, lane="sqs", suite=payload["suite"], ids=ids, count=count, child_pid=child.pid)
        assert child.wait() == 0
    elif mode not in ("stale", "failure", "timeout") or count > 1:
        child = start_child(ids, "")
        assert child.wait() == 0
    record("completed", context, lane="sqs", suite=payload["suite"], ids=ids, count=count)
    return {"ok": True}


def sns_attempt(message_id):
    with closing(database()) as connection:
        connection.execute("INSERT INTO attempts VALUES(?,1) ON CONFLICT(message_id) DO UPDATE SET count=count+1", (message_id,))
        number = connection.execute("SELECT count FROM attempts WHERE message_id=?", (message_id,)).fetchone()[0]
        connection.commit()
        return number


def sns_handler(event, context):
    assert len(event["Records"]) == 1 and event["Records"][0]["EventSource"] == "aws:sns"
    native = event["Records"][0]["Sns"]
    payload, ids = json.loads(native["Message"]), [native["MessageId"]]
    number = sns_attempt(ids[0])
    mode = payload["mode"]
    record("started", context, lane="sns", suite=payload["suite"], ids=ids, count=number)
    print("native-sns-stdout:" + context.aws_request_id, flush=True)
    print("native-sns-stderr:" + context.aws_request_id, file=sys.stderr, flush=True)
    if mode in ("caught", "unhandled") or mode == "retry" and number == 1:
        business(ids, payload, context, "FAILURE", number)
        try:
            raise RuntimeError(os.environ["NATIVE_EVIDENCE_PRIVATE_ERROR"] + ":" + mode)
        except RuntimeError:
            logging.exception("native callback business exception")
            if mode != "caught":
                raise
        record("completed", context, lane="sns", suite=payload["suite"], ids=ids, count=number)
        return {"accepted": True}
    business(ids, payload, context, "WORKING" if mode == "timeout" else "SUCCESS", number)
    if mode == "timeout":
        child = start_child(ids, release_path(context.aws_request_id))
        record("failure-held", context, lane="sns", suite=payload["suite"], ids=ids, count=number, child_pid=child.pid)
        assert child.wait() == 0
    if mode == "truncation":
        print("native-log-prefix:" + "x" * (256 * 1024) + "native-log-tail:" + context.aws_request_id, flush=True)
    record("completed", context, lane="sns", suite=payload["suite"], ids=ids, count=number)
    return {"ok": True}


def run():
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError

    initialize_business()
    source = os.environ["NATIVE_EVIDENCE_SOURCE"]
    config = Config(retries={"max_attempts": 0}, connect_timeout=2, read_timeout=10)
    common = dict(endpoint_url=source, region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test", config=config)
    sqs, sns, functions = (boto3.client(service, **common) for service in ("sqs", "sns", "lambda"))
    paths = {key: Path(os.environ["NATIVE_EVIDENCE_" + key]) for key in ("DELIVERY", "DIAGNOSTICS", "ASYNC", "SNS")}
    secret = os.environ["NATIVE_EVIDENCE_PRIVATE_ERROR"]

    def capture(name):
        try:
            data = paths[name].read_bytes()
        except FileNotFoundError:
            return []
        # A concurrently appended last fragment is not a complete record yet.
        return [json.loads(line) for line in data.split(b"\n")[:-1] if line]

    def observations(suite, stage=None):
        path = root() / "observations.jsonl"
        if not path.exists():
            return []
        with path.open(encoding="utf-8") as stream:
            fcntl.flock(stream, fcntl.LOCK_SH)
            values = [json.loads(line) for line in stream]
            fcntl.flock(stream, fcntl.LOCK_UN)
        return [row for row in values if row["suite"] == suite and (stage is None or row["stage"] == stage)]

    def wait(probe, label, timeout=15):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = probe()
            if value:
                return value
            time.sleep(0.01)
        raise AssertionError("did not observe " + label)

    def alive(pid):
        if not pid:
            return False
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False

    def release(row):
        Path(release_path(row["request_id"])).write_text("released", encoding="utf-8")

    def business_rows(ids):
        with closing(database()) as connection:
            rows = connection.execute("SELECT message_id,status,request_id,receive_count,child_done FROM business").fetchall()
            return {row[0]: row[1:] for row in rows if row[0] in ids}

    def http_json(url, payload, want=200):
        request = urllib.request.Request(url, data=json.dumps(payload).encode(), method="POST", headers={"Content-Type": "application/json"})
        try:
            response = urllib.request.urlopen(request, timeout=12)
        except urllib.error.HTTPError as failure:
            response = failure
        with response:
            assert response.status == want, "native/control HTTP status"
            return json.load(response)

    def produce(lane, suite, target, count=1, mode="normal", manual=True):
        payload = {"lane": lane, "suite": suite, "count": count, "mode": mode, "manual": manual}
        payload["queue_url" if lane == "sqs" else "topic_arn"] = target
        result = http_json(source + "/2015-03-31/functions/native-producer:live/invocations", payload)
        assert result["producer_request_id"] and len(result["message_ids"]) == count
        with closing(database()) as connection:
            rows = connection.execute("SELECT message_id,producer_request_id FROM produced WHERE suite=?", (suite,)).fetchall()
        assert {row[0] for row in rows} == set(result["message_ids"])
        assert all(row[1] == result["producer_request_id"] for row in rows)
        return result["message_ids"]

    def control(operation, payload, want=200):
        result = http_json(os.environ["NATIVE_EVIDENCE_CONTROL"] + "/__eventbus/dev/retained-owner" + operation, payload, want)
        assert result["schema_version"] == "eventbus.retained-owner.v1"
        return result

    def queue(name):
        url = sqs.create_queue(QueueName="native-evidence-" + name, Attributes={"VisibilityTimeout": "6"})["QueueUrl"]
        arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        return url, arn

    def counts(url):
        result = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"])["Attributes"]
        return int(result["ApproximateNumberOfMessages"]), int(result["ApproximateNumberOfMessagesNotVisible"])

    def mapping(arn, target="native-queue:live", batch=5):
        result = functions.create_event_source_mapping(EventSourceArn=arn, FunctionName=target, BatchSize=batch,
                   MaximumBatchingWindowInSeconds=0, ScalingConfig={"MaximumConcurrency": 2}, Enabled=True)
        return result["UUID"]

    def deliveries(ident, ids, state=None):
        return [row for row in capture("DELIVERY") if row["mapping_uuid"] == ident
                and any(item["message_id"] in ids for item in row["messages"])
                and (state is None or row["state"] == state)]

    def terminals(ident, ids):
        return [row for row in deliveries(ident, ids) if row["state"] != "admitted"]

    def assert_terminal(row, *, state, invocation, settlement):
        assert row["schema_version"] == "eventbus.sqs.delivery.v1"
        assert row["state"] == state and row["invocation_state"] == invocation and row["joined"] is True
        assert row["request_id"] and row["invoked_function_arn"] == row["function_arn"]
        assert all(item["settlement"] == settlement and not item.get("evidence_error", False) for item in row["messages"])
        admission = [item for item in capture("DELIVERY") if item["delivery_id"] == row["delivery_id"] and item["state"] == "admitted"]
        assert len(admission) == 1 and admission[0]["request_id"] == row["request_id"] and admission[0]["joined"] is False
        assert admission[0]["messages"] == [{"message_id": item["message_id"], "receive_count": item["receive_count"]} for item in row["messages"]]
        diagnostics = [item for item in capture("DIAGNOSTICS") if item["request_id"] == row["request_id"]]
        assert len(diagnostics) == 1 and diagnostics[0]["function_arn"] == row["invoked_function_arn"]
        assert diagnostics[0]["state"] == invocation and diagnostics[0]["ownership_confirmed"] is True

    def assert_joined(suite):
        wait(lambda: all(not alive(row["pid"]) and not alive(row["child_pid"]) for row in observations(suite)), "observed handler and descendant stopped")

    def successful_delivery(ident, ids):
        rows = [row for row in terminals(ident, ids) if row["state"] == "succeeded"]
        return rows if {item["message_id"] for row in rows for item in row["messages"]} == set(ids) else None

    main_url, main_arn = queue("main")
    foreign_url, foreign_arn = queue("foreign")
    first_ids = produce("sqs", "first", main_url, 5, "blocked")
    foreign_ids = produce("sqs", "foreign", foreign_url)
    ident = mapping(main_arn)
    held = wait(lambda: observations("first", "manual-held"), "real manual-deleted batch5 and blocked child")[0]
    assert len(held["message_ids"]) == 5 and set(held["message_ids"]) == set(first_ids)
    admission = wait(lambda: deliveries(ident, first_ids, "admitted"), "actual native admission identity")[0]
    assert admission["request_id"] == held["request_id"] and admission["event_source_arn"] == main_arn
    assert not terminals(ident, first_ids) and not [row for row in capture("DIAGNOSTICS") if row["request_id"] == held["request_id"]]
    assert alive(held["pid"]) and alive(held["child_pid"]) and counts(main_url) == (0, 0)
    assert set(business_rows(first_ids)) == set(first_ids) and all(row[0] == "SUCCESS" and row[3] == 0 for row in business_rows(first_ids).values())
    assert counts(foreign_url) == (1, 0) and not business_rows(foreign_ids) and not deliveries(ident, foreign_ids)
    dirty = control("/quiesce", {"timeout_ms": 100}, 408)
    assert dirty["work_count"] > 0 and not dirty["fixture_safe"]
    release(held)
    first_terminals = wait(lambda: successful_delivery(ident, first_ids), "joined first-suite evidence")
    for terminal in first_terminals:
        assert_terminal(terminal, state="succeeded", invocation="succeeded", settlement="native_settled")
        assert all(item["acknowledge_attempted"] for item in terminal["messages"])
    assert_joined("first")
    safe = control("/quiesce", {"timeout_ms": 5000})
    assert safe["fixture_safe"] and safe["work_count"] == 0
    control("/resume", {"generation": safe["generation"]})
    assert functions.get_event_source_mapping(UUID=ident)["EventSourceArn"] == main_arn
    second_ids = produce("sqs", "second", main_url, 5, manual=False)
    for terminal in wait(lambda: successful_delivery(ident, second_ids), "second HTTP suite on same mapping"):
        assert_terminal(terminal, state="succeeded", invocation="succeeded", settlement="mapping_settled")
    assert_joined("second")
    assert set(business_rows(second_ids)) == set(second_ids) and all(row[0] == "SUCCESS" and row[3] == 1 for row in business_rows(second_ids).values())
    assert counts(foreign_url) == (1, 0) and not business_rows(foreign_ids) and not deliveries(ident, foreign_ids)
    print("actual HTTP producer -> native batch5/manual delete -> blocked child -> joined lineage; same mapping reused for second suite")

    stale_url, stale_arn = queue("stale")
    stale_ids = produce("sqs", "stale", stale_url, mode="stale")
    stale_mapping = mapping(stale_arn, batch=1)
    live = wait(lambda: observations("stale", "stale-held") if len(observations("stale", "stale-held")) == 2 else None, "two actual native lease owners")
    original = next(row for row in live if row["count"] == 1)
    current = next(row for row in live if row["count"] == 2)
    assert original["request_id"] != current["request_id"] and alive(original["child_pid"]) and alive(current["child_pid"])
    release(original)
    old_terminal = wait(lambda: [row for row in terminals(stale_mapping, stale_ids) if row["request_id"] == original["request_id"]], "joined stale-lease evidence")[0]
    assert_terminal(old_terminal, state="ack_failed", invocation="succeeded", settlement="stale_or_expired")
    assert not [row for row in terminals(stale_mapping, stale_ids) if row["request_id"] == current["request_id"]]
    assert counts(stale_url) == (0, 1) and alive(current["child_pid"])
    release(current)
    latest = wait(lambda: [row for row in terminals(stale_mapping, stale_ids) if row["request_id"] == current["request_id"]], "joined current-lease evidence")[0]
    assert_terminal(latest, state="succeeded", invocation="succeeded", settlement="native_settled")
    assert latest["messages"][0]["receive_count"] == 2
    assert_joined("stale")
    functions.delete_event_source_mapping(UUID=stale_mapping)

    for suite, target, invocation in (("failure", "native-queue:live", "failed"), ("timeout", "native-queue:short", "timed_out")):
        url, arn = queue(suite)
        ids = produce("sqs", suite, url, mode=suite)
        item = mapping(arn, target, 1)
        failed = wait(lambda: [row for row in terminals(item, ids) if row["invocation_state"] == invocation], "joined " + suite + " evidence")[0]
        assert_terminal(failed, state=invocation, invocation=invocation, settlement="unacknowledged")
        assert all(not message.get("acknowledge_attempted", False) for message in failed["messages"])
        succeeded = wait(lambda: successful_delivery(item, ids), "native visibility retry succeeded")[0]
        assert_terminal(succeeded, state="succeeded", invocation="succeeded", settlement="native_settled")
        assert failed["request_id"] != succeeded["request_id"] and succeeded["messages"][0]["receive_count"] == 2
        assert_joined(suite)
        assert all(row[0] == "SUCCESS" for row in business_rows(ids).values())
        functions.delete_event_source_mapping(UUID=item)
    print("native stale/current invocation identities, failed/time-out joins and visibility retries retained exact disposition")

    live_topic = sns.create_topic(Name="native-evidence-live")["TopicArn"]
    short_topic = sns.create_topic(Name="native-evidence-short")["TopicArn"]
    sns.subscribe(TopicArn=live_topic, Protocol="lambda", Endpoint="arn:aws:lambda:us-east-1:000000000000:function:native-sns:live")
    sns.subscribe(TopicArn=short_topic, Protocol="lambda", Endpoint="arn:aws:lambda:us-east-1:000000000000:function:native-sns:short")
    for mode in ("caught", "unhandled", "retry", "normal", "truncation", "timeout"):
        suite = "sns-" + mode
        ids = produce("sns", suite, short_topic if mode == "timeout" else live_topic, mode=mode)
        publish = wait(lambda: [row for row in capture("SNS") if row.get("operation") == "Publish" and row.get("message_id") in ids], "one native SNS publication")
        assert len(publish) == 1 and len(publish[0]["deliveries"]) == 1
        admitted = wait(lambda: [row for row in capture("SNS") if row.get("operation") == "DeliveryAdmission" and row.get("message_id") in ids], "actual native SNS admission")
        assert len(admitted) == 1 and len(admitted[0]["deliveries"]) == 1
        delivery = admitted[0]["deliveries"][0]
        assert delivery["status"] == "admitted" and delivery["message_id"] == ids[0]
        request_id = delivery["invocation_request_id"]
        expected_state = "failed" if mode in ("unhandled", "timeout") else "succeeded"
        terminal = wait(lambda: [row for row in capture("ASYNC") if row["request_id"] == request_id and row["state"] == expected_state], "native SNS runtime terminal")[0]
        diagnostics = [row for row in capture("DIAGNOSTICS") if row["request_id"] == request_id]
        assert len(diagnostics) == terminal["attempts"] and [row["attempt"] for row in diagnostics] == list(range(1, terminal["attempts"] + 1))
        assert all(row["schema_version"] == "eventbus.lambda.invocation-diagnostic.v1" and row["ownership_confirmed"]
                   and row["invocation_type"] == "Event" and row["function_arn"].endswith(":native-sns:" + ("short" if mode == "timeout" else "live")) for row in diagnostics)
        assert all(row["started_at"] and row["completed_at"] for row in diagnostics)
        if mode in ("caught", "unhandled", "retry"):
            assert secret + ":" + mode in diagnostics[0]["stderr"]["data"], "exact private business exception retained"
        if mode == "caught":
            assert diagnostics[0]["state"] == "succeeded" and not diagnostics[0]["function_error"]
            assert business_rows(ids)[ids[0]][0] == "FAILURE"
            try:
                assert business_rows(ids)[ids[0]][0] == "SUCCESS"
            except AssertionError:
                pass
            else:
                raise AssertionError("successful runtime/logs cannot attest business success")
        elif mode == "retry":
            assert [row["state"] for row in diagnostics] == ["failed", "succeeded"]
            assert business_rows(ids)[ids[0]][0] == "SUCCESS"
        elif mode == "unhandled":
            assert len(diagnostics) == 3 and all(row["state"] == "failed" and row["function_error"] for row in diagnostics)
            assert business_rows(ids)[ids[0]][0] == "FAILURE"
        elif mode == "timeout":
            assert len(diagnostics) == 3 and all(row["state"] == "timed_out" for row in diagnostics)
            assert business_rows(ids)[ids[0]][0] == "WORKING"
        else:
            assert diagnostics[0]["state"] == "succeeded" and business_rows(ids)[ids[0]][0] == "SUCCESS"
            if mode == "normal":
                assert "native-sns-stdout:" + request_id in diagnostics[0]["stdout"]["data"]
                assert "native-sns-stderr:" + request_id in diagnostics[0]["stderr"]["data"]
            else:
                stdout = diagnostics[0]["stdout"]
                assert stdout["truncated"] and stdout["bytes"] >= 256 * 1024 and len(stdout["data"].encode()) <= 64 * 1024
                assert "native-log-tail:" + request_id in stdout["data"]
                assert diagnostics[0]["tail"]["truncated"]
        assert_joined(suite)
        assert len([row for row in capture("SNS") if row.get("operation") == "Publish" and row.get("message_id") in ids]) == 1
    print("original native SNS callbacks retained exact private errors; runtime success stayed distinct from business FAILURE; retry/timeout/truncation verified")

    # The sentinel remained outside both suites. Its exact original ID is now
    # observed through a separately declared real mapping/handler, never Receive.
    foreign_mapping = mapping(foreign_arn, batch=1)
    for terminal in wait(lambda: successful_delivery(foreign_mapping, foreign_ids), "exact original sentinel through actual native handler"):
        assert_terminal(terminal, state="succeeded", invocation="succeeded", settlement="native_settled")
    assert_joined("foreign")
    assert set(business_rows(foreign_ids)) == set(foreign_ids)
    functions.delete_event_source_mapping(UUID=foreign_mapping)

    teardown_ids = produce("sqs", "teardown", main_url, mode="blocked")
    pending = wait(lambda: observations("teardown", "manual-held"), "actual teardown child")[0]
    assert not terminals(ident, teardown_ids) and alive(pending["child_pid"])
    functions.delete_event_source_mapping(UUID=ident)
    canceled = wait(lambda: terminals(ident, teardown_ids), "joined native mapping teardown evidence")[0]
    assert_terminal(canceled, state="canceled", invocation="canceled", settlement="native_settled")
    assert not canceled["messages"][0].get("acknowledge_attempted", False)
    assert_joined("teardown")
    safe = control("/quiesce", {"timeout_ms": 5000})
    assert safe["fixture_safe"] and safe["work_count"] == 0
    for path in (paths["DIAGNOSTICS"], paths["DELIVERY"]):
        assert path.is_file() and path.stat().st_mode & 0o777 == 0o600, "configured evidence paths are private regular files"
    for name in ("DELIVERY", "ASYNC", "SNS"):
        assert secret not in paths[name].read_text(), "private handler diagnostics must not enter existing evidence"
    for name in ("DELIVERY", "ASYNC"):
        assert "native-input-must-not-enter-delivery-evidence" not in paths[name].read_text()
    print("foreign message identity preserved; native teardown supervisor records joined cancellation before safe retained barrier")
    print("PASS")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--child-work":
        child_work(json.loads(sys.argv[2]), sys.argv[3])
    else:
        run()
