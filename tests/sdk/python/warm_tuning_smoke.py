"""Pinned native boto3 resources and held calls survive public warm tuning."""
from __future__ import annotations

import concurrent.futures
import json
import os
from pathlib import Path
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


def append(row):
    with Path(os.environ["WARM_TUNING_OUTPUT"]).open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(row, separators=(",", ":")) + "\n")


def handler(event, context):
    if "Records" in event:
        event = json.loads(event["Records"][0]["Sns"]["Message"])
    identifier = event["id"]
    append({"phase": "start", "id": identifier, "pid": os.getpid(), "request_id": context.aws_request_id})
    directory = Path(os.environ["WARM_TUNING_GATES"])
    if event.get("block"):
        deadline = time.monotonic() + 20
        while not (directory / identifier).exists() and not (directory / "cleanup").exists():
            assert time.monotonic() < deadline, "owned gate deadline"
            time.sleep(0.01)
    append({"phase": "finish", "id": identifier, "pid": os.getpid(), "request_id": context.aws_request_id})
    return {"id": identifier, "pid": os.getpid(), "request_id": context.aws_request_id}


def main():
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError

    endpoint = os.environ["WARM_TUNING_ENDPOINT"]
    output = Path(os.environ["WARM_TUNING_OUTPUT"])
    directory = Path(os.environ["WARM_TUNING_GATES"])
    assert urllib.parse.urlparse(endpoint).hostname in ("127.0.0.1", "localhost", "::1")
    configuration = Config(connect_timeout=2, read_timeout=25, retries={"total_max_attempts": 1})
    credentials = dict(endpoint_url=endpoint, region_name="us-east-1", aws_access_key_id="sdk-test",
                       aws_secret_access_key="sdk-secret", config=configuration)
    sns, sqs, functions = (boto3.client(service, **credentials) for service in ("sns", "sqs", "lambda"))
    arn = "arn:aws:lambda:us-east-1:000000000000:function:warm-tuning:live"
    suffix = uuid.uuid4().hex[:12]
    topic = queue = None
    subscriptions = []
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=3)

    def rows():
        if not output.exists():
            return []
        return [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines(keepends=True) if line.endswith("\n")]

    def phase(identifier, name):
        return [row for row in rows() if row["id"] == identifier and row["phase"] == name]

    def wait(predicate, label):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            result = predicate()
            if result:
                return result
            time.sleep(0.02)
        raise AssertionError((label, rows()))

    def tune(value, status=200):
        request = urllib.request.Request(endpoint + "/__eventbus/dev/lambda/warm", data=json.dumps(value).encode(),
                                         headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                assert response.status == status
                result = json.load(response)
        except urllib.error.HTTPError as error:
            assert error.code == status, error.read().decode()
            result = json.load(error)
        else:
            assert status == 200
        if status == 200:
            assert result == value, result
        return result

    def invoke(identifier, block=False, kind="RequestResponse"):
        result = functions.invoke(FunctionName=arn, InvocationType=kind,
                                  Payload=json.dumps({"id": identifier, "block": block}).encode())
        assert result["StatusCode"] == (202 if kind == "Event" else 200), result
        assert "FunctionError" not in result, result
        payload = result["Payload"].read()
        if kind == "Event":
            assert payload == b""
            return result["ResponseMetadata"]["RequestId"]
        payload = json.loads(payload)
        assert payload["id"] == identifier, payload
        return payload

    def throttled(identifier):
        try:
            invoke(identifier)
        except ClientError as error:
            assert error.response["Error"]["Code"] == "TooManyRequestsException", error.response
            assert error.response["ResponseMetadata"]["HTTPStatusCode"] == 429
        else:
            raise AssertionError("new synchronous call bypassed lower active limit")
        assert not phase(identifier, "start")

    def assert_resources():
        assert sns.get_topic_attributes(TopicArn=topic)["Attributes"]["TopicArn"] == topic
        actual = sns.list_subscriptions_by_topic(TopicArn=topic)["Subscriptions"]
        assert {item["SubscriptionArn"] for item in actual} == set(subscriptions), actual
        assert sqs.get_queue_url(QueueName="warm-tuning-" + suffix)["QueueUrl"] == queue
        assert sqs.get_queue_attributes(QueueUrl=queue, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"] == queue_arn

    try:
        topic = sns.create_topic(Name="warm-tuning-" + suffix)["TopicArn"]
        queue = sqs.create_queue(QueueName="warm-tuning-" + suffix)["QueueUrl"]
        queue_arn = sqs.get_queue_attributes(QueueUrl=queue, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        subscriptions.append(sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=queue_arn,
                                           Attributes={"RawMessageDelivery": "true"})["SubscriptionArn"])
        subscriptions.append(sns.subscribe(TopicArn=topic, Protocol="lambda", Endpoint=arn)["SubscriptionArn"])
        direct_before = sqs.send_message(QueueUrl=queue, MessageBody="before-tuning")["MessageId"]
        first = executor.submit(invoke, "active-a", True)
        wait(lambda: phase("active-a", "start"), "first active request")
        queued_request = invoke("queued-event", True, "Event")
        time.sleep(0.15)
        assert not phase("queued-event", "start"), "queued event bypassed initial active limit"
        tune({"max_workers": 2, "max_concurrency": 2})
        wait(lambda: phase("queued-event", "start"), "raising admits existing queued Event")
        tune({"max_workers": 3, "max_concurrency": 3})
        third = executor.submit(invoke, "active-b", True)
        wait(lambda: phase("active-b", "start"), "raising admits another native call")
        active = [phase(identifier, "start")[0] for identifier in ("active-a", "queued-event", "active-b")]
        assert len({row["pid"] for row in active}) == 3, active
        assert len({row["request_id"] for row in active}) == 3, active
        assert queued_request and phase("queued-event", "start")[0]["request_id"]
        tune({"max_workers": 1, "max_concurrency": 1})
        assert_resources()
        for value in ({"max_workers": 0, "max_concurrency": 1}, {"max_workers": 33, "max_concurrency": 1},
                      {"max_workers": 1, "max_concurrency": 0}, {"max_workers": 1, "max_concurrency": 1025},
                      {"max_workers": 1}, {"max_workers": 1, "max_concurrency": 1, "unexpected": True}):
            tune(value, 400)
        assert not any(phase(identifier, "finish") for identifier in ("active-a", "queued-event", "active-b"))
        throttled("above-three")
        message = json.dumps({"id": "queued-sns"})
        published = sns.publish(TopicArn=topic, Message=message)["MessageId"]
        assert published
        direct_after = sqs.send_message(QueueUrl=queue, MessageBody="after-tuning")["MessageId"]
        assert_resources()
        assert not phase("queued-sns", "start")
        (directory / "active-a").write_text("finish")
        assert first.result(timeout=5)["pid"] == active[0]["pid"]
        throttled("above-two")
        (directory / "queued-event").write_text("finish")
        wait(lambda: phase("queued-event", "finish"), "accepted Event finishes after lowering")
        throttled("at-one")
        assert not phase("queued-sns", "start")
        (directory / "active-b").write_text("finish")
        assert third.result(timeout=5)["pid"] == active[2]["pid"]
        wait(lambda: phase("queued-sns", "finish"), "existing SNS queue drains under lower limit")
        returned = invoke("after-tuning")
        wait(lambda: len([row for row in rows() if row["phase"] == "finish"]) == 5, "all admitted work completed")
        starts = [row for row in rows() if row["phase"] == "start"]
        assert len(starts) == 5 and len({row["request_id"] for row in starts}) == 5, rows()
        assert returned["pid"] in {row["pid"] for row in active}, "retained worker reused without restart"
        received = []
        for _ in range(3):
            for item in sqs.receive_message(QueueUrl=queue, MaxNumberOfMessages=10).get("Messages", []):
                received.append(item)
                sqs.delete_message(QueueUrl=queue, ReceiptHandle=item["ReceiptHandle"])
            if len(received) == 3:
                break
        assert {item["MessageId"] for item in received if item["Body"] != message} == {direct_before, direct_after}, received
        assert [item["Body"] for item in received].count(message) == 1, received
        assert_resources()
        assert not sqs.receive_message(QueueUrl=queue).get("Messages")
        live = []
        for pid in {row["pid"] for row in starts}:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                continue
            live.append(pid)
        assert live == [returned["pid"]], (live, rows())
        print(json.dumps({"status": "passed", "preserved_active_calls": 3, "completed_calls": len(starts),
                          "preserved_sdk_subscriptions": len(subscriptions), "retained_pid": returned["pid"]}))
    finally:
        (directory / "cleanup").write_text("finish")
        executor.shutdown(wait=True, cancel_futures=True)
        if topic is not None:
            for subscription in subscriptions:
                sns.unsubscribe(SubscriptionArn=subscription)
            sns.delete_topic(TopicArn=topic)
        if queue is not None:
            sqs.delete_queue(QueueUrl=queue)
        for client in (sns, sqs, functions):
            client.close()


if __name__ == "__main__":
    main()
    print("PASS")
