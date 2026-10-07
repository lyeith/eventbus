"""Actual SDK origins and registered handlers for retained-owner acceptance.

Transport faults and gates are configured by the owning Go fixture. Handlers
use unmodified native boto3 clients against the trusted callback listener;
there is no alternate event, execution, retry or settlement implementation.
"""
import concurrent.futures
import fcntl
import json
import os
from pathlib import Path
import time
import urllib.request


PRIVATE_INPUT = "private input must not appear"


def client(service, endpoint, read_timeout=8):
    import boto3
    from botocore.config import Config
    return boto3.client(service, endpoint_url=endpoint, region_name="us-east-1",
                        aws_access_key_id="retained-fake-key", aws_secret_access_key="retained-fake-secret",
                        config=Config(retries={"max_attempts": 0}, connect_timeout=3, read_timeout=read_timeout))


def atomic_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value), encoding="utf-8")
    temporary.replace(path)


def row(stage, chain, attempt, context, **extra):
    value = {"stage": stage, "chain": chain, "attempt": attempt, "pid": os.getpid(),
             "owner": os.environ["RETAINED_OWNER"], "request_id": context.aws_request_id, **extra}
    filename = "row-" + stage + "-" + context.aws_request_id + "-" + str(attempt) + ".json"
    atomic_json(Path(os.environ["RETAINED_ROOT"]) / filename, value)
    return value


def attempt_number(chain):
    with (Path(os.environ["RETAINED_ROOT"]) / ("counter-" + chain)).open("a+", encoding="utf-8") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        stream.seek(0)
        attempt = int(stream.read() or "0") + 1
        stream.seek(0)
        stream.truncate()
        stream.write(str(attempt))
        stream.flush()
        fcntl.flock(stream, fcntl.LOCK_UN)
        return attempt


def root_handler(event, context):
    from botocore.exceptions import BotoCoreError, ClientError
    assert len(event["Records"]) == 1 and event["Records"][0]["EventSource"] == "aws:sns"
    input = json.loads(event["Records"][0]["Sns"]["Message"])
    chain = input["chain"]
    attempt = attempt_number(chain)
    row("root-started", chain, attempt, context)
    endpoint = os.environ["RETAINED_CALLBACK_ENDPOINT"]
    if chain == "shutdown":
        gate = Path(os.environ["RETAINED_ROOT"]) / "release-shutdown-root"
        while not gate.exists():
            time.sleep(0.01)
        result = client("lambda", endpoint).invoke(FunctionName="retained-nested:live", InvocationType="Event",
                        Payload=json.dumps({**input, "attempt": attempt}).encode())
        assert result["StatusCode"] == 202 and result["Payload"].read() == b"", result
        row("root-descendant-admitted", chain, attempt, context)
        row("root-completed", chain, attempt, context)
        return {"descendant_admitted": True}
    try:
        result = client("lambda", endpoint).invoke(FunctionName="retained-nested:live", InvocationType="RequestResponse",
                        Payload=json.dumps({**input, "attempt": attempt}).encode())
        assert result["StatusCode"] == 200 and "FunctionError" not in result, result
        assert json.load(result["Payload"])["persisted"] is True
    except (BotoCoreError, ClientError):
        row("root-failed", chain, attempt, context)
        raise
    # A real accepted callback chain consumes and acknowledges the native SNS
    # fanout, including the independently completed first nested attempt.
    sqs = client("sqs", endpoint)
    queue_url = sqs.get_queue_url(QueueName=input["queue_name"])["QueueUrl"]
    expected = 2 if chain == "lost" else 1
    received = []
    deadline = time.monotonic() + 5
    while len(received) < expected:
        assert time.monotonic() < deadline, "native downstream fanout did not arrive"
        for message in sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1).get("Messages", []):
            notification = json.loads(message["Body"])
            body = json.loads(notification["Message"])
            assert body["chain"] == chain and body["owner"] == os.environ["RETAINED_OWNER"]
            received.append(body["attempt"])
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"])
    assert sorted(received) == ([1, 2] if chain == "lost" else [1]), received
    row("root-completed", chain, attempt, context, downstream_attempts=received)
    return {"persisted": True, "downstream": len(received)}


def nested_handler(event, context):
    chain, attempt = event["chain"], event["attempt"]
    observed = row("nested-started", chain, attempt, context)
    request = urllib.request.Request(os.environ["RETAINED_CONTROL_URL"] + "/__fixture/nested-started",
                                     data=json.dumps(observed).encode(), headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(request, timeout=3) as response:
        assert response.status == 204
    if chain in ("lost", "shutdown"):
        gate = Path(os.environ["RETAINED_ROOT"]) / ("release-" + chain + "-" + str(attempt))
        while not gate.exists():
            time.sleep(0.01)
    client("sns", os.environ["RETAINED_CALLBACK_ENDPOINT"]).publish(TopicArn=event["downstream_topic"],
                Message=json.dumps({"chain": chain, "attempt": attempt, "owner": os.environ["RETAINED_OWNER"]}))
    row("nested-completed", chain, attempt, context)
    return {"persisted": True}


def run():
    from botocore.exceptions import BotoCoreError, ClientError
    phase, chain = os.environ["RETAINED_PHASE"], os.environ["RETAINED_CHAIN"]
    root = Path(os.environ["RETAINED_ROOT"])
    source, callback = os.environ["RETAINED_SOURCE_ENDPOINT"], os.environ["RETAINED_CALLBACK_ENDPOINT"]
    sns, sqs = client("sns", source), client("sqs", source)
    sentinel_topic = "arn:aws:sns:us-east-1:000000000000:retained-sentinel"
    sentinel_queue = "retained-sentinel"

    def persisted_sentinels():
        assert sns.get_topic_attributes(TopicArn=sentinel_topic)["Attributes"]["TopicArn"] == sentinel_topic
        url = sqs.get_queue_url(QueueName=sentinel_queue)["QueueUrl"]
        assert sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"].endswith(":" + sentinel_queue)

    def expect_fenced(operation, **kwargs):
        try:
            operation(**kwargs)
        except ClientError as failure:
            assert failure.response["ResponseMetadata"]["HTTPStatusCode"] == 503, failure.response
        else:
            raise AssertionError("held source admitted new work")

    if phase == "blocked-publications":
        sns.create_topic(Name="retained-sentinel")
        sqs.create_queue(QueueName=sentinel_queue)
        topic = sns.create_topic(Name="retained-blocked")["TopicArn"]
        publisher = client("sns", source, read_timeout=0.3)
        def publish(index):
            try:
                publisher.publish(TopicArn=topic, Message="blocked-native-" + str(index))
            except BotoCoreError:
                return
            raise AssertionError("borrowed capture gate unexpectedly returned")
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(publish, (1, 2)))
        print("both actual SDK publication origins timed out/exited while source envelopes stayed live")
    elif phase == "cleanup-blocked":
        client("sns", callback).delete_topic(TopicArn="arn:aws:sns:us-east-1:000000000000:retained-blocked")
    elif phase == "origin":
        if chain in ("lost", "next", "shutdown"):
            persisted_sentinels()
        else:
            sns.create_topic(Name="retained-sentinel")
            sqs.create_queue(QueueName=sentinel_queue)
        root_topic = sns.create_topic(Name="retained-root-" + chain)["TopicArn"]
        downstream = sns.create_topic(Name="retained-downstream-" + chain)["TopicArn"]
        queue_name = "retained-downstream-" + chain
        queue_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
        queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        sns.subscribe(TopicArn=downstream, Protocol="sqs", Endpoint=queue_arn)
        sns.subscribe(TopicArn=root_topic, Protocol="lambda", Endpoint="arn:aws:lambda:us-east-1:000000000000:function:retained-root:live")
        atomic_json(root / ("resources-" + chain + ".json"), {"root_topic": root_topic, "downstream_topic": downstream,
                    "queue_url": queue_url, "queue_name": queue_name})
        payload = json.dumps({"chain": chain, "downstream_topic": downstream, "queue_name": queue_name,
                              "drop_source": chain == "lost", "private": PRIVATE_INPUT})
        if chain == "lost":
            try:
                sns.publish(TopicArn=root_topic, Message=payload)
            except BotoCoreError:
                pass
            else:
                raise AssertionError("origin did not lose its actual accepted Publish reply")
        else:
            assert sns.publish(TopicArn=root_topic, Message=payload)["MessageId"]
        print("actual SNS origin exited after " + chain + " admission; sentinel survived")
    elif phase == "held-cleanup":
        resources = json.loads((root / ("resources-" + chain + ".json")).read_text())
        expect_fenced(sns.publish, TopicArn=resources["root_topic"], Message="new work while held")
        expect_fenced(client("lambda", source).invoke, FunctionName="retained-root:live", InvocationType="Event", Payload=b"{}")
        expect_fenced(client("sns", callback).publish, TopicArn=resources["root_topic"], Message="new callback work while held")
        expect_fenced(client("lambda", callback).invoke, FunctionName="retained-root:live", InvocationType="Event", Payload=b"{}")
        cleanup_sns, cleanup_sqs = client("sns", callback), client("sqs", callback)
        cleanup_sns.delete_topic(TopicArn=resources["root_topic"])
        cleanup_sns.delete_topic(TopicArn=resources["downstream_topic"])
        cleanup_sqs.delete_queue(QueueUrl=resources["queue_url"])
        completed = [json.loads(path.read_text()) for path in root.glob("row-root-completed-*.json")
                     if json.loads(path.read_text())["chain"] == chain]
        assert len(completed) == 1 and completed[0]["downstream_attempts"] == ([1, 2] if chain == "lost" else [1])
        print("held SDK producers refused; exact native fixture cleanup succeeded; sentinel left intact")
    else:
        raise AssertionError("unknown owned fixture phase: " + phase)
    print("PASS")


if __name__ == "__main__":
    run()
