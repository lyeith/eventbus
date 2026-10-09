"""Unchanged boto3 SNS/SQS producer APIs reuse one opt-in managed Python worker."""
from __future__ import annotations

import json
import os
from pathlib import Path
import time
import uuid

INITIALIZATION = uuid.uuid4().hex
INVOCATIONS = 0


def append(row):
    with Path(os.environ["WARM_MESSAGING_OUTPUT"]).open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(row, separators=(",", ":")) + "\n")


# The SDK driver is a different interpreter and is not a managed handler import.
if __name__ != "__main__":
    append({"phase": "init", "pid": os.getpid(), "initialization": INITIALIZATION})


def handler(event, context):
    global INVOCATIONS
    INVOCATIONS += 1
    records = event["Records"]
    assert len(records) == 1, records
    record = records[0]
    if "Sns" in record:
        assert record["EventSource"] == "aws:sns", record
        source, native_id, body = "sns", record["Sns"]["MessageId"], record["Sns"]["Message"]
        assert record["Sns"]["Type"] == "Notification", record
    else:
        assert record["eventSource"] == "aws:sqs", record
        source, native_id, body = "sqs", record["messageId"], record["body"]
        assert record["receiptHandle"], record
    print("warm-native-" + source + "-" + context.aws_request_id, flush=True)
    append({"phase": "invoke", "pid": os.getpid(), "initialization": INITIALIZATION,
            "invocation": INVOCATIONS, "request_id": context.aws_request_id,
            "function_arn": context.invoked_function_arn, "source": source,
            "native_message_id": native_id, "body": body})
    return {"observed": True}


def main():
    import boto3
    from botocore.config import Config

    endpoint = os.environ["WARM_MESSAGING_ENDPOINT"]
    function_arn = os.environ["WARM_MESSAGING_FUNCTION_ARN"]
    output = Path(os.environ["WARM_MESSAGING_OUTPUT"])
    suffix = uuid.uuid4().hex[:12]
    configuration = Config(connect_timeout=2, read_timeout=8, retries={"total_max_attempts": 1})
    credentials = dict(endpoint_url=endpoint, region_name="us-east-1", aws_access_key_id="sdk-test",
                       aws_secret_access_key="sdk-secret", config=configuration)
    sns, sqs, functions = (boto3.client(service, **credentials) for service in ("sns", "sqs", "lambda"))
    topic = subscription = queue = mapping = None
    expected = []

    def rows():
        if not output.exists():
            return []
        return [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines(keepends=True) if line.endswith("\n")]

    def wait(predicate, label):
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            result = predicate()
            if result:
                return result
            time.sleep(0.03)
        raise AssertionError((label, rows()))

    def invocation(native_id):
        return [row for row in rows() if row["phase"] == "invoke" and row["native_message_id"] == native_id]

    def queue_empty():
        attributes = sqs.get_queue_attributes(QueueUrl=queue, AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"])["Attributes"]
        return attributes["ApproximateNumberOfMessages"] == "0" and attributes["ApproximateNumberOfMessagesNotVisible"] == "0"

    try:
        topic = sns.create_topic(Name="warm-native-" + suffix)["TopicArn"]
        subscription = sns.subscribe(TopicArn=topic, Protocol="lambda", Endpoint=function_arn)["SubscriptionArn"]
        for index in range(2):
            body = json.dumps({"native_source": "sns", "ordinal": index})
            message_id = sns.publish(TopicArn=topic, Message=body)["MessageId"]
            expected.append(("sns", message_id, body))
            wait(lambda: invocation(message_id), "SNS invocation")
        queue = sqs.create_queue(QueueName="warm-native-" + suffix, Attributes={"VisibilityTimeout": "10"})["QueueUrl"]
        queue_arn = sqs.get_queue_attributes(QueueUrl=queue, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        mapping = functions.create_event_source_mapping(EventSourceArn=queue_arn, FunctionName=function_arn, BatchSize=1, Enabled=True)["UUID"]
        for index in range(2):
            body = json.dumps({"native_source": "sqs", "ordinal": index})
            message_id = sqs.send_message(QueueUrl=queue, MessageBody=body)["MessageId"]
            expected.append(("sqs", message_id, body))
            wait(lambda: invocation(message_id), "SQS mapped invocation")
            # Native settlement follows a verified invocation response/log
            # boundary; the reusable process is separately owned until drain.
            wait(queue_empty, "native SQS acknowledgement")
        initialized = [row for row in rows() if row["phase"] == "init"]
        observed = [row for row in rows() if row["phase"] == "invoke"]
        assert len(initialized) == 1, rows()
        assert len(observed) == 4, rows()
        assert [row["invocation"] for row in observed] == [1, 2, 3, 4], observed
        assert len({row["request_id"] for row in observed}) == 4, observed
        assert len({row["pid"] for row in rows()}) == 1, rows()
        assert len({row["initialization"] for row in rows()}) == 1, rows()
        assert all(row["function_arn"] == function_arn for row in observed), observed
        assert [(row["source"], row["native_message_id"], row["body"]) for row in observed] == expected, observed
        print(json.dumps({"status": "passed", "module_initializations": len(initialized),
                          "native_invocations": len(observed), "retained_pid": initialized[0]["pid"]}))
    finally:
        if mapping is not None:
            functions.delete_event_source_mapping(UUID=mapping)
        if queue is not None:
            sqs.delete_queue(QueueUrl=queue)
        if subscription is not None:
            sns.unsubscribe(SubscriptionArn=subscription)
        if topic is not None:
            sns.delete_topic(TopicArn=topic)


if __name__ == "__main__":
    main()
    print("PASS")
