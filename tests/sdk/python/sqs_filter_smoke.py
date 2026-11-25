"""Real SDK proof of filtered native SQS settlement and Update reset."""
import fcntl
import json
import os
from pathlib import Path
import time

import boto3
from botocore.config import Config


def main():
    endpoint = os.environ["SQS_FILTER_ENDPOINT"]
    output = Path(os.environ["SQS_FILTER_ROOT"]) / "effects.jsonl"
    common = dict(endpoint_url=endpoint, region_name="us-east-1",
                  aws_access_key_id="test", aws_secret_access_key="test",
                  config=Config(connect_timeout=2, read_timeout=5, retries={"max_attempts": 0}))
    sqs = boto3.client("sqs", **common)
    functions = boto3.client("lambda", **common)
    url = sqs.create_queue(QueueName="native-sqs-filter", Attributes={"VisibilityTimeout": "8"})["QueueUrl"]
    arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    criteria = {"Filters": [
        {"Pattern": json.dumps({"body": {"code": ["allowed"]}})},
        {"Pattern": json.dumps({"body": {"amount": [{"numeric": [">", 10]}]}})},
    ]}

    def send(**fields):
        return sqs.send_message(QueueUrl=url, MessageBody=json.dumps(
            {"mode": "ok", "scenario": "filter", **fields}))["MessageId"]

    def wait_empty():
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            counts = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=[
                "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"
            ])["Attributes"]
            if all(v == "0" for v in counts.values()):
                return
            time.sleep(0.02)
        raise AssertionError("matched and filtered records did not settle")

    def observed():
        with output.open(encoding="utf-8") as stream:
            fcntl.flock(stream, fcntl.LOCK_SH)
            return [item["messageId"]
                    for line in stream if (row := json.loads(line))["stage"] == "completed"
                    for item in row["event"]["Records"]]

    first = send(code="allowed")
    sqs.send_message(QueueUrl=url, MessageBody="plain-text should be discarded")
    send(code="denied", amount=1)
    second = send(amount=20)
    mapping = functions.create_event_source_mapping(
        EventSourceArn=arn, FunctionName="batch-handler:live", BatchSize=4,
        FilterCriteria=criteria,
    )
    ident = mapping["UUID"]
    try:
        assert functions.get_event_source_mapping(UUID=ident)["FilterCriteria"] == criteria
        wait_empty()
        assert set(observed()) == {first, second} and len(observed()) == 2
        updated = functions.update_event_source_mapping(UUID=ident, FilterCriteria={})
        assert not updated.get("FilterCriteria", {}).get("Filters")
        third = send(code="previously-denied")
        wait_empty()
        assert set(observed()) == {first, second, third} and len(observed()) == 3
        print("Nonmatching JSON/plain text removed, OR filters worked, and Update reset reused the mapping")
        print("PASS")
    finally:
        functions.delete_event_source_mapping(UUID=ident)
        sqs.delete_queue(QueueUrl=url)


if __name__ == "__main__":
    main()
