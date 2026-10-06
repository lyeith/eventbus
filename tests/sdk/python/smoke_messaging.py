"""Owned-listener boto3 messaging contracts; no developer resources are used."""
import hashlib
import json
import os
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


def main():
    options = dict(
        endpoint_url=os.environ["MESSAGING_ENDPOINT_URL"],
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(connect_timeout=3, read_timeout=5, retries={"max_attempts": 0}),
    )
    sqs = boto3.client("sqs", **options)
    sns = boto3.client("sns", **options)
    queue = sqs.create_queue(QueueName="python-sdk", Attributes={"VisibilityTimeout": "30"})["QueueUrl"]
    attributes = {"binary": {"DataType": "Binary", "BinaryValue": bytes([0, 1, 255])}}
    sent = sqs.send_message(QueueUrl=queue, MessageBody="direct", MessageAttributes=attributes)
    assert sent["MD5OfMessageBody"] == hashlib.md5(b"direct").hexdigest()
    received = sqs.receive_message(QueueUrl=queue, MessageAttributeNames=["All"], MessageSystemAttributeNames=["All"])["Messages"][0]
    assert received["MessageId"] == sent["MessageId"]
    assert received["MessageAttributes"]["binary"]["BinaryValue"] == bytes([0, 1, 255])
    assert received["Attributes"]["ApproximateReceiveCount"] == "1"
    assert received["MD5OfMessageAttributes"] == sent["MD5OfMessageAttributes"]
    sqs.delete_message(QueueUrl=queue, ReceiptHandle=received["ReceiptHandle"])
    try:
        sqs.send_message(QueueUrl=queue.replace("python-sdk", "missing"), MessageBody="fail")
        raise AssertionError("missing queue accepted")
    except ClientError as error:
        assert error.response["Error"]["Code"] in ("AWS.SimpleQueueService.NonExistentQueue", "QueueDoesNotExist")
    arn = sqs.get_queue_attributes(QueueUrl=queue, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    topic = sns.create_topic(Name="python-topic", Tags=[{"Key": "owner", "Value": "agent"}])["TopicArn"]
    sub = sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=arn, Attributes={"RawMessageDelivery": "true"})["SubscriptionArn"]
    assert sns.get_subscription_attributes(SubscriptionArn=sub)["Attributes"]["RawMessageDelivery"] == "true"
    sns.publish(TopicArn=topic, Message="fanout", MessageAttributes=attributes)
    message = sqs.receive_message(QueueUrl=queue, MessageAttributeNames=["All"])["Messages"][0]
    assert message["Body"] == "fanout"
    assert message["MessageAttributes"]["binary"]["BinaryValue"] == bytes([0, 1, 255])
    app = sns.create_platform_application(Name="python-push", Platform="GCM", Attributes={"PlatformCredential": "local-fixture"})["PlatformApplicationArn"]
    endpoint = sns.create_platform_endpoint(PlatformApplicationArn=app, Token="local-token")["EndpointArn"]
    push = sns.publish(TargetArn=endpoint, Message="push-intent")
    records = [json.loads(line) for line in Path(os.environ["SNS_CAPTURE_LOG"]).read_text().splitlines()]
    evidence = next(record for record in records if record.get("message_id") == push["MessageId"])
    assert evidence["schema_version"] == "eventbus.sns.capture.v1"
    assert evidence["target_arn"] == endpoint and evidence["message"] == "push-intent"
    assert evidence["deliveries"][0]["status"] == "captured"
    sns.unsubscribe(SubscriptionArn=sub)
    sns.delete_endpoint(EndpointArn=endpoint)
    sns.delete_platform_application(PlatformApplicationArn=app)
    sns.delete_topic(TopicArn=topic)
    sqs.delete_queue(QueueUrl=queue)
    print("PASS")


if __name__ == "__main__":
    main()
