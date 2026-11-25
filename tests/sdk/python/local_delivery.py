"""SDK proof for local SNS webhooks and Lambda asynchronous destinations."""

import json
import os
from pathlib import Path
import time


def handler(event, context):
    payload = json.loads(event["Records"][0]["Sns"]["Message"])
    if payload["mode"] == "fail":
        raise RuntimeError("owned SDK handler failure")
    return {"result": payload["mode"], "request_id": context.aws_request_id}


def run():
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError

    endpoint = os.environ["LOCAL_DELIVERY_ENDPOINT"]
    webhook = os.environ["LOCAL_DELIVERY_WEBHOOK"]
    secure_webhook = os.environ["LOCAL_DELIVERY_HTTPS_WEBHOOK"]
    evidence = Path(os.environ["LOCAL_DELIVERY_EVIDENCE"])
    options = dict(endpoint_url=endpoint, region_name="us-east-1", aws_access_key_id="fixture",
                   aws_secret_access_key="fixture", config=Config(connect_timeout=2, read_timeout=8,
                   retries={"total_max_attempts": 1}))
    sns = boto3.client("sns", **options)
    sqs = boto3.client("sqs", **options)
    functions = boto3.client("lambda", **options)

    def wait(probe, label):
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            value = probe()
            if value:
                return value
            time.sleep(0.02)
        raise AssertionError(f"did not observe {label}")

    def rows():
        return [json.loads(line) for line in evidence.read_text().splitlines(keepends=True)
                if line.endswith("\n")]

    def sends(message_id):
        return [row for row in rows() if row["message_id"] == message_id]

    def queue(name):
        url = sqs.create_queue(QueueName=name)["QueueUrl"]
        return url, sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]

    def read_queue(url, label):
        messages = wait(lambda: sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=1).get("Messages"), label)
        assert len(messages) == 1, label
        sqs.delete_message(QueueUrl=url, ReceiptHandle=messages[0]["ReceiptHandle"])
        return json.loads(messages[0]["Body"])

    def publish(topic, message):
        return sns.publish(TopicArn=topic, Message=message)["MessageId"]

    dlq_url, dlq_arn = queue("webhook-dead")
    topic_policy = {"http": {
        "defaultHealthyRetryPolicy": {"numRetries": 3, "minDelayTarget": 2, "maxDelayTarget": 2},
        "defaultRequestPolicy": {"headerContentType": "application/json"},
    }}
    topic = sns.create_topic(Name="local-webhook",
                             Attributes={"DeliveryPolicy": json.dumps(topic_policy)})["TopicArn"]
    subscribed = sns.subscribe(TopicArn=topic, Protocol="http", Endpoint=webhook, ReturnSubscriptionArn=True,
                               Attributes={"DeliveryPolicy": json.dumps({"healthyRetryPolicy": {"numRetries": 2}}),
                                           "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn})})
    subscription = subscribed["SubscriptionArn"]
    confirmation = wait(lambda: [row for row in rows() if row["kind"] == "SubscriptionConfirmation"
                                and row["status"] == 204], "successful confirmation callback")[0]
    assert confirmation["topic_arn"] == topic and confirmation["message_id"], confirmation

    def subscription_attributes():
        return sns.get_subscription_attributes(SubscriptionArn=subscription)["Attributes"]

    attributes = subscription_attributes()
    assert attributes["PendingConfirmation"] == "false", attributes
    policy = json.loads(attributes["EffectiveDeliveryPolicy"])
    assert policy["healthyRetryPolicy"]["numRetries"] == 2, policy
    assert policy["healthyRetryPolicy"]["minDelayTarget"] == 2, policy
    assert policy["requestPolicy"]["headerContentType"] == "application/json", policy

    retry_id = publish(topic, "retry-the-same-message")
    attempts = wait(lambda: sends(retry_id) if len(sends(retry_id)) >= 3 else None, "two webhook retries")
    assert [row["status"] for row in attempts] == [429, 503, 204], attempts
    assert all(row["kind"] == "Notification" and row["topic_arn"] == topic
               and row["subscription_arn"] == subscription and row["content_type"] == "application/json"
               for row in attempts), attempts
    envelopes = [json.loads(row["body"]) for row in attempts]
    assert envelopes[0] == envelopes[1] == envelopes[2], envelopes
    assert envelopes[0]["MessageId"] == retry_id and envelopes[0]["Message"] == "retry-the-same-message"

    for message, expected in (("permanent-failure", [400]), ("exhaust-retries", [503, 503, 503])):
        message_id = publish(topic, message)
        dead = read_queue(dlq_url, message + " subscription dead letter")
        assert dead["MessageId"] == message_id and dead["Message"] == message, dead
        assert [row["status"] for row in sends(message_id)] == expected, sends(message_id)

    sns.set_subscription_attributes(SubscriptionArn=subscription, AttributeName="RawMessageDelivery", AttributeValue="true")
    sns.set_subscription_attributes(SubscriptionArn=subscription, AttributeName="DeliveryPolicy", AttributeValue=json.dumps(
        {"healthyRetryPolicy": {"numRetries": 2}, "requestPolicy": {"headerContentType": "text/csv"}}))
    raw_message = "col1,col2\ncafé,雪\n"
    raw_id = publish(topic, raw_message)
    raw = wait(lambda: sends(raw_id), "raw webhook")[0]
    assert raw["status"] == 204 and raw["raw"] == "true", raw
    assert raw["body"] == raw_message and raw["content_type"] == "text/csv", raw

    sns.unsubscribe(SubscriptionArn=subscription)
    wait(lambda: [row for row in rows() if row["kind"] == "UnsubscribeConfirmation"], "unsubscribe confirmation")
    sns.delete_topic(TopicArn=topic)
    sqs.delete_queue(QueueUrl=dlq_url)
    secure_topic = sns.create_topic(Name="local-https-webhook")["TopicArn"]
    secure_sub = sns.subscribe(TopicArn=secure_topic, Protocol="https", Endpoint=secure_webhook,
                               ReturnSubscriptionArn=True)["SubscriptionArn"]
    wait(lambda: [row for row in rows() if row["kind"] == "SubscriptionConfirmation"
                  and row["topic_arn"] == secure_topic and row["status"] == 204], "TLS webhook confirmation")
    secure_message = publish(secure_topic, "owned-https-message")
    secure = wait(lambda: sends(secure_message), "TLS webhook notification")[0]
    assert secure["status"] == 204 and secure["subscription_arn"] == secure_sub, secure
    assert json.loads(secure["body"])["Message"] == "owned-https-message", secure
    sns.unsubscribe(SubscriptionArn=secure_sub)
    wait(lambda: [row for row in rows() if row["kind"] == "UnsubscribeConfirmation"
                  and row["topic_arn"] == secure_topic], "TLS unsubscribe confirmation")
    sns.delete_topic(TopicArn=secure_topic)
    print("SNS: HTTP/S confirmation, effective policy, stable-ID 429/503 retries, permanent/exhausted DLQ, raw body")

    success_url, success_arn = queue("async-success")
    failure_url, failure_arn = queue("async-failure")
    function_arn = "arn:aws:lambda:us-east-1:000000000000:function:local-delivery:live"
    target = {"FunctionName": "local-delivery", "Qualifier": "live"}
    destinations = {"OnSuccess": {"Destination": success_arn}, "OnFailure": {"Destination": failure_arn}}
    configured = functions.put_function_event_invoke_config(
        **target, MaximumEventAgeInSeconds=120, MaximumRetryAttempts=0, DestinationConfig=destinations)
    assert configured["FunctionArn"] == function_arn and configured["DestinationConfig"] == destinations, configured
    patched = functions.update_function_event_invoke_config(**target, MaximumRetryAttempts=1)
    assert patched["MaximumEventAgeInSeconds"] == 120 and patched["DestinationConfig"] == destinations, patched
    readback = functions.get_function_event_invoke_config(**target)
    assert readback["MaximumRetryAttempts"] == 1 and readback["DestinationConfig"] == destinations, readback
    other = dict(target, Qualifier="other")
    functions.put_function_event_invoke_config(**other, MaximumRetryAttempts=0)
    page = functions.list_function_event_invoke_configs(FunctionName=target["FunctionName"], MaxItems=1)
    next_page = functions.list_function_event_invoke_configs(
        FunctionName=target["FunctionName"], MaxItems=1, Marker=page["NextMarker"])
    records = page["FunctionEventInvokeConfigs"] + next_page["FunctionEventInvokeConfigs"]
    assert {row["FunctionArn"] for row in records} == {function_arn, function_arn.removesuffix("live") + "other"}

    target_topic = sns.create_topic(Name="local-lambda")["TopicArn"]
    sns.subscribe(TopicArn=target_topic, Protocol="lambda", Endpoint=function_arn)
    for mode, queue_url, condition, count in (
        ("succeed", success_url, "Success", 1), ("fail", failure_url, "RetriesExhausted", 2),
    ):
        sns_id = publish(target_topic, json.dumps({"mode": mode}))
        destination = read_queue(queue_url, "Lambda " + condition + " destination")
        assert destination["version"] == "1.0" and destination["timestamp"], destination
        request = destination["requestContext"]
        assert request["requestId"] and request["functionArn"] == function_arn, request
        assert request["condition"] == condition and request["approximateInvokeCount"] == count, request
        source = destination["requestPayload"]["Records"][0]["Sns"]
        assert source["MessageId"] == sns_id and json.loads(source["Message"]) == {"mode": mode}, source
        assert destination["responseContext"]["statusCode"] == 200, destination
        if mode == "succeed":
            assert destination["responsePayload"] == {"result": mode, "request_id": request["requestId"]}, destination
        else:
            assert destination["responseContext"]["functionError"] == "Unhandled", destination

    functions.delete_function_event_invoke_config(**target)
    try:
        functions.get_function_event_invoke_config(**target)
    except ClientError as error:
        assert error.response["Error"]["Code"] == "ResourceNotFoundException", error.response
    else:
        raise AssertionError("deleted config still readable")
    functions.delete_function_event_invoke_config(**other)
    assert not functions.list_function_event_invoke_configs(
        FunctionName=target["FunctionName"])["FunctionEventInvokeConfigs"]
    sns.delete_topic(TopicArn=target_topic)
    sqs.delete_queue(QueueUrl=success_url)
    sqs.delete_queue(QueueUrl=failure_url)
    print("Lambda: native Put/Get/List/Update/Delete, alias policy, SNS invocation, SQS success/failure records and configured retry count")
    # Leave one accepted retry in the CLI. The Go owner sends SIGTERM as
    # soon as this first HTTP 503 is observed and checks that it joins retry.
    drain_topic = sns.create_topic(Name="local-webhook-drain")["TopicArn"]
    sns.subscribe(TopicArn=drain_topic, Protocol="http", Endpoint=webhook,
                  Attributes={"DeliveryPolicy": json.dumps({"healthyRetryPolicy": {
                      "numRetries": 1, "minDelayTarget": 400, "maxDelayTarget": 400}})})
    wait(lambda: [row for row in rows() if row["kind"] == "SubscriptionConfirmation"
                  and row["topic_arn"] == drain_topic and row["status"] == 204], "drain webhook confirmation")
    drain_id = publish(drain_topic, "drain-retry-on-exit")
    assert wait(lambda: sends(drain_id), "first drain attempt")[0]["status"] == 503
    print("PASS")


if __name__ == "__main__":
    run()
