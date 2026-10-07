"""Real boto3 SNS subscriptions executed by two owned Python Lambda runtimes."""
import json
import os
import time
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

TARGET = "arn:aws:lambda:us-east-1:000000000000:function:sns-handler:live"


def handler(event, context):
    message = event["Records"][0]["Sns"]["Message"]
    if message == "held":
        while not Path(os.environ["SNS_LAMBDA_GATE"]).exists():
            time.sleep(0.005)
    row = {"owner": os.environ["SNS_LAMBDA_OWNER"], "variant": os.environ["SNS_LAMBDA_VARIANT"],
           "event": event, "request_id": context.aws_request_id}
    with open(os.environ["SNS_LAMBDA_OUTPUT"], "a", encoding="utf-8") as stream:
        stream.write(json.dumps(row) + "\n")
    if message == "failure":
        raise RuntimeError("private handler error must remain outside async evidence")
    if message == "timeout":
        time.sleep(60)
    return {"accepted": True}


def rows(path):
    if not path.exists():
        return []
    data = path.read_text()
    return [json.loads(line) for line in data.splitlines(keepends=True) if line.endswith("\n")]


def wait_for(assertion, label, timeout=12):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = assertion()
        if value:
            return value
        time.sleep(0.01)
    raise AssertionError(f"did not observe {label}")


def client(service, endpoint):
    return boto3.client(service, endpoint_url=endpoint, region_name="us-east-1", aws_access_key_id="local-key",
                        aws_secret_access_key="local-secret", config=Config(retries={"max_attempts": 0}, connect_timeout=3, read_timeout=5))


def expect_error(call, code="InvalidParameter"):
    try:
        call()
    except ClientError as error:
        assert error.response["Error"]["Code"] == code, error.response
    else:
        raise AssertionError(f"expected {code}")


def run():
    captures = {owner: Path(os.environ[f"SNS_LAMBDA_CAPTURE_{owner}"]) for owner in ("A", "B")}
    asynchronous = {owner: Path(os.environ[f"SNS_LAMBDA_ASYNC_{owner}"]) for owner in ("A", "B")}
    outputs = {owner: Path(os.environ[f"SNS_LAMBDA_OUTPUT_{owner}"]) for owner in ("A", "B")}
    topics, subscriptions, clients = {}, {}, {}
    attributes = {"kind": {"DataType": "String", "StringValue": "accepted"},
                  "number": {"DataType": "Number", "StringValue": "001.23000"},
                  "array": {"DataType": "String.Array", "StringValue": '["one",2]'},
                  "binary": {"DataType": "Binary", "BinaryValue": b"\x00\x01\xff"}}
    for owner in ("A", "B"):
        clients[owner] = client("sns", os.environ[f"SNS_LAMBDA_ENDPOINT_{owner}"])
        topics[owner] = clients[owner].create_topic(Name="sdk-lambda")["TopicArn"]
        subscriptions[owner] = clients[owner].subscribe(TopicArn=topics[owner], Protocol="lambda", Endpoint=TARGET,
            Attributes={"FilterPolicy": '{"kind":["accepted"]}'}, ReturnSubscriptionArn=True)["SubscriptionArn"]

    def evidence(owner, message_id, operation):
        return [record for record in rows(captures[owner]) if record.get("message_id") == message_id and record["operation"] == operation]

    def terminal(owner, message_id, state="succeeded"):
        admitted = wait_for(lambda: evidence(owner, message_id, "DeliveryAdmission"), "SNS admission")
        delivery = admitted[0]["deliveries"][0]
        assert delivery["status"] == "admitted"
        request_id = delivery["invocation_request_id"]
        return wait_for(lambda: next((row for row in reversed(rows(asynchronous[owner]))
                        if row["request_id"] == request_id and row["state"] == state), None), f"Lambda {state}")

    refused = clients["A"].publish(TopicArn=topics["A"], Message="filtered", MessageAttributes={"kind": {"DataType": "String", "StringValue": "refused"}})["MessageId"]
    assert evidence("A", refused, "Publish")[0]["deliveries"][0]["status"] == "filtered"
    assert not evidence("A", refused, "DeliveryAdmission") and not rows(outputs["A"])

    # Registered alias, native SNS event and independent owners of identical ARNs.
    for owner in ("A", "B"):
        message_id = clients[owner].publish(TopicArn=topics[owner], Subject="native subject", Message=f"native-{owner}", MessageAttributes=attributes)["MessageId"]
        completed = terminal(owner, message_id)
        effects = rows(outputs[owner])
        assert len(effects) == 1 and effects[0]["owner"] == owner and effects[0]["variant"] == "alias", effects
        effect = effects[0]
        assert effect["request_id"] == completed["request_id"]
        record = effect["event"]["Records"][0]
        assert len(effect["event"]["Records"]) == 1 and record["EventSource"] == "aws:sns" and record["EventVersion"] == "1.0"
        assert record["EventSubscriptionArn"] == subscriptions[owner]
        notification = record["Sns"]
        assert notification["MessageId"] == message_id and notification["TopicArn"] == topics[owner]
        assert notification["Message"] == f"native-{owner}" and notification["Subject"] == "native subject"
        assert notification["Type"] == "Notification" and notification["Timestamp"].endswith("Z")
        assert notification["MessageAttributes"]["number"] == {"Type": "String", "Value": "1.23"}
        assert notification["MessageAttributes"]["array"] == {"Type": "String", "Value": '["one",2]'}
        assert notification["MessageAttributes"]["binary"] == {"Type": "Binary", "Value": "AAH/"}

    # Target lookup follows filtering; unknown names and aliases never fall back.
    unknown_topic = clients["A"].create_topic(Name="unknown")["TopicArn"]
    unknowns = [TARGET.replace("sns-handler:live", "absent"), TARGET.replace(":live", ":missing")]
    for target in unknowns:
        clients["A"].subscribe(TopicArn=unknown_topic, Protocol="lambda", Endpoint=target)
    missing_id = clients["A"].publish(TopicArn=unknown_topic, Message="missing")["MessageId"]
    assert len(evidence("A", missing_id, "DeliveryFailure")) == 2
    assert not evidence("A", missing_id, "DeliveryAdmission")
    foreign = TARGET.replace("000000000000", "111111111111")
    expect_error(lambda: clients["A"].subscribe(TopicArn=topics["A"], Protocol="lambda", Endpoint=foreign))
    expect_error(lambda: clients["A"].subscribe(TopicArn=topics["A"], Protocol="lambda", Endpoint=TARGET, Attributes={"RawMessageDelivery": "true"}))
    expect_error(lambda: clients["A"].subscribe(TopicArn=topics["A"], Protocol="lambda", Endpoint=TARGET, Attributes={"DeliveryPolicy": "{}"}))

    # One outstanding slot includes running work. Acceptance is not completion.
    sqs = client("sqs", os.environ["SNS_LAMBDA_ENDPOINT_A"])
    dead_url = sqs.create_queue(QueueName="sns-admission-dlq")["QueueUrl"]
    dead_arn = sqs.get_queue_attributes(QueueUrl=dead_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    clients["A"].set_subscription_attributes(SubscriptionArn=subscriptions["A"], AttributeName="RedrivePolicy",
                                              AttributeValue=json.dumps({"deadLetterTargetArn": dead_arn}))
    held_id = clients["A"].publish(TopicArn=topics["A"], Message="held", MessageAttributes=attributes)["MessageId"]
    assert evidence("A", held_id, "DeliveryAdmission")
    assert len(rows(outputs["A"])) == 1, "SNS publish waited for handler completion"
    pressure_id = clients["A"].publish(TopicArn=topics["A"], Message="pressure", MessageAttributes=attributes)["MessageId"]
    pressure = evidence("A", pressure_id, "DeliveryFailure")
    assert len(pressure) == 1 and pressure[0]["deliveries"][0]["status"] == "dead_lettered", pressure
    assert not evidence("A", pressure_id, "DeliveryAdmission")
    dead = sqs.receive_message(QueueUrl=dead_url, MaxNumberOfMessages=1)["Messages"][0]
    assert json.loads(dead["Body"])["MessageId"] == pressure_id
    Path(os.environ["SNS_LAMBDA_GATE_A"]).write_text("released")
    terminal("A", held_id)

    # Accepted failures/timeouts are Lambda execution failures, not SNS DLQs.
    timeout_topic = clients["A"].create_topic(Name="short-timeout")["TopicArn"]
    clients["A"].subscribe(TopicArn=timeout_topic, Protocol="lambda", Endpoint=TARGET.replace(":live", ":short"))
    for message, error_type in (("failure", "FunctionError"), ("timeout", "Sandbox.Timedout")):
        selected = timeout_topic if message == "timeout" else topics["A"]
        message_id = clients["A"].publish(TopicArn=selected, Message=message, MessageAttributes=attributes)["MessageId"]
        completed = terminal("A", message_id, "failed")
        assert completed["attempts"] == 3 and completed["error_type"] == error_type, completed
        assert not evidence("A", message_id, "DeliveryFailure")
        assert "private handler error" not in asynchronous["A"].read_text()
    assert len(rows(outputs["B"])) == 1, "another owner's registry was invoked"
    print("PASS")


if __name__ == "__main__":
    run()
