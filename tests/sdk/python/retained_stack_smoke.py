"""Actual boto3 origins and registered application fixtures for retained stacks.

Only gates and observations are fixture controls. Queue settlement, retries,
execution, authentication orchestration and delivery belong to native owners.
"""
from collections import Counter
import fcntl
import json
import os
from pathlib import Path
import time
import urllib.request
import uuid


def root():
    return Path(os.environ["STACK_ROOT"])


def atomic_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value), encoding="utf-8")
    temporary.replace(path)


def record(stage, chain, context, **details):
    value = {"stage": stage, "chain": chain, "pid": os.getpid(),
             "request_id": context.aws_request_id, **details}
    atomic_json(root() / ("row-" + stage + "-" + context.aws_request_id + ".json"), value)
    request = urllib.request.Request(os.environ["STACK_CONTROL"] + "/__fixture/observed",
                                     data=json.dumps(value).encode(), method="POST",
                                     headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=3) as response:
        assert response.status == 204


def gate(name):
    while not (root() / name).exists():
        time.sleep(0.01)


def resources():
    return json.loads((root() / "resources.json").read_text())


def client(service, endpoint=None):
    import boto3
    from botocore.config import Config
    return boto3.client(service, endpoint_url=endpoint or os.environ["STACK_CALLBACK"],
                        region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="testtest123",
                        config=Config(retries={"max_attempts": 0}, connect_timeout=3, read_timeout=20,
                                      s3={"addressing_style": "path"}))


def invocation(name, payload, mode="RequestResponse"):
    result = client("lambda").invoke(FunctionName=name, InvocationType=mode,
                                     Payload=json.dumps(payload).encode())
    assert result["StatusCode"] == (202 if mode == "Event" else 200), result
    assert "FunctionError" not in result, result
    if mode == "Event":
        assert result["Payload"].read() == b""
        return None
    return json.load(result["Payload"])


def batch_handler(event, context):
    records = event["Records"]
    assert 1 <= len(records) <= 5 and all(item["eventSource"] == "aws:sqs" for item in records), event
    bodies = [json.loads(item["body"]) for item in records]
    chain = bodies[0]["chain"]
    assert all(item["chain"] == chain for item in bodies), bodies
    facts = {"ids": [item["messageId"] for item in records],
             "receipts": [item["receiptHandle"] for item in records],
             "counts": [int(item["attributes"]["ApproximateReceiveCount"]) for item in records],
             "source_arns": [item["eventSourceARN"] for item in records]}
    record("batch-started", chain, context, **facts)
    if chain in ("initial", "shutdown") and all(count == 1 for count in facts["counts"]):
        gate("release-batches" if chain == "initial" else "release-shutdown")
    if any(body.get("fail_once") and count == 1 for body, count in zip(bodies, facts["counts"])):
        record("batch-failed", chain, context, **facts)
        raise RuntimeError("whole accepted batch must return to native visibility")
    assert invocation("stack-persist:live", {"suite": bodies[0]["suite"], "chain": chain,
                                             "count": len(records), "attempt": max(facts["counts"])})["persisted"]
    record("batch-completed", chain, context, **facts)
    return {"ok": True}


def attempt(chain):
    with (root() / ("counter-" + chain)).open("a+", encoding="utf-8") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        stream.seek(0)
        value = int(stream.read() or "0") + 1
        stream.seek(0)
        stream.truncate()
        stream.write(str(value))
        stream.flush()
        fcntl.flock(stream, fcntl.LOCK_UN)
        return value


def sns_handler(event, context):
    from botocore.exceptions import BotoCoreError, ClientError
    assert len(event["Records"]) == 1 and event["Records"][0]["EventSource"] == "aws:sns"
    value = json.loads(event["Records"][0]["Sns"]["Message"])
    number = attempt(value["chain"])
    record("sns-started", value["chain"], context, attempt=number)
    try:
        assert invocation("stack-persist:live", {**value, "attempt": number})["persisted"]
    except (BotoCoreError, ClientError):
        record("sns-failed", value["chain"], context, attempt=number)
        raise
    record("sns-completed", value["chain"], context, attempt=number)
    return {"ok": True}


def persist_handler(event, context):
    chain = event["chain"]
    record("persist-started", chain, context, attempt=event.get("attempt", 1))
    if chain == "lost":
        gate("release-lost-" + str(event["attempt"]))
    value = {"suite": event["suite"], "chain": chain, "attempt": event.get("attempt", 1),
             "count": event.get("count", 1)}
    if chain == "cleanup":
        current = resources()
        sentinel = client("s3", os.environ["STACK_S3"]).get_object(Bucket=current["bucket"], Key="sentinel/keep.txt")["Body"].read()
        assert sentinel == b"unrelated fixture"
    else:
        client("sns").publish(TopicArn=resources()["delivery_topic"], Message=json.dumps(value))
    record("persist-completed", chain, context, suite=value["suite"], count=value["count"], attempt=value["attempt"])
    return {"persisted": True}


def scheduled_handler(event, context):
    record("scheduled-started", event["chain"], context)
    if event["chain"] == "scheduled":
        gate("release-scheduled")
        assert invocation("stack-persist:live", event)["persisted"]
    record("scheduled-completed", event["chain"], context)
    return {"ok": True}


def gateway_handler(event, context):
    assert event["version"] == "2.0" and event["requestContext"]["http"]["method"] == "POST", event
    assert event["requestContext"]["authorizer"]["lambda"]["authenticated"] is True
    value = json.loads(event["body"])
    record("gateway-started", value["chain"], context)
    assert invocation("stack-persist:live", value)["persisted"]
    record("gateway-completed", value["chain"], context)
    return {"statusCode": 200, "headers": {"content-type": "application/json"}, "body": '{"ok":true}'}


def authenticated_claims(token):
    import jwt
    state = json.loads((root() / "auth.json").read_text())
    header = jwt.get_unverified_header(token)
    key = next(item for item in state["jwks"]["keys"] if item["kid"] == header["kid"])
    claims = jwt.decode(token, jwt.PyJWK(key).key, algorithms=["RS256"], audience=state["clientID"], issuer=state["issuer"])
    assert claims["token_use"] == "id" and claims["cognito:username"] == state["username"]
    return claims


def authorizer_handler(event, context):
    assert event["version"] == "2.0" and event["type"] == "REQUEST"
    headers = {name.lower(): value for name, value in event["headers"].items()}
    claims = authenticated_claims(headers["authorization"].removeprefix("Bearer "))
    return {"isAuthorized": True, "context": {"authenticated": True, "principal_id": claims["sub"]}}


def cleanup_handler(event, context):
    # This is an application fixture, not an emulator authorization/deletion
    # adapter. A real Cognito-issued JWT authenticates an exact DELETE request.
    assert event["version"] == "2.0" and event["requestContext"]["http"]["method"] == "DELETE"
    expected = "/api/identity/" + resources()["suite"]
    assert event["rawPath"] == expected and event["pathParameters"]["id"] == resources()["suite"]
    token = event["headers"]["authorization"].removeprefix("Bearer ")
    authenticated_claims(token)
    record("cleanup-started", "cleanup", context)
    client("sqs").send_message(QueueUrl=resources()["queue_url"],
                MessageBody=json.dumps({"suite": resources()["suite"], "chain": "cleanup"}))
    invocation("stack-cleanup-nested:live", {"suite": resources()["suite"]}, "Event")
    record("cleanup-returned", "cleanup", context)
    return {"statusCode": 202, "body": '{"cleanup_admitted":true}'}


def cleanup_nested_handler(event, context):
    record("cleanup-nested-started", "cleanup", context)
    gate("release-cleanup")
    current = resources()
    assert event["suite"] == current["suite"]
    deadline = time.monotonic() + 10
    while not any(json.loads(path.read_text())["chain"] == "cleanup"
                  for path in root().glob("row-batch-completed-*.json")):
        assert time.monotonic() < deadline, "accepted cleanup Send descendant did not finish"
        time.sleep(0.01)
    # Delete only the application fixture's identity and S3 objects. Providers,
    # queue/mapping, schedules, pool/client, stream and sentinel remain intact.
    client("cognito-idp").admin_delete_user(UserPoolId=current["pool"], Username=current["cleanup_user"])
    s3 = client("s3", os.environ["STACK_S3"])
    prefix = "suite/" + current["suite"] + "/"
    for item in s3.list_objects_v2(Bucket=current["bucket"], Prefix=prefix).get("Contents", []):
        s3.delete_object(Bucket=current["bucket"], Key=item["Key"])
    record("cleanup-nested-completed", "cleanup", context)
    return {"absent": True}


def phase():
    from botocore.exceptions import BotoCoreError, ClientError
    current_phase = os.environ["STACK_PHASE"]
    source = os.environ["STACK_SOURCE"]
    sns, sqs, functions = (client(service, source) for service in ("sns", "sqs", "lambda"))
    s3 = client("s3", os.environ["STACK_S3"])

    def fenced(operation, **kwargs):
        try:
            operation(**kwargs)
        except ClientError as failure:
            assert failure.response["ResponseMetadata"]["HTTPStatusCode"] == 503, failure.response
        else:
            raise AssertionError("fenced native root was admitted")

    if current_phase == "provision":
        state = json.loads((root() / "auth.json").read_text())
        suffix = uuid.uuid4().hex[:12]
        bucket = "eventbus-retained-stack-" + suffix
        s3.create_bucket(Bucket=bucket)
        s3.put_object(Bucket=bucket, Key="sentinel/keep.txt", Body=b"unrelated fixture")
        queue_url = sqs.create_queue(QueueName="retained-stack-" + suffix,
                                     Attributes={"VisibilityTimeout": "10"})["QueueUrl"]
        arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        delivery_topic = sns.create_topic(Name="retained-stack-delivery-" + suffix)["TopicArn"]
        root_topic = sns.create_topic(Name="retained-stack-root-" + suffix)["TopicArn"]
        stream = "retained-stack-" + suffix
        firehose = client("firehose", source)
        stream_arn = firehose.create_delivery_stream(DeliveryStreamName=stream, DeliveryStreamType="DirectPut",
            ExtendedS3DestinationConfiguration={"RoleARN": "arn:aws:iam::000000000000:role/firehose",
                "BucketARN": "arn:aws:s3:::" + bucket, "Prefix": "suite/!{partitionKeyFromQuery:suite}/",
                "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/", "CompressionFormat": "UNCOMPRESSED",
                "BufferingHints": {"SizeInMBs": 1, "IntervalInSeconds": 60},
                "DynamicPartitioningConfiguration": {"Enabled": True},
                "ProcessingConfiguration": {"Enabled": True, "Processors": [
                    {"Type": "MetadataExtraction", "Parameters": [
                        {"ParameterName": "JsonParsingEngine", "ParameterValue": "JQ-1.6"},
                        {"ParameterName": "MetadataExtractionQuery", "ParameterValue": "{suite:.suite}"}]},
                    {"Type": "AppendDelimiterToRecord"}]}})["DeliveryStreamARN"]
        sns.subscribe(TopicArn=delivery_topic, Protocol="firehose", Endpoint=stream_arn,
                      Attributes={"SubscriptionRoleArn": "arn:aws:iam::000000000000:role/sns-firehose", "RawMessageDelivery": "true"})
        sns.subscribe(TopicArn=root_topic, Protocol="lambda",
                      Endpoint="arn:aws:lambda:us-east-1:000000000000:function:stack-sns:live")
        suite = "first-" + suffix
        current = {"bucket": bucket, "queue_url": queue_url, "queue_arn": arn, "queue_name": "retained-stack-" + suffix,
                   "delivery_topic": delivery_topic, "root_topic": root_topic, "stream": stream, "stream_arn": stream_arn,
                   "suite": suite, "pool": state["poolID"], "cleanup_user": state["cleanupUser"]}
        atomic_json(root() / "resources.json", current)
        for index in range(10):
            sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps({"suite": suite, "chain": "initial", "slot": index, "fail_once": index == 0}))
        mapping = functions.create_event_source_mapping(EventSourceArn=arn, FunctionName="stack-batch:live", BatchSize=5,
                            MaximumBatchingWindowInSeconds=0, ScalingConfig={"MaximumConcurrency": 2}, Enabled=True)
        assert mapping["BatchSize"] == 5 and mapping["ScalingConfig"]["MaximumConcurrency"] == 2
        current["mapping"] = mapping["UUID"]
        atomic_json(root() / "resources.json", current)
        deadline = time.monotonic() + 5
        while True:
            attributes = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["All"])["Attributes"]
            if int(attributes["ApproximateNumberOfMessagesNotVisible"]) == 10:
                break
            assert time.monotonic() < deadline, attributes
            time.sleep(0.01)
        sns.publish(TopicArn=delivery_topic, Message=json.dumps({"suite": suite, "chain": "buffered", "attempt": 1}))
        try:
            sns.publish(TopicArn=root_topic, Message=json.dumps({"suite": suite, "chain": "lost", "drop_source": True}))
        except BotoCoreError:
            pass
        else:
            raise AssertionError("native SNS origin did not lose its accepted reply")
        print("native batch5/concurrency2 provisioned; lost SNS origin exited")
    elif current_phase == "source-refusals":
        current = resources()
        fenced(sns.publish, TopicArn=current["root_topic"], Message="new source while draining")
        fenced(functions.invoke, FunctionName="stack-persist:live", InvocationType="Event", Payload=b"{}")
        fenced(sqs.delete_queue, QueueUrl=current["queue_url"])
        print("actual SDK source producers and premature cleanup remain fenced")
    elif current_phase in ("held-refusals", "cleanup", "bad-auth"):
        current = resources()
        fenced(sns.publish, TopicArn=current["root_topic"], Message="unrelated new root")
        fenced(functions.invoke, FunctionName="stack-persist:live", InvocationType="Event", Payload=b"{}")
        cleanup = client("lambda")
        for name, mode in (("stack-persist:live", "RequestResponse"), ("stack-cleanup:other", "RequestResponse"), ("stack-cleanup:live", "Event")):
            fenced(cleanup.invoke, FunctionName=name, InvocationType=mode, Payload=b"{}")
        if current_phase != "held-refusals":
            state = json.loads((root() / "auth.json").read_text())
            path = "/api/identity/" + current["suite"]
            payload = {"version": "2.0", "rawPath": path, "routeKey": "DELETE /api/identity/{id}",
                       "headers": {"authorization": "Bearer " + (state["idToken"] if current_phase == "cleanup" else "invalid.jwt.signature")},
                       "pathParameters": {"id": current["suite"]}, "requestContext": {"http": {"method": "DELETE", "path": path}},
                       "body": "{}", "isBase64Encoded": False}
            result = cleanup.invoke(FunctionName="stack-cleanup:live", InvocationType="RequestResponse", Payload=json.dumps(payload).encode())
            assert result["StatusCode"] == 200
            if current_phase == "bad-auth":
                assert result.get("FunctionError"), result
            else:
                assert "FunctionError" not in result, result
                value = json.load(result["Payload"])
                assert value["statusCode"] == 202 and json.loads(value["body"])["cleanup_admitted"]
        print("native source/target/mode fences checked; exact signed cleanup mode=" + current_phase)
    elif current_phase == "resume":
        current = resources()
        mapping = functions.get_event_source_mapping(UUID=current["mapping"])
        assert mapping["UUID"] == current["mapping"] and mapping["EventSourceArn"] == current["queue_arn"]
        assert mapping["State"] == "Enabled" and mapping["BatchSize"] == 5 and mapping["ScalingConfig"] == {"MaximumConcurrency": 2}
        assert sqs.get_queue_url(QueueName=current["queue_name"])["QueueUrl"] == current["queue_url"]
        assert client("firehose", source).describe_delivery_stream(DeliveryStreamName=current["stream"])["DeliveryStreamDescription"]["DeliveryStreamARN"] == current["stream_arn"]
        cognito = client("cognito-idp", source)
        cognito.admin_get_user(UserPoolId=current["pool"], Username="stack-sentinel")
        try:
            cognito.admin_get_user(UserPoolId=current["pool"], Username=current["cleanup_user"])
        except ClientError as failure:
            assert failure.response["Error"]["Code"] == "UserNotFoundException"
        else:
            raise AssertionError("exact cleaned identity remains")
        assert s3.get_object(Bucket=current["bucket"], Key="sentinel/keep.txt")["Body"].read() == b"unrelated fixture"
        assert not s3.list_objects_v2(Bucket=current["bucket"], Prefix="suite/" + current["suite"] + "/").get("Contents")
        for index in range(5):
            sqs.send_message(QueueUrl=current["queue_url"], MessageBody=json.dumps({"suite": "next", "chain": "next", "slot": index}))
        assert sns.publish(TopicArn=current["root_topic"], Message=json.dumps({"suite": "next", "chain": "next"}))["MessageId"]
        print("same mapping/queue/stream/store reused; sentinel survived exact cleanup")
    elif current_phase == "shutdown":
        current = resources()
        for index in range(10):
            sqs.send_message(QueueUrl=current["queue_url"], MessageBody=json.dumps({"suite": "shutdown", "chain": "shutdown", "slot": index}))
    elif current_phase in ("s3-proof", "s3-proof-next"):
        current = resources()
        suite = current["suite"] if current_phase == "s3-proof" else "next"
        content = []
        for item in s3.list_objects_v2(Bucket=current["bucket"], Prefix="suite/").get("Contents", []):
            content.extend(json.loads(line) for line in s3.get_object(Bucket=current["bucket"], Key=item["Key"])["Body"].read().splitlines())
        assert content and all(row["suite"] == suite for row in content), content

        def identity(row):
            return row["suite"], row["chain"], row.get("attempt", 1), row.get("count", 1)

        completed = [json.loads(path.read_text()) for path in root().glob("row-persist-completed-*.json")]
        expected = Counter(identity(row) for row in completed if row["suite"] == suite and row["chain"] != "cleanup")
        if current_phase == "s3-proof":
            expected[(suite, "buffered", 1, 1)] += 1
            assert {item[1] for item in expected} == {"buffered", "initial", "lost", "scheduled", "gateway"}, expected
        else:
            assert {item[1] for item in expected} == {"next"}, expected
        assert Counter(identity(row) for row in content) == expected, (content, expected)
        assert s3.get_object(Bucket=current["bucket"], Key="sentinel/keep.txt")["Body"].read() == b"unrelated fixture"
        atomic_json(root() / "s3-proof.json", content)
        print("actual native RustFS objects match every retained callback delivery, including late drain writes")
    else:
        raise AssertionError("unknown retained stack phase: " + current_phase)
    print("PASS")


if __name__ == "__main__":
    phase()
