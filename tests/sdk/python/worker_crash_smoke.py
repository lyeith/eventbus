"""Real SNS/SQS producers select existing worker attempts without PID selection."""
from __future__ import annotations

import json
import os
from pathlib import Path
import time
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import Request, urlopen
import uuid

MODULE_INITIALIZATION = uuid.uuid4().hex
_worker_calls = 0
FUNCTION_ARN = "arn:aws:lambda:us-east-1:000000000000:function:"
CONTROL = "/__eventbus/dev/lambda/invocations"


def rows(path):
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines(keepends=True)
            if line.endswith("\n")]


def commit(root, row):
    descriptor = os.open(root / "effects.jsonl", os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        data = (json.dumps(row, sort_keys=True) + "\n").encode()
        assert os.write(descriptor, data) == len(data)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def handler(event, context):
    global _worker_calls
    _worker_calls += 1
    root = Path(os.environ["WORKER_CRASH_ROOT"])
    record = event["Records"][0]
    assert len(event["Records"]) == 1
    if "Sns" in record:
        source, message_id, body = "sns", record["Sns"]["MessageId"], record["Sns"]["Message"]
        assert record["EventSource"] == "aws:sns"
        receive_count = None
    else:
        source, message_id, body = "sqs", record["messageId"], record["body"]
        assert record["eventSource"] == "aws:sqs"
        receive_count = int(record["attributes"]["ApproximateReceiveCount"])
    data = json.loads(body)
    index = 1 + len([row for row in rows(root / "effects.jsonl")
                     if row["phase"] == "start" and row["message_id"] == message_id])
    row = {"source": source, "message_id": message_id, "request_id": context.aws_request_id,
           "function_arn": context.invoked_function_arn, "label": data["label"], "effect_index": index,
           "module_initialization": MODULE_INITIALIZATION, "worker_calls": _worker_calls,
           "receive_count": receive_count, "event": event}
    commit(root, {**row, "phase": "start"})
    print("native-worker-start-" + context.aws_request_id, flush=True)
    while (not (root / f"release-{context.aws_request_id}-{index}").exists()
           and not (root / "cleanup").exists()):
        time.sleep(0.005)
    commit(root, {**row, "phase": "finish"})
    return {"committed": True}


def wait_for(predicate, label, timeout=12):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.01)
    raise AssertionError("did not observe " + label)


def main():
    import boto3
    from botocore.config import Config

    endpoint = os.environ["WORKER_CRASH_ENDPOINT"]
    assert urlparse(endpoint).hostname in ("127.0.0.1", "localhost", "::1")
    root = Path(os.environ["WORKER_CRASH_ROOT"])
    mode = os.environ["WORKER_CRASH_MODE"]
    credentials = dict(endpoint_url=endpoint, region_name="us-east-1", aws_access_key_id="local-key",
                       aws_secret_access_key="local-secret", config=Config(retries={"max_attempts": 0}, connect_timeout=3, read_timeout=5))
    sns, sqs, functions, cognito = (boto3.client(name, **credentials) for name in ("sns", "sqs", "lambda", "cognito-idp"))
    suffix = uuid.uuid4().hex[:12]
    topic = queue = probe_topic = probe_queue = pool = mapping = None
    subscription = probe_subscription = None
    expected_probes = []

    def effects(message_id, phase="start"):
        return [row for row in rows(root / "effects.jsonl") if row["message_id"] == message_id and row["phase"] == phase]

    def release(row):
        (root / f"release-{row['request_id']}-{row['effect_index']}").write_text("finish")

    def control(method, request_id=None, function=None, attempt=1, status=200):
        body = None if function is None else {"function_name": function, "attempt": attempt}
        path = CONTROL if request_id is None else CONTROL + f"/{request_id}/worker-crash"
        request = Request(endpoint + path, method=method, data=None if body is None else json.dumps(body).encode(),
                          headers={"Content-Type": "application/json"})
        try:
            response = urlopen(request, timeout=5)
        except HTTPError as error:
            with error:
                assert error.code == status, (method, request_id, error.code, error.read())
            return None
        with response:
            assert response.status == status
            result = json.load(response)
            assert "pid" not in json.dumps(result).lower(), "control must not expose worker PIDs"
            return result

    def select(row, function, method="PUT", status=200, attempt=1):
        return control(method, row["request_id"], function, attempt, status)

    def diagnostics(row, attempt=1):
        return [record for record in rows(root / "private-diagnostics.jsonl")
                if record["request_id"] == row["request_id"] and record["attempt"] == attempt]

    def failed(row):
        diagnostic = wait_for(lambda: diagnostics(row), "joined injected runtime failure")[0]
        assert diagnostic["state"] == "failed" and diagnostic["function_error"] is True, diagnostic
        assert diagnostic["injected_worker_crash"] is True and diagnostic["ownership_confirmed"] is True, diagnostic
        assert diagnostic["termination_cause"] == "injected_worker_crash", diagnostic
        assert diagnostic["native_response_synthesized"] is True, diagnostic
        assert diagnostic["completion_scope"] == "process", diagnostic
        assert effects(row["message_id"])[0] == row, "real pre-crash external write must remain unchanged"
        assert not [effect for effect in effects(row["message_id"], "finish") if effect["effect_index"] == row["effect_index"]], "killed handler returned"
        return diagnostic

    def successful(row, attempt=1):
        diagnostic = wait_for(lambda: diagnostics(row, attempt), "healthy joined native execution")[0]
        assert diagnostic["state"] == "succeeded" and diagnostic["function_error"] is False, diagnostic
        assert diagnostic["ownership_confirmed"] is True and not diagnostic.get("injected_worker_crash"), diagnostic
        assert diagnostic["completion_scope"] == ("invocation" if mode == "warm" else "process"), diagnostic
        return diagnostic

    def async_records(row):
        return [record for record in rows(root / "async.jsonl") if record["request_id"] == row["request_id"]]

    def async_terminal(row, attempts):
        terminal = wait_for(lambda: next((record for record in reversed(async_records(row))
                           if record["state"] in ("succeeded", "failed", "canceled")), None), "native async terminal")
        assert terminal["state"] == "succeeded" and terminal["attempts"] == attempts, terminal
        # A JSONL append becomes readable before its covering Sync and the
        # owner's terminal publication. Selection is stale only after that
        # publication, so synchronize with the real owner snapshot as well.
        published = wait_for(lambda: next((record for record in control("GET")["events"]
                             if record["request_id"] == row["request_id"]
                             and record["state"] == terminal["state"]
                             and record["attempts"] == attempts), None), "published native async terminal")
        assert published["function_name"] == terminal["function_name"], published
        return terminal

    def publish(label, wait_start=True):
        response = sns.publish(TopicArn=topic, Message=json.dumps({"label": label}), Subject="native worker-crash proof",
                               MessageAttributes={"trace": {"DataType": "String", "StringValue": label}})
        message_id = response["MessageId"]
        admission = wait_for(lambda: [row for row in rows(root / "sns.jsonl")
                            if row.get("message_id") == message_id and row["operation"] == "DeliveryAdmission"], "actual SNS admission")[0]
        assert len(admission["deliveries"]) == 1 and admission["deliveries"][0]["status"] == "admitted", admission
        request_id = admission["deliveries"][0]["invocation_request_id"]
        if not wait_start:
            return {"request_id": request_id, "message_id": message_id}
        effect = wait_for(lambda: effects(message_id), "native SNS handler write")[0]
        assert effect["request_id"] == request_id and effect["function_arn"] == FUNCTION_ARN + "crash-sns:live", effect
        assert effect["event"]["Records"][0]["Sns"]["MessageId"] == message_id
        return effect

    def normal_sns(label):
        row = publish(label)
        release(row)
        async_terminal(row, 1)
        successful(row)
        assert len(effects(row["message_id"])) == len(effects(row["message_id"], "finish")) == 1
        return row

    def mapping_records(row):
        return [record for record in rows(root / "deliveries.jsonl") if record.get("request_id") == row["request_id"]]

    def mapping_terminal(row, state):
        terminal = wait_for(lambda: next((record for record in reversed(mapping_records(row))
                           if record["state"] != "admitted"), None), "joined native mapping terminal")
        assert terminal["state"] == state and terminal["joined"] is True, terminal
        assert terminal["messages"][0]["message_id"] == row["message_id"]
        return terminal

    def send(label):
        message_id = sqs.send_message(QueueUrl=queue, MessageBody=json.dumps({"label": label}),
                                     MessageAttributes={"trace": {"DataType": "String", "StringValue": label}})["MessageId"]
        effect = wait_for(lambda: effects(message_id), "native SQS handler write")[0]
        admission = wait_for(lambda: [record for record in mapping_records(effect) if record["state"] == "admitted"], "actual mapping admission")[0]
        assert admission["mapping_uuid"] == mapping and admission["invoked_function_arn"] == FUNCTION_ARN + "crash-sqs:live", admission
        assert admission["messages"] == [{"message_id": message_id, "receive_count": 1}], admission
        assert effect["receive_count"] == 1
        return effect

    def broker_probe(label):
        assert sns.get_topic_attributes(TopicArn=topic)["Attributes"]["TopicArn"] == topic
        assert sqs.get_queue_url(QueueName="crash-probe-" + suffix)["QueueUrl"] == probe_queue
        direct = "direct-" + label
        sqs.send_message(QueueUrl=probe_queue, MessageBody=direct)
        sns.publish(TopicArn=probe_topic, Message="sns-" + label)
        expected_probes.extend([direct, "sns-" + label])
        pools = cognito.list_user_pools(MaxResults=60)["UserPools"]
        assert pool in {item["Id"] for item in pools}, pools
        with urlopen(endpoint + f"/{pool}/.well-known/jwks.json", timeout=3) as response:
            assert json.load(response) == baseline_jwks, "Cognito signing material changed during crash"

    try:
        pool = cognito.create_user_pool(PoolName="crash-sentinel-" + suffix)["UserPool"]["Id"]
        with urlopen(endpoint + f"/{pool}/.well-known/jwks.json", timeout=3) as response:
            baseline_jwks = json.load(response)
        assert baseline_jwks["keys"]
        topic = sns.create_topic(Name="crash-sns-" + suffix)["TopicArn"]
        subscription = sns.subscribe(TopicArn=topic, Protocol="lambda", Endpoint=FUNCTION_ARN + "crash-sns:live")["SubscriptionArn"]
        probe_queue = sqs.create_queue(QueueName="crash-probe-" + suffix)["QueueUrl"]
        probe_arn = sqs.get_queue_attributes(QueueUrl=probe_queue, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        probe_topic = sns.create_topic(Name="crash-probe-" + suffix)["TopicArn"]
        probe_subscription = sns.subscribe(TopicArn=probe_topic, Protocol="sqs", Endpoint=probe_arn,
                                           Attributes={"RawMessageDelivery": "true"})["SubscriptionArn"]
        before = normal_sns("prewarm-sns")
        target, peer = publish("selected-sns"), publish("overlapping-sns")
        assert (target["module_initialization"] == before["module_initialization"]) == (mode == "warm"), (before, target)
        if mode == "warm":
            assert target["worker_calls"] == before["worker_calls"] + 1, "selected SNS worker was actually retained and reused"
        assert peer["module_initialization"] != target["module_initialization"], "overlap needs independent workers"
        snapshot = control("GET")
        selected = next(row for row in snapshot["invocations"] if row["request_id"] == target["request_id"])
        assert selected["function_name"] == "crash-sns:live" and selected["attempt"] == 1 and selected["state"] == "running", selected
        select(target, "crash-sqs:live", status=404)
        select(target, "crash-sns:live", attempt=2, status=409)
        response = select(target, "crash-sns:live")
        assert response == {"request_id": target["request_id"], "function_name": "crash-sns:live", "attempt": 1,
                            "armed": False, "fired": True}, response
        broker_probe("during-sns-death")
        failed(target)
        assert not effects(peer["message_id"], "finish"), "unrelated held invocation remains active"
        retry = wait_for(lambda: effects(target["message_id"])[1:] or None, "real native SNS retry")[0]
        assert retry["request_id"] == target["request_id"] and retry["event"] == target["event"], (target, retry)
        assert retry["module_initialization"] != target["module_initialization"], "retry must execute another real runtime worker"
        select(target, "crash-sns:live", status=409)
        release(peer)
        release(retry)
        async_terminal(peer, 1)
        async_terminal(target, 2)
        successful(peer)
        successful(retry, 2)
        assert effects(peer["message_id"], "finish")[0]["module_initialization"] == peer["module_initialization"]
        transitions = async_records(target)
        assert [(row["state"], row["attempts"]) for row in transitions] == [
            ("queued", 0), ("running", 1), ("retrying", 1), ("running", 2), ("succeeded", 2)], transitions
        assert transitions[2]["error_type"] == "Runtime.ExitError" and transitions[2]["injected_worker_crash"] is True, transitions
        select(target, "crash-sns:live", status=404)
        normal_sns("later-sns-one-shot")

        # Saturate only this owner's two async executors with actual SNS events.
        # Arming and clearing the already-owned queued ID must leave it healthy.
        first, second = publish("queue-holder-a"), publish("queue-holder-b")
        queued = publish("cleared-queued-sns", wait_start=False)
        assert not effects(queued["message_id"])
        armed = select(queued, "crash-sns:live")
        assert armed == {"request_id": queued["request_id"], "function_name": "crash-sns:live", "attempt": 1,
                         "armed": True, "fired": False}, armed
        select(queued, "crash-sns:live", status=409)
        cleared = select(queued, "crash-sns:live", method="DELETE")
        assert cleared == {"request_id": queued["request_id"], "function_name": "crash-sns:live", "attempt": 1,
                           "armed": False, "fired": False}, cleared
        release(first)
        queued_row = wait_for(lambda: effects(queued["message_id"]), "cleared native queued event")[0]
        release(second)
        release(queued_row)
        for row in (first, second, queued_row):
            async_terminal(row, 1)
            successful(row)

        queue = sqs.create_queue(QueueName="crash-sqs-" + suffix, Attributes={"VisibilityTimeout": "6"})["QueueUrl"]
        queue_arn = sqs.get_queue_attributes(QueueUrl=queue, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        mapping = functions.create_event_source_mapping(EventSourceArn=queue_arn, FunctionName=FUNCTION_ARN + "crash-sqs:live",
                                                       BatchSize=1, Enabled=True, ScalingConfig={"MaximumConcurrency": 2})["UUID"]
        before = send("prewarm-sqs")
        release(before)
        mapping_terminal(before, "succeeded")
        successful(before)
        target, peer = send("selected-sqs"), send("overlapping-sqs")
        assert (target["module_initialization"] == before["module_initialization"]) == (mode == "warm"), (before, target)
        if mode == "warm":
            assert target["worker_calls"] == before["worker_calls"] + 1, "selected SQS worker was actually retained and reused"
        response = select(target, "crash-sqs:live")
        assert response["fired"] is True and response["armed"] is False, response
        broker_probe("during-sqs-death")
        failed(target)
        first_delivery = mapping_terminal(target, "failed")
        assert first_delivery["completion_scope"] == "process", first_delivery
        assert first_delivery["messages"] == [{"message_id": target["message_id"], "receive_count": 1,
                                               "settlement": "unacknowledged"}], first_delivery
        assert not effects(peer["message_id"], "finish")
        release(peer)
        mapping_terminal(peer, "succeeded")
        successful(peer)
        redelivered = wait_for(lambda: effects(target["message_id"])[1:] or None, "native SQS visibility redelivery")[0]
        assert redelivered["request_id"] != target["request_id"], "SQS creates a new native invocation identity per delivery"
        assert redelivered["receive_count"] == 2 and redelivered["message_id"] == target["message_id"], redelivered
        old_record, new_record = target["event"]["Records"][0], redelivered["event"]["Records"][0]
        assert new_record["body"] == old_record["body"] and new_record["messageAttributes"] == old_record["messageAttributes"]
        assert new_record["receiptHandle"] != old_record["receiptHandle"], "native retry needs a new receipt"
        assert redelivered["module_initialization"] != target["module_initialization"]
        select(target, "crash-sqs:live", status=404)
        release(redelivered)
        terminal = mapping_terminal(redelivered, "succeeded")
        successful(redelivered)
        assert terminal["messages"] == [{"message_id": target["message_id"], "receive_count": 2,
                                         "settlement": "mapping_settled", "acknowledge_attempted": True}], terminal
        assert len(effects(target["message_id"])) == 2 and len(effects(target["message_id"], "finish")) == 1
        later = send("later-sqs-one-shot")
        release(later)
        mapping_terminal(later, "succeeded")
        successful(later)
        attributes = sqs.get_queue_attributes(QueueUrl=queue, AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"])["Attributes"]
        assert attributes == {"ApproximateNumberOfMessages": "0", "ApproximateNumberOfMessagesNotVisible": "0"}, attributes
        received = []
        while len(received) < len(expected_probes):
            messages = sqs.receive_message(QueueUrl=probe_queue, MaxNumberOfMessages=10).get("Messages", [])
            assert messages, received
            received.extend(message["Body"] for message in messages)
            for message in messages:
                sqs.delete_message(QueueUrl=probe_queue, ReceiptHandle=message["ReceiptHandle"])
        assert sorted(received) == sorted(expected_probes), received
        injected = [record for record in rows(root / "private-diagnostics.jsonl") if record.get("injected_worker_crash")]
        assert len(injected) == 2, injected
        print(json.dumps({"status": "passed", "mode": mode, "native_worker_deaths": len(injected),
                          "real_producer_retries": 2, "healthy_broker_probe_messages": len(received)}))
    finally:
        (root / "cleanup").write_text("finish")
        if mapping is not None:
            functions.delete_event_source_mapping(UUID=mapping)
        if queue is not None:
            sqs.delete_queue(QueueUrl=queue)
        if subscription is not None:
            sns.unsubscribe(SubscriptionArn=subscription)
        if topic is not None:
            sns.delete_topic(TopicArn=topic)
        if probe_subscription is not None:
            sns.unsubscribe(SubscriptionArn=probe_subscription)
        if probe_topic is not None:
            sns.delete_topic(TopicArn=probe_topic)
        if probe_queue is not None:
            sqs.delete_queue(QueueUrl=probe_queue)
        if pool is not None:
            cognito.delete_user_pool(UserPoolId=pool)
        for client in (sns, sqs, functions, cognito):
            client.close()


if __name__ == "__main__":
    main()
    print("PASS")
