"""Real SNS -> Python Lambda completion-loss proof with committed side effects."""
import json
import os
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import boto3
from botocore.config import Config

FUNCTION_ARN = "arn:aws:lambda:us-east-1:000000000000:function:"
CONTROL = "/__eventbus/dev/lambda/async/events"
_worker_invocations = 0


def handler(event, context):
    global _worker_invocations
    _worker_invocations += 1
    root = Path(os.environ["ASYNC_COMPLETION_ROOT"])
    effect_index = 1 + len([row for row in rows(root / "effects.jsonl")
                            if row["request_id"] == context.aws_request_id])
    effect = {
        "effect_index": effect_index,
        "owner": os.environ["ASYNC_COMPLETION_OWNER"],
        "function": os.environ["ASYNC_COMPLETION_FUNCTION"],
        "request_id": context.aws_request_id,
        "function_arn": context.invoked_function_arn,
        "pid": os.getpid(),
        "worker_invocations": _worker_invocations,
        "event": event,
    }
    # This is the real business effect. Completion loss never substitutes a
    # handler result or modifies the native SNS event delivered to the handler.
    descriptor = os.open(root / "effects.jsonl", os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        encoded = (json.dumps(effect, sort_keys=True) + "\n").encode()
        assert os.write(descriptor, encoded) == len(encoded)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    if os.environ.get("ASYNC_COMPLETION_UNGATED") != "true":
        while (not (root / f"release-{context.aws_request_id}-{effect_index}").exists()
               and not (root / "cleanup").exists()):
            time.sleep(0.005)
    return {"committed": True}


def rows(path):
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines(keepends=True)
            if line.endswith("\n")]


def wait_for(assertion, label, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = assertion()
        if result:
            return result
        time.sleep(0.01)
    raise AssertionError(f"did not observe {label}")


def control(endpoint, method, suffix="", body=None, status=200):
    data = None if body is None else json.dumps(body).encode()
    request = Request(endpoint + CONTROL + suffix, data=data, method=method,
                      headers={"Content-Type": "application/json"})
    try:
        response = urlopen(request, timeout=3)
    except HTTPError as error:
        with error:
            assert error.code == status, (method, suffix, error.code, error.read())
        return None
    with response:
        assert response.status == status, (method, suffix, response.status)
        return json.load(response)


def run():
    mode = os.environ["ASYNC_COMPLETION_MODE"]
    endpoints = {owner: os.environ[f"ASYNC_COMPLETION_ENDPOINT_{owner}"]
                 for owner in ("A", "B", "disabled")}
    roots = {owner: Path(os.environ[f"ASYNC_COMPLETION_ROOT_{owner}"])
             for owner in endpoints}
    clients = {
        owner: boto3.client("sns", endpoint_url=endpoints[owner], region_name="us-east-1",
                            aws_access_key_id="local-key", aws_secret_access_key="local-secret",
                            config=Config(retries={"max_attempts": 0}, connect_timeout=3, read_timeout=5))
        for owner in ("A", "B")
    }
    topics, subscriptions = {}, {}
    for owner in clients:
        for function in ("processor:live", "other:live"):
            topic = clients[owner].create_topic(Name=function.replace(":", "-"))["TopicArn"]
            subscription = clients[owner].subscribe(
                TopicArn=topic, Protocol="lambda", Endpoint=FUNCTION_ARN + function,
                ReturnSubscriptionArn=True)["SubscriptionArn"]
            topics[owner, function], subscriptions[owner, function] = topic, subscription

    def sns_evidence(owner, message_id, operation):
        return [row for row in rows(roots[owner] / "sns.jsonl")
                if row.get("message_id") == message_id and row["operation"] == operation]

    def effects(owner, request_id):
        return [row for row in rows(roots[owner] / "effects.jsonl")
                if row["request_id"] == request_id]

    def publish(owner, function, message):
        response = clients[owner].publish(
            TopicArn=topics[owner, function], Subject="native completion-loss acceptance",
            Message=message, MessageAttributes={"trace": {"DataType": "String", "StringValue": message}})
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        message_id = response["MessageId"]
        admissions = wait_for(lambda: sns_evidence(owner, message_id, "DeliveryAdmission"),
                              f"{owner} native SNS admission")
        assert len(admissions) == 1 and len(admissions[0]["deliveries"]) == 1, admissions
        delivery = admissions[0]["deliveries"][0]
        assert delivery["status"] == "admitted" and delivery["endpoint"] == FUNCTION_ARN + function
        request_id = delivery["invocation_request_id"]
        assert request_id and request_id != message_id, delivery
        first = wait_for(lambda: effects(owner, request_id), f"{owner} committed effect")[0]
        assert first["owner"] == owner and first["function"] == function, first
        assert first["function_arn"] == FUNCTION_ARN + function
        record = first["event"]["Records"][0]
        assert len(first["event"]["Records"]) == 1 and record["EventSource"] == "aws:sns", record
        assert record["EventVersion"] == "1.0" and record["EventSubscriptionArn"] == subscriptions[owner, function]
        sns = record["Sns"]
        assert sns["MessageId"] == message_id and sns["TopicArn"] == topics[owner, function]
        assert sns["Message"] == message and sns["Subject"] == "native completion-loss acceptance"
        assert sns["MessageAttributes"] == {"trace": {"Type": "String", "Value": message}}, sns
        assert "completion_loss" not in json.dumps(first["event"]), first
        return {"owner": owner, "function": function, "request_id": request_id, "message_id": message_id}

    def set_loss(event, method="PUT", status=200, attempt=1, owner=None, function=None):
        return control(endpoints[owner or event["owner"]], method,
                       f"/{event['request_id']}/completion-loss",
                       {"function_name": function or event["function"], "attempt": attempt}, status)

    def release(event, effect_index=1):
        (roots[event["owner"]] / f"release-{event['request_id']}-{effect_index}").write_text("released")

    def async_records(event):
        return [row for row in rows(roots[event["owner"]] / "async.jsonl")
                if row["request_id"] == event["request_id"]]

    def terminal(event, attempts):
        completed = wait_for(lambda: next((row for row in reversed(async_records(event))
                            if row["state"] in ("succeeded", "failed", "canceled")), None),
                            f"{event['owner']} joined async completion")
        assert completed["state"] == "succeeded" and completed["attempts"] == attempts, completed
        assert completed["function_name"] == event["function"] and not completed.get("error_type"), completed
        if "message_id" in event:
            assert not sns_evidence(event["owner"], event["message_id"], "DeliveryFailure")
            assert len(sns_evidence(event["owner"], event["message_id"], "DeliveryAdmission")) == 1
        wait_for(lambda: next((row for row in control(endpoints[event["owner"]], "GET")["events"]
                              if row["request_id"] == event["request_id"] and row["state"] == "succeeded"), None),
                 "published terminal snapshot")
        return completed

    try:
        # Actual admitted events in one owner are concurrent; identical names in
        # another owner are independent and cannot be selected by a foreign ID.
        target = publish("A", "processor:live", "target")
        same_function = publish("A", "processor:live", "same-function-peer")
        other_function = publish("A", "other:live", "different-function-peer")
        foreign = publish("B", "processor:live", "foreign-owner-peer")
        for owner in ("A", "B"):
            snapshot = control(endpoints[owner], "GET")["events"]
            expected = {event["request_id"] for event in (target, same_function, other_function, foreign)
                        if event["owner"] == owner}
            assert {row["request_id"] for row in snapshot} == expected, snapshot
            assert all(row["state"] == "running" and row["attempts"] == 1 for row in snapshot), snapshot
            assert all("payload" not in row and "function_output" not in row for row in snapshot), snapshot

        control(endpoints["disabled"], "GET", status=404)
        set_loss(target, owner="disabled", status=404)
        set_loss(target, function="other:live", status=404)
        set_loss(target, owner="B", status=404)
        set_loss(foreign, owner="A", status=404)
        control(endpoints["A"], "PUT", f"/{target['message_id']}/completion-loss",
                {"function_name": "processor:live", "attempt": 1}, status=404)
        set_loss(target, attempt=2, status=409)
        armed = set_loss(target)
        assert armed == {"request_id": target["request_id"], "function_name": "processor:live",
                         "attempt": 1, "armed": True}, armed
        assert set_loss(target) == armed  # A retried control ACK is idempotent.

        # All handlers have already committed their side effect. Releasing only
        # their own gates proves loss acts at completion, not during admission.
        for event in (target, same_function, other_function, foreign):
            release(event)
        wait_for(lambda: len(effects("A", target["request_id"])) == 2, "real retry side effect")
        set_loss(target, status=409)  # Attempt 1 was consumed; attempt 2 is still held.
        release(target, effect_index=2)
        terminal(target, 2)
        for event in (same_function, other_function, foreign):
            terminal(event, 1)
            assert len(effects(event["owner"], event["request_id"])) == 1, event

        replayed = effects("A", target["request_id"])
        assert len(replayed) == 2 and replayed[0]["event"] == replayed[1]["event"], replayed
        assert all(row["request_id"] == target["request_id"] for row in replayed)
        transitions = async_records(target)
        assert [(row["state"], row["attempts"]) for row in transitions] == [
            ("queued", 0), ("running", 1), ("retrying", 1), ("running", 2), ("succeeded", 2)
        ], transitions
        assert transitions[2]["error_type"] == "Dev.CompletionLost", transitions
        assert transitions[2].get("injected_completion_loss") is True, transitions
        assert all(not row.get("injected_completion_loss") for index, row in enumerate(transitions)
                   if index != 2), transitions
        set_loss(target, status=404)  # A terminal request ID is not a new rule.

        # The consumed rule cannot affect a later request to the same alias.
        later = publish("A", "processor:live", "later-after-one-shot")
        release(later)
        terminal(later, 1)
        assert len(effects("A", later["request_id"])) == 1

        # Clearing an armed active event preserves its actual successful first
        # completion. A subsequent event also remains unaffected.
        disarmed = publish("A", "processor:live", "disarmed")
        assert set_loss(disarmed)["armed"] is True
        cleared = set_loss(disarmed, method="DELETE")
        assert cleared == {"request_id": disarmed["request_id"], "function_name": "processor:live",
                           "attempt": 1, "armed": False}, cleared
        release(disarmed)
        terminal(disarmed, 1)
        after_clear = publish("A", "processor:live", "later-after-clear")
        release(after_clear)
        terminal(after_clear, 1)
        assert len(effects("A", disarmed["request_id"])) == len(effects("A", after_clear["request_id"])) == 1

        # The ungated alias commits and returns immediately. Warm the actual
        # handler through ordinary boto3 Invoke before atomically arming it;
        # there are no file gates or application delays on this path.
        functions = boto3.client(
            "lambda", endpoint_url=endpoints["A"], region_name="us-east-1",
            aws_access_key_id="local-key", aws_secret_access_key="local-secret",
            config=Config(retries={"max_attempts": 0}, connect_timeout=3, read_timeout=5))

        def native_fast(message):
            payload = {"opaque": message, "nested": {"unchanged": [1, True, None]}}
            response = functions.invoke(
                FunctionName="ungated:live", InvocationType="Event",
                Payload=json.dumps(payload).encode())
            assert response["StatusCode"] == response["ResponseMetadata"]["HTTPStatusCode"] == 202
            assert "FunctionError" not in response and "LogResult" not in response
            assert response["Payload"].read() == b""
            response["Payload"].close()
            # Native Invoke has a separate API request ID. Correlate the
            # owned Event from the real handler effect, using its unique input.
            committed = wait_for(lambda: [row for row in rows(roots["A"] / "effects.jsonl")
                                          if row["function"] == "ungated:live" and row["event"] == payload],
                                 "actual native ungated Event effect")
            request_id = committed[0]["request_id"]
            event = {"owner": "A", "function": "ungated:live", "request_id": request_id}
            terminal(event, 1)
            observed = effects("A", request_id)
            assert len(observed) == 1 and observed[0]["event"] == payload, observed
            return observed[0]

        baseline_fast = native_fast("ordinary-before-atomic")
        atomic_payload = {"opaque": "atomic-completion-loss",
                          "nested": {"unchanged": [1, True, None]}}
        reply = control(endpoints["A"], "POST", "/completion-loss",
                        {"function_name": "ungated:live", "payload": atomic_payload}, status=202)
        assert set(reply) == {"request_id", "armed"} and reply["armed"] is True, reply
        atomic = {"owner": "A", "function": "ungated:live", "request_id": reply["request_id"]}
        terminal(atomic, 2)
        atomic_effects = effects("A", atomic["request_id"])
        assert len(atomic_effects) == 2, atomic_effects
        assert all(row["request_id"] == atomic["request_id"] and row["event"] == atomic_payload
                   and row["function_arn"] == FUNCTION_ARN + "ungated:live"
                   for row in atomic_effects), atomic_effects
        assert not any((roots["A"] / f"release-{atomic['request_id']}-{index}").exists()
                       for index in (1, 2)), "fast handler proof accidentally used a gate"
        atomic_transitions = async_records(atomic)
        assert [(row["state"], row["attempts"]) for row in atomic_transitions] == [
            ("queued", 0), ("running", 1), ("retrying", 1), ("running", 2), ("succeeded", 2)
        ], atomic_transitions
        assert atomic_transitions[2]["error_type"] == "Dev.CompletionLost"
        assert atomic_transitions[2].get("injected_completion_loss") is True
        assert all(not row.get("injected_completion_loss")
                   for index, row in enumerate(atomic_transitions) if index != 2)
        if mode == "warm":
            assert all(row["pid"] == baseline_fast["pid"] for row in atomic_effects), atomic_effects
            assert [row["worker_invocations"] for row in atomic_effects] == [2, 3], atomic_effects
        native_fast("ordinary-after-atomic")
        control(endpoints["disabled"], "POST", "/completion-loss",
                {"function_name": "ungated:live", "payload": atomic_payload}, status=404)

        diagnostic_rows = rows(roots["A"] / "private-diagnostics.jsonl")
        targeted = [row for row in diagnostic_rows if row["request_id"] == target["request_id"]]
        assert len(targeted) == 2 and [row["attempt"] for row in targeted] == [1, 2], targeted
        assert targeted[0].get("injected_completion_loss") is True, targeted
        assert not targeted[1].get("injected_completion_loss"), targeted
        all_diagnostics = diagnostic_rows + rows(roots["B"] / "private-diagnostics.jsonl")
        assert all(row["state"] == "succeeded" and row["function_error"] is False
                   and row["ownership_confirmed"] is True for row in all_diagnostics), all_diagnostics
        injected = {(row["request_id"], row["attempt"]) for row in all_diagnostics
                    if row.get("injected_completion_loss")}
        assert injected == {(target["request_id"], 1), (atomic["request_id"], 1)}, injected
        scope = "invocation" if mode == "warm" else "process"
        assert all(row["completion_scope"] == scope for row in all_diagnostics), all_diagnostics
        all_effects = rows(roots["A"] / "effects.jsonl") + rows(roots["B"] / "effects.jsonl")
        if mode == "warm":
            assert any(row["worker_invocations"] > 1 for row in all_effects), all_effects
        else:
            assert all(row["worker_invocations"] == 1 for row in all_effects), all_effects
        for owner in ("A", "B"):
            snapshot = control(endpoints[owner], "GET")["events"]
            assert all(row["state"] == "succeeded" for row in snapshot), snapshot
        print("PASS")
    finally:
        # Release fixture-owned gates even when an assertion fails. Go owns
        # joined runtime/server closure and removal of the temporary directory.
        for root in roots.values():
            (root / "cleanup").write_text("released")


if __name__ == "__main__":
    run()
