"""Native boto3 Secrets Manager contracts and registered Lambda rotation."""
import json
import os
from pathlib import Path
import time
import uuid

import boto3
from botocore.config import Config


def main():
    client = boto3.client(
        "secretsmanager", region_name="us-east-1",
        endpoint_url=os.environ["SECRETS_MANAGER_ENDPOINT"],
        aws_access_key_id="test", aws_secret_access_key="test",
        config=Config(connect_timeout=2, read_timeout=3, retries={"max_attempts": 0}),
    )
    name = "sdk-secret-" + uuid.uuid4().hex
    created = client.create_secret(Name=name, Description="metadata only")
    assert "VersionId" not in created
    described = client.describe_secret(SecretId=created["ARN"])
    assert described["VersionIdsToStages"] == {}
    assert "SecretString" not in described and "SecretBinary" not in described
    expect_error(client.exceptions.ResourceNotFoundException, lambda: client.get_secret_value(SecretId=name))
    first, second = str(uuid.uuid4()), str(uuid.uuid4())
    binary = b"\x00\xff\x80\nexact"
    client.put_secret_value(SecretId=name, ClientRequestToken=first, SecretBinary=binary)
    expect_error(client.exceptions.ResourceNotFoundException, lambda: client.get_secret_value(SecretId=name, VersionStage="AWSPREVIOUS"))
    client.put_secret_value(SecretId=name, ClientRequestToken=second, SecretString="pending", VersionStages=["AWSPENDING"])
    assert client.get_secret_value(SecretId=name)["SecretBinary"] == binary
    assert client.get_secret_value(SecretId=name, VersionStage="AWSPENDING")["VersionId"] == second
    expect_error(client.exceptions.InvalidParameterException, lambda: client.get_secret_value(SecretId=name, VersionId=first, VersionStage="AWSPENDING"))
    client.put_secret_value(SecretId=name, ClientRequestToken=second, SecretString="pending", VersionStages=["AWSCURRENT"])
    assert client.get_secret_value(SecretId=name)["VersionId"] == first, "replay must not move labels"
    expect_error(client.exceptions.ResourceExistsException, lambda: client.put_secret_value(SecretId=name, ClientRequestToken=second, SecretString="conflict"))
    client.update_secret_version_stage(SecretId=name, VersionStage="AWSCURRENT", MoveToVersionId=second, RemoveFromVersionId=first)
    assert client.get_secret_value(SecretId=name, VersionStage="AWSPREVIOUS")["SecretBinary"] == binary
    update = client.update_secret(SecretId=name, Description="updated metadata")
    assert "VersionId" not in update
    assert len(client.describe_secret(SecretId=name)["VersionIdsToStages"]) == 2
    client.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)

    rotation_name = "sdk-rotation-" + uuid.uuid4().hex
    initial = {"host": "owned-fixture", "username": "fixture", "password": "initial-password"}
    target = Path(os.environ["ROTATION_TARGET"])
    target.write_text(json.dumps(initial), encoding="utf-8")
    initial_token = str(uuid.uuid4())
    created = client.create_secret(Name=rotation_name, ClientRequestToken=initial_token, SecretString=json.dumps(initial))
    rotation_token = str(uuid.uuid4())
    result = client.rotate_secret(SecretId=created["ARN"], ClientRequestToken=rotation_token, RotationLambdaARN=os.environ["ROTATION_FUNCTION_ARN"])
    assert result["VersionId"] == rotation_token
    completed = wait_current(client, rotation_name, rotation_token)
    rotated = json.loads(completed["SecretString"])
    assert len(rotated["password"]) == 24 and rotated["password"] != initial["password"]
    assert json.loads(target.read_text(encoding="utf-8")) == rotated
    assert client.get_secret_value(SecretId=rotation_name, VersionStage="AWSPREVIOUS")["VersionId"] == initial_token
    metadata = client.describe_secret(SecretId=rotation_name)
    assert metadata["RotationEnabled"] and metadata["RotationLambdaARN"] == os.environ["ROTATION_FUNCTION_ARN"]
    wait_rotated_date(client, rotation_name)
    wait_outcome(rotation_token, "succeeded")
    events_path = Path(os.environ["ROTATION_EVENTS"])
    events = read_events(events_path)
    assert [event["Step"] for event in events] == ["createSecret", "setSecret", "testSecret", "finishSecret"]
    assert all(event["ClientRequestToken"] == rotation_token for event in events)
    assert all(event["SecretId"] == created["ARN"] for event in events)
    assert rotated["password"] not in events_path.read_text(encoding="utf-8")
    client.rotate_secret(SecretId=rotation_name, ClientRequestToken=rotation_token)
    time.sleep(0.1)
    assert len(read_events(events_path)) == 4
    # Configuration-only testing invokes just testSecret with current credentials.
    test_token = str(uuid.uuid4())
    client.rotate_secret(SecretId=rotation_name, ClientRequestToken=test_token, RotateImmediately=False)
    wait_outcome(test_token, "succeeded")
    assert [event["Step"] for event in read_events(events_path) if event["ClientRequestToken"] == test_token] == ["testSecret"]
    assert client.get_secret_value(SecretId=rotation_name)["VersionId"] == rotation_token
    expect_error(client.exceptions.ResourceNotFoundException, lambda: client.get_secret_value(SecretId=rotation_name, VersionStage="AWSPENDING"))
    assert json.loads(target.read_text(encoding="utf-8")) == rotated

    # A handler failure is accepted asynchronously and never promotes a value.
    failure = Path(os.environ["ROTATION_FAILURE"])
    failure.write_text("owned fixture failure", encoding="utf-8")
    failed_token = str(uuid.uuid4())
    client.rotate_secret(SecretId=rotation_name, ClientRequestToken=failed_token)
    wait_step(events_path, failed_token, "testSecret")
    wait_outcome(failed_token, "handler_failure")
    assert client.get_secret_value(SecretId=rotation_name)["VersionId"] == rotation_token
    pending = client.get_secret_value(SecretId=rotation_name, VersionStage="AWSPENDING")
    assert pending["VersionId"] == failed_token
    expect_error(client.exceptions.InvalidRequestException, lambda: client.rotate_secret(SecretId=rotation_name, ClientRequestToken=str(uuid.uuid4())))
    failure.unlink()
    # Same-token replay resumes the native idempotent handler, preserving value.
    client.rotate_secret(SecretId=rotation_name, ClientRequestToken=failed_token)
    retried = wait_current(client, rotation_name, failed_token)
    wait_outcome(failed_token, "succeeded")
    assert retried["SecretString"] == pending["SecretString"]
    assert client.get_secret_value(SecretId=rotation_name, VersionStage="AWSPREVIOUS")["VersionId"] == rotation_token
    client.cancel_rotate_secret(SecretId=rotation_name)
    assert client.describe_secret(SecretId=rotation_name)["RotationEnabled"] is False
    expect_error(client.exceptions.InvalidParameterException, lambda: client.rotate_secret(SecretId=rotation_name, RotationRules={"AutomaticallyAfterDays": 30}))
    client.delete_secret(SecretId=rotation_name, ForceDeleteWithoutRecovery=True)
    print("PASS", flush=True)


def expect_error(error_type, operation):
    try:
        operation()
    except error_type:
        return
    raise AssertionError("native typed error not returned")


def wait_current(client, name, token):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        current = client.get_secret_value(SecretId=name)
        if current["VersionId"] == token:
            return current
        time.sleep(0.05)
    raise AssertionError("rotation did not promote through the handler")


def wait_rotated_date(client, name):
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        if "LastRotatedDate" in client.describe_secret(SecretId=name):
            return
        time.sleep(0.05)
    raise AssertionError("rotation completion metadata absent")


def wait_outcome(token, status):
    path = Path(os.environ["ROTATION_OUTCOMES"])
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if any(item["version_id"] == token and item["status"] == status for item in read_events(path)):
            return
        time.sleep(0.05)
    raise AssertionError("rotation did not report completion")


def read_events(path):
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def wait_step(path, token, step):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if any(event["ClientRequestToken"] == token and event["Step"] == step for event in read_events(path)):
            return
        time.sleep(0.05)
    raise AssertionError("registered handler did not execute requested step")


if __name__ == "__main__":
    main()
