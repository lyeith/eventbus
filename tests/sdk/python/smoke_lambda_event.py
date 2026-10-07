"""Real frozen boto3 proof of native Lambda Event admission and execution."""
import concurrent.futures
import json
import os
import time
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


def run():
    client = boto3.client(
        "lambda",
        endpoint_url=os.environ["LAMBDA_ENDPOINT_URL"],
        region_name="us-east-1",
        aws_access_key_id="sdk-fake-key",
        aws_secret_access_key="sdk-fake-secret",
        config=Config(retries={"max_attempts": 0}, connect_timeout=3, read_timeout=5),
    )
    output = Path(os.environ["LAMBDA_OUTPUT_FILE"])
    result = client.invoke(
        FunctionName="sdk-event", Qualifier="live", InvocationType="Event",
        LogType="Tail", Payload=b'{ "id" : "delayed" }',
    )
    assert result["StatusCode"] == 202
    assert result["ResponseMetadata"]["HTTPStatusCode"] == 202
    assert result["Payload"].read() == b""
    assert "FunctionError" not in result and "LogResult" not in result
    assert not output.exists(), "202 awaited business completion"
    Path(os.environ["LAMBDA_GATE_FILE"]).write_text("ready")

    for function in ("sdk-event:absent", "absent"):
        try:
            client.invoke(FunctionName=function, InvocationType="Event", Payload=b"{}")
        except ClientError as error:
            assert error.response["Error"]["Code"] == "ResourceNotFoundException"
            assert error.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        else:
            raise AssertionError("unknown target accepted")
    try:
        client.invoke(FunctionName="sdk-event", InvocationType="Event", Payload=b'"' + b"x" * (1024 * 1024 - 1) + b'"')
    except ClientError as error:
        assert error.response["Error"]["Code"] == "RequestTooLargeException"
        assert error.response["ResponseMetadata"]["HTTPStatusCode"] == 413
    else:
        raise AssertionError("oversized Event accepted")

    def invoke(index):
        payload = json.dumps({"id": f"parallel-{index}", "value": index}).encode()
        response = client.invoke(FunctionName="sdk-event:live", InvocationType="Event", Payload=payload)
        assert response["StatusCode"] == 202 and response["Payload"].read() == b""

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(invoke, range(8)))
    deadline = time.monotonic() + 10
    rows = []
    while time.monotonic() < deadline:
        if output.exists():
            rows = [json.loads(line) for line in output.read_text().splitlines()]
        if len(rows) == 9:
            break
        time.sleep(0.02)
    assert len(rows) == 9, rows
    assert all(row["kind"] == "alias" for row in rows), rows
    indexed = {row["event"]["id"]: row["event"] for row in rows}
    assert len(indexed) == 9 and indexed["delayed"] == {"id": "delayed"}
    for index in range(8):
        assert indexed[f"parallel-{index}"] == {"id": f"parallel-{index}", "value": index}
    sync = client.invoke(FunctionName="sdk-event", Payload=b'{"id":"sync"}')
    assert sync["StatusCode"] == 200
    assert json.loads(sync["Payload"].read()) == {"kind": "base", "event": {"id": "sync"}}
    dry = client.invoke(FunctionName="sdk-event", InvocationType="DryRun", Payload=b"invalid JSON")
    assert dry["StatusCode"] == 204 and dry["Payload"].read() == b""
    print("PASS")


if __name__ == "__main__":
    run()
