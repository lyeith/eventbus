"""Real LocalStack S3 events, unchanged native Lambda Invoke admission/execution.

The Go owner provisions private endpoints and container lifetime. This client
never fabricates Invoke events or skips S3 destination validation.
"""
import json
import os
from pathlib import Path
import time
from urllib.parse import quote, unquote
import uuid


def rows():
    path = Path(os.environ["S3_LAMBDA_OUTPUT"])
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def handler(event, context):
    assert len(event["Records"]) == 1
    record = event["Records"][0]
    assert record["eventSource"] == "aws:s3"
    key = unquote(record["s3"]["object"]["key"])
    request_id = context.aws_request_id
    attempts = sum(row.get("phase") == "started" and row.get("request_id") == request_id for row in rows()) + 1

    def capture(phase):
        with open(os.environ["S3_LAMBDA_OUTPUT"], "a", encoding="utf-8") as output:
            output.write(json.dumps({"phase": phase, "request_id": request_id, "pid": os.getpid(),
                                     "attempt": attempts, "key": key, "event": event}, ensure_ascii=False) + "\n")
    capture("started")
    if key.endswith("retry.csv") and attempts < 3:
        capture("failed")
        raise RuntimeError("owned S3 fixture transient function failure")
    if key.endswith("shutdown.csv"):
        gate = Path(os.environ["S3_LAMBDA_GATE"])
        while not gate.exists():
            time.sleep(0.01)
    capture("completed")
    return {"accepted": True}


def wait_for(check, label, seconds=20):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = check()
        if result:
            return result
        time.sleep(0.02)
    raise AssertionError(label)


def run():
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError

    configuration = Config(retries={"max_attempts": 2}, connect_timeout=2, read_timeout=10,
                           s3={"addressing_style": "path"})
    options = dict(region_name="us-east-1", aws_access_key_id="000000000000",
                   aws_secret_access_key="owned-fixture", config=configuration)
    s3 = boto3.client("s3", endpoint_url=os.environ["S3_LAMBDA_S3_ENDPOINT"], **options)
    functions = boto3.client("lambda", endpoint_url=os.environ["S3_LAMBDA_ENDPOINT"], **options)
    arn = "arn:aws:lambda:us-east-1:000000000000:function:s3-handler:live"
    for name, qualifier in [("s3-handler", {}), ("s3-handler", {"Qualifier": "live"}), (arn, {})]:
        result = functions.get_function(FunctionName=name, **qualifier)
        metadata = result["Configuration"]
        assert metadata["FunctionName"] == "s3-handler"
        assert metadata["FunctionArn"].endswith(":live") == (name == arn or bool(qualifier))
        assert metadata["Runtime"] == "python"
        assert metadata["Handler"] == "s3_lambda_smoke.handler"
        assert metadata["Timeout"] == 20
        assert "Code" not in result and "Environment" not in metadata
        assert os.environ["S3_LAMBDA_OUTPUT"] not in json.dumps(result)
    assert rows() == [], "GetFunction started a handler"
    for name, qualifier in [("missing", {}), ("s3-handler", {"Qualifier": "missing"})]:
        try:
            functions.get_function(FunctionName=name, **qualifier)
        except ClientError as error:
            assert error.response["Error"]["Code"] == "ResourceNotFoundException"
            assert error.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        else:
            raise AssertionError("missing target metadata accepted")

    bucket = "eventbus-s3-" + uuid.uuid4().hex
    s3.create_bucket(Bucket=bucket)
    try:
        s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
        notification = {"LambdaFunctionConfigurations": [{
            "Id": "owned-native-lambda", "LambdaFunctionArn": arn, "Events": ["s3:ObjectCreated:*"],
            "Filter": {"Key": {"FilterRules": [{"Name": "prefix", "Value": "accepted/"},
                                               {"Name": "suffix", "Value": ".csv"}]}}
        }]}
        invalid = {"LambdaFunctionConfigurations": [{
            "LambdaFunctionArn": arn.replace("s3-handler:live", "missing"), "Events": ["s3:ObjectCreated:*"]
        }]}
        try:
            s3.put_bucket_notification_configuration(Bucket=bucket, NotificationConfiguration=invalid)
        except ClientError as error:
            assert error.response["Error"]["Code"] == "InvalidArgument"
        else:
            raise AssertionError("normal S3 validation did not refuse an unknown Lambda")
        # SkipDestinationValidation is deliberately absent.
        s3.put_bucket_notification_configuration(Bucket=bucket, NotificationConfiguration=notification)
        configured = s3.get_bucket_notification_configuration(Bucket=bucket)["LambdaFunctionConfigurations"]
        assert configured[0]["LambdaFunctionArn"] == arn
        assert rows() == [], "metadata/DryRun validation executed a function"

        def delivered(key, body, version, event_name, attempts=1):
            completed = wait_for(lambda: [row for row in rows() if row["key"] == key and row["phase"] == "completed"],
                                 "actual S3 handler completion for " + key)
            assert len(completed) == 1
            record = completed[0]["event"]["Records"][0]
            assert record["eventSource"] == "aws:s3" and record["eventName"] == event_name
            assert record["awsRegion"] == "us-east-1"
            assert record["s3"]["bucket"]["name"] == bucket
            assert record["s3"]["bucket"]["arn"] == "arn:aws:s3:::" + bucket
            native_object = record["s3"]["object"]
            assert native_object["key"] == quote(key)
            assert native_object["size"] == len(body)
            assert native_object["versionId"] == version
            assert native_object["eTag"] and native_object["sequencer"]
            started = [row for row in rows() if row["key"] == key and row["phase"] == "started"]
            assert len(started) == attempts
            assert len({row["request_id"] for row in started}) == 1
            assert all(row["event"] == completed[0]["event"] for row in started)
            return completed[0]

        key, body = "accepted/report +% é.csv", b"original uploaded object"
        uploaded = s3.put_object(Bucket=bucket, Key=key, Body=body)
        delivered(key, body, uploaded["VersionId"], "ObjectCreated:Put")
        copied_key = "accepted/copied.csv"
        copied = s3.copy_object(Bucket=bucket, Key=copied_key, CopySource={"Bucket": bucket, "Key": key})
        delivered(copied_key, body, copied["VersionId"], "ObjectCreated:Copy")

        multipart_key = "accepted/multipart.csv"
        multipart = s3.create_multipart_upload(Bucket=bucket, Key=multipart_key, ChecksumAlgorithm="CRC32")["UploadId"]
        parts = [b"a" * (5 << 20), b"tail"]
        receipts = []
        for number, data in enumerate(parts, 1):
            uploaded_part = s3.upload_part(Bucket=bucket, Key=multipart_key, UploadId=multipart,
                                          PartNumber=number, Body=data, ChecksumAlgorithm="CRC32")
            receipts.append({"PartNumber": number, "ETag": uploaded_part["ETag"],
                             "ChecksumCRC32": uploaded_part["ChecksumCRC32"]})
        assert not [row for row in rows() if row["key"] == multipart_key], "multipart emitted before completion"
        completed = s3.complete_multipart_upload(Bucket=bucket, Key=multipart_key, UploadId=multipart,
                                               MultipartUpload={"Parts": receipts})
        delivered(multipart_key, b"".join(parts), completed["VersionId"], "ObjectCreated:CompleteMultipartUpload")

        retry_key, retry_body = "accepted/retry.csv", b"native asynchronous retry"
        retry = s3.put_object(Bucket=bucket, Key=retry_key, Body=retry_body)
        delivered(retry_key, retry_body, retry["VersionId"], "ObjectCreated:Put", attempts=3)
        failed = [row for row in rows() if row["key"] == retry_key and row["phase"] == "failed"]
        assert len(failed) == 2

        before = len(rows())
        s3.put_object(Bucket=bucket, Key="other/no-prefix.csv", Body=b"filtered")
        s3.put_object(Bucket=bucket, Key="accepted/no-suffix.txt", Body=b"filtered")
        s3.delete_object(Bucket=bucket, Key=key)  # ObjectRemoved is not selected.
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            assert len(rows()) == before, "unselected prefix/suffix/event was delivered"
            time.sleep(0.02)

        shutdown_key = "accepted/shutdown.csv"
        s3.put_object(Bucket=bucket, Key=shutdown_key, Body=b"joined shutdown")
        wait_for(lambda: [row for row in rows() if row["key"] == shutdown_key and row["phase"] == "started"],
                 "actual S3 shutdown invocation admission")
        assert not [row for row in rows() if row["key"] == shutdown_key and row["phase"] == "completed"]
        print("Validated GetFunction/DryRun; actual Put/Copy/multipart, encoded key/version/size, filters and two native retries; held actual final upload for Go-owned drain", flush=True)
    finally:
        # Disable the exact owned producer before removing its versioned data.
        s3.put_bucket_notification_configuration(Bucket=bucket, NotificationConfiguration={})
        for page in s3.get_paginator("list_multipart_uploads").paginate(Bucket=bucket):
            for upload in page.get("Uploads", []):
                s3.abort_multipart_upload(Bucket=bucket, Key=upload["Key"], UploadId=upload["UploadId"])
        for page in s3.get_paginator("list_object_versions").paginate(Bucket=bucket):
            objects = [{"Key": item["Key"], "VersionId": item["VersionId"]}
                       for item in page.get("Versions", []) + page.get("DeleteMarkers", [])]
            if objects:
                deleted = s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})
                assert not deleted.get("Errors"), deleted
        s3.delete_bucket(Bucket=bucket)
        try:
            s3.head_bucket(Bucket=bucket)
        except ClientError as error:
            assert error.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        else:
            raise AssertionError("owned bucket was not removed")
        print("Exact owned bucket, all versions/delete markers/multipart uploads and notifications removed", flush=True)
    print("PASS", flush=True)


if __name__ == "__main__":
    run()
