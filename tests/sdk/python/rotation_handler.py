"""Application rotation policy, using boto3 and an owned credential target.

The emulator sequences native events; this fixture owns credential generation,
setting/testing the target and promotion. No secret is written to evidence.
"""
import json
import os
from pathlib import Path

import boto3
from botocore.config import Config


def handler(event, context):
    assert set(event) == {"SecretId", "ClientRequestToken", "Step"}
    client = boto3.client(
        "secretsmanager", region_name="us-east-1",
        endpoint_url=os.environ["SECRETS_MANAGER_ENDPOINT"],
        aws_access_key_id="test", aws_secret_access_key="test",
        config=Config(connect_timeout=2, read_timeout=3, retries={"max_attempts": 0}),
    )
    secret, token, step = event["SecretId"], event["ClientRequestToken"], event["Step"]
    with open(os.environ["ROTATION_EVENTS"], "a", encoding="utf-8") as evidence:
        evidence.write(json.dumps({"SecretId": secret, "ClientRequestToken": token, "Step": step}) + "\n")
    metadata = client.describe_secret(SecretId=secret)
    assert metadata["RotationEnabled"]
    stages = metadata["VersionIdsToStages"][token]
    if "AWSCURRENT" in stages:
        return
    assert "AWSPENDING" in stages
    target_path = Path(os.environ["ROTATION_TARGET"])
    if step == "createSecret":
        try:
            client.get_secret_value(SecretId=secret, VersionId=token, VersionStage="AWSPENDING")
            return
        except client.exceptions.ResourceNotFoundException:
            current = json.loads(client.get_secret_value(SecretId=secret)["SecretString"])
            current["password"] = client.get_random_password(
                PasswordLength=24, ExcludePunctuation=True, ExcludeCharacters="0Oo1Il",
            )["RandomPassword"]
            client.put_secret_value(
                SecretId=secret, ClientRequestToken=token,
                SecretString=json.dumps(current), VersionStages=["AWSPENDING"],
            )
    elif step in ("setSecret", "testSecret"):
        pending = json.loads(client.get_secret_value(
            SecretId=secret, VersionId=token, VersionStage="AWSPENDING",
        )["SecretString"])
        current = json.loads(client.get_secret_value(SecretId=secret)["SecretString"])
        assert (pending["host"], pending["username"]) == (current["host"], current["username"])
        if step == "setSecret":
            target_path.write_text(json.dumps(pending), encoding="utf-8")
        else:
            if Path(os.environ["ROTATION_FAILURE"]).exists():
                raise ValueError("private-handler-error-with-secret-material")
            assert json.loads(target_path.read_text(encoding="utf-8")) == pending
    elif step == "finishSecret":
        current = next(version for version, labels in metadata["VersionIdsToStages"].items() if "AWSCURRENT" in labels)
        client.update_secret_version_stage(
            SecretId=secret, VersionStage="AWSCURRENT", MoveToVersionId=token,
            RemoveFromVersionId=current,
        )
    else:
        raise ValueError("invalid rotation step")
