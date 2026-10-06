#!/usr/bin/env python3
# boto3 SDK smoke for the supported Cognito admin operations.
#
# Run through the isolated SDK lane documented in tests/sdk/README.md.
#
# Exit code: 0 on PASS, non-zero on FAIL (with traceback printed).
"""Smoke test: AdminCreateUser / AdminDeleteUser via real boto3 client.

Asserts the dev service's wire shape is interchangeable with real Cognito
for the three admin/user-introspection operations shipped in GO-COGNITO-2.
GetUser is exercised in unit tests; this script focuses on the boto3-driven
admin path because that's what `bootstrap_platform.py` calls in production.
"""

from __future__ import annotations

import os
import sys
import traceback

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("COGNITO_ENDPOINT_URL", "http://localhost:4100")
POOL_ID = os.environ.get("COGNITO_USER_POOL_ID", "local-pool-1")
EMAIL = os.environ.get("SMOKE_EMAIL", "alice@test.local")


def make_client():
    """Return a cognito-idp client pointing at the local eventbus shim."""
    # Fake credentials are required by botocore even though the eventbus
    # ignores them — same convention as the rest of the LocalStack-shaped
    # tooling in the project.
    return boto3.client(
        "cognito-idp",
        endpoint_url=ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(connect_timeout=2, read_timeout=5, retries={"total_max_attempts": 1}),
    )


def assert_username_exists(fn) -> None:
    """Run `fn` and assert it raises UsernameExistsException."""
    try:
        fn()
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code != "UsernameExistsException":
            raise AssertionError(f"expected UsernameExistsException, got {code}: {e}") from e
        return
    raise AssertionError("expected UsernameExistsException, got no error")


def main() -> int:
    print(f"Smoke target: {ENDPOINT} pool={POOL_ID} email={EMAIL}")
    client = make_client()

    # 1. Create user — should succeed with sub in attributes.
    resp = client.admin_create_user(
        UserPoolId=POOL_ID,
        Username=EMAIL,
        UserAttributes=[{"Name": "email", "Value": EMAIL}],
        TemporaryPassword="TempPass1!",
        MessageAction="SUPPRESS",
    )
    user = resp.get("User", {})
    assert user.get("UserStatus") == "FORCE_CHANGE_PASSWORD", user
    attrs = {a["Name"]: a["Value"] for a in user.get("Attributes", [])}
    assert "sub" in attrs, f"missing sub in response attributes: {attrs}"
    assert "email_verified" not in attrs, f"AdminCreateUser must not invent verification: {attrs}"
    print(f"  [1/5] AdminCreateUser ok (sub={attrs['sub']})")

    # 2. Create same user again — must raise UsernameExistsException.
    assert_username_exists(
        lambda: client.admin_create_user(
            UserPoolId=POOL_ID,
            Username=EMAIL,
            UserAttributes=[{"Name": "email", "Value": EMAIL}],
            TemporaryPassword="TempPass1!",
            MessageAction="SUPPRESS",
        )
    )
    print("  [2/5] AdminCreateUser collision raises UsernameExistsException")

    # 3. Delete user — must succeed.
    client.admin_delete_user(UserPoolId=POOL_ID, Username=EMAIL)
    print("  [3/5] AdminDeleteUser ok")

    # 4. Delete again — must succeed (idempotent).
    client.admin_delete_user(UserPoolId=POOL_ID, Username=EMAIL)
    print("  [4/5] AdminDeleteUser idempotent on missing user")

    # 5. Pool-not-found — must raise ResourceNotFoundException.
    try:
        client.admin_delete_user(UserPoolId="no-such-pool", Username=EMAIL)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        assert code == "ResourceNotFoundException", f"expected ResourceNotFoundException, got {code}"
        print("  [5/5] missing pool raises ResourceNotFoundException")
    else:
        raise AssertionError("expected ResourceNotFoundException for missing pool")

    print("PASS")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        print("FAIL")
        sys.exit(1)
