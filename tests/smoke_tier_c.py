#!/usr/bin/env python3
# boto3 SDK smoke for client secrets and pool/client management.
#
# Run through the isolated SDK lane documented in tests/README.md.
#
# Exit code: 0 on PASS, non-zero on FAIL (with traceback printed).
"""Smoke test: SECRET_HASH + pool/client management ops.

Asserts the Tier-C polish layer behaves correctly through real boto3:

  1. Create an isolated pool + client (with secret) via the new
     CreateUserPool / CreateUserPoolClient ops.
  2. Compute SECRET_HASH locally (same formula as the Go provider) and
     authenticate with InitiateAuth USER_PASSWORD_AUTH. Expect success.
  3. Authenticate WITHOUT SECRET_HASH against the same secret-bearing
     client. Expect NotAuthorizedException.
  4. Delete the pool. A supported pool-scoped command must reject the missing pool.

This script is the cross-language counterpart to
`cognito_tier_c_test.go` — if both pass, the dev service is
interchangeable with real Cognito for the SECRET_HASH + management-op
flows the platform might add in the future.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import sys
import traceback
from uuid import uuid4

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("COGNITO_ENDPOINT_URL", "http://localhost:4100")
EMAIL = os.environ.get("SMOKE_EMAIL", "smoke-tier-c@test.local")
PASSWORD = os.environ.get("SMOKE_PASSWORD", "TempPass1!")


def make_client():
    """Return a cognito-idp client pointing at the local eventbus shim."""
    return boto3.client(
        "cognito-idp",
        endpoint_url=ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(connect_timeout=2, read_timeout=5, retries={"total_max_attempts": 1}),
    )


def compute_secret_hash(client_secret: str, username: str, client_id: str) -> str:
    """Match the Go provider's SECRET_HASH computation
    (go/libs/platform-lib/internal/auth/providers/cognito.go:1086-1095):
    Base64(HMAC_SHA256(client_secret, username + client_id)).
    """
    digest = hmac.new(
        client_secret.encode("utf-8"),
        (username + client_id).encode("utf-8"),
        hashlib.sha256,
    ).digest()
    return base64.b64encode(digest).decode("utf-8")


def assert_pool_missing(client, pool_id: str, email: str) -> None:
    """The supported delete-user command must reject the removed pool.

    AdminGetUser is not a maintained operation of this dev service. Its generic
    InvalidAction response would say nothing about whether deletion succeeded.
    """
    try:
        client.admin_delete_user(UserPoolId=pool_id, Username=email)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        assert code == "ResourceNotFoundException", f"unexpected post-delete error: {code!r}"
    else:
        raise AssertionError("expected ResourceNotFoundException after delete")


def main() -> int:
    print(f"Smoke target: {ENDPOINT} email={EMAIL}")
    client = make_client()

    # Request a fresh owner-issued ID using the real boto3 operation model.
    pool_resp = client.create_user_pool(PoolName="smoke-tier-c-" + uuid4().hex)
    pool_id = pool_resp["UserPool"]["Id"]
    assert isinstance(pool_id, str) and pool_id, "CreateUserPool must return an ID"
    print(f"  [1/6] CreateUserPool ok ({pool_id})")

    try:
        pool_client = client.create_user_pool_client(
            UserPoolId=pool_id,
            ClientName="smoke-tier-c-client",
            GenerateSecret=True,
        )
        client_id = pool_client["UserPoolClient"]["ClientId"]
        client_secret = pool_client["UserPoolClient"]["ClientSecret"]
        assert client_id and client_secret, "CreateUserPoolClient must return credentials"
        print(f"  [2/6] CreateUserPoolClient ok (client_id={client_id})")
        # 2. AdminCreateUser inside the new pool.
        client.admin_create_user(
            UserPoolId=pool_id,
            Username=EMAIL,
            UserAttributes=[{"Name": "email", "Value": EMAIL}],
            TemporaryPassword=PASSWORD,
            MessageAction="SUPPRESS",
        )
        print(f"  [3/6] AdminCreateUser ok ({EMAIL})")

        # 3. InitiateAuth WITH the right SECRET_HASH → success.
        secret_hash = compute_secret_hash(client_secret, EMAIL, client_id)
        login = client.initiate_auth(
            ClientId=client_id,
            AuthFlow="USER_PASSWORD_AUTH",
            AuthParameters={
                "USERNAME": EMAIL,
                "PASSWORD": PASSWORD,
                "SECRET_HASH": secret_hash,
            },
        )
        assert login["AuthenticationResult"]["AccessToken"], login
        print("  [4/6] InitiateAuth with SECRET_HASH ok")

        # 4. InitiateAuth WITHOUT SECRET_HASH → NotAuthorizedException.
        try:
            client.initiate_auth(
                ClientId=client_id,
                AuthFlow="USER_PASSWORD_AUTH",
                AuthParameters={"USERNAME": EMAIL, "PASSWORD": PASSWORD},
            )
            raise AssertionError("expected NotAuthorizedException, got success")
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            assert code == "NotAuthorizedException", f"unexpected error code: {code!r}"
            print("  [5/6] InitiateAuth without SECRET_HASH correctly rejected")

    finally:
        # 5. Delete the pool. Children cascade-delete server-side.
        client.delete_user_pool(UserPoolId=pool_id)
        assert_pool_missing(client, pool_id, EMAIL)
        print("  [6/6] DeleteUserPool ok (missing pool is rejected)")

    print("PASS")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        print("FAIL")
        sys.exit(1)
