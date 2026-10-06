#!/usr/bin/env python3
# boto3 and PyJWT SDK smoke for password and refresh authentication.
#
# Run through the isolated SDK lane documented in tests/sdk/README.md.
#
# Exit code: 0 on PASS, non-zero on FAIL (with traceback printed).
"""Smoke test: InitiateAuth USER_PASSWORD_AUTH + REFRESH_TOKEN_AUTH.

Asserts the dev service's InitiateAuth wire shape AND the JWT it emits is
interchangeable with real Cognito for both auth flows the platform uses:

  - USER_PASSWORD_AUTH mints access + refresh tokens; access token validates
    against the published JWKS using PyJWT (the same library
    `infrastructure/authorizer/lambda_authorizer.py` uses).
  - REFRESH_TOKEN_AUTH mints a fresh access token whose `exp` is later than
    the original; the response MUST NOT include a RefreshToken (design §3f).

This script is the cross-language counterpart to
`cognito_initiate_auth_test.go::TestInitiateAuth_UserPasswordAuth_AccessTokenValidatesAgainstJWKS`
which uses Go + keyfunc. If both pass, the dev service is interchangeable
with real Cognito for the language pairs the platform actually runs.
"""

from __future__ import annotations

import os
import sys
import traceback

import boto3
from botocore.config import Config
import jwt as pyjwt
from jwt import PyJWKClient

ENDPOINT = os.environ.get("COGNITO_ENDPOINT_URL", "http://localhost:4100")
ISSUER_BASE = os.environ.get("COGNITO_ISSUER_BASE", ENDPOINT)
POOL_ID = os.environ.get("COGNITO_USER_POOL_ID", "local-pool-1")
CLIENT_ID = os.environ.get("COGNITO_CLIENT_ID", "local-client-1")
EMAIL = os.environ.get("SMOKE_EMAIL", "smoke-initiate@test.local")
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


def validate_access_token(access_token: str) -> dict:
    """Validate the access token against the dev service's published JWKS.

    Mirrors what `infrastructure/authorizer/lambda_authorizer.py:23-37` does
    in production: PyJWKClient → jwt.decode with issuer + audience.
    """
    jwks_url = f"{ISSUER_BASE}/{POOL_ID}/.well-known/jwks.json"
    jwk_client = PyJWKClient(jwks_url, timeout=5)
    signing_key = jwk_client.get_signing_key_from_jwt(access_token)
    return pyjwt.decode(
        access_token,
        signing_key.key,
        algorithms=["RS256"],
        audience=CLIENT_ID,
        issuer=f"{ISSUER_BASE}/{POOL_ID}",
    )


def main() -> int:
    print(f"Smoke target: {ENDPOINT} pool={POOL_ID} client={CLIENT_ID} email={EMAIL}")
    client = make_client()

    # Own only a successfully created fixture. A pre-existing user is not ours
    # to authenticate, overwrite or delete after a collided create attempt.
    client.admin_create_user(
        UserPoolId=POOL_ID,
        Username=EMAIL,
        UserAttributes=[{"Name": "email", "Value": EMAIL}],
        TemporaryPassword=PASSWORD,
        MessageAction="SUPPRESS",
    )
    print(f"  [1/6] AdminCreateUser ok ({EMAIL})")

    try:
        client.admin_set_user_password(
            UserPoolId=POOL_ID, Username=EMAIL, Password=PASSWORD, Permanent=True,
        )
        # 2. USER_PASSWORD_AUTH — mint access + refresh tokens.
        login = client.initiate_auth(
            ClientId=CLIENT_ID,
            AuthFlow="USER_PASSWORD_AUTH",
            AuthParameters={"USERNAME": EMAIL, "PASSWORD": PASSWORD},
        )
        result = login.get("AuthenticationResult", {})
        access = result.get("AccessToken")
        refresh = result.get("RefreshToken")
        assert access, f"AccessToken missing from response: {login!r}"
        assert refresh, f"RefreshToken missing from response: {login!r}"
        assert result.get("TokenType") == "Bearer", f"TokenType: {result!r}"
        assert isinstance(result.get("ExpiresIn"), int), f"ExpiresIn type: {result!r}"
        print(f"  [2/6] USER_PASSWORD_AUTH ok (ExpiresIn={result['ExpiresIn']}s)")

        # 3. Validate the access token via PyJWT + JWKS — same code path the
        #    Lambda authorizer uses. Failure here means the dev service is
        #    NOT interchangeable with real Cognito for our PyJWT consumers.
        claims = validate_access_token(access)
        assert claims["aud"] == CLIENT_ID, f"aud mismatch: {claims!r}"
        assert claims["iss"] == f"{ISSUER_BASE}/{POOL_ID}", f"iss mismatch: {claims!r}"
        assert claims["token_use"] == "access", f"token_use: {claims!r}"
        assert claims.get("sub"), f"sub missing: {claims!r}"
        assert claims.get("email") == EMAIL, f"email mismatch: {claims!r}"
        original_exp = int(claims["exp"])
        print(f"  [3/6] PyJWT validate (sub={claims['sub']}, exp={original_exp})")

        # 4. REFRESH_TOKEN_AUTH — mint a fresh access token. exp must be later.
        # Sleep just past the second-resolution boundary so iat advances.
        import time as _t

        _t.sleep(1.1)

        refreshed = client.initiate_auth(
            ClientId=CLIENT_ID,
            AuthFlow="REFRESH_TOKEN_AUTH",
            AuthParameters={"REFRESH_TOKEN": refresh},
        )
        ref_result = refreshed.get("AuthenticationResult", {})
        new_access = ref_result.get("AccessToken")
        assert new_access, f"new AccessToken missing: {refreshed!r}"
        assert new_access != access, "new access token must differ from original"

        # Design §3f — REFRESH_TOKEN_AUTH MUST NOT return a new RefreshToken.
        # boto3 surfaces the raw response shape; we check the underlying
        # field is missing (or the boto3 SDK turns missing → empty string).
        assert not ref_result.get("RefreshToken"), (
            f"REFRESH_TOKEN_AUTH must NOT carry a new RefreshToken (design §3f); got: {ref_result!r}"
        )
        print(f"  [4/6] REFRESH_TOKEN_AUTH ok (no rotation, ExpiresIn={ref_result['ExpiresIn']}s)")

        # 5. Validate the refreshed access token. exp must be > original_exp.
        new_claims = validate_access_token(new_access)
        new_exp = int(new_claims["exp"])
        assert new_exp > original_exp, f"refreshed access exp must be later: original={original_exp}, new={new_exp}"
        assert new_claims["sub"] == claims["sub"], f"sub must round-trip: {new_claims['sub']!r} vs {claims['sub']!r}"
        print(f"  [5/6] PyJWT validate refreshed (exp diff: +{new_exp - original_exp}s)")

    finally:
        # 6. Always clean up the test user. Idempotent on missing.
        client.admin_delete_user(UserPoolId=POOL_ID, Username=EMAIL)
        print("  [6/6] AdminDeleteUser cleanup")

    print("PASS")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        print("FAIL")
        sys.exit(1)
