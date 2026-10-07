"""Owned boto3 native configuration, notification, permissions and restart proof."""
from __future__ import annotations
import base64
import hashlib
import hmac
import json
import os
from pathlib import Path
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import jwt

endpoint = os.environ["COGNITO_ENDPOINT_URL"]
state_path = Path(os.environ["SMOKE_STATE_PATH"])
capture_path = Path(os.environ["COGNITO_CAPTURE_PATH"])
phase = os.environ["SMOKE_PHASE"]

def client_at(url):
    return boto3.client("cognito-idp", endpoint_url=url, region_name="eu-west-1", aws_access_key_id="test", aws_secret_access_key="test", config=Config(connect_timeout=2, read_timeout=5, retries={"total_max_attempts": 1}))
client = client_at(endpoint)

def expect_error(code, operation, **request):
    try:
        operation(**request)
    except ClientError as error:
        assert error.response["Error"]["Code"] == code, error.response
        assert error.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert error.response["ResponseMetadata"]["RequestId"]
        return
    raise AssertionError(f"expected {code}")

def captured(purpose, username):
    text = capture_path.read_text()
    assert text.endswith("\n")
    records = [json.loads(line) for line in text.splitlines()]
    matching = [record for record in records if record["purpose"] == purpose and record["username"] == username]
    assert matching
    record = matching[-1]
    assert record["schema_version"] == "eventbus.cognito.notification.v1" and record["delivery_medium"] == "EMAIL"
    return record

def secret_hash(state, username):
    return base64.b64encode(hmac.new(state["secret"].encode(), (username + state["client"]).encode(), hashlib.sha256).digest()).decode()

def public_request(state, username="sdk-user"):
    return {"ClientId": state["client"], "Username": username, "SecretHash": secret_hash(state, username)}

def authenticate(state):
    return client.initiate_auth(ClientId=state["client"], AuthFlow="USER_PASSWORD_AUTH", AuthParameters={"USERNAME": "sdk-user", "PASSWORD": "RecoveredPass2!", "SECRET_HASH": secret_hash(state, "sdk-user")})["AuthenticationResult"]

def readback_pool(pool):
    result = client.describe_user_pool(UserPoolId=pool)["UserPool"]
    return {"id": result["Id"], "name": result["Name"], "arn": result["Arn"], "created": result["CreationDate"].timestamp(), "modified": result["LastModifiedDate"].timestamp(), "schema": result["SchemaAttributes"], "auto": result["AutoVerifiedAttributes"], "recovery": result["AccountRecoverySetting"], "admin": result["AdminCreateUserConfig"]}

def readback_client(pool, client_id):
    result = client.describe_user_pool_client(UserPoolId=pool, ClientId=client_id)["UserPoolClient"]
    return {key: result[key].timestamp() if key in ("CreationDate", "LastModifiedDate") else result[key] for key in ("ClientName", "ClientId", "UserPoolId", "ClientSecret", "CreationDate", "LastModifiedDate", "ExplicitAuthFlows", "AccessTokenValidity", "IdTokenValidity", "RefreshTokenValidity", "TokenValidityUnits", "ReadAttributes", "WriteAttributes")}

def token_claims(token):
    return jwt.decode(token, options={"verify_signature": False})

def verify_tokens(state, result):
    key_client = jwt.PyJWKClient(f"{endpoint}/{state['pool']}/.well-known/jwks.json", timeout=5)
    for token_name, use, duration in (("AccessToken", "access", 300), ("IdToken", "id", 600)):
        token = result[token_name]
        key = key_client.get_signing_key_from_jwt(token)
        claims = jwt.decode(token, key.key, algorithms=["RS256"], issuer=f"{endpoint}/{state['pool']}", options={"verify_aud": False})
        assert claims["token_use"] == use and claims["exp"] - claims["iat"] == duration and claims["sub"] == state["sub"]
        if use == "access":
            assert claims["client_id"] == state["client"] and claims["scope"] == "aws.cognito.signin.user.admin"
            assert "email" not in claims and "aud" not in claims
        else:
            assert claims["aud"] == state["client"] and claims["email"] == "changed@example.test" and claims["custom:tenant"] == "tenant-a"
    assert result["ExpiresIn"] == 300

if phase == "create":
    pool = client.create_user_pool(PoolName="native-sdk-pool", Schema=[{"Name": "email", "Required": True, "Mutable": True}, {"Name": "tenant", "AttributeDataType": "String", "Mutable": True, "StringAttributeConstraints": {"MinLength": "1", "MaxLength": "20"}}], AutoVerifiedAttributes=["email"], AccountRecoverySetting={"RecoveryMechanisms": [{"Name": "verified_email", "Priority": 1}]}, AdminCreateUserConfig={"AllowAdminCreateUserOnly": False})["UserPool"]
    pool_id = pool["Id"]
    assert pool_id.startswith("eu-west-1_") and pool["Arn"] == f"arn:aws:cognito-idp:eu-west-1:123456789012:userpool/{pool_id}"
    app = client.create_user_pool_client(UserPoolId=pool_id, ClientName="native-sdk-client", GenerateSecret=True, ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH", "ALLOW_USER_SRP_AUTH"], AccessTokenValidity=5, IdTokenValidity=600, RefreshTokenValidity=1, TokenValidityUnits={"AccessToken": "minutes", "IdToken": "seconds", "RefreshToken": "hours"}, ReadAttributes=["email", "email_verified", "custom:tenant"], WriteAttributes=["email", "custom:tenant"])["UserPoolClient"]
    state = {"pool": pool_id, "client": app["ClientId"], "secret": app["ClientSecret"]}
    default_app = client.create_user_pool_client(UserPoolId=pool_id, ClientName="native-defaults")["UserPoolClient"]
    assert (default_app["AccessTokenValidity"], default_app["IdTokenValidity"], default_app["RefreshTokenValidity"]) == (1, 1, 30)
    assert default_app["TokenValidityUnits"] == {"AccessToken": "hours", "IdToken": "hours", "RefreshToken": "days"}
    other = client.create_user_pool(PoolName="other-native-pool")["UserPool"]["Id"]
    state["other"] = other
    expect_error("ResourceNotFoundException", client.describe_user_pool_client, UserPoolId=other, ClientId=state["client"])
    expect_error("ResourceNotFoundException", client.delete_user_pool_client, UserPoolId=other, ClientId=state["client"])
    expect_error("ResourceNotFoundException", client_at(os.environ["COGNITO_ISOLATED_ENDPOINT"]).describe_user_pool, UserPoolId=pool_id)
    expect_error("InvalidParameterException", client.create_user_pool_client, UserPoolId=pool_id, ClientName="unsupported", EnableTokenRevocation=False)
    expect_error("InvalidParameterException", client.create_user_pool, PoolName="unsupported", MfaConfiguration="ON")
    signup = client.sign_up(**public_request(state), Password="InitialPass1!", UserAttributes=[{"Name": "email", "Value": "initial@example.test"}, {"Name": "custom:tenant", "Value": "tenant-a"}])
    state["sub"] = signup["UserSub"]
    assert signup["UserConfirmed"] is False
    note = captured("signup", "sdk-user")
    assert note["destination"] == "initial@example.test" and note["user_sub"] == state["sub"]
    expect_error("CodeMismatchException", client.confirm_sign_up, **public_request(state), ConfirmationCode="000000" if note["code"] != "000000" else "000001")
    client.confirm_sign_up(**public_request(state), ConfirmationCode=note["code"])
    result = client.initiate_auth(ClientId=state["client"], AuthFlow="USER_PASSWORD_AUTH", AuthParameters={"USERNAME": "sdk-user", "PASSWORD": "InitialPass1!", "SECRET_HASH": secret_hash(state, "sdk-user")})["AuthenticationResult"]
    expect_error("NotAuthorizedException", client.update_user_attributes, AccessToken=result["AccessToken"], UserAttributes=[{"Name": "name", "Value": "not-permitted"}])
    client.update_user_attributes(AccessToken=result["AccessToken"], UserAttributes=[{"Name": "email", "Value": "changed@example.test"}])
    note = captured("attribute:email", "sdk-user")
    client.verify_user_attribute(AccessToken=result["AccessToken"], AttributeName="email", Code=note["code"])
    client.forgot_password(**public_request(state))
    recovery = captured("recovery", "sdk-user")
    client.confirm_forgot_password(**public_request(state), ConfirmationCode=recovery["code"], Password="RecoveredPass2!")
    expect_error("NotAuthorizedException", client.initiate_auth, ClientId=state["client"], AuthFlow="REFRESH_TOKEN_AUTH", AuthParameters={"REFRESH_TOKEN": result["RefreshToken"], "SECRET_HASH": secret_hash(state, "sdk-user")})
    result = authenticate(state)
    verify_tokens(state, result)
    state["refresh"], state["access"] = result["RefreshToken"], result["AccessToken"]
    state["refresh_exp"] = token_claims(state["refresh"])["exp"]
    assert state["refresh_exp"] - token_claims(state["refresh"])["iat"] == 3600
    client.get_user_attribute_verification_code(AccessToken=state["access"], AttributeName="email")
    state["pending_code"] = captured("attribute:email", "sdk-user")["code"]
    client.admin_create_user(UserPoolId=pool_id, Username="invited", TemporaryPassword="InvitationPass1!", UserAttributes=[{"Name": "email", "Value": "invite@example.test"}], DesiredDeliveryMediums=["EMAIL"])
    assert captured("invitation", "invited")["temporary_password"] == "InvitationPass1!"
    state["pool_metadata"], state["client_metadata"] = readback_pool(pool_id), readback_client(pool_id, state["client"])
    state_path.write_text(json.dumps(state))
elif phase == "restart":
    state = json.loads(state_path.read_text())
    assert readback_pool(state["pool"]) == state["pool_metadata"] and readback_client(state["pool"], state["client"]) == state["client_metadata"]
    refreshed = client.initiate_auth(ClientId=state["client"], AuthFlow="REFRESH_TOKEN_AUTH", AuthParameters={"REFRESH_TOKEN": state["refresh"], "SECRET_HASH": secret_hash(state, "sdk-user")})["AuthenticationResult"]
    assert "RefreshToken" not in refreshed
    verify_tokens(state, refreshed)
    attrs = {value["Name"]: value["Value"] for value in client.get_user(AccessToken=refreshed["AccessToken"])["UserAttributes"]}
    assert attrs["email"] == "changed@example.test" and attrs["custom:tenant"] == "tenant-a" and "name" not in attrs
    client.verify_user_attribute(AccessToken=refreshed["AccessToken"], AttributeName="email", Code=state["pending_code"])
    expect_error("ExpiredCodeException", client.verify_user_attribute, AccessToken=refreshed["AccessToken"], AttributeName="email", Code=state["pending_code"])
elif phase == "expiry-cleanup":
    state = json.loads(state_path.read_text())
    expect_error("NotAuthorizedException", client.initiate_auth, ClientId=state["client"], AuthFlow="REFRESH_TOKEN_AUTH", AuthParameters={"REFRESH_TOKEN": state["refresh"], "SECRET_HASH": secret_hash(state, "sdk-user")})
    expect_error("NotAuthorizedException", client.get_user, AccessToken=state["access"])
    client.delete_user_pool(UserPoolId=state["pool"])
    client.delete_user_pool(UserPoolId=state["other"])
    expect_error("ResourceNotFoundException", client.describe_user_pool, UserPoolId=state["pool"])
    expect_error("ResourceNotFoundException", client.describe_user_pool_client, UserPoolId=state["pool"], ClientId=state["client"])
else:
    raise AssertionError("unknown owned fixture phase")
print("PASS")
