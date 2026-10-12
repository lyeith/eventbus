"""Pinned boto3 native pool/client inventory, persistence and isolation proof."""
from __future__ import annotations

import json
import os
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, ParamValidationError


def at(endpoint):
    return boto3.client(
        "cognito-idp", endpoint_url=endpoint, region_name="eu-west-1",
        aws_access_key_id="test", aws_secret_access_key="test",
        config=Config(connect_timeout=2, read_timeout=5, retries={"total_max_attempts": 1}),
    )


first = at(os.environ["COGNITO_ENDPOINT_URL"])
second = at(os.environ["COGNITO_SECOND_ENDPOINT_URL"])
state_path = Path(os.environ["SMOKE_STATE_PATH"])
phase = os.environ["SMOKE_PHASE"]


def pool_summary(pool):
    return {
        "Id": pool["Id"], "Name": pool["Name"],
        "CreationDate": pool["CreationDate"].isoformat(),
        "LastModifiedDate": pool["LastModifiedDate"].isoformat(),
        "Status": "Enabled", "LambdaConfig": {},
    }


def normalized_pool(pool):
    assert set(pool) == {"Id", "Name", "CreationDate", "LastModifiedDate", "Status", "LambdaConfig"}, pool
    assert pool["Status"] == "Enabled" and pool["LambdaConfig"] == {}, pool
    return pool_summary(pool)


def client_summary(client):
    return {key: client[key] for key in ("ClientId", "ClientName", "UserPoolId")}


def normalized_client(client):
    assert set(client) == {"ClientId", "ClientName", "UserPoolId"}, client
    assert "ClientSecret" not in client, client
    return client_summary(client)


def sorted_pools(pools):
    return sorted(pools, key=lambda pool: pool["Id"])


def sorted_clients(clients):
    return sorted(clients, key=lambda client: client["ClientId"])


def expect_error(code, operation, **request):
    try:
        operation(**request)
    except ClientError as error:
        assert error.response["Error"]["Code"] == code, error.response
        metadata = error.response["ResponseMetadata"]
        assert metadata["HTTPStatusCode"] == 400 and metadata["RequestId"], metadata
        return
    raise AssertionError(f"expected {code}")


def expect_invalid(operation, **request):
    # boto3 validates required fields/ranges locally before the request. Other
    # invalid inputs reach the emulator and retain the native error envelope.
    try:
        operation(**request)
    except ParamValidationError:
        return
    except ClientError as error:
        assert error.response["Error"]["Code"] == "InvalidParameterException", error.response
        metadata = error.response["ResponseMetadata"]
        assert metadata["HTTPStatusCode"] == 400 and metadata["RequestId"], metadata
        return
    raise AssertionError("expected invalid parameter rejection")


def all_pools(client):
    pools = []
    for count, page in enumerate(client.get_paginator("list_user_pools").paginate(
        MaxResults=1, PaginationConfig={"PageSize": 1}
    )):
        assert count < 20, "pool paginator must terminate"
        assert isinstance(page["UserPools"], list) and len(page["UserPools"]) <= 1
        pools.extend(normalized_pool(pool) for pool in page["UserPools"])
    return pools


def all_clients(client, pool_id):
    clients = []
    for count, page in enumerate(client.get_paginator("list_user_pool_clients").paginate(
        UserPoolId=pool_id, PaginationConfig={"PageSize": 1}
    )):
        assert count < 20, "client paginator must terminate"
        assert isinstance(page["UserPoolClients"], list) and len(page["UserPoolClients"]) <= 1
        clients.extend(normalized_client(app) for app in page["UserPoolClients"])
    return clients


def create_pool(client, name):
    pool = client.create_user_pool(PoolName=name)["UserPool"]
    assert pool["Id"].startswith("eu-west-1_"), pool
    assert pool["Arn"] == f"arn:aws:cognito-idp:eu-west-1:123456789012:userpool/{pool['Id']}", pool
    return pool_summary(pool)


def create_client(client, pool_id, name):
    app = client.create_user_pool_client(
        UserPoolId=pool_id, ClientName=name, GenerateSecret=True
    )["UserPoolClient"]
    assert app["ClientSecret"], app
    return client_summary(app)


def exercise():
    for client in (first, second):
        empty = client.list_user_pools(MaxResults=60)
        assert empty["UserPools"] == [] and "NextToken" not in empty, empty
    pools = [create_pool(first, name) for name in ("same name", "same name", "empty pool")]
    foreign_pools = [create_pool(second, name) for name in ("same name", "independent empty pool")]
    owner_id, other_id = pools[0]["Id"], pools[1]["Id"]
    clients = [create_client(first, owner_id, name) for name in ("same client", "same client", "third client")]
    other_clients = [create_client(first, other_id, "foreign to owner")]
    independent_clients = [create_client(second, foreign_pools[0]["Id"], "independent client")]
    assert all_pools(first) == sorted_pools(pools)
    assert all_pools(second) == sorted_pools(foreign_pools)
    assert all_clients(first, owner_id) == sorted_clients(clients)
    assert all_clients(first, other_id) == other_clients
    assert all_clients(second, foreign_pools[0]["Id"]) == independent_clients
    empty = first.list_user_pool_clients(UserPoolId=pools[2]["Id"])
    assert empty["UserPoolClients"] == [] and "NextToken" not in empty, empty
    default = first.list_user_pool_clients(UserPoolId=owner_id)
    assert [normalized_client(app) for app in default["UserPoolClients"]] == sorted_clients(clients)
    assert "NextToken" not in default
    pool_page = first.list_user_pools(MaxResults=1)
    client_page = first.list_user_pool_clients(UserPoolId=owner_id, MaxResults=1)
    assert pool_page["NextToken"] and client_page["NextToken"]
    for maximum in (0, 61):
        expect_invalid(first.list_user_pools, MaxResults=maximum)
        expect_invalid(first.list_user_pool_clients, UserPoolId=owner_id, MaxResults=maximum)
    expect_invalid(first.list_user_pools)
    expect_invalid(first.list_user_pool_clients)
    expect_invalid(first.list_user_pool_clients, UserPoolId="bad-id")
    expect_error("ResourceNotFoundException", first.list_user_pool_clients, UserPoolId="eu-west-1_Missing")
    expect_error("InvalidParameterException", first.list_user_pools, MaxResults=1, NextToken="malformed")
    expect_error("InvalidParameterException", first.list_user_pool_clients, UserPoolId=owner_id, MaxResults=1, NextToken="malformed")
    expect_error("InvalidParameterException", first.list_user_pool_clients, UserPoolId=other_id, MaxResults=1, NextToken=client_page["NextToken"])
    expect_error("InvalidParameterException", first.list_user_pools, MaxResults=1, NextToken=client_page["NextToken"])
    expect_error("InvalidParameterException", second.list_user_pools, MaxResults=1, NextToken=pool_page["NextToken"])
    expect_error("ResourceNotFoundException", second.list_user_pool_clients, UserPoolId=owner_id)
    state_path.write_text(json.dumps({
        "pools": sorted_pools(pools), "foreign_pools": sorted_pools(foreign_pools),
        "owner_id": owner_id, "other_id": other_id, "clients": sorted_clients(clients),
        "other_clients": other_clients, "independent_clients": independent_clients,
        "independent_id": foreign_pools[0]["Id"],
        "pool_first": [normalized_pool(pool) for pool in pool_page["UserPools"]],
        "pool_token": pool_page["NextToken"],
        "client_first": [normalized_client(app) for app in client_page["UserPoolClients"]],
        "client_token": client_page["NextToken"],
    }))


def restart():
    state = json.loads(state_path.read_text())
    assert all_pools(first) == state["pools"]
    assert all_pools(second) == state["foreign_pools"]
    assert all_clients(first, state["owner_id"]) == state["clients"]
    assert all_clients(first, state["other_id"]) == state["other_clients"]
    assert all_clients(second, state["independent_id"]) == state["independent_clients"]
    # Native NextToken is an operation token. Resume it directly, then continue
    # the exact keyset; paginator StartingToken has a separate botocore envelope.
    resumed = list(state["pool_first"])
    token = state["pool_token"]
    for _ in range(20):
        page = first.list_user_pools(MaxResults=1, NextToken=token)
        resumed.extend(normalized_pool(pool) for pool in page["UserPools"])
        token = page.get("NextToken")
        if not token:
            break
    else:
        raise AssertionError("resumed pool pagination must terminate")
    assert resumed == state["pools"]
    resumed = list(state["client_first"])
    token = state["client_token"]
    for _ in range(20):
        page = first.list_user_pool_clients(UserPoolId=state["owner_id"], MaxResults=1, NextToken=token)
        resumed.extend(normalized_client(app) for app in page["UserPoolClients"])
        token = page.get("NextToken")
        if not token:
            break
    else:
        raise AssertionError("resumed client pagination must terminate")
    assert resumed == state["clients"]
    for pool in state["pools"]:
        described = first.describe_user_pool(UserPoolId=pool["Id"])["UserPool"]
        assert pool_summary(described) == pool
    for app in state["clients"]:
        described = first.describe_user_pool_client(UserPoolId=state["owner_id"], ClientId=app["ClientId"])["UserPoolClient"]
        assert client_summary(described) == app
        assert described["ClientSecret"], "Describe keeps its native detailed response"
    first.delete_user_pool_client(UserPoolId=state["owner_id"], ClientId=state["clients"][0]["ClientId"])
    assert all_clients(first, state["owner_id"]) == state["clients"][1:]
    first.delete_user_pool(UserPoolId=state["owner_id"])
    survivors = [pool for pool in state["pools"] if pool["Id"] != state["owner_id"]]
    assert all_pools(first) == survivors
    expect_error("ResourceNotFoundException", first.list_user_pool_clients, UserPoolId=state["owner_id"])
    assert all_clients(first, state["other_id"]) == state["other_clients"]
    assert all_pools(second) == state["foreign_pools"]
    for pool in survivors:
        first.delete_user_pool(UserPoolId=pool["Id"])
    for pool in state["foreign_pools"]:
        second.delete_user_pool(UserPoolId=pool["Id"])
    assert all_pools(first) == [] and all_pools(second) == []


try:
    if phase == "exercise":
        exercise()
    elif phase == "restart":
        restart()
    else:
        raise AssertionError("unknown inventory phase")
    print("PASS")
finally:
    first.close()
    second.close()
