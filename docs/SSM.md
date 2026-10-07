# SSM Parameter Store

Use a normal AWS SSM client with the EventBus endpoint and explicit local
credentials. EventBus implements four Parameter Store operations through AWS
JSON 1.1 (`X-Amz-Target: AmazonSSM.<operation>`); other Systems Manager operations
return `UnknownOperationException`.

| Operation | Supported behavior |
| --- | --- |
| `PutParameter` | Create or overwrite String, StringList and SecureString values; return the exact written `Version` and `Tier: Standard`. |
| `GetParameter` | Read the latest value or a numeric version selector; return name, value, type, version, data type and last-modified time. |
| `GetParametersByPath` | Read latest versions under a hierarchy, with recursion, decryption choices and pagination. |
| `DeleteParameter` | Remove the parameter and all its versions; a missing parameter returns `ParameterNotFound`. |

All parameters, version history, pagination keys and encryption keys are in
memory. Reprovision after restart. There is no application-specific seed format;
provision through the supported AWS APIs.

## SDK example

Start EventBus using the [startup command](../README.md#start), then run this
with the application's existing boto3 environment:

```python
import boto3

ssm = boto3.client(
    "ssm",
    endpoint_url="http://localhost:14100",
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test",
)
name = "/example/database/password"
ssm.put_parameter(Name=name, Value="first", Type="SecureString")
written = ssm.put_parameter(Name=name, Value="second", Overwrite=True)
assert written["Version"] == 2
assert ssm.get_parameter(Name=name + ":1", WithDecryption=True)["Parameter"]["Value"] == "first"
assert ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"] == "second"

for page in ssm.get_paginator("get_parameters_by_path").paginate(
    Path="/example", Recursive=True, WithDecryption=True, MaxResults=2
):
    for parameter in page["Parameters"]:
        print(parameter["Name"], parameter["Version"])

ssm.delete_parameter(Name=name)
```

The example assumes a fresh parameter name. Use an application-owned prefix
and delete it after the scenario. The SDK handles protocol headers and tokens.

## Versions and hierarchy

A new parameter starts at version 1. Every successful overwrite increments the
version, including a write of the same value. Without `Overwrite=True`, an
existing name returns `ParameterAlreadyExists`. Omitting `Type` defaults to
String on creation and preserves the existing type on overwrite; changing type
returns `HierarchyTypeMismatchException`. Omitting `Description` on overwrite
preserves it; an explicit empty description clears it.

`GetParameter(Name="/name:3")` reads version 3 and returns `Selector: ":3"`.
Only positive numeric selectors are supported. The latest 100 versions are
retained; an absent or expired version returns `ParameterVersionNotFound`.
Labels and ARN/shared-parameter selectors are rejected. There is no
`GetParameterHistory` operation. Deleting and recreating a name starts again at
version 1.

`GetParametersByPath` defaults to `Recursive=False`, `WithDecryption=False`
and `MaxResults=10`. `MaxResults` must be 1 through 10. `/app` and `/app/` select
the same hierarchy: direct reads include `/app/a`, recursive reads also include
`/app/nested/a`, and neither includes `/app` itself or `/application/a`.
The root path `/` is supported.

Pages are sorted by parameter name and contain current versions. Continue until
`NextToken` is absent, keeping `Path`, `Recursive` and `WithDecryption` unchanged.
Tokens are opaque and bound to those options and the current process; malformed,
tampered or mismatched tokens return `InvalidNextToken`. Pagination is not a
snapshot across pages, so concurrent writes can change later results.

## Values and limits

- Only Standard tier and `DataType="text"` are supported. Values must contain
  1 through 4,096 UTF-8 bytes; multibyte characters count by bytes.
- StringList trims each comma-separated item and returns a normalized value
  such as `one,two,three`. Empty items are rejected.
- Names are trimmed, then limited to 1 through 1,011 ASCII characters from
  letters, digits, `_`, `.`, `-` and `/`, with at most 15 hierarchy levels.
  Empty levels and trailing slashes are rejected. The first level cannot begin
  with `aws` or `ssm`, case-insensitively. Description accepts up to 1,024
  characters.
- The JSON transport limit is 1 MiB, separate from the decoded value limit.
  Malformed JSON and wrong option types return `SerializationException`.

SecureString uses local AES-GCM authenticated encryption with a random key owned
by the in-memory store. Stored records contain ciphertext rather than plaintext.
`WithDecryption=True` returns plaintext; false returns base64 of the local sealed
value. String and StringList reads ignore this option. This does not reproduce
AWS KMS ciphertext, key ownership, key policies, grants or IAM enforcement.
Omitting `KeyId` or using `alias/aws/ssm` is accepted; custom keys are rejected.

Non-empty `AllowedPattern`, tags and parameter filters are rejected, as are
Advanced/Intelligent-Tiering, non-text data types and non-empty parameter policies
(other than `[]`). Batch reads/deletes, history listing, labels, parameter
sharing, tagging and the rest of Systems Manager are unsupported.

[Native protocol tests](../internal/ssm/native_protocol_test.go) exercise real
Go SDK requests, errors and pagination. [Store regression tests](../internal/ssm/regression_store_test.go)
cover history retention, sealed values, hierarchy boundaries and independent
concurrent snapshots.
