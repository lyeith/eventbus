# EventBus SDK smoke

Run from this repository:

```sh
uv sync --frozen
uv run --frozen python -m unittest discover -s tests -p 'test_*.py'
EVENTBUS_SMOKE_PYTHON="$PWD/.venv/bin/python" go test -race -count=1 -tags sdksmoke ./...
```

The Go smoke lane launches the real HTTP dispatcher against a fresh SQLite store
for each Python client. The operating system assigns listener ports; each test
owns its pool/client IDs and users. It does not use or reset a developer EventBus.

The scripts exercise admin creation/collision/deletion, password and refresh
authentication with real PyJWT/JWKS verification, and secret-hash plus pool/client
management through boto3. After pool deletion, the check requires the exact
missing-pool error from a supported command. `AdminGetUser` is unsupported.

SES smoke covers all nine v1/v2 sending operations, current optional fields,
exact binary capture, error envelopes and ordered partial bulk results. It uses
[the pinned official sending models](aws_models/README.md) with explicit SigV4
for its local endpoint; it requires no AWS CRT dependency or model/SDK upgrade.

The runner requires a successful child exit and exactly one PASS marker. Missing
dependencies, empty success, false/conflicting markers, assertion failures and
timeouts fail the lane. Network calls have deadlines; inherited AWS profiles,
endpoints, proxies and Python optimization settings are not forwarded. Owned
fixture stores are cleaned up after their listeners close.

The ordinary Go suite does not require Python. These tests prove the supported
HTTP/SDK/JWT contracts; they do not promise complete AWS compatibility.
