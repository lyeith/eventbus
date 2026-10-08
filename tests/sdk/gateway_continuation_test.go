//go:build sdksmoke

package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/cognito"
	"github.com/lyeith/eventbus/internal/devquiescence"
	"github.com/lyeith/eventbus/internal/gateway"
	"github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// All application code is external to the emulator. The authorizer fetches cold
// persisted JWKS and validates a Cognito-issued JWT; fixture gates only delay
// application work. Successful policy results require signature and claims.
const gatewayContinuationPython = `
import json
import os
from pathlib import Path
import time
import urllib.request

def root():
    return Path(os.environ["CONTINUATION_ROOT"])

def save(path, value):
    temporary = path.with_suffix(".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(value, stream)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)

def row(stage, chain, context, **facts):
    save(root() / (stage + "-" + chain + ".json"),
         {"stage": stage, "chain": chain, "pid": os.getpid(),
          "request_id": context.aws_request_id, **facts})

def gate(name):
    while not (root() / name).exists():
        time.sleep(0.01)

def auth():
    return json.loads((root() / "auth.json").read_text())

def caller(event, context):
    chain = event["chain"]
    row("caller-started", chain, context)
    gate("release-http-" + chain)
    state = auth()
    payload = json.dumps({"chain": chain, "marker": "native-payload"}).encode()
    request = urllib.request.Request(os.environ["CONTINUATION_GATEWAY"] + "/app/work?fixture=owned",
        data=payload, method="POST", headers={"Authorization": "Bearer " + state["token"],
                                            "Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=15) as response:
        result = json.load(response)
        assert response.status == 200 and result == {"chain": chain, "authenticated": True}, result
    row("caller-returned", chain, context)
    gate("release-parent-" + chain)
    return {"chain": chain, "completed": True}

def authorize(event, context):
    import jwt
    assert event["version"] == "2.0" and event["type"] == "REQUEST", event
    state = auth()
    headers = {key.lower(): value for key, value in event["headers"].items()}
    resource = event["routeArn"]
    try:
        header = headers["authorization"]
        assert header.startswith("Bearer ")
        token = header[7:]
        # New registered invocation/process means no signing-key cache is warm.
        key = jwt.PyJWKClient(state["issuer"] + "/.well-known/jwks.json", timeout=5).get_signing_key_from_jwt(token)
        claims = jwt.decode(token, key.key, algorithms=["RS256"], audience=state["client"],
                            issuer=state["issuer"], options={"require": ["exp", "iat", "iss", "aud", "sub"]})
        assert claims["token_use"] == "id" and claims["cognito:username"] == state["username"]
        assert claims["sub"] == state["sub"]
    except Exception as error:
        row("denied", context.aws_request_id, context, reason=type(error).__name__)
        return {"principalId": "refused", "policyDocument": {"Version": "2012-10-17", "Statement": [
            {"Action": "execute-api:Invoke", "Effect": "Deny", "Resource": resource}]}}
    row("authorized", context.aws_request_id, context, issuer=claims["iss"], subject=claims["sub"], audience=claims["aud"])
    return {"principalId": claims["sub"], "policyDocument": {"Version": "2012-10-17", "Statement": [
        {"Action": "execute-api:Invoke", "Effect": "Allow", "Resource": resource}]},
        "context": {"authenticated": True, "principal_id": claims["sub"]}}

def integrate(event, context):
    state = auth()
    assert event["version"] == "2.0" and event["rawPath"] == "/app/work", event
    assert event["requestContext"]["http"]["method"] == "POST"
    assert event["queryStringParameters"]["fixture"] == "owned"
    trusted = event["requestContext"]["authorizer"]["lambda"]
    assert trusted["authenticated"] is True and trusted["principal_id"] == state["sub"]
    payload = json.loads(event["body"])
    assert payload["marker"] == "native-payload" and event["isBase64Encoded"] is False
    chain = payload["chain"]
    row("integration-started", chain, context)
    gate("release-integration-" + chain)
    save(root() / ("business-" + chain + ".json"), {"chain": chain, "persisted": True})
    row("integration-completed", chain, context)
    return {"statusCode": 200, "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"chain": chain, "authenticated": True})}

if __name__ == "__main__":
    import boto3
    import jwt
    from botocore.config import Config
    client = boto3.client("cognito-idp", endpoint_url=os.environ["CONTINUATION_SOURCE"],
        region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test",
        config=Config(retries={"total_max_attempts": 1}, connect_timeout=3, read_timeout=5))
    pool = client.create_user_pool(PoolName="gateway-continuation")["UserPool"]["Id"]
    clients = [client.create_user_pool_client(UserPoolId=pool, ClientName=name, GenerateSecret=False,
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH"])["UserPoolClient"]["ClientId"]
        for name in ["owned-audience", "wrong-audience"]]
    username, password = "continuation-user", "GatewayPass1!"
    client.admin_create_user(UserPoolId=pool, Username=username, MessageAction="SUPPRESS",
        TemporaryPassword=password, UserAttributes=[{"Name": "email", "Value": "continuation@sdk.test"}])
    client.admin_set_user_password(UserPoolId=pool, Username=username, Password=password, Permanent=True)
    tokens = [client.initiate_auth(ClientId=identifier, AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": username, "PASSWORD": password})["AuthenticationResult"]["IdToken"]
        for identifier in clients]
    # Parsing here is provisioning only; the cold registered authorizer performs
    # independent signature, time, issuer, audience and application-claim checks.
    claims = jwt.decode(tokens[0], options={"verify_signature": False})
    save(root() / "auth.json", {"token": tokens[0], "wrong_token": tokens[1], "client": clients[0],
        "issuer": os.environ["CONTINUATION_CALLBACK"] + "/" + pool, "username": username,
        "sub": claims["sub"]})
    print("PASS")
`

type gatewayContinuationHTTPResult struct {
	status int
	body   []byte
	err    error
}

type gatewayContinuationRow struct {
	Stage     string `json:"stage"`
	Chain     string `json:"chain"`
	PID       int    `json:"pid"`
	RequestID string `json:"request_id"`
}

// This is the native composition involved in #24: accepted Lambda A reaches
// authenticated gateway B during drain, a typed cleanup epoch does the same, then
// irreversible shutdown preserves one last accepted chain until actual join.
func TestRetainedGatewayAuthenticatedNativeContinuations(t *testing.T) {
	python, directory := sdkPython(t), t.TempDir()
	source, callback := httptest.NewUnstartedServer(nil), httptest.NewUnstartedServer(nil)
	public, continuation := httptest.NewUnstartedServer(nil), httptest.NewUnstartedServer(nil)
	sourceURL, callbackURL := "http://"+source.Listener.Addr().String(), "http://"+callback.Listener.Addr().String()
	continuationURL := "http://" + continuation.Listener.Addr().String()
	script := filepath.Join(directory, "continuation.py")
	require.NoError(t, os.WriteFile(script, []byte(gatewayContinuationPython), 0600))
	var functions *lambda.Service
	var edge *gateway.Gateway
	owner := devquiescence.New(func() error {
		if functions != nil {
			return functions.DevEvidence()
		}
		return nil
	})
	require.NoError(t, owner.SetCallbackOrigin(callbackURL))
	store, err := cognito.OpenCognitoStore(filepath.Join(directory, "cognito.db"))
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, chain := range []string{"accepted", "cleanup", "shutdown"} {
			for _, stage := range []string{"http", "integration", "parent"} {
				_ = os.WriteFile(filepath.Join(directory, "release-"+stage+"-"+chain), nil, 0600)
			}
		}
		owner.Shutdown()
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()
		check := func(err error) {
			if err != nil {
				t.Errorf("native continuation fixture cleanup: %v", err)
			}
		}
		_, joinErr := owner.Quiesce(ctx)
		check(joinErr)
		if edge != nil {
			check(edge.Close())
		}
		public.Close()
		continuation.Close()
		if functions != nil {
			closeErr := functions.Close(ctx)
			check(closeErr)
			if closeErr != nil {
				check(functions.Close(context.Background()))
			}
		}
		source.Close()
		callback.Close()
		check(store.Close())
		paths, _ := filepath.Glob(filepath.Join(directory, "*.json"))
		for _, path := range paths {
			var row gatewayContinuationRow
			if data, err := os.ReadFile(path); err == nil && json.Unmarshal(data, &row) == nil && row.PID != 0 {
				require.False(t, retainedSDKAlive(row.PID), "actual registered child must be reaped after joined fixture teardown")
			}
		}
	})
	environment := map[string]string{"CONTINUATION_ROOT": directory, "CONTINUATION_GATEWAY": continuationURL}
	registered := map[string]lambda.Function{}
	for name, handler := range map[string]string{"continuation-a:live": "caller", "continuation-cleanup:live": "caller",
		"continuation-authorizer:live": "authorize", "continuation-b:live": "integrate"} {
		registered[name] = lambda.Function{Runtime: "python", Command: []string{python, "-E", "-s"},
			Handler: script + "#" + handler, Environment: environment, Timeout: 20 * time.Second}
	}
	functions, err = lambda.NewService(&lambda.Config{Functions: registered, DevActivity: owner}, directory)
	require.NoError(t, err)
	aws := server.New(server.Services{Lambda: functions,
		Cognito:     cognito.NewHandler(store, cognito.Options{Region: "us-east-1", IssuerBase: callbackURL}),
		CognitoURLs: &server.CognitoURLs{Issuer: callbackURL, JWKS: callbackURL}})
	controls := devquiescence.NewHandler(owner)
	sourceWork := owner.Wrap(devquiescence.Source, aws, nil)
	source.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, devquiescence.ControlPath) {
			controls.ServeHTTP(w, r)
			return
		}
		sourceWork.ServeHTTP(w, r)
	})
	var jwksCalls atomic.Int64
	callbackWork := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/.well-known/jwks.json") {
			jwksCalls.Add(1)
		}
		aws.ServeHTTP(w, r)
	})
	callback.Config.Handler = owner.Wrap(devquiescence.Callback, callbackWork, nil)
	source.Start()
	callback.Start()
	zero := 0
	edge, err = gateway.New(gateway.Config{Stage: "$default", RetainedOwnerControlURL: sourceURL + devquiescence.ControlPath,
		RetainedOwnerContinuationPort: continuation.Listener.Addr().(*net.TCPAddr).Port,
		Authorizers: map[string]gateway.AuthorizerConfig{"jwt": {Type: "REQUEST", PayloadFormatVersion: "2.0",
			InvokeURL: callbackURL + "/2015-03-31/functions/continuation-authorizer:live/invocations", TTL: &zero,
			IdentitySources: []string{"$request.header.Authorization"}, Timeout: 8 * time.Second}},
		Routes: []gateway.RouteConfig{{Path: "/app/work", Method: "POST", Authorizer: "jwt",
			Integration: gateway.IntegrationConfig{Type: "AWS_PROXY", PayloadFormatVersion: "2.0",
				InvokeURL: callbackURL + "/2015-03-31/functions/continuation-b:live/invocations", Timeout: 15 * time.Second}}},
	}, gateway.Options{Logger: zerolog.Nop()})
	require.NoError(t, err)
	public.Config.Handler = edge
	continuation.Config.Handler = edge.RetainedContinuationHandler()
	public.Start()
	continuation.Start()
	env := append(sdkEnvironment(t.TempDir(), "", "", ""), "CONTINUATION_ROOT="+directory,
		"CONTINUATION_SOURCE="+sourceURL, "CONTINUATION_CALLBACK="+callbackURL)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	output, err := runSDKProcess(ctx, python, script, env)
	cancel()
	t.Logf("actual Cognito SDK provisioning:\n%s", output)
	require.NoError(t, err)
	var auth struct {
		Token      string `json:"token"`
		WrongToken string `json:"wrong_token"`
		Issuer     string `json:"issuer"`
	}
	authData, err := os.ReadFile(filepath.Join(directory, "auth.json"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(authData, &auth))
	require.True(t, strings.HasPrefix(auth.Issuer, callbackURL+"/"), "local issuer/JWKS origin explicitly uses callback endpoint")
	require.Zero(t, jwksCalls.Load(), "registered authorizer must start with cold JWKS")

	client := &http.Client{Timeout: 25 * time.Second}
	defer client.CloseIdleConnections()
	request := func(endpoint, body, token string, extra http.Header) gatewayContinuationHTTPResult {
		req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewBufferString(body))
		if err != nil {
			return gatewayContinuationHTTPResult{err: err}
		}
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		for name, values := range extra {
			req.Header[name] = values
		}
		response, err := client.Do(req)
		if err != nil {
			return gatewayContinuationHTTPResult{err: err}
		}
		defer response.Body.Close()
		data, err := io.ReadAll(response.Body)
		return gatewayContinuationHTTPResult{status: response.StatusCode, body: data, err: err}
	}
	asyncRequest := func(endpoint, body string) <-chan gatewayContinuationHTTPResult {
		result := make(chan gatewayContinuationHTTPResult, 1)
		go func() { result <- request(endpoint, body, "", nil) }()
		return result
	}
	waitRow := func(stage, chain string) {
		require.Eventually(t, func() bool {
			_, err := os.Stat(filepath.Join(directory, stage+"-"+chain+".json"))
			return err == nil
		}, 8*time.Second, 10*time.Millisecond, "actual registered %s/%s", stage, chain)
	}
	release := func(stage, chain string) {
		require.NoError(t, os.WriteFile(filepath.Join(directory, "release-"+stage+"-"+chain), nil, 0600))
	}
	assertPending := func(result <-chan gatewayContinuationHTTPResult, reason string) {
		select {
		case returned := <-result:
			t.Fatalf("%s: premature response %d %s %v", reason, returned.status, returned.body, returned.err)
		default:
		}
	}
	await := func(result <-chan gatewayContinuationHTTPResult) gatewayContinuationHTTPResult {
		select {
		case returned := <-result:
			require.NoError(t, returned.err)
			require.Equal(t, http.StatusOK, returned.status, string(returned.body))
			return returned
		case <-time.After(25 * time.Second):
			t.Fatal("native accepted chain did not join")
			return gatewayContinuationHTTPResult{}
		}
	}
	assertBarrier := func(result <-chan gatewayContinuationHTTPResult, shutdown bool) devquiescence.Snapshot {
		var snapshot devquiescence.Snapshot
		returned := await(result)
		require.NoError(t, json.Unmarshal(returned.body, &snapshot))
		require.Zero(t, snapshot.WorkCount)
		require.Zero(t, snapshot.CleanupEnvelopes)
		require.Empty(t, snapshot.EvidenceFailure)
		if shutdown {
			require.Equal(t, devquiescence.Shutdown, snapshot.State)
			require.False(t, snapshot.FixtureSafe)
		} else {
			require.Equal(t, devquiescence.Held, snapshot.State)
			require.True(t, snapshot.FixtureSafe)
		}
		return snapshot
	}
	runChain := func(chain, endpoint string, shutdown bool, cleanupComplete func(error)) devquiescence.Snapshot {
		call := asyncRequest(endpoint, fmt.Sprintf(`{"chain":%q}`, chain))
		waitRow("caller-started", chain)
		if shutdown {
			owner.Shutdown()
		}
		barrier := asyncRequest(sourceURL+devquiescence.ControlPath+"/quiesce", `{"timeout_ms":20000}`)
		require.Eventually(t, func() bool {
			snapshot := owner.Snapshot()
			return snapshot.State == devquiescence.Draining || shutdown && snapshot.State == devquiescence.Shutdown
		}, time.Second, time.Millisecond)
		refused := request(public.URL+"/app/work?fixture=owned", `{"marker":"caller-headers"}`, auth.Token,
			http.Header{"X-Eventbus-Continuation": {"true"}, "X-Eventbus-Retained-Owner": {owner.Snapshot().OwnerID},
				"X-Eventbus-Source-Lane": {"callback"}})
		require.NoError(t, refused.err)
		require.Equal(t, http.StatusServiceUnavailable, refused.status, "public callers cannot select trusted continuation admission")
		release("http", chain)
		waitRow("integration-started", chain)
		require.Positive(t, jwksCalls.Load(), "cold native authorizer obtains persisted JWKS through callback during fenced epoch")
		assertPending(barrier, "native integration work remains joined")
		if chain == "accepted" {
			// Wrong audience is genuinely signed by the same native pool; the
			// altered signature proves refusal independently of claims.
			parts := strings.Split(auth.Token, ".")
			signature := []byte(parts[2])
			if signature[0] == 'A' {
				signature[0] = 'B'
			} else {
				signature[0] = 'A'
			}
			parts[2] = string(signature)
			for _, invalid := range []string{strings.Join(parts, "."), auth.WrongToken} {
				denied := request(continuation.URL+"/app/work?fixture=owned", `{"chain":"forbidden","marker":"native-payload"}`, invalid, nil)
				require.NoError(t, denied.err)
				require.Equal(t, http.StatusForbidden, denied.status, "private transport never bypasses signature/audience validation")
			}
			_, err := os.Stat(filepath.Join(directory, "integration-started-forbidden.json"))
			require.ErrorIs(t, err, os.ErrNotExist)
		}
		release("integration", chain)
		waitRow("caller-returned", chain)
		assertPending(barrier, "native parent still owns post-downstream work")
		release("parent", chain)
		native := await(call)
		var result struct {
			Chain     string `json:"chain"`
			Completed bool   `json:"completed"`
		}
		require.NoError(t, json.Unmarshal(native.body, &result))
		require.Equal(t, chain, result.Chain)
		require.True(t, result.Completed)
		if cleanupComplete != nil {
			cleanupComplete(nil)
		}
		return assertBarrier(barrier, shutdown)
	}
	unowned := request(continuation.URL+"/app/work?fixture=owned", `{"chain":"forbidden","marker":"native-payload"}`, auth.Token, nil)
	require.NoError(t, unowned.err)
	require.Equal(t, http.StatusServiceUnavailable, unowned.status, "open private ingress needs an actual accepted ancestor too")
	require.Zero(t, jwksCalls.Load(), "unowned private callers are refused before authorization")
	held := runChain("accepted", sourceURL+"/2015-03-31/functions/continuation-a:live/invocations", false, nil)
	// Exercise the coordinator's typed cleanup epoch without implementing an app
	// declaration/allowlist here. Production run configuration has its own app
	// acceptance proof; this fixture owns only native HTTP/JWT composition.
	completeCleanup, err := owner.BeginCleanup(held.Generation, "cleanup.fixture", "SDK-native-cleanup")
	require.NoError(t, err)
	t.Cleanup(func() { completeCleanup(nil) })
	cleanupHeld := runChain("cleanup", callbackURL+"/2015-03-31/functions/continuation-cleanup:live/invocations", false, completeCleanup)
	require.Equal(t, held.Generation, cleanupHeld.Generation)
	refused := request(continuation.URL+"/app/work?fixture=owned", `{"chain":"forbidden","marker":"native-payload"}`, auth.Token, nil)
	require.NoError(t, refused.err)
	require.Equal(t, http.StatusServiceUnavailable, refused.status, "held with no accepted ancestor cannot admit private application work")
	resumed := request(sourceURL+devquiescence.ControlPath+"/resume", fmt.Sprintf(`{"generation":%d}`, cleanupHeld.Generation), "", nil)
	require.NoError(t, resumed.err)
	require.Equal(t, http.StatusOK, resumed.status, string(resumed.body))
	// Received envelopes keep their entry generation. Wait for a future public
	// envelope to observe the resumed epoch; never retry a stale private call.
	require.Eventually(t, func() bool {
		probe := request(public.URL+"/app/work?fixture=owned", `{"chain":"forbidden","marker":"native-payload"}`, "invalid-jwt", nil)
		return probe.err == nil && probe.status == http.StatusForbidden
	}, 5*time.Second, 10*time.Millisecond)
	runChain("shutdown", sourceURL+"/2015-03-31/functions/continuation-a:live/invocations", true, nil)
}
