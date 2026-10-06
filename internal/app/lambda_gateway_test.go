package app

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/gateway"
	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// This subprocess speaks the same Runtime API used by native Go Lambda
// binaries. Language execution and gateway authorization remain separate owners.
func TestProvidedGatewayAuthorizerProcess(t *testing.T) {
	if os.Getenv("EVENTBUS_GATEWAY_PROVIDED_TEST") != "1" {
		return
	}
	endpoint := "http://" + os.Getenv("AWS_LAMBDA_RUNTIME_API") + "/2018-06-01/runtime/invocation/"
	response, err := http.Get(endpoint + "next")
	if err != nil {
		os.Exit(2)
	}
	requestID := response.Header.Get("Lambda-Runtime-Aws-Request-Id")
	var event map[string]any
	if json.NewDecoder(response.Body).Decode(&event) != nil {
		os.Exit(3)
	}
	response.Body.Close()
	result, _ := json.Marshal(map[string]any{
		"principalId": "provided-user",
		"policyDocument": map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{
			"Action": "execute-api:Invoke", "Effect": "Allow", "Resource": event["methodArn"],
		}}},
		"context": map[string]any{"runtime": "provided"},
	})
	reply, err := http.Post(endpoint+requestID+"/response", "application/json", bytes.NewReader(result))
	if err != nil {
		os.Exit(4)
	}
	reply.Body.Close()
	os.Exit(0)
}

func TestGatewayInvokesProvidedPythonAndNodeAuthorizersThroughDispatcher(t *testing.T) {
	directory := t.TempDir()
	node := `exports.handler = async (event, context) => {
      if (event.type !== 'REQUEST' || !context.awsRequestId) throw Error('bad event');
      if (event.headers.Authorization !== 'Bearer owned') throw Error('Unauthorized');
      return {principalId:'node-user',policyDocument:{Version:'2012-10-17',Statement:[{
        Action:'execute-api:Invoke',Effect:'Allow',Resource:event.methodArn}]},context:{runtime:'node'}};
    };`
	python := `def handler(event, context):
    assert event['type'] == 'REQUEST' and context.aws_request_id
    if event['headers'].get('Authorization') != 'Bearer owned':
        raise Exception('Unauthorized')
    return {'principalId':'python-user','policyDocument':{'Version':'2012-10-17','Statement':[{
        'Action':'execute-api:Invoke','Effect':'Allow','Resource':event['methodArn']}]},'context':{'runtime':'python'}}
`
	require.NoError(t, os.WriteFile(filepath.Join(directory, "authorize.cjs"), []byte(node), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(directory, "authorize.py"), []byte(python), 0600))
	executable, err := os.Executable()
	require.NoError(t, err)
	pythonEnvironment := map[string]string{}
	for _, name := range []string{"SSD_DEV_RUN_ID", "SSD_DEV_RUN_RECEIPT", "SSD_DEV_RUN_SCOPE", "SSD_DEV_OPERATION_ID", "SSD_DEV_RECEIPT", "SSD_DEV_SCOPE", "TMPDIR", "GOTMPDIR", "GOCACHE", "UV_CACHE_DIR", "XDG_RUNTIME_DIR", "DBUS_SESSION_BUS_ADDRESS", "INVOCATION_ID"} {
		if value := os.Getenv(name); value != "" {
			pythonEnvironment[name] = value
		}
	}
	functions, err := lambdaservice.NewService(&lambdaservice.Config{Functions: map[string]lambdaservice.Function{
		"provided": {Runtime: "provided", Command: []string{executable, "-test.run=^TestProvidedGatewayAuthorizerProcess$"}, Environment: map[string]string{"EVENTBUS_GATEWAY_PROVIDED_TEST": "1"}},
		"python":   {Runtime: "python", Command: []string{"uv", "run", "--no-project", "python"}, Handler: "authorize.py#handler", Environment: pythonEnvironment},
		"node":     {Runtime: "node", Handler: "authorize.cjs#handler"},
	}}, directory)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, functions.Close(context.Background())) })
	aws := httptest.NewServer(server.New(server.Services{Lambda: functions}))
	t.Cleanup(aws.Close)
	receipt := make(chan http.Header, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receipt <- r.Header.Clone()
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	t.Cleanup(backend.Close)
	zero := 0
	for _, runtime := range []string{"provided", "python", "node"} {
		t.Run(runtime, func(t *testing.T) {
			edge, err := gateway.New(gateway.Config{
				Authorizers: map[string]gateway.AuthorizerConfig{"app": {Type: "REQUEST", InvokeURL: aws.URL + "/2015-03-31/functions/" + runtime + "/invocations", TTL: &zero, Timeout: 10 * time.Second}},
				Routes: []gateway.RouteConfig{{Path: "/api/{proxy+}", Method: "ANY", Authorizer: "app", Integration: gateway.IntegrationConfig{
					Type: "HTTP_PROXY", URI: backend.URL + "/api/{proxy}", RemoveHeaders: []string{"Authorization", "Cookie"},
					RequestParameters: map[string]string{"integration.request.header.Runtime": "context.authorizer.runtime"},
				}}},
			}, gateway.Options{Logger: zerolog.Nop()})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, edge.Close()) })
			request := httptest.NewRequest("POST", "/api/resource/nested?tag=one&tag=two", bytes.NewBufferString(`{"request":"owned"}`))
			request.Header.Set("Authorization", "Bearer owned")
			request.Header.Set("Cookie", "private-cookie")
			response := httptest.NewRecorder()
			edge.ServeHTTP(response, request)
			require.Equal(t, 200, response.Code, response.Body.String())
			observed := <-receipt
			require.Equal(t, runtime, observed.Get("Runtime"))
			require.Empty(t, observed.Get("Authorization"))
			require.Empty(t, observed.Get("Cookie"))
			require.NotContains(t, response.Body.String(), "principalId")
			if runtime != "provided" {
				denied := httptest.NewRecorder()
				edge.ServeHTTP(denied, httptest.NewRequest("GET", "/api/resource", nil))
				require.Equal(t, 401, denied.Code, denied.Body.String())
				require.Empty(t, receipt)
			}
		})
	}
}
