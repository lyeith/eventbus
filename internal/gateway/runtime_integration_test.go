package gateway_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/gateway"
	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/rs/zerolog"
)

// The test binary is a native Go provided-runtime fixture. It talks only to the
// AWS Runtime API, exactly as a compiled application Lambda binary does.
func TestNativeGatewayProvidedRuntime(t *testing.T) {
	if os.Getenv("EVENTBUS_NATIVE_GATEWAY_PROVIDED") != "1" {
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
	result := nativeFixtureResponse(event, "provided")
	data, err := json.Marshal(result)
	if err != nil {
		os.Exit(4)
	}
	reply, err := http.Post(endpoint+requestID+"/response", "application/json", bytes.NewReader(data))
	if err != nil {
		os.Exit(5)
	}
	reply.Body.Close()
	os.Exit(0)
}

func nativeFixtureResponse(event map[string]any, runtime string) map[string]any {
	if event["type"] == "REQUEST" {
		resource := event["routeArn"]
		if resource == nil {
			resource = event["methodArn"]
		}
		effect := "Deny"
		if event["headers"].(map[string]any)["authorization"] == "Bearer owned" {
			effect = "Allow"
		}
		return map[string]any{"principalId": "native-user", "policyDocument": map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{"Action": "execute-api:Invoke", "Effect": effect, "Resource": resource}}}, "context": map[string]any{"runtime": runtime, "roles": []string{"reader"}, "tenant": map[string]any{"id": "owned"}}}
	}
	if event["queryStringParameters"].(map[string]any)["binary"] == "1" {
		return map[string]any{"statusCode": 200, "body": event["body"], "isBase64Encoded": true, "headers": map[string]string{"Content-Type": "application/octet-stream"}}
	}
	data, _ := json.Marshal(event)
	result := map[string]any{"statusCode": 200, "body": string(data), "headers": map[string]string{"Content-Type": "application/json"}}
	if event["version"] == "2.0" {
		result["cookies"] = []string{"a=1", "b=2"}
	} else {
		result["multiValueHeaders"] = map[string][]string{"Set-Cookie": {"a=1", "b=2"}}
	}
	return result
}

func TestNativeGatewayRealGoPythonNodeLambdaHandlers(t *testing.T) {
	directory := t.TempDir()
	node := `exports.handler = async (event, context) => {
  console.log('owned handler diagnostic');
  if (!context.awsRequestId) throw Error('missing Lambda context');
  if (event.type === 'REQUEST') return {
   principalId:'native-user',policyDocument:{Version:'2012-10-17',Statement:[{
    Action:'execute-api:Invoke',Effect:event.headers.authorization === 'Bearer owned' ? 'Allow':'Deny',Resource:event.routeArn || event.methodArn}]},
   context:{runtime:'node',roles:['reader'],tenant:{id:'owned'}}};
  if (event.queryStringParameters.binary === '1') return {statusCode:200,body:event.body,isBase64Encoded:true,headers:{'Content-Type':'application/octet-stream'}};
  const response = {statusCode:200,body:JSON.stringify(event),headers:{'Content-Type':'application/json'}};
  if (event.version === '2.0') response.cookies=['a=1','b=2']; else response.multiValueHeaders={'Set-Cookie':['a=1','b=2']};
  return response;
 };`
	python := `import json

def handler(event, context):
    print('owned handler diagnostic')
    assert context.aws_request_id
    if event.get('type') == 'REQUEST':
        return {'principalId':'native-user','policyDocument':{'Version':'2012-10-17','Statement':[{
            'Action':'execute-api:Invoke','Effect':'Allow' if event['headers'].get('authorization') == 'Bearer owned' else 'Deny','Resource':event.get('routeArn',event.get('methodArn'))}]},
            'context':{'runtime':'python','roles':['reader'],'tenant':{'id':'owned'}}}
    if event['queryStringParameters'].get('binary') == '1':
        return {'statusCode':200,'body':event['body'],'isBase64Encoded':True,'headers':{'Content-Type':'application/octet-stream'}}
    response = {'statusCode':200,'body':json.dumps(event),'headers':{'Content-Type':'application/json'}}
    if event['version'] == '2.0':
        response['cookies'] = ['a=1','b=2']
    else:
        response['multiValueHeaders'] = {'Set-Cookie':['a=1','b=2']}
    return response
`
	for name, contents := range map[string]string{"handler.cjs": node, "handler.py": python} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	provided := lambdaservice.Function{Runtime: "provided", Command: []string{executable, "-test.run=^TestNativeGatewayProvidedRuntime$"}, Environment: map[string]string{"EVENTBUS_NATIVE_GATEWAY_PROVIDED": "1"}}
	pythonEnvironment := map[string]string{}
	for _, name := range []string{"SSD_DEV_RUN_ID", "SSD_DEV_RUN_RECEIPT", "SSD_DEV_RUN_SCOPE", "SSD_DEV_OPERATION_ID", "SSD_DEV_RECEIPT", "SSD_DEV_SCOPE", "TMPDIR", "GOTMPDIR", "GOCACHE", "UV_CACHE_DIR", "XDG_RUNTIME_DIR", "DBUS_SESSION_BUS_ADDRESS", "INVOCATION_ID"} {
		if value := os.Getenv(name); value != "" {
			pythonEnvironment[name] = value
		}
	}
	pythonFunction := lambdaservice.Function{Runtime: "python", Command: []string{"uv", "run", "--no-project", "python"}, Handler: "handler.py#handler", Environment: pythonEnvironment}
	nodeFunction := lambdaservice.Function{Runtime: "node", Handler: "handler.cjs#handler"}
	functions, err := lambdaservice.NewService(&lambdaservice.Config{Functions: map[string]lambdaservice.Function{
		"provided-auth": provided, "provided-app:live": provided, "python-auth": pythonFunction, "python-app:live": pythonFunction, "node-auth": nodeFunction, "node-app:live": nodeFunction,
	}}, directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := functions.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	aws := httptest.NewServer(server.New(server.Services{Lambda: functions}))
	t.Cleanup(aws.Close)
	for _, runtime := range []string{"provided", "python", "node"} {
		for _, version := range []string{"1.0", "2.0"} {
			t.Run(runtime+"/"+version, func(t *testing.T) {
				zero := 0
				integration := gateway.IntegrationConfig{Type: "AWS_PROXY", InvokeURL: aws.URL + "/2015-03-31/functions/" + runtime + "-app/invocations?Qualifier=live", PayloadFormatVersion: version, Timeout: 5 * time.Second}
				cfg := gateway.Config{Stage: "$default", DevHealthPath: "/.eventbus/ready", Authorizers: map[string]gateway.AuthorizerConfig{"auth": {Type: "REQUEST", InvokeURL: aws.URL + "/2015-03-31/functions/" + runtime + "-auth/invocations", TTL: &zero, PayloadFormatVersion: "2.0", IdentitySources: []string{"$request.header.Authorization"}, Timeout: 5 * time.Second}}, Routes: []gateway.RouteConfig{
					{Path: "/private/{id}", Method: "POST", Authorizer: "auth", Integration: integration}, {Path: "/public/{id}", Method: "POST", Integration: integration},
					{Path: "/health", Method: "GET", Authorizer: "auth", Integration: integration},
					{Path: "/docs", Method: "GET", Integration: integration},
					{Path: "/docs/", Method: "GET", Integration: integration},
					{Path: "/docs/{proxy+}", Method: "GET", Integration: integration},
				}}
				edge, err := gateway.New(cfg, gateway.Options{Logger: zerolog.Nop()})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := edge.Close(); err != nil {
						t.Error(err)
					}
				})
				frontend := httptest.NewServer(edge)
				t.Cleanup(frontend.Close)
				client := frontend.Client()
				client.Timeout = 10 * time.Second
				for _, path := range []string{"/private/42", "/public/42"} {
					request, _ := http.NewRequest("POST", frontend.URL+path+"?x=one&x=two&plus=a%2Bb", strings.NewReader(`{"owned":true}`))
					request.Header.Set("Authorization", "Bearer owned")
					request.Header.Set("Content-Type", "application/json")
					request.Header.Add("Cookie", "first=1")
					request.Header.Add("Cookie", "second=2")
					request.Header.Add("X-Repeated", "one")
					request.Header.Add("X-Repeated", "two")
					response, err := client.Do(request)
					if err != nil {
						t.Fatal(err)
					}
					data, _ := io.ReadAll(response.Body)
					response.Body.Close()
					if response.StatusCode != 200 {
						t.Fatalf("native %s returned %d: %s", path, response.StatusCode, data)
					}
					if len(response.Header.Values("Set-Cookie")) != 2 {
						t.Fatalf("real runtime cookies %v", response.Header)
					}
					var event map[string]any
					if err := json.Unmarshal(data, &event); err != nil {
						t.Fatalf("handler stdout corrupted payload: %s", data)
					}
					if event["version"] != version || event["body"] != `{"owned":true}` || event["isBase64Encoded"] != false {
						t.Fatalf("native request changed: %v", event)
					}
					context := event["requestContext"].(map[string]any)
					if path == "/private/42" {
						authorization := context["authorizer"].(map[string]any)
						if version == "2.0" {
							authorization = authorization["lambda"].(map[string]any)
						}
						if authorization["runtime"] != runtime || authorization["principalId"] != "native-user" || authorization["tenant"].(map[string]any)["id"] != "owned" {
							t.Fatalf("trusted native context %v", authorization)
						}
					} else if _, ok := context["authorizer"]; ok {
						t.Fatal("public route invented authorizer context")
					}
					if version == "2.0" {
						if event["rawQueryString"] != "x=one&x=two&plus=a%2Bb" || event["headers"].(map[string]any)["x-repeated"] != "one,two" {
							t.Fatal("native v2 repeated values changed")
						}
					} else {
						if len(event["multiValueQueryStringParameters"].(map[string]any)["x"].([]any)) != 2 {
							t.Fatal("native v1 repeated values lost")
						}
					}
				}
				readiness, err := client.Get(frontend.URL + "/.eventbus/ready")
				if err != nil {
					t.Fatal(err)
				}
				readyBody, _ := io.ReadAll(readiness.Body)
				readiness.Body.Close()
				if readiness.StatusCode != 200 || !strings.Contains(string(readyBody), "eventbus-gateway") {
					t.Fatalf("management readiness changed: %d %s", readiness.StatusCode, readyBody)
				}
				for _, tc := range []struct {
					credential string
					status     int
				}{{"", 401}, {"invalid", 403}, {"Bearer owned", 200}} {
					request, _ := http.NewRequest("GET", frontend.URL+"/health?original=1", nil)
					request.Header.Set("Authorization", tc.credential)
					response, err := client.Do(request)
					if err != nil {
						t.Fatal(err)
					}
					data, _ := io.ReadAll(response.Body)
					response.Body.Close()
					if response.StatusCode != tc.status {
						t.Fatalf("application health with credential %q: %d %s", tc.credential, response.StatusCode, data)
					}
					if tc.status == 200 {
						var event map[string]any
						if err := json.Unmarshal(data, &event); err != nil {
							t.Fatal(err)
						}
						pathKey := "path"
						if version == "2.0" {
							pathKey = "rawPath"
						}
						if event["version"] != version || event[pathKey] != "/health" {
							t.Fatalf("real runtime health path changed: %v", event)
						}
					}
				}
				for _, path := range []string{"/docs", "/docs/", "/docs/assets/nested.js"} {
					response, err := client.Get(frontend.URL + path)
					if err != nil {
						t.Fatal(err)
					}
					data, _ := io.ReadAll(response.Body)
					response.Body.Close()
					if response.StatusCode != 200 {
						t.Fatalf("native docs path %s: %d %s", path, response.StatusCode, data)
					}
					var event map[string]any
					if err := json.Unmarshal(data, &event); err != nil {
						t.Fatal(err)
					}
					pathKey := "path"
					if version == "2.0" {
						pathKey = "rawPath"
					}
					if event[pathKey] != path {
						t.Fatalf("real %s runtime rewrote docs path: %v", runtime, event)
					}
				}
				binary, _ := http.NewRequest("POST", frontend.URL+"/public/42?binary=1", bytes.NewReader([]byte{0, 255, 1}))
				binary.Header.Set("Content-Type", "application/octet-stream")
				response, err := client.Do(binary)
				if err != nil {
					t.Fatal(err)
				}
				body, _ := io.ReadAll(response.Body)
				response.Body.Close()
				if response.StatusCode != 200 || !bytes.Equal(body, []byte{0, 255, 1}) {
					t.Fatalf("real runtime binary result %d %v", response.StatusCode, body)
				}
				denied, _ := http.NewRequest("POST", frontend.URL+"/private/42", nil)
				denied.Header.Set("Authorization", "invalid")
				response, err = client.Do(denied)
				if err != nil {
					t.Fatal(err)
				}
				response.Body.Close()
				if response.StatusCode != 403 {
					t.Fatalf("IAM denied request returned %d", response.StatusCode)
				}
				cfg.Routes[0].Integration.InvokeURL = aws.URL + "/2015-03-31/functions/" + runtime + "-app/invocations?Qualifier=missing"
				unknown, err := gateway.New(cfg, gateway.Options{Logger: zerolog.Nop()})
				if err != nil {
					t.Fatal(err)
				}
				defer unknown.Close()
				request := httptest.NewRequest("POST", "/private/42", nil)
				request.Header.Set("Authorization", "Bearer owned")
				recorder := httptest.NewRecorder()
				unknown.ServeHTTP(recorder, request)
				if recorder.Code != 500 {
					t.Fatalf("missing alias fell back to base function: %d %s", recorder.Code, recorder.Body.String())
				}
			})
		}
	}
}
