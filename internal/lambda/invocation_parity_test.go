package lambda

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestHTTPAndTypedInvocationTargetContextParity(t *testing.T) {
	functions := make(map[string]Function)
	for _, name := range []string{"echo", "echo:2", "echo:live", "error:2"} {
		mode := "echo"
		if name == "error:2" {
			mode = "error"
		}
		function := providedFunction(t, mode)
		function.Environment["AWS_REGION"] = "ap-south-1"
		function.Environment["AWS_ACCOUNT_ID"] = "987654321098"
		functions[name] = function
	}
	service := newTestService(t, functions, t.TempDir())
	payload := []byte(`{"parity":true,"nested":[1,2]}`)
	clientContext := `{"custom":{"origin":"invocation-parity"}}`
	for _, test := range []struct {
		name, function, qualifier, arn, version string
		functionError                           bool
	}{
		{"numeric-name", "echo:2", "", "arn:aws:lambda:ap-south-1:987654321098:function:echo:2", "2", false},
		{"full-arn-qualifier", "arn:aws:lambda:eu-west-1:123456789012:function:echo", "2", "arn:aws:lambda:eu-west-1:123456789012:function:echo:2", "2", false},
		{"partial-arn-alias", "123456789012:function:echo", "live", "arn:aws:lambda:ap-south-1:123456789012:function:echo:live", "$LATEST", false},
		{"explicit-latest", "echo", "$LATEST", "arn:aws:lambda:ap-south-1:987654321098:function:echo:$LATEST", "$LATEST", false},
		{"function-error", "error", "2", "", "2", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := InvokeInput{FunctionName: test.function, Qualifier: test.qualifier, Payload: payload, ClientContext: clientContext}
			typed, err := service.Execute(context.Background(), input)
			if err != nil || typed.FunctionError != test.functionError || typed.RequestID == "" || typed.ExecutedVersion != test.version {
				t.Fatalf("typed invocation changed: %+v; %v", typed, err)
			}
			path := invokePrefix + test.function + invokeSuffix
			if test.qualifier != "" {
				path += "?" + (url.Values{"Qualifier": []string{test.qualifier}}).Encode()
			}
			request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(payload))
			request.Header.Set("X-Amz-Client-Context", base64.StdEncoding.EncodeToString([]byte(clientContext)))
			request.Header.Set("X-Amz-Log-Type", "Tail")
			response := httptest.NewRecorder()
			service.ServeHTTP(response, request)
			wantError := ""
			if test.functionError {
				wantError = "Unhandled"
			}
			if response.Code != http.StatusOK || response.Header().Get("X-Amz-Function-Error") != wantError ||
				response.Header().Get("X-Amz-Executed-Version") != typed.ExecutedVersion || response.Header().Get("X-Amzn-RequestId") == "" {
				t.Fatalf("HTTP result disagreed with typed invocation: %d %v %s", response.Code, response.Header(), response.Body.String())
			}
			logs, err := base64.StdEncoding.DecodeString(response.Header().Get("X-Amz-Log-Result"))
			if err != nil || !strings.Contains(string(logs), "Go application stdout") || !strings.Contains(string(logs), "Go application stderr") {
				t.Fatalf("HTTP Tail changed: %q; %v", logs, err)
			}
			if test.functionError {
				if !bytes.Equal(typed.Payload, response.Body.Bytes()) || !bytes.Contains(typed.Payload, []byte(`"errorMessage":"Unauthorized"`)) {
					t.Fatalf("HTTP rewrote native function failure: typed=%s; HTTP=%s", typed.Payload, response.Body.Bytes())
				}
				return
			}
			var wire, direct struct {
				Input        json.RawMessage `json:"input"`
				RequestID    string          `json:"requestId"`
				InvocationID string          `json:"invocationId"`
				ARN          string          `json:"arn"`
				Client       string          `json:"client"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &wire); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(typed.Payload, &direct); err != nil {
				t.Fatal(err)
			}
			if wire.ARN != test.arn || direct.ARN != test.arn || wire.Client != clientContext || direct.Client != clientContext ||
				!bytes.Equal(wire.Input, payload) || !bytes.Equal(direct.Input, payload) ||
				wire.RequestID == "" || wire.RequestID != wire.InvocationID ||
				direct.RequestID != typed.RequestID || direct.RequestID != direct.InvocationID || wire.RequestID == direct.RequestID {
				t.Fatalf("native target/context/actual invocation identity diverged: HTTP=%+v; typed=%+v", wire, direct)
			}
		})
	}
}

func TestHTTPRequestResponseCancellationStillSuppressesNativeReply(t *testing.T) {
	service := newTestService(t, map[string]Function{"echo": providedFunction(t, "echo")}, t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.Execute(ctx, InvokeInput{FunctionName: "echo", Payload: []byte(`{}`)}); !errors.Is(err, context.Canceled) {
		t.Fatalf("typed cancellation changed: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, invokePrefix+"echo"+invokeSuffix, strings.NewReader(`{}`)).WithContext(ctx)
	request.Header.Set("X-Amz-Log-Type", "Tail")
	response := httptest.NewRecorder()
	service.ServeHTTP(response, request)
	if response.Body.Len() != 0 || response.Header().Get("X-Amz-Executed-Version") != "" ||
		response.Header().Get("X-Amz-Function-Error") != "" || response.Header().Get("X-Amz-Log-Result") != "" {
		t.Fatalf("canceled HTTP invocation exposed a synthesized result: %d %v %s", response.Code, response.Header(), response.Body.String())
	}
	service.mu.Lock()
	active := len(service.active)
	service.mu.Unlock()
	if active != 0 {
		t.Fatal("HTTP returned before canceled invocation ownership joined")
	}
}
