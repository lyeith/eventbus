package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/gateway"
	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// A real provided-runtime subprocess keeps the native reservation occupied.
// It writes only its actual request identity, never the private event or env.
func TestAdmissionDiagnosticsProvidedProcess(t *testing.T) {
	if os.Getenv("EVENTBUS_ADMISSION_DIAGNOSTIC_PROCESS") != "1" {
		return
	}
	endpoint := "http://" + os.Getenv("AWS_LAMBDA_RUNTIME_API") + "/2018-06-01/runtime/invocation/"
	client := &http.Client{Timeout: 15 * time.Second}
	response, err := client.Get(endpoint + "next")
	if err != nil {
		os.Exit(2)
	}
	requestID := response.Header.Get("Lambda-Runtime-Aws-Request-Id")
	var event map[string]any
	err = json.NewDecoder(response.Body).Decode(&event)
	_ = response.Body.Close()
	if err != nil || requestID == "" {
		os.Exit(3)
	}
	root := os.Getenv("EVENTBUS_ADMISSION_DIAGNOSTIC_ROOT")
	if err := os.WriteFile(filepath.Join(root, "started-"+requestID), []byte(os.Getenv("EVENTBUS_ADMISSION_DIAGNOSTIC_FUNCTION")), 0600); err != nil {
		os.Exit(4)
	}
	if event["hold"] == true {
		deadline := time.Now().Add(12 * time.Second)
		for {
			if _, err := os.Stat(filepath.Join(root, "release")); err == nil {
				break
			}
			if time.Now().After(deadline) {
				os.Exit(5)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	result := map[string]any{"statusCode": 200, "body": fmt.Sprintf(`{"requestId":%q}`, requestID)}
	if event["type"] == "REQUEST" {
		result = map[string]any{"principalId": "provided-user", "policyDocument": map[string]any{
			"Version": "2012-10-17", "Statement": []any{map[string]any{"Action": "execute-api:Invoke", "Effect": "Allow", "Resource": event["methodArn"]}},
		}}
	}
	payload, _ := json.Marshal(result)
	reply, err := client.Post(endpoint+requestID+"/response", "application/json", bytes.NewReader(payload))
	if err != nil {
		os.Exit(6)
	}
	_ = reply.Body.Close()
	if reply.StatusCode != http.StatusAccepted {
		os.Exit(7)
	}
	os.Exit(0)
}

type admissionDiagnosticIDs struct{ ids chan string }

func (activity admissionDiagnosticIDs) BeginActivity(kind, requestID string) (func(error), error) {
	if kind == "lambda_invoke" {
		activity.ids <- requestID
	}
	return func(error) {}, nil
}

type privateAdmissionDiagnostic struct {
	RequestID          string            `json:"request_id"`
	FunctionName       string            `json:"function_name"`
	FunctionARN        string            `json:"function_arn"`
	State              string            `json:"state"`
	OwnershipConfirmed bool              `json:"ownership_confirmed"`
	FunctionError      bool              `json:"function_error"`
	AdmissionError     json.RawMessage   `json:"admission_error"`
	ProcessError       string            `json:"process_error"`
	ContextError       string            `json:"context_error"`
	TerminationCause   string            `json:"termination_cause"`
	OwnershipError     string            `json:"ownership_error"`
	ExecutionPhases    []json.RawMessage `json:"execution_phases"`
	Stdout             struct {
		Data  string `json:"data"`
		Bytes int64  `json:"bytes"`
	} `json:"stdout"`
	Stderr struct {
		Data  string `json:"data"`
		Bytes int64  `json:"bytes"`
	} `json:"stderr"`
	Tail struct {
		Data  string `json:"data"`
		Bytes int64  `json:"bytes"`
	} `json:"tail"`
}

func TestNativeLambdaAndGatewayAdmissionDiagnostics(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "private-diagnostics.jsonl")
	ids := admissionDiagnosticIDs{ids: make(chan string, 16)}
	executable, err := os.Executable()
	require.NoError(t, err)
	function := func(name string) lambdaservice.Function {
		return lambdaservice.Function{Runtime: "provided", Command: []string{executable, "-test.run=^TestAdmissionDiagnosticsProvidedProcess$"}, Timeout: 15 * time.Second,
			Environment: map[string]string{"EVENTBUS_ADMISSION_DIAGNOSTIC_PROCESS": "1", "EVENTBUS_ADMISSION_DIAGNOSTIC_ROOT": root,
				"EVENTBUS_ADMISSION_DIAGNOSTIC_FUNCTION": name, "PRIVATE_FUNCTION_SECRET": "environment-value-must-not-be-captured"}}
	}
	functions, err := lambdaservice.NewService(&lambdaservice.Config{
		Functions: map[string]lambdaservice.Function{"holder:live": function("holder:live"), "integration:live": function("integration:live"), "authorizer:live": function("authorizer:live")},
		DevWarm:   &lambdaservice.DevWarmConfig{MaxWorkers: 1, MaxConcurrency: 1}, DevActivity: ids,
		DevDiagnostics: &lambdaservice.DevDiagnosticsConfig{LogPath: path}, DevAsync: &lambdaservice.DevAsyncConfig{LogWriter: io.Discard},
	}, root)
	require.NoError(t, err)
	aws := httptest.NewServer(server.New(server.Services{Lambda: functions}))
	client := &http.Client{Timeout: 20 * time.Second}
	t.Cleanup(func() {
		require.NoError(t, os.WriteFile(filepath.Join(root, "release"), nil, 0600))
		aws.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, functions.Close(ctx))
		client.CloseIdleConnections()
	})
	invokeURL := func(name string) string { return aws.URL + "/2015-03-31/functions/" + name + "/invocations" }
	type reply struct {
		status int
		header http.Header
		body   []byte
		err    error
	}
	invoke := func(name, payload string) reply {
		request, _ := http.NewRequest(http.MethodPost, invokeURL(name), bytes.NewBufferString(payload))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer native-header-must-not-be-captured")
		response, err := client.Do(request)
		if err != nil {
			return reply{err: err}
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		return reply{response.StatusCode, response.Header.Clone(), body, err}
	}
	nextID := func() string {
		select {
		case id := <-ids.ids:
			require.NotEmpty(t, id)
			return id
		case <-time.After(5 * time.Second):
			t.Fatal("native invocation identity was not admitted")
			return ""
		}
	}
	completed := make(chan reply, 1)
	go func() {
		completed <- invoke("holder:live", `{"hold":true,"secret":"event-value-must-not-be-captured"}`)
	}()
	holderID := nextID()
	require.Eventually(t, func() bool { _, err := os.Stat(filepath.Join(root, "started-"+holderID)); return err == nil }, 5*time.Second, 5*time.Millisecond, "actual native holder never started")

	zero := 0
	var backendCalls atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendCalls.Add(1)
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	t.Cleanup(backend.Close)
	edge, err := gateway.New(gateway.Config{
		Authorizers: map[string]gateway.AuthorizerConfig{"request": {Type: "REQUEST", InvokeURL: invokeURL("authorizer:live"), TTL: &zero, Timeout: 5 * time.Second}},
		Routes: []gateway.RouteConfig{
			{Path: "/integration", Method: "POST", Integration: gateway.IntegrationConfig{Type: "AWS_PROXY", InvokeURL: invokeURL("integration:live"), PayloadFormatVersion: "1.0", Timeout: 5 * time.Second}},
			{Path: "/authorized", Method: "GET", Authorizer: "request", Integration: gateway.IntegrationConfig{Type: "HTTP_PROXY", URI: backend.URL + "/"}},
		},
	}, gateway.Options{Logger: zerolog.Nop()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, edge.Close()) })
	gatewayInvoke := func(method, path string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(method, path, bytes.NewBufferString(`{"private":"gateway-body-must-not-be-captured"}`))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer gateway-auth-must-not-be-captured")
		request.Header.Set("Cookie", "private-cookie-must-not-be-captured")
		response := httptest.NewRecorder()
		edge.ServeHTTP(response, request)
		return response
	}
	rejected := map[string]string{}
	native := invoke("integration:live", `{"private":"rejected-event-must-not-be-captured"}`)
	require.NoError(t, native.err)
	require.Equal(t, http.StatusTooManyRequests, native.status, string(native.body))
	require.Equal(t, "TooManyRequestsException", native.header.Get("X-Amzn-ErrorType"))
	require.Equal(t, "1", native.header.Get("Retry-After"))
	rejected[nextID()] = "integration:live"
	for _, route := range []struct{ method, path, function string }{{"POST", "/integration", "integration:live"}, {"GET", "/authorized", "authorizer:live"}} {
		response := gatewayInvoke(route.method, route.path)
		require.Equal(t, http.StatusInternalServerError, response.Code, response.Body.String())
		require.JSONEq(t, `{"message":"Internal server error"}`, response.Body.String())
		rejected[nextID()] = route.function
	}
	require.Zero(t, backendCalls.Load(), "rejected REQUEST authorizer cannot call backend")
	readRecords := func() map[string]privateAdmissionDiagnostic {
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		records := map[string]privateAdmissionDiagnostic{}
		decoder := json.NewDecoder(bytes.NewReader(data))
		for {
			var record privateAdmissionDiagnostic
			err := decoder.Decode(&record)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			records[record.RequestID] = record
		}
		return records
	}
	records := readRecords()
	require.Len(t, records, 3)
	for id, name := range rejected {
		record, ok := records[id]
		require.True(t, ok, "diagnostic must retain actual native ID %s", id)
		require.Equal(t, name, record.FunctionName)
		require.Equal(t, "arn:aws:lambda:us-east-1:000000000000:function:"+name, record.FunctionARN)
		require.Equal(t, "not_started", record.State)
		require.True(t, record.OwnershipConfirmed)
		require.False(t, record.FunctionError)
		var admissionError map[string]any
		require.NoError(t, json.Unmarshal(record.AdmissionError, &admissionError))
		require.Equal(t, map[string]any{"code": "TooManyRequestsException", "http_status": float64(429)}, admissionError)
		require.Empty(t, record.ProcessError)
		require.Empty(t, record.ContextError)
		require.Empty(t, record.TerminationCause)
		require.Empty(t, record.OwnershipError)
		require.Empty(t, record.ExecutionPhases)
		require.Empty(t, record.Stdout.Data)
		require.Empty(t, record.Stderr.Data)
		require.Empty(t, record.Tail.Data)
		require.Zero(t, record.Stdout.Bytes)
		require.Zero(t, record.Stderr.Bytes)
		require.Zero(t, record.Tail.Bytes)
		_, err := os.Stat(filepath.Join(root, "started-"+id))
		require.ErrorIs(t, err, os.ErrNotExist, "rejected invocation must never execute handler")
	}
	require.NoError(t, os.WriteFile(filepath.Join(root, "release"), nil, 0600))
	select {
	case result := <-completed:
		require.NoError(t, result.err)
		require.Equal(t, http.StatusOK, result.status, string(result.body))
	case <-time.After(10 * time.Second):
		t.Fatal("held native handler did not complete")
	}
	for _, route := range []struct{ method, path string }{{"POST", "/integration"}, {"GET", "/authorized"}} {
		response := gatewayInvoke(route.method, route.path)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		id := nextID()
		data, err := os.ReadFile(filepath.Join(root, "started-"+id))
		require.NoError(t, err)
		require.NotEmpty(t, data)
	}
	require.EqualValues(t, 1, backendCalls.Load())
	records = readRecords()
	require.Len(t, records, 6)
	for id, record := range records {
		if _, rejected := rejected[id]; !rejected {
			require.Equal(t, "succeeded", record.State)
			require.Nil(t, record.AdmissionError)
			require.True(t, record.OwnershipConfirmed)
		}
	}
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	for _, private := range []string{"environment-value-must-not-be-captured", "event-value-must-not-be-captured", "native-header-must-not-be-captured", "gateway-auth-must-not-be-captured", "private-cookie-must-not-be-captured", "gateway-body-must-not-be-captured", "rejected-event-must-not-be-captured", "ConcurrentInvocationLimitExceeded", "Lambda execution concurrency is exhausted"} {
		require.NotContains(t, string(data), private)
	}
	require.NoError(t, functions.DevEvidence())
}
