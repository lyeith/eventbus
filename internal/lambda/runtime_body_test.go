package lambda

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type runtimeBodyReadError struct{}

func (runtimeBodyReadError) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

// Content-Length supplies capacity only. Actual bytes and read errors still
// decide admission; neither a small declaration nor missing length truncates.
func TestRuntimeResponseBoundedReadPreservesNativeAdmission(t *testing.T) {
	for _, test := range []struct {
		name, payload, errorType string
		length                   int64
		status                   int
	}{
		{"known", `{"ok":true}`, "", 11, http.StatusAccepted},
		{"underdeclared", `{"ok":true}`, "", 1, http.StatusAccepted},
		{"overdeclared", `{}`, "", 1 << 40, http.StatusAccepted},
		{"unknown", `[true,"é"]`, "", -1, http.StatusAccepted},
		{"invalid", `not JSON`, "Runtime.InvalidResponse", 8, http.StatusAccepted},
		{"exact_limit", "\"" + strings.Repeat("x", maxPayload-2) + "\"", "", maxPayload, http.StatusAccepted},
		{"over_limit_underdeclared", "\"" + strings.Repeat("x", maxPayload-1) + "\"", "Function.ResponseSizeTooLarge", 1, http.StatusRequestEntityTooLarge},
	} {
		t.Run(test.name, func(t *testing.T) {
			owner := &runtimeInvocation{ctx: context.Background(), input: invocation{requestID: "request"}, delivered: true, result: make(chan invocationResult, 1)}
			request := httptest.NewRequest(http.MethodPost, runtimePrefix+"invocation/request/response", strings.NewReader(test.payload))
			request.ContentLength = test.length
			reply := httptest.NewRecorder()
			owner.ServeHTTP(reply, request)
			if reply.Code != test.status {
				t.Fatalf("status %d expected %d", reply.Code, test.status)
			}
			result := <-owner.result
			if result.functionError != (test.errorType != "") {
				t.Fatalf("function error %+v", result)
			}
			if test.errorType == "" && string(result.payload) != test.payload {
				t.Fatal("response bytes changed")
			}
			if test.errorType != "" && !strings.Contains(string(result.payload), `"errorType":"`+test.errorType+`"`) {
				t.Fatalf("native error %s", result.payload)
			}
			duplicate := httptest.NewRecorder()
			owner.ServeHTTP(duplicate, httptest.NewRequest(http.MethodPost, runtimePrefix+"invocation/request/response", strings.NewReader(`{}`)))
			if duplicate.Code != http.StatusConflict {
				t.Fatalf("terminal claim reused: %d", duplicate.Code)
			}
		})
	}
}

func TestRuntimeResponseReadFailureDoesNotClaimCompletion(t *testing.T) {
	owner := &runtimeInvocation{ctx: context.Background(), input: invocation{requestID: "request"}, delivered: true, result: make(chan invocationResult, 1)}
	request := httptest.NewRequest(http.MethodPost, runtimePrefix+"invocation/request/response", nil)
	request.Body = io.NopCloser(io.MultiReader(strings.NewReader(`{"partial":`), runtimeBodyReadError{}))
	request.ContentLength = 100
	reply := httptest.NewRecorder()
	owner.ServeHTTP(reply, request)
	if reply.Code != http.StatusBadRequest || owner.completed || len(owner.result) != 0 {
		t.Fatalf("read failure admitted: %d completed=%t", reply.Code, owner.completed)
	}
	retry := httptest.NewRecorder()
	owner.ServeHTTP(retry, httptest.NewRequest(http.MethodPost, runtimePrefix+"invocation/request/response", strings.NewReader(`{"ok":true}`)))
	if retry.Code != http.StatusAccepted {
		t.Fatalf("read failure prevented native retry: %d", retry.Code)
	}
	result := <-owner.result
	if result.functionError || string(result.payload) != `{"ok":true}` {
		t.Fatalf("retry result: %+v", result)
	}
}
