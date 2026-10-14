package lambda

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestWorkerCrashHTTPSeparateStrictTargetOnlyContract(t *testing.T) {
	service, task := workerCrashTestOwner("queued", 0)
	handler := NewDevWorkerCrashHandler(service)
	body := "{\"function_name\":\"processor:live\",\"attempt\":1}"
	path := devWorkerInvocationPath + "/" + task.record.RequestID + "/worker-crash"
	for _, method := range []string{http.MethodPut, http.MethodDelete} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(method, path, strings.NewReader(body)))
		if response.Code != 200 || strings.Contains(response.Body.String(), "pid") {
			t.Fatalf("control:%d %s", response.Code, response.Body.String())
		}
	}
	for _, test := range []struct {
		method, path, body string
		status             int
	}{
		{http.MethodPost, path, body, 405},
		{http.MethodPut, path, "{}", 400},
		{http.MethodPut, path, "null", 400},
		{http.MethodPut, path, body + "{}", 400},
		{http.MethodPut, path, strings.Replace(body, "}", ",\"payload\":{}}", 1), 400},
		{http.MethodPut, path, strings.Replace(body, "1", "0", 1), 400},
		{http.MethodPut, path, strings.Replace(body, "1", "4", 1), 400},
		{http.MethodPut, path, body + strings.Repeat(" ", 4096), 400},
		{http.MethodPut, devWorkerInvocationPath + "/wrong/worker-crash", body, 400},
		{http.MethodPut, devWorkerInvocationPath + "/" + uuid.NewString() + "/worker-crash", body, 404},
		{http.MethodPut, devWorkerInvocationPath + "/events/worker-crash", body, 400},
		{http.MethodPost, devWorkerInvocationPath + "/worker-crash", body, 404},
		{http.MethodPut, invokePrefix + "processor:live" + invokeSuffix, body, 404},
		{http.MethodGet, devWorkerInvocationPath + "/", body, 404},
		{http.MethodPost, devWorkerInvocationPath, body, 405},
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(test.method, test.path, strings.NewReader(test.body)))
		if response.Code != test.status {
			t.Fatalf("%s %s:got%d wanted%d %s", test.method, test.path, response.Code, test.status, response.Body.String())
		}
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, devWorkerInvocationPath, nil))
	if response.Code != 200 || !strings.Contains(response.Body.String(), "\"invocations\":[]") || !strings.Contains(response.Body.String(), task.record.RequestID) {
		t.Fatalf("native snapshots:%s", response.Body.String())
	}
	for _, disabled := range []*Service{nil, {}} {
		response := httptest.NewRecorder()
		NewDevWorkerCrashHandler(disabled).ServeHTTP(response, httptest.NewRequest(http.MethodGet, devWorkerInvocationPath, nil))
		if response.Code != 404 {
			t.Fatal("default enabled fault control")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := service.DevArmWorkerCrash(ctx, task.record.FunctionName, task.record.RequestID, 1)
	if err != context.Canceled {
		t.Fatalf("interrupted control:%v", err)
	}
}

func TestWorkerCrashConfigurationPreservesPrivateAndNormalDefaults(t *testing.T) {
	config := &Config{Functions: map[string]Function{"fixture": {Runtime: "command", Command: []string{"echo"}}}, ExecutionPolicy: &ExecutionPolicy{PrivateErrors: true}, DevWorkerCrash: &DevWorkerCrashConfig{Enabled: true}}
	if err := config.Validate(); err == nil {
		t.Fatal("private execution enabled crash controls")
	}
	config.DevWorkerCrash.Enabled = false
	if err := config.Validate(); err != nil {
		t.Fatalf("explicit disabled private:%v", err)
	}
	config.DevWorkerCrash = nil
	if err := config.Validate(); err != nil {
		t.Fatalf("normal default:%v", err)
	}
}
