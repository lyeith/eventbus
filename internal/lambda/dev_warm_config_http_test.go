package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type warmControlProbe struct {
	calls []DevWarmConfig
	err   error
}

func (probe *warmControlProbe) DevUpdateWarm(_ context.Context, config DevWarmConfig) (DevWarmConfig, error) {
	probe.calls = append(probe.calls, config)
	return config, probe.err
}

func TestDevWarmHTTPStrictExplicitLimitsAndNamespace(t *testing.T) {
	probe := &warmControlProbe{}
	handler := NewDevWarmHandler(probe)
	for _, limits := range []DevWarmConfig{{1, 1}, {32, 1024}} {
		body := fmt.Sprintf("{\"max_workers\":%d,\"max_concurrency\":%d}", limits.MaxWorkers, limits.MaxConcurrency)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, devWarmPath, strings.NewReader(body)))
		if response.Code != http.StatusOK {
			t.Fatalf("valid control: %d %s", response.Code, response.Body.String())
		}
		var published DevWarmConfig
		if err := json.Unmarshal(response.Body.Bytes(), &published); err != nil || published != limits {
			t.Fatalf("published limits: %+v %v", published, err)
		}
	}
	for _, body := range []string{
		"", "null", "[]", "{}", "{\"max_workers\":1}", "{\"max_concurrency\":1}",
		"{\"max_workers\":null,\"max_concurrency\":1}",
		"{\"max_workers\":0,\"max_concurrency\":1}",
		"{\"max_workers\":-1,\"max_concurrency\":1}",
		"{\"max_workers\":33,\"max_concurrency\":1}",
		"{\"max_workers\":1,\"max_concurrency\":0}",
		"{\"max_workers\":1,\"max_concurrency\":1025}",
		"{\"max_workers\":1.5,\"max_concurrency\":1}",
		"{\"max_workers\":\"1\",\"max_concurrency\":1}",
		"{\"max_workers\":1,\"max_concurrency\":1,\"private\":\"secret-source\"}",
		"{\"max_workers\":1,\"max_concurrency\":1} {}",
		"{\"max_workers\":1,\"max_concurrency\":1}" + strings.Repeat(" ", 4096),
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, devWarmPath, strings.NewReader(body)))
		if response.Code != http.StatusBadRequest || strings.Contains(response.Body.String(), "secret-source") {
			t.Fatalf("invalid body %q: %d %s", body, response.Code, response.Body.String())
		}
	}
	for _, route := range []string{devWarmPath + "/", devWarmPath + "/other", "/__eventbus/dev/lambda/warmish", invokePrefix + "warm" + invokeSuffix} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, route, strings.NewReader("{}")))
		if response.Code != http.StatusNotFound {
			t.Fatalf("route %s: %d", route, response.Code)
		}
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPut, devWarmPath, nil))
	if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("method: %d %v", response.Code, response.Header())
	}
	if len(probe.calls) != 2 {
		t.Fatalf("rejected requests mutated limits: %+v", probe.calls)
	}
}

func TestDevWarmHTTPStaticErrorsAndDisabledPool(t *testing.T) {
	for _, test := range []struct {
		name   string
		port   DevWarmUpdater
		status int
		code   string
	}{
		{"absent", nil, 404, "warm_disabled"},
		{"nil-service", (*Service)(nil), 404, "warm_disabled"},
		{"fresh-service", &Service{}, 404, "warm_disabled"},
		{"invalid", &warmControlProbe{err: devWarmControlError("invalid_warm_limits")}, 400, "invalid_warm_limits"},
		{"unavailable", &warmControlProbe{err: devWarmControlError("warm_unavailable")}, 503, "warm_unavailable"},
		{"deadline", &warmControlProbe{err: fmt.Errorf("secret-path: %w", context.DeadlineExceeded)}, 504, "warm_update_wait_interrupted"},
		{"canceled", &warmControlProbe{err: context.Canceled}, 504, "warm_update_wait_interrupted"},
		{"private-runtime", &warmControlProbe{err: errors.New("secret-path credentials")}, 503, "warm_update_failed"},
		{"unknown-kind", &warmControlProbe{err: devWarmControlError("secret-path")}, 503, "warm_update_failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			NewDevWarmHandler(test.port).ServeHTTP(response, httptest.NewRequest(http.MethodPost, devWarmPath,
				strings.NewReader("{\"max_workers\":1,\"max_concurrency\":1}")))
			if response.Code != test.status || strings.TrimSpace(response.Body.String()) != "{\"error\":\""+test.code+"\"}" {
				t.Fatalf("error: %d %s", response.Code, response.Body.String())
			}
		})
	}
}
