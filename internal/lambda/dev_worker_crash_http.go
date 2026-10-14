// Separate development adapter; native Invoke has no fault selection fields.
package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

const devWorkerInvocationPath = "/__eventbus/dev/lambda/invocations"

func NewDevWorkerCrashHandler(service *Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fail := func(status int, code string) {
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": code})
		}
		if service == nil || !service.workerCrashEnabled {
			fail(404, "worker_crash_disabled")
			return
		}
		if r.URL.Path == devWorkerInvocationPath {
			if r.Method != http.MethodGet {
				w.Header().Set("Allow", http.MethodGet)
				fail(405, "worker_snapshot_requires_get")
				return
			}
			_ = json.NewEncoder(w).Encode(service.DevWorkerInvocations())
			return
		}
		id, matched := strings.CutSuffix(strings.TrimPrefix(r.URL.Path, devWorkerInvocationPath+"/"), "/worker-crash")
		if !strings.HasPrefix(r.URL.Path, devWorkerInvocationPath+"/") || !matched || id == "" || strings.Contains(id, "/") {
			fail(404, "worker_control_route_not_found")
			return
		}
		if r.Method != http.MethodPut && r.Method != http.MethodDelete {
			w.Header().Set("Allow", "PUT, DELETE")
			fail(405, "worker_crash_requires_put_or_delete")
			return
		}
		var selection struct {
			FunctionName string `json:"function_name"`
			Attempt      int    `json:"attempt"`
		}
		if err := decodeDevControlBody(r, 4096, &selection); err != nil {
			fail(400, "invalid_worker_crash_request")
			return
		}
		var result DevWorkerCrash
		var err error
		if r.Method == http.MethodPut {
			result, err = service.DevArmWorkerCrash(r.Context(), selection.FunctionName, id, selection.Attempt)
		} else {
			result, err = service.DevClearWorkerCrash(r.Context(), selection.FunctionName, id, selection.Attempt)
		}
		if err != nil {
			status, code := 503, "worker_crash_failed"
			var control *DevWorkerCrashError
			if errors.As(err, &control) {
				status, code = control.Status, control.Code
			} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				code = "worker_crash_interrupted"
			}
			fail(status, code)
			return
		}
		_ = json.NewEncoder(w).Encode(result)
	})
}
