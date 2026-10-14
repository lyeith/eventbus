// Named development HTTP adapter for an existing warm pool. Hosts attach it
// separately from Lambda's native AWS surface and private Cognito execution.
package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
)

const devWarmPath = "/__eventbus/dev/lambda/warm"

// DevWarmUpdater is the HTTP adapter's consumer-owned runtime port.
type DevWarmUpdater interface {
	DevUpdateWarm(context.Context, DevWarmConfig) (DevWarmConfig, error)
}

// NewDevWarmHandler updates the existing public pool without reading recipes or
// restarting the broker. Hosts retain ingress/lifecycle admission responsibility.
func NewDevWarmHandler(updater DevWarmUpdater) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writeError := func(status int, code string) {
			writer.WriteHeader(status)
			_ = json.NewEncoder(writer).Encode(map[string]string{"error": code})
		}
		if request.URL.Path != devWarmPath {
			writeError(http.StatusNotFound, "warm_control_route_not_found")
			return
		}
		if request.Method != http.MethodPost {
			writer.Header().Set("Allow", http.MethodPost)
			writeError(http.StatusMethodNotAllowed, "warm_update_requires_post")
			return
		}
		var config DevWarmConfig
		if err := decodeDevControlBody(request, 4096, &config); err != nil || validateDevWarmUpdate(config) != nil {
			writeError(http.StatusBadRequest, "invalid_warm_limits")
			return
		}
		if updater == nil {
			writeError(http.StatusNotFound, "warm_disabled")
			return
		}
		published, err := updater.DevUpdateWarm(request.Context(), config)
		if err != nil {
			status, code := http.StatusServiceUnavailable, "warm_update_failed"
			switch {
			case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
				status, code = http.StatusGatewayTimeout, "warm_update_wait_interrupted"
			default:
				var control *DevWarmControlError
				if errors.As(err, &control) {
					switch control.Code {
					case "invalid_warm_limits":
						status, code = http.StatusBadRequest, control.Code
					case "warm_disabled":
						status, code = http.StatusNotFound, control.Code
					case "warm_unavailable":
						code = control.Code
					}
				}
			}
			writeError(status, code)
			return
		}
		_ = json.NewEncoder(writer).Encode(published)
	})
}
