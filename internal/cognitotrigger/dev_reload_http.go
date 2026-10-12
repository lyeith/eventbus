// Local development HTTP adapter. Native Cognito APIs and hidden Lambda
// targets remain separate from this source-only control.
package cognitotrigger

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

const devReloadPrefix = "/__eventbus/dev/cognito/pools/"

// NewDevReloadHandler serves only the selected pool/trigger reload control.
// Hosts register it separately from native Cognito and Lambda routing.
func NewDevReloadHandler(reloader DevTriggerReloader) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writeError := func(status int, code string) {
			writer.WriteHeader(status)
			_ = json.NewEncoder(writer).Encode(map[string]string{"error": code})
		}
		if !strings.HasPrefix(request.URL.Path, devReloadPrefix) {
			writeError(http.StatusNotFound, "reload_route_not_found")
			return
		}
		parts := strings.Split(strings.TrimPrefix(request.URL.Path, devReloadPrefix), "/")
		if len(parts) != 4 || parts[0] == "" || parts[1] != "triggers" || parts[2] == "" || parts[3] != "reload" {
			writeError(http.StatusNotFound, "reload_route_not_found")
			return
		}
		if request.Method != http.MethodPost {
			writer.Header().Set("Allow", http.MethodPost)
			writeError(http.StatusMethodNotAllowed, "reload_requires_post")
			return
		}
		if reloader == nil {
			writeError(http.StatusNotFound, "trigger_not_configured")
			return
		}
		if err := reloader.DevReloadTrigger(request.Context(), parts[0], parts[2]); err != nil {
			status, code := http.StatusServiceUnavailable, "reload_failed"
			var failure *DevReloadError
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				status, code = http.StatusGatewayTimeout, "reload_wait_interrupted"
			} else if errors.As(err, &failure) {
				switch failure.Kind {
				case DevReloadNotConfigured:
					status, code = http.StatusNotFound, "trigger_not_configured"
				case DevReloadWaitInterrupted:
					status, code = http.StatusGatewayTimeout, "reload_wait_interrupted"
				case DevReloadUnsupported:
					code = "trigger_reload_unsupported"
				case DevReloadClosed:
					code = "trigger_reload_closed"
				}
			}
			writeError(status, code)
			return
		}

		_ = json.NewEncoder(writer).Encode(map[string]string{"pool_id": parts[0], "trigger": parts[2], "status": "reloaded"})
	})
}
