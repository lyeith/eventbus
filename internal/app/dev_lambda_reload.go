// Development recipe reload composition. AWS function management remains owned
// by Lambda's native surface; this endpoint only reloads the selected local recipe.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"strings"

	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
)

const devLambdaReloadPrefix = "/__eventbus/dev/lambda/functions/"

type devFunctionRegistrar interface {
	RegisterFunction(context.Context, string, lambdaservice.Function) error
}

func withDevLambdaReload(next http.Handler, functions devFunctionRegistrar, filename, workDir string) http.Handler {
	if functions == nil || filename == "" {
		return next
	}
	if !filepath.IsAbs(filename) {
		filename = filepath.Join(workDir, filename)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, devLambdaReloadPrefix) {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		writeError := func(status int, code string) {
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": code})
		}
		path := strings.TrimPrefix(r.URL.Path, devLambdaReloadPrefix)
		name, matched := strings.CutSuffix(path, "/reload")
		if !matched || name == "" || strings.Contains(name, "/") {
			writeError(http.StatusNotFound, "reload_route_not_found")
			return
		}
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeError(http.StatusMethodNotAllowed, "reload_requires_post")
			return
		}
		recipe, err := lambdaservice.LoadConfig(filename)
		if err != nil {
			writeError(http.StatusBadRequest, "function_recipe_unavailable")
			return
		}
		function, found := recipe.Functions[name]
		if !found {
			writeError(http.StatusNotFound, "function_not_in_recipe")
			return
		}
		// RegisterFunction publishes the validated generation before joining old
		// workers. A wait deadline does not roll back a published registration.
		if err := functions.RegisterFunction(r.Context(), name, function); err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				writeError(http.StatusGatewayTimeout, "reload_wait_interrupted")
			} else {
				writeError(http.StatusServiceUnavailable, "reload_failed")
			}
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"function_name": name, "status": "reloaded"})
	})
}
