package gateway

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
)

// validateDevHealth owns the harness readiness reservation. Its path is on the
// listener, independent of the application's API mapping and native events.
func (cfg *Config) validateDevHealth() error {
	if cfg.DevHealthPath == "" {
		cfg.DevHealthPath = "/health"
	}
	template, err := parseTemplate(cfg.DevHealthPath)
	endpoint := &url.URL{Path: cfg.DevHealthPath}
	if err != nil || path.Clean(cfg.DevHealthPath) != cfg.DevHealthPath || endpoint.EscapedPath() != cfg.DevHealthPath {
		return fmt.Errorf("dev_health_path must be a canonical absolute literal path")
	}
	for _, item := range template.segments {
		if item.name != "" {
			return fmt.Errorf("dev_health_path cannot contain path parameters")
		}
	}
	routingPath, mapped := apiPath(cfg.BasePath, cfg.DevHealthPath)
	if !mapped {
		return nil
	}
	for _, route := range cfg.Routes {
		if (route.Method != http.MethodGet && route.Method != "ANY") || route.RouteKey == "$default" {
			continue
		}
		application, _ := parseTemplate(route.Path) // Native route validation runs first.
		// Catch-all routes retain the intentional readiness exception, including
		// the historical /health default. Concrete route collisions fail startup.
		if _, matches := application.match(routingPath); matches && !application.isGreedy() {
			return fmt.Errorf("dev_health_path %q conflicts with application route %q; select another readiness path", cfg.DevHealthPath, route.RouteKey)
		}
	}
	return nil
}

func (gateway *Gateway) serveDevHealth(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodGet || r.URL.Path != gateway.config.DevHealthPath {
		return false
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, `{"status":"healthy","service":"eventbus-gateway"}`)
	return true
}
