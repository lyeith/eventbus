// Development HTTP composition. Local outcomes are explicitly separate from
// AWS SES sending and configuration-set operations.
package app

import (
	"net/http"

	"github.com/lyeith/eventbus/internal/ses"
)

type devSESOutcomeHandler interface {
	ServeDevOutcome(http.ResponseWriter, *http.Request)
}

func withDevSESOutcomes(next http.Handler, outcomes devSESOutcomeHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == ses.DevOutcomePath {
			outcomes.ServeDevOutcome(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}
