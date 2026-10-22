// Development HTTP composition. Local outcomes and tracked views are separate from
// AWS SES sending and configuration-set operations.
package app

import (
	"net/http"
	"strings"

	"github.com/lyeith/eventbus/internal/ses"
)

type devSESOutcomeHandler interface {
	ServeDevOutcome(http.ResponseWriter, *http.Request)
}

type devSESTrackingHandler interface {
	ServeDevTracking(http.ResponseWriter, *http.Request)
}

func withDevSES(next http.Handler, outcomes devSESOutcomeHandler, tracking devSESTrackingHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == ses.DevOutcomePath {
			outcomes.ServeDevOutcome(w, r)
			return
		}
		if strings.HasPrefix(r.URL.Path, ses.DevTrackingMessagesPrefix) || strings.HasPrefix(r.URL.Path, ses.DevTrackingPixelPrefix) {
			if tracking == nil {
				http.NotFound(w, r)
			} else {
				tracking.ServeDevTracking(w, r)
			}
			return
		}
		next.ServeHTTP(w, r)
	})
}
