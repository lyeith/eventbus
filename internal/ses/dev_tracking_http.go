package ses

import (
	"encoding/base64"
	"encoding/json"
	"mime"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/lyeith/eventbus/internal/awsprotocol"
)

const DevTrackingMessagesPrefix = "/__eventbus/dev/ses/messages/"
const DevTrackingPixelPrefix = "/__eventbus/dev/ses/tracking/"

// A transparent one-pixel GIF; no resource fetch itself renders message HTML.
var devTrackingPixel, _ = base64.StdEncoding.DecodeString("R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7")

// ServeDevTracking owns explicit local discovery, isolated rendered views and
// actual pixel fetches. It is separate from native sending and outcome controls.
func (handler *Handler) ServeDevTracking(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "Use GET for local SES tracking resources", http.StatusMethodNotAllowed)
		return
	}
	manager := handler.ses
	now := time.Now().UTC()
	manager.mu.Lock()
	if manager.devTracking == nil {
		manager.mu.Unlock()
		http.NotFound(w, r)
		return
	}
	manager.pruneAccepted(now)
	if strings.HasPrefix(r.URL.Path, DevTrackingMessagesPrefix) {
		resource := strings.TrimPrefix(r.URL.Path, DevTrackingMessagesPrefix)
		id, htmlView := resource, false
		if strings.HasSuffix(resource, "/html") {
			id, htmlView = strings.TrimSuffix(resource, "/html"), true
		}
		accepted := manager.accepted[id]
		if accepted == nil || accepted.devTracking == nil {
			manager.mu.Unlock()
			http.NotFound(w, r)
			return
		}
		tracking, expires := accepted.devTracking, accepted.retainedAt.Add(acceptedMessageTTL)
		manager.mu.Unlock()
		if htmlView {
			// The sandbox removes app-origin privileges. Only same-origin image
			// fetches are enabled; forms/scripts/navigation remain restricted.
			w.Header().Set("Content-Security-Policy", "sandbox; default-src 'none'; img-src 'self'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'")
			w.Header().Set("Content-Type", mime.FormatMediaType("text/html", map[string]string{"charset": tracking.charset}))
			_, _ = w.Write([]byte(tracking.html))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"local": true, "message_id": id,
			"view_url":     DevTrackingMessagesPrefix + id + "/html",
			"tracking_url": DevTrackingPixelPrefix + tracking.token + ".gif", "expires_at": expires})
		return
	}
	if !strings.HasPrefix(r.URL.Path, DevTrackingPixelPrefix) {
		manager.mu.Unlock()
		http.NotFound(w, r)
		return
	}
	resource := strings.TrimPrefix(r.URL.Path, DevTrackingPixelPrefix)
	if !strings.HasSuffix(resource, ".gif") {
		manager.mu.Unlock()
		http.NotFound(w, r)
		return
	}
	token := strings.TrimSuffix(resource, ".gif")
	id := manager.devTracking.tokens[token]
	accepted, topics := manager.retainedEventLocked(id, "open")
	manager.mu.Unlock()
	if accepted == nil {
		http.NotFound(w, r)
		return
	}
	ip := r.RemoteAddr
	if host, _, err := net.SplitHostPort(ip); err == nil {
		ip = host
	}
	if parsed := net.ParseIP(ip); parsed != nil {
		ip = parsed.String()
	} else {
		ip = ""
	}
	admissions := manager.publishEvent(r.Context(), awsprotocol.RequestID(), accepted, "Open",
		map[string]any{"timestamp": now.Format(time.RFC3339Nano), "ipAddress": ip, "userAgent": r.UserAgent()}, topics)
	for _, admission := range admissions {
		if admission.Error != "" {
			http.Error(w, "SES Open event admission failed", http.StatusBadGateway)
			return
		}
	}
	w.Header().Set("Content-Type", "image/gif")
	_, _ = w.Write(devTrackingPixel)
}
