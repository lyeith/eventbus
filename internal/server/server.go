// Package server selects AWS protocols and publishes emulator health.
// Service state and operation behavior belong to the injected handlers.
package server

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/lyeith/eventbus/internal/awsprotocol"
)

type ActionHandler interface {
	ServeAction(http.ResponseWriter, *http.Request, string)
}

type MessagingHandler interface {
	ActionHandler
	ServeQuery(http.ResponseWriter, *http.Request, string)
}

type CognitoHandler interface {
	ActionHandler
	ServeJWKS(http.ResponseWriter, *http.Request)
}

type SESHandler interface {
	http.Handler
	MatchQuery(*http.Request, string) bool
	ServeQuery(http.ResponseWriter, *http.Request, string, error)
}

// CognitoURLs advertises configured base URLs, without exposing an identity store.
type CognitoURLs struct {
	Issuer string
	JWKS   string
}

// Services are consumer-owned HTTP ports. This package imports no service stores.
type Services struct {
	Messaging      MessagingHandler
	Firehose       ActionHandler
	SSM            ActionHandler
	Secrets        ActionHandler
	Cognito        CognitoHandler
	SES            SESHandler
	CognitoURLs    *CognitoURLs
	QueryBodyLimit int64
}

type Server struct {
	services Services
	mux      *http.ServeMux
}

func New(services Services) *Server {
	if services.QueryBodyLimit <= 0 {
		services.QueryBodyLimit = 64 << 20
	}
	s := &Server{services: services, mux: http.NewServeMux()}
	s.mux.HandleFunc("/", s.handleAWSAction)
	s.mux.HandleFunc("/queue/", s.handleAWSAction)
	s.mux.HandleFunc("/health", s.handleHealth)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.services.SES != nil && strings.HasPrefix(r.URL.Path, "/v2/email/") {
		s.services.SES.ServeHTTP(w, r)
		return
	}
	if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/.well-known/jwks.json") {
		if s.services.Cognito == nil {
			http.Error(w, "cognito store not configured", http.StatusServiceUnavailable)
		} else {
			s.services.Cognito.ServeJWKS(w, r)
		}
		return
	}
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	body := map[string]interface{}{"status": "healthy", "service": "eventbus"}
	if urls := s.services.CognitoURLs; urls != nil {
		body["cognito_issuer"] = urls.Issuer
		body["cognito_jwks_url"] = urls.JWKS
	}
	_ = json.NewEncoder(w).Encode(body)
}

func (s *Server) handleAWSAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		awsprotocol.XMLError(w, http.StatusMethodNotAllowed, "InvalidMethod", "Only POST is supported")
		return
	}
	if target := r.Header.Get("X-Amz-Target"); target != "" {
		var handler ActionHandler
		switch {
		case strings.HasPrefix(target, "Firehose_"):
			handler = s.services.Firehose
		case strings.HasPrefix(target, "AmazonSSM."):
			handler = s.services.SSM
		case strings.HasPrefix(target, "secretsmanager."):
			handler = s.services.Secrets
		case strings.HasPrefix(target, "AWSCognitoIdentityProviderService."):
			handler = s.services.Cognito
		default:
			// Preserve the AWS JSON dispatcher’s existing SQS fallback.
			handler = s.services.Messaging
		}
		if handler == nil {
			awsprotocol.JSONError(w, http.StatusServiceUnavailable, "ServiceUnavailable", "Service not configured")
			return
		}
		handler.ServeAction(w, r, targetAction(target))
		return
	}

	// ParseForm normally caps requests at 10 MB. SES applies decoded MIME quotas
	// independently, so permit its transport encoding before service selection.
	r.Body = http.MaxBytesReader(w, r.Body, s.services.QueryBodyLimit)
	parseErr := r.ParseForm()
	action := r.FormValue("Action")
	if s.services.SES != nil && s.services.SES.MatchQuery(r, action) {
		s.services.SES.ServeQuery(w, r, action, parseErr)
		return
	}
	if parseErr != nil {
		awsprotocol.XMLError(w, http.StatusBadRequest, "InvalidParameter", "Malformed Query request")
		return
	}
	if s.services.Messaging == nil {
		awsprotocol.XMLError(w, http.StatusServiceUnavailable, "ServiceUnavailable", "Messaging not configured")
		return
	}
	s.services.Messaging.ServeQuery(w, r, action)
}

func targetAction(target string) string {
	if _, action, ok := strings.Cut(target, "."); ok {
		return action
	}
	return ""
}
